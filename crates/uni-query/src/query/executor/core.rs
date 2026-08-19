// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use uni_algo::algo::AlgorithmRegistry;
use uni_common::{TemporalValue, Value};
use uni_cypher::ast::{BinaryOp, Expr};
use uni_store::PropertyManager;
use uni_store::QueryContext;
use uni_store::runtime::l0_manager::L0Manager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;
use uni_xervo::runtime::ModelRuntime;

use crate::query::expr_eval::eval_binary_op;
use crate::types::QueryWarning;

use super::procedure::ProcedureRegistry;

/// Mutable accumulator for Cypher aggregate functions (COUNT, SUM, AVG, ...).
#[derive(Debug)]
pub(crate) enum Accumulator {
    Count(i64),
    /// Running sum kept exact for integers: `int_sum` accumulates i64 values
    /// while `all_ints` holds; `float_sum` mirrors every value as f64 for the
    /// float/overflow fallback. Summing purely in f64 lost precision above 2^53.
    Sum {
        int_sum: i64,
        float_sum: f64,
        all_ints: bool,
    },
    Min(Option<Value>),
    Max(Option<Value>),
    Avg {
        sum: f64,
        count: i64,
    },
    Collect(Vec<Value>),
    CountDistinct(HashSet<Value>),
    PercentileDisc {
        values: Vec<f64>,
        percentile: f64,
    },
    PercentileCont {
        values: Vec<f64>,
        percentile: f64,
    },
}

/// Convert f64 to Value, preserving integer representation when possible.
fn numeric_to_value(val: f64) -> Value {
    if val.fract() == 0.0 && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
        Value::Int(val as i64)
    } else {
        Value::Float(val)
    }
}

/// Collapse an integral finite `Float` to the equivalent `Int`; leave every
/// other value untouched.
///
/// Shared *mechanism* only. The callers below each document their own
/// specification for why they fold this way — they are governed by different
/// rules (openCypher aggregation vs. the MERGE match-key coercion) and are
/// deliberately kept as separate, individually-documented entry points.
pub(crate) fn canonical_numeric_key(val: &Value) -> Value {
    match val {
        Value::Float(f)
            if f.is_finite()
                && f.fract() == 0.0
                && *f >= i64::MIN as f64
                && *f <= i64::MAX as f64 =>
        {
            Value::Int(*f as i64)
        }
        other => other.clone(),
    }
}

/// Canonical key for `COUNT(DISTINCT …)`: integral finite floats collapse to
/// `Int` so `1` and `1.0` count once (matching `cypher_eq`'s numeric
/// coercion); every other value keeps its type. The accumulator previously
/// stringified values, which also collapsed `1` with `'1'` — a wrong answer.
fn distinct_key(val: &Value) -> Value {
    canonical_numeric_key(val)
}

/// Cross-type ordering rank for Cypher min/max (lower rank = smaller).
///
/// NOTE: This is deliberately a *different* ordering from
/// [`Executor::compare_values`] (used for `ORDER BY`). The two systems must
/// not be merged: `min`/`max` follow the openCypher aggregation ordering
/// (Null < List < String < Boolean < Number), whereas `ORDER BY` follows the
/// openCypher comparability/ordering rules for sort, which rank the type
/// families differently. Unifying them would silently break one or the other.
fn cypher_type_rank(val: &Value) -> u8 {
    match val {
        Value::Null => 0,
        Value::List(_) => 1,
        Value::String(_) => 2,
        Value::Bool(_) => 3,
        Value::Int(_) | Value::Float(_) => 4,
        _ => 5,
    }
}

/// Compare two Cypher values for min/max with cross-type ordering.
fn cypher_cross_type_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let ra = cypher_type_rank(a);
    let rb = cypher_type_rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Int(l), Value::Int(r)) => l.cmp(r),
        (Value::Float(l), Value::Float(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Int(l), Value::Float(r)) => (*l as f64).partial_cmp(r).unwrap_or(Ordering::Equal),
        (Value::Float(l), Value::Int(r)) => l.partial_cmp(&(*r as f64)).unwrap_or(Ordering::Equal),
        (Value::String(l), Value::String(r)) => l.cmp(r),
        (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
        // Temporal and Bytes share `cypher_type_rank` 5, so a same-rank pair
        // reaches this match. Without these arms `min`/`max` over dates or byte
        // strings collapsed to `Ordering::Equal` and returned the first-seen
        // value. Order them by their natural comparison instead.
        (Value::Temporal(l), Value::Temporal(r)) => Executor::compare_temporal(l, r),
        (Value::Bytes(l), Value::Bytes(r)) => l.cmp(r),
        _ => Ordering::Equal,
    }
}

/// Collect every edge ID tombstoned in the writer's current L0 plus, when
/// present, the transaction's L0.
///
/// Used by the non-DETACH DELETE guards so edges deleted earlier in the same
/// statement are not counted as "still attached".
pub(crate) fn collect_tombstoned_eids(
    writer: &Writer,
    tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
) -> HashSet<uni_common::core::id::Eid> {
    let mut tombstoned_eids = HashSet::new();
    {
        let writer_l0 = writer.l0_manager.get_current();
        let guard = writer_l0.read();
        for &eid in guard.tombstones.keys() {
            tombstoned_eids.insert(eid);
        }
    }
    if let Some(tx) = tx_l0 {
        let guard = tx.read();
        for &eid in guard.tombstones.keys() {
            tombstoned_eids.insert(eid);
        }
    }
    tombstoned_eids
}

impl Accumulator {
    pub(crate) fn new(op: &str, distinct: bool) -> Self {
        Self::new_with_percentile(op, distinct, 0.0)
    }

    pub(crate) fn new_with_percentile(op: &str, distinct: bool, percentile: f64) -> Self {
        let op_upper = op.to_uppercase();
        match op_upper.as_str() {
            "COUNT" if distinct => Accumulator::CountDistinct(HashSet::new()),
            "COUNT" => Accumulator::Count(0),
            "SUM" => Accumulator::Sum {
                int_sum: 0,
                float_sum: 0.0,
                all_ints: true,
            },
            "MIN" => Accumulator::Min(None),
            "MAX" => Accumulator::Max(None),
            "AVG" => Accumulator::Avg { sum: 0.0, count: 0 },
            "COLLECT" => Accumulator::Collect(Vec::new()),
            "PERCENTILEDISC" => Accumulator::PercentileDisc {
                values: Vec::new(),
                percentile,
            },
            "PERCENTILECONT" => Accumulator::PercentileCont {
                values: Vec::new(),
                percentile,
            },
            _ => Accumulator::Count(0),
        }
    }

    pub(crate) fn update(&mut self, val: &Value, is_wildcard: bool) {
        match self {
            Accumulator::Count(c) => {
                if is_wildcard || !val.is_null() {
                    *c += 1;
                }
            }
            Accumulator::Sum {
                int_sum,
                float_sum,
                all_ints,
            } => match val {
                Value::Int(i) => {
                    *float_sum += *i as f64;
                    // Exact while it fits; on i64 overflow fall back to the f64 sum.
                    match int_sum.checked_add(*i) {
                        Some(s) => *int_sum = s,
                        None => *all_ints = false,
                    }
                }
                _ => {
                    if let Some(n) = val.as_f64() {
                        *float_sum += n;
                        *all_ints = false;
                    }
                }
            },
            Accumulator::Min(current) => {
                if !val.is_null() {
                    *current = Some(match current.take() {
                        None => val.clone(),
                        Some(cur) if cypher_cross_type_cmp(val, &cur).is_lt() => val.clone(),
                        Some(cur) => cur,
                    });
                }
            }
            Accumulator::Max(current) => {
                if !val.is_null() {
                    *current = Some(match current.take() {
                        None => val.clone(),
                        Some(cur) if cypher_cross_type_cmp(val, &cur).is_gt() => val.clone(),
                        Some(cur) => cur,
                    });
                }
            }
            Accumulator::Avg { sum, count } => {
                if let Some(n) = val.as_f64() {
                    *sum += n;
                    *count += 1;
                }
            }
            Accumulator::Collect(v) => {
                if !val.is_null() {
                    v.push(val.clone());
                }
            }
            Accumulator::CountDistinct(s) => {
                if !val.is_null() {
                    s.insert(distinct_key(val));
                }
            }
            Accumulator::PercentileDisc { values, .. }
            | Accumulator::PercentileCont { values, .. } => {
                if let Some(n) = val.as_f64() {
                    values.push(n);
                }
            }
        }
    }

    pub(crate) fn finish(&self) -> Value {
        match self {
            Accumulator::Count(c) => Value::Int(*c),
            Accumulator::Sum {
                int_sum,
                float_sum,
                all_ints,
            } => {
                if *all_ints {
                    Value::Int(*int_sum)
                } else {
                    numeric_to_value(*float_sum)
                }
            }
            Accumulator::Min(opt) => opt.as_ref().cloned().unwrap_or(Value::Null),
            Accumulator::Max(opt) => opt.as_ref().cloned().unwrap_or(Value::Null),
            Accumulator::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float(*sum / (*count as f64))
                } else {
                    Value::Null
                }
            }
            Accumulator::Collect(v) => Value::List(v.clone()),
            Accumulator::CountDistinct(s) => Value::Int(s.len() as i64),
            Accumulator::PercentileDisc { values, percentile } => {
                if values.is_empty() {
                    return Value::Null;
                }
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.total_cmp(b));
                let n = sorted.len();
                let idx = (percentile * (n as f64 - 1.0)).round() as usize;
                numeric_to_value(sorted[idx.min(n - 1)])
            }
            Accumulator::PercentileCont { values, percentile } => {
                if values.is_empty() {
                    return Value::Null;
                }
                let mut sorted = values.clone();
                sorted.sort_by(|a, b| a.total_cmp(b));
                let n = sorted.len();
                if n == 1 {
                    return Value::Float(sorted[0]);
                }
                let pos = percentile * (n as f64 - 1.0);
                let lower = (pos.floor() as usize).min(n - 1);
                let upper = (pos.ceil() as usize).min(n - 1);
                if lower == upper {
                    Value::Float(sorted[lower])
                } else {
                    let frac = pos - lower as f64;
                    Value::Float(sorted[lower] + frac * (sorted[upper] - sorted[lower]))
                }
            }
        }
    }
}

/// Cache key for parsed generation expressions: (label_name, property_name)
pub(crate) type GenExprCacheKey = (String, String);

/// Query executor: runs logical plans against a Uni storage backend.
///
/// `Executor` bridges the logical query plan produced by [`crate::query::planner::QueryPlanner`]
/// with the underlying `StorageManager`. It handles both read-only sessions and
/// write-enabled sessions (via `Writer` and `L0Manager`).
///
/// # Cloning
///
/// `Executor` is cheaply cloneable — all expensive state is held behind `Arc`s.
/// Clone it freely to share across tasks.
///
/// **Note on `Clone` semantics**: the derived `Clone` would alias the
/// `warnings` `Arc<Mutex<…>>`, making warnings collected on the clone
/// bleed into the original. A clone is intended to be a "fresh executor
/// borrowing shared state," so the manual impl below installs a fresh
/// `warnings` accumulator. All other Arc-shared state is intentionally
/// shared (caches, registries, runtimes).
// M-PUBLIC-DEBUG: Manual impl because Writer/ModelRuntime do not implement Debug.
pub struct Executor {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) writer: Option<Arc<Writer>>,
    pub(crate) l0_manager: Option<Arc<L0Manager>>,
    pub(crate) algo_registry: Arc<AlgorithmRegistry>,
    pub(crate) use_transaction: bool,
    /// File sandbox configuration for BACKUP/COPY/EXPORT commands
    pub(crate) file_sandbox: uni_common::config::FileSandboxConfig,
    pub(crate) config: uni_common::config::UniConfig,
    /// Cache for parsed generation expressions to avoid re-parsing on every row
    pub(crate) gen_expr_cache: Arc<RwLock<HashMap<GenExprCacheKey, Expr>>>,
    /// External procedure registry for test/user-defined procedures.
    pub(crate) procedure_registry: Option<Arc<ProcedureRegistry>>,
    /// Uni-Xervo runtime used by vector auto-embedding paths.
    pub(crate) xervo_runtime: Option<Arc<ModelRuntime>>,
    /// Warnings collected during the last execution.
    pub(crate) warnings: Arc<std::sync::Mutex<Vec<QueryWarning>>>,
    /// Per-query execution counters for the last execution.
    ///
    /// Fresh per clone for the same reason `warnings` is: the write path clones
    /// a cached executor template, and a shared handle would accumulate one
    /// query's counts into the next one's result.
    pub(crate) counters: Arc<uni_store::QueryCounters>,
    /// Private transaction L0 buffer for query context and mutations.
    /// Used by Transaction to route reads and writes through a private L0 buffer
    /// without requiring the writer lock at transaction-creation time.
    pub(crate) transaction_l0_override:
        Option<Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    /// Per-transaction VID/EID reservoir. When set, CREATE/MERGE paths pull
    /// IDs from this batched cache instead of `Writer::next_vid()` /
    /// `Writer::next_eid()`, amortizing the `IdAllocator`'s global mutex.
    pub(crate) id_reservoir: Option<Arc<uni_store::runtime::TxIdReservoir>>,
    /// User-defined custom scalar function registry.
    pub(crate) custom_function_registry:
        Option<Arc<uni_query_functions::custom_functions::CustomFunctionRegistry>>,
    /// Cooperative cancellation token. Passed to `QueryContext` and
    /// `GraphExecutionContext` so in-flight operators can detect cancellation.
    pub(crate) cancellation_token: Option<tokio_util::sync::CancellationToken>,
    /// Shared `PropertyManager` to reuse inside `create_datafusion_planner`
    /// instead of allocating a fresh one (with empty LRU caches) per query.
    /// When `None`, the planner falls back to constructing from the
    /// `&PropertyManager` parameter — preserving existing behavior for any
    /// caller that does not set this.
    pub(crate) prop_manager_arc: Option<Arc<PropertyManager>>,
    /// Pre-built DataFusion `SessionContext` template with all Cypher UDFs
    /// already registered. When set AND no custom UDFs are installed,
    /// `create_datafusion_planner` clones this Arc (O(1)) instead of building
    /// a fresh `SessionContext` and re-registering UDFs every query
    /// (~140 µs/query saved on the InMemory backend).
    pub(crate) df_session_template: Option<Arc<datafusion::execution::context::SessionContext>>,
    /// Pinned L0 snapshot for snapshot-isolated reads (Component C1). When set,
    /// `get_context` builds the read context from this frozen view instead of the
    /// live L0, so a transaction's reads are isolated from concurrent commits.
    /// `None` (the default, when no snapshot has been pinned) means live reads.
    pub(crate) read_snapshot: Option<uni_store::runtime::SnapshotView>,
}

impl std::fmt::Debug for Executor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Executor")
            .field("use_transaction", &self.use_transaction)
            .field("has_writer", &self.writer.is_some())
            .field("has_l0_manager", &self.l0_manager.is_some())
            .field("has_xervo_runtime", &self.xervo_runtime.is_some())
            .finish_non_exhaustive()
    }
}

impl Clone for Executor {
    /// Clone an `Executor`, sharing all Arc-backed state but installing a
    /// fresh `warnings` accumulator. Warnings are query-scoped; aliasing
    /// the Mutex across clones would let one query's warnings spill into
    /// another.
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            writer: self.writer.clone(),
            l0_manager: self.l0_manager.clone(),
            algo_registry: self.algo_registry.clone(),
            use_transaction: self.use_transaction,
            file_sandbox: self.file_sandbox.clone(),
            config: self.config.clone(),
            gen_expr_cache: self.gen_expr_cache.clone(),
            procedure_registry: self.procedure_registry.clone(),
            xervo_runtime: self.xervo_runtime.clone(),
            // Fresh warnings per clone — see struct doc.
            warnings: Arc::new(std::sync::Mutex::new(Vec::new())),
            // Fresh counters per clone, same reason.
            counters: Arc::new(uni_store::QueryCounters::new()),
            transaction_l0_override: self.transaction_l0_override.clone(),
            id_reservoir: self.id_reservoir.clone(),
            custom_function_registry: self.custom_function_registry.clone(),
            cancellation_token: self.cancellation_token.clone(),
            prop_manager_arc: self.prop_manager_arc.clone(),
            df_session_template: self.df_session_template.clone(),
            read_snapshot: self.read_snapshot.clone(),
        }
    }
}

impl Executor {
    /// Create a read-only executor backed by the given storage manager.
    pub fn new(storage: Arc<StorageManager>) -> Self {
        // M4: auto-wire the host plugin registry so low-level test
        // setups that bypass `Uni::build` still see `uni.schema.*` and
        // `uni.algo.*` procedures (which no longer have hardcoded
        // dispatch arms in `procedure_call.rs`).
        let proc_registry = Arc::new(ProcedureRegistry::new());
        proc_registry.set_plugin_registry(crate::procedures_plugin::default_host_plugin_registry());
        Self {
            storage,
            writer: None,
            l0_manager: None,
            algo_registry: Arc::new(AlgorithmRegistry::new()),
            use_transaction: false,
            file_sandbox: uni_common::config::FileSandboxConfig::default(),
            config: uni_common::config::UniConfig::default(),
            gen_expr_cache: Arc::new(RwLock::new(HashMap::new())),
            procedure_registry: Some(proc_registry),
            xervo_runtime: None,
            warnings: Arc::new(std::sync::Mutex::new(Vec::new())),
            counters: Arc::new(uni_store::QueryCounters::new()),
            transaction_l0_override: None,
            id_reservoir: None,
            custom_function_registry: None,
            cancellation_token: None,
            prop_manager_arc: None,
            df_session_template: None,
            read_snapshot: None,
        }
    }

    /// Install a shared `PropertyManager` so the planner can reuse it
    /// instead of constructing a fresh one per query.
    ///
    /// Reusing the caller's `Arc<PropertyManager>` skips the LRU cache
    /// allocation cost (~80 µs/query in the InMemory backend).
    pub fn set_prop_manager(&mut self, pm: Arc<PropertyManager>) {
        self.prop_manager_arc = Some(pm);
    }

    /// Install a pre-built `SessionContext` template with Cypher UDFs already
    /// registered, so the planner can clone it (O(1)) instead of rebuilding.
    ///
    /// When custom UDFs are also installed, the template is bypassed and a
    /// fresh `SessionContext` is built for that query (preserves UDF
    /// isolation across executors).
    pub fn set_df_session_template(
        &mut self,
        tmpl: Arc<datafusion::execution::context::SessionContext>,
    ) {
        self.df_session_template = Some(tmpl);
    }

    /// Create a write-enabled executor with an attached `Writer`.
    pub fn new_with_writer(storage: Arc<StorageManager>, writer: Arc<Writer>) -> Self {
        let mut executor = Self::new(storage);
        executor.writer = Some(writer);
        executor
    }

    /// Attach an external procedure registry for user-defined procedures.
    pub fn set_procedure_registry(&mut self, registry: Arc<ProcedureRegistry>) {
        self.procedure_registry = Some(registry);
    }

    /// Attach or detach the Uni-Xervo model runtime for vector auto-embedding.
    pub fn set_xervo_runtime(&mut self, runtime: Option<Arc<ModelRuntime>>) {
        self.xervo_runtime = runtime;
    }

    /// Set the file sandbox configuration for BACKUP/COPY/EXPORT commands.
    /// MUST be called with sandboxed config in server mode.
    pub fn set_file_sandbox(&mut self, sandbox: uni_common::config::FileSandboxConfig) {
        self.file_sandbox = sandbox;
    }

    /// Apply a runtime configuration to this executor.
    pub fn set_config(&mut self, config: uni_common::config::UniConfig) {
        self.config = config;
    }

    /// Validate a file path against the sandbox configuration.
    pub(crate) fn validate_path(&self, path: &str) -> Result<std::path::PathBuf> {
        self.file_sandbox
            .validate_path(path)
            .map_err(|e| anyhow!("Path validation failed: {}", e))
    }

    /// Attach a `Writer` after construction, enabling write operations.
    pub fn set_writer(&mut self, writer: Arc<Writer>) {
        self.writer = Some(writer);
    }

    /// Attach a standalone `L0Manager` for a view that has no `Writer`.
    ///
    /// A read-only open still replays the WAL suffix above the snapshot
    /// manifest's high-water mark into an `L0Manager` — those are *committed*
    /// mutations — and only then drops the writer. Without this the recovered
    /// tier is unreachable, `get_context` returns `None`, and every read
    /// on the session silently sees flushed L1 only.
    ///
    /// Leaving it `None` is the *deliberately L0-free* case: a pinned
    /// time-travel snapshot, whose rows are entirely in L1 because
    /// `create_snapshot` flushes first, and whose live L0 holds only
    /// post-snapshot writes that must stay invisible.
    pub fn set_l0_manager(&mut self, manager: Arc<L0Manager>) {
        self.l0_manager = Some(manager);
    }

    /// The per-query counters for the last execution.
    ///
    /// Unlike [`Self::take_warnings`] this does not drain: the counters are an
    /// `Arc` of atomics shared with the query context and every physical
    /// operator, so the executor simply reads its own handle. Call after
    /// `execute` returns.
    pub fn counters(&self) -> &Arc<uni_store::QueryCounters> {
        &self.counters
    }

    /// Snapshot the counters into the shape `QueryMetrics` wants.
    ///
    /// Returns `(l0_reads, storage_reads, rows_scanned, branch_scans, snapshot_reads)`.
    pub fn take_counters(&self) -> uni_store::CounterSnapshot {
        self.counters.snapshot()
    }

    /// Take all collected warnings from the last execution, leaving the collector empty.
    pub fn take_warnings(&self) -> Vec<QueryWarning> {
        self.warnings
            .lock()
            .map(|mut w| std::mem::take(&mut *w))
            .unwrap_or_default()
    }

    /// Configure whether query execution should operate within a transaction context.
    pub fn set_use_transaction(&mut self, use_transaction: bool) {
        self.use_transaction = use_transaction;
    }

    /// Set a private transaction L0 buffer for both read visibility (QueryContext)
    /// and mutation routing.
    pub fn set_transaction_l0(
        &mut self,
        l0: Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    ) {
        self.transaction_l0_override = Some(l0);
    }

    /// Attach a pinned L0 snapshot for snapshot-isolated reads (Component C1).
    ///
    /// When `Some`, `get_context` reads from the frozen view instead of
    /// the live L0. A `None` is a no-op (live reads), so threading it is always
    /// safe — callers that do not pin a snapshot leave it `None`.
    pub fn set_read_snapshot(&mut self, snapshot: Option<uni_store::runtime::SnapshotView>) {
        self.read_snapshot = snapshot;
    }

    /// The storage manager queries should read through: the transaction's
    /// version-pinned manager when the read snapshot carries one (C2 — L1
    /// scans then filter to `_version <= started_at_version`, so a flush
    /// completing mid-transaction cannot leak post-snapshot rows), else the
    /// live manager.
    pub(crate) fn effective_storage(&self) -> Arc<StorageManager> {
        self.read_snapshot
            .as_ref()
            .and_then(|s| s.pinned_storage.clone())
            .unwrap_or_else(|| self.storage.clone())
    }

    /// Attach a per-transaction VID/EID reservoir. When set, CREATE/MERGE
    /// paths in `execute_create_pattern` pull IDs from the reservoir's
    /// pre-reserved cache, amortizing the global `IdAllocator` mutex.
    pub fn set_id_reservoir(&mut self, r: Arc<uni_store::runtime::TxIdReservoir>) {
        self.id_reservoir = Some(r);
    }

    /// Attach a custom scalar function registry for user-defined functions.
    pub fn set_custom_functions(
        &mut self,
        registry: Arc<uni_query_functions::custom_functions::CustomFunctionRegistry>,
    ) {
        self.custom_function_registry = Some(registry);
    }

    /// Set a cooperative cancellation token for in-flight query cancellation.
    pub fn set_cancellation_token(&mut self, token: tokio_util::sync::CancellationToken) {
        self.cancellation_token = Some(token);
    }

    /// Build a `QueryContext` from the current writer or standalone L0 manager.
    /// When `transaction_l0_override` is set, it is used as the transaction L0 —
    /// this is how private-per-transaction L0 buffers become visible to reads
    /// without requiring the writer lock at tx creation.
    pub(crate) async fn get_context(&self) -> Option<QueryContext> {
        if let Some(writer) = &self.writer {
            // Prefer the override (private tx L0) over the writer's slot
            let tx_l0 = self.transaction_l0_override.clone();
            // Component C1: when a snapshot is pinned, read from the frozen
            // generation (`main` + `extra`) so concurrent commits are invisible;
            // `transaction_l0` stays live for read-your-writes. `None` (the
            // default, when no snapshot is pinned) ⇒ live reads.
            let mut ctx = match &self.read_snapshot {
                Some(snap) => {
                    QueryContext::new_with_pending(snap.main.clone(), tx_l0, snap.extra.clone())
                }
                None => QueryContext::new_with_pending(
                    writer.l0_manager.get_current(),
                    tx_l0,
                    writer.l0_manager.get_pending_flush(),
                ),
            };
            ctx.set_deadline(Instant::now() + self.config.query_timeout);
            if let Some(ref token) = self.cancellation_token {
                ctx.set_cancellation_token(token.clone());
            }
            ctx.set_counters(self.counters.clone());
            Some(ctx)
        } else {
            self.l0_manager.as_ref().map(|m| {
                // `new_with_pending`, not `new`: a background rotation moves the
                // active generation into the pending-flush list, so reading only
                // `get_current()` would silently drop rows that are still in L0.
                // The writer branch above has always done this; this branch had
                // not, which mattered the moment a writer-less view became one
                // that carries real data (a read-only open after WAL replay).
                let mut ctx = QueryContext::new_with_pending(
                    m.get_current(),
                    self.transaction_l0_override.clone(),
                    m.get_pending_flush(),
                );
                ctx.set_deadline(Instant::now() + self.config.query_timeout);
                if let Some(ref token) = self.cancellation_token {
                    ctx.set_cancellation_token(token.clone());
                }
                ctx.set_counters(self.counters.clone());
                ctx
            })
        }
    }

    /// Total ordering for Cypher ORDER BY, including cross-type comparisons.
    ///
    /// NOTE: This uses a different cross-type rank from
    /// [`cypher_cross_type_cmp`]/[`cypher_type_rank`] (the `min`/`max`
    /// aggregation ordering). The divergence is intentional — sort ordering
    /// and aggregation ordering follow distinct openCypher rules — so the two
    /// must stay separate.
    pub(crate) fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        let temporal_a = Self::extract_temporal_value(a);
        let temporal_b = Self::extract_temporal_value(b);

        if let (Some(ta), Some(tb)) = (&temporal_a, &temporal_b) {
            return Self::compare_temporal(ta, tb);
        }

        // Temporal strings (e.g. "1984-10-11T...") and Value::Temporal should
        // compare using Cypher temporal semantics when compatible.
        if matches!(
            (a, b),
            (Value::String(_), Value::Temporal(_)) | (Value::Temporal(_), Value::String(_))
        ) && let Some(ord) = Self::try_eval_ordering(a, b)
        {
            return ord;
        }
        if let (Value::String(_), Some(tb)) = (a, temporal_b)
            && let Some(ord) = Self::try_eval_ordering(a, &Value::Temporal(tb))
        {
            return ord;
        }
        if let (Some(ta), Value::String(_)) = (temporal_a, b)
            && let Some(ord) = Self::try_eval_ordering(&Value::Temporal(ta), b)
        {
            return ord;
        }

        let ra = Self::order_by_type_rank(a);
        let rb = Self::order_by_type_rank(b);
        if ra != rb {
            return ra.cmp(&rb);
        }

        match (a, b) {
            (Value::Map(l), Value::Map(r)) => Self::compare_maps(l, r),
            (Value::Node(l), Value::Node(r)) => Self::compare_nodes(l, r),
            (Value::Edge(l), Value::Edge(r)) => Self::compare_edges(l, r),
            (Value::List(l), Value::List(r)) => Self::compare_lists(l, r),
            (Value::Path(l), Value::Path(r)) => Self::compare_paths(l, r),
            (Value::String(l), Value::String(r)) => {
                // Use eval_binary_op on the original references to avoid cloning.
                Self::try_eval_ordering(a, b).unwrap_or_else(|| l.cmp(r))
            }
            (Value::Bool(l), Value::Bool(r)) => l.cmp(r),
            (Value::Temporal(l), Value::Temporal(r)) => Self::compare_temporal(l, r),
            (Value::Int(l), Value::Int(r)) => l.cmp(r),
            (Value::Float(l), Value::Float(r)) => {
                if l.is_nan() && r.is_nan() {
                    Ordering::Equal
                } else if l.is_nan() {
                    Ordering::Greater
                } else if r.is_nan() {
                    Ordering::Less
                } else {
                    l.partial_cmp(r).unwrap_or(Ordering::Equal)
                }
            }
            (Value::Int(l), Value::Float(r)) => {
                if r.is_nan() {
                    Ordering::Less
                } else {
                    // Exact i64-vs-f64 order (no lossy `as f64` cast above 2^53).
                    uni_common::cmp_i64_f64(*l, *r)
                }
            }
            (Value::Float(l), Value::Int(r)) => {
                if l.is_nan() {
                    Ordering::Greater
                } else {
                    uni_common::cmp_i64_f64(*r, *l).reverse()
                }
            }
            (Value::Bytes(l), Value::Bytes(r)) => l.cmp(r),
            (Value::Vector(l), Value::Vector(r)) => {
                for (lv, rv) in l.iter().zip(r.iter()) {
                    let ord = lv.total_cmp(rv);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                l.len().cmp(&r.len())
            }
            _ => Ordering::Equal,
        }
    }

    fn try_eval_ordering(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        if matches!(eval_binary_op(a, &BinaryOp::Lt, b), Ok(Value::Bool(true))) {
            Some(Ordering::Less)
        } else if matches!(eval_binary_op(a, &BinaryOp::Gt, b), Ok(Value::Bool(true))) {
            Some(Ordering::Greater)
        } else if matches!(eval_binary_op(a, &BinaryOp::Eq, b), Ok(Value::Bool(true))) {
            Some(Ordering::Equal)
        } else {
            None
        }
    }

    /// Cypher ORDER BY total precedence:
    /// MAP < NODE < RELATIONSHIP < LIST < PATH < STRING < BOOLEAN < TEMPORAL < NUMBER < NaN < NULL
    fn order_by_type_rank(v: &Value) -> u8 {
        match v {
            Value::Map(map) => Self::map_order_rank(map),
            Value::Node(_) => 1,
            Value::Edge(_) => 2,
            Value::List(_) => 3,
            Value::Path(_) => 4,
            Value::String(_) => 5,
            Value::Bool(_) => 6,
            Value::Temporal(_) => 7,
            Value::Int(_) => 8,
            Value::Float(f) if f.is_nan() => 9,
            Value::Float(_) => 8,
            Value::Null => 10,
            Value::Bytes(_) | Value::Vector(_) => 11,
            _ => 11,
        }
    }

    fn map_order_rank(map: &HashMap<String, Value>) -> u8 {
        if Self::map_as_temporal(map).is_some() {
            7
        } else if map.contains_key("nodes")
            && (map.contains_key("relationships") || map.contains_key("edges"))
        {
            4
        } else if map.contains_key("_eid")
            || map.contains_key("_src")
            || map.contains_key("_dst")
            || map.contains_key("_type")
            || map.contains_key("_type_name")
        {
            2
        } else if map.contains_key("_vid")
            || map.contains_key("_labels")
            || map.contains_key("_label")
        {
            1
        } else {
            0
        }
    }

    fn extract_temporal_value(value: &Value) -> Option<TemporalValue> {
        crate::query::expr_eval::temporal_from_value(value)
    }

    fn map_as_temporal(map: &HashMap<String, Value>) -> Option<TemporalValue> {
        crate::query::expr_eval::temporal_from_map_wrapper(map)
    }

    fn compare_lists(left: &[Value], right: &[Value]) -> std::cmp::Ordering {
        left.iter()
            .zip(right.iter())
            .map(|(l, r)| Self::compare_values(l, r))
            .find(|o| o.is_ne())
            .unwrap_or_else(|| left.len().cmp(&right.len()))
    }

    fn compare_maps(
        left: &HashMap<String, Value>,
        right: &HashMap<String, Value>,
    ) -> std::cmp::Ordering {
        let mut l_pairs: Vec<_> = left.iter().collect();
        let mut r_pairs: Vec<_> = right.iter().collect();
        l_pairs.sort_by_key(|(k, _)| *k);
        r_pairs.sort_by_key(|(k, _)| *k);

        l_pairs
            .iter()
            .zip(r_pairs.iter())
            .map(|((lk, lv), (rk, rv))| lk.cmp(rk).then_with(|| Self::compare_values(lv, rv)))
            .find(|o| o.is_ne())
            .unwrap_or_else(|| l_pairs.len().cmp(&r_pairs.len()))
    }

    fn compare_nodes(left: &uni_common::Node, right: &uni_common::Node) -> std::cmp::Ordering {
        let mut l_labels = left.labels.clone();
        let mut r_labels = right.labels.clone();
        l_labels.sort();
        r_labels.sort();

        l_labels
            .cmp(&r_labels)
            .then_with(|| left.vid.cmp(&right.vid))
            .then_with(|| Self::compare_maps(&left.properties, &right.properties))
    }

    fn compare_edges(left: &uni_common::Edge, right: &uni_common::Edge) -> std::cmp::Ordering {
        left.edge_type
            .cmp(&right.edge_type)
            .then_with(|| left.src.cmp(&right.src))
            .then_with(|| left.dst.cmp(&right.dst))
            .then_with(|| left.eid.cmp(&right.eid))
            .then_with(|| Self::compare_maps(&left.properties, &right.properties))
    }

    fn compare_paths(left: &uni_common::Path, right: &uni_common::Path) -> std::cmp::Ordering {
        left.nodes
            .iter()
            .zip(right.nodes.iter())
            .map(|(l, r)| Self::compare_nodes(l, r))
            .find(|o| o.is_ne())
            .unwrap_or_else(|| left.nodes.len().cmp(&right.nodes.len()))
            .then_with(|| {
                left.edges
                    .iter()
                    .zip(right.edges.iter())
                    .map(|(l, r)| Self::compare_edges(l, r))
                    .find(|o| o.is_ne())
                    .unwrap_or_else(|| left.edges.len().cmp(&right.edges.len()))
            })
    }

    fn compare_temporal(left: &TemporalValue, right: &TemporalValue) -> std::cmp::Ordering {
        match (left, right) {
            (
                TemporalValue::Date {
                    days_since_epoch: l,
                },
                TemporalValue::Date {
                    days_since_epoch: r,
                },
            ) => l.cmp(r),
            (
                TemporalValue::LocalTime {
                    nanos_since_midnight: l,
                },
                TemporalValue::LocalTime {
                    nanos_since_midnight: r,
                },
            ) => l.cmp(r),
            (
                TemporalValue::Time {
                    nanos_since_midnight: lm,
                    offset_seconds: lo,
                },
                TemporalValue::Time {
                    nanos_since_midnight: rm,
                    offset_seconds: ro,
                },
            ) => {
                let l_utc = *lm as i128 - (*lo as i128) * 1_000_000_000;
                let r_utc = *rm as i128 - (*ro as i128) * 1_000_000_000;
                l_utc.cmp(&r_utc)
            }
            (
                TemporalValue::LocalDateTime {
                    nanos_since_epoch: l,
                },
                TemporalValue::LocalDateTime {
                    nanos_since_epoch: r,
                },
            ) => l.cmp(r),
            (
                TemporalValue::DateTime {
                    nanos_since_epoch: l,
                    ..
                },
                TemporalValue::DateTime {
                    nanos_since_epoch: r,
                    ..
                },
            ) => l.cmp(r),
            (
                TemporalValue::Duration {
                    months: lm,
                    days: ld,
                    nanos: ln,
                },
                TemporalValue::Duration {
                    months: rm,
                    days: rd,
                    nanos: rn,
                },
            ) => (*lm, *ld, *ln).cmp(&(*rm, *rd, *rn)),
            _ => Self::temporal_variant_rank(left).cmp(&Self::temporal_variant_rank(right)),
        }
    }

    fn temporal_variant_rank(v: &TemporalValue) -> u8 {
        match v {
            TemporalValue::Date { .. } => 0,
            TemporalValue::LocalTime { .. } => 1,
            TemporalValue::Time { .. } => 2,
            TemporalValue::LocalDateTime { .. } => 3,
            TemporalValue::DateTime { .. } => 4,
            TemporalValue::Duration { .. } => 5,
            TemporalValue::Btic { .. } => 6,
        }
    }
}

/// Combined output of a `PROFILE` query execution.
///
/// Contains both the logical plan explanation and per-operator runtime
/// statistics collected during execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileOutput {
    /// Logical plan explanation with index usage and cost estimates.
    pub explain: crate::query::planner::ExplainOutput,
    /// Per-operator timing and memory statistics.
    pub runtime_stats: Vec<OperatorStats>,
    /// Wall-clock time for the entire execution in milliseconds.
    pub total_time_ms: u64,
    /// Peak memory used during execution in bytes.
    pub peak_memory_bytes: usize,
}

/// Runtime statistics for a single logical plan operator.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OperatorStats {
    /// Human-readable operator name (e.g., `"GraphScan"`, `"Filter"`).
    pub operator: String,
    /// Number of rows produced by this operator.
    pub actual_rows: usize,
    /// Wall-clock time spent in this operator in milliseconds.
    pub time_ms: f64,
    /// Memory allocated by this operator in bytes.
    pub memory_bytes: usize,
    /// Number of index cache hits (if applicable).
    pub index_hits: Option<usize>,
    /// Number of index cache misses (if applicable).
    pub index_misses: Option<usize>,
}

/// Walk a DataFusion physical plan tree (post-order DFS) and collect
/// per-operator metrics recorded by `BaselineMetrics` during execution.
///
/// Children are visited before parents so the resulting `Vec` flows from
/// data-producers (leaf scans) up to consumers (projections, filters).
pub(crate) fn collect_plan_metrics(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Vec<OperatorStats> {
    let mut stats = Vec::new();
    collect_plan_metrics_inner(plan, &mut stats);
    stats
}

fn collect_plan_metrics_inner(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    out: &mut Vec<OperatorStats>,
) {
    // Recurse into children first (post-order)
    for child in plan.children() {
        collect_plan_metrics_inner(child, out);
    }

    let operator = plan.name().to_string();

    let (actual_rows, time_ms, index_hits) = match plan.metrics() {
        Some(metrics) => {
            let rows = metrics.output_rows().unwrap_or(0);
            // elapsed_compute() returns nanoseconds
            let nanos = metrics.elapsed_compute().unwrap_or(0);
            let ms = nanos as f64 / 1_000_000.0;
            // Read by name, so only the operator that registered it reports a
            // value. Every other node keeps `None`, which is the honest answer:
            // a projection has no index opinion, and copying a query-level total
            // onto it would be a number that looks like data and is not.
            let hits = metrics.sum_by_name("index_consulted").map(|v| v.as_usize());
            (rows, ms, hits)
        }
        None => (0, 0.0, None),
    };

    out.push(OperatorStats {
        operator,
        actual_rows,
        time_ms,
        memory_bytes: 0,
        index_hits,
        // Deliberately left unset. Lance emits no "an index was available and I
        // chose not to use it" signal, and the tempting definition — a filter
        // was pushed but no index was consulted — conflates that with an index
        // served from cache and with a lookup that matched no page. A field
        // that reads plausible and means nothing is what retired `cache_hits`.
        index_misses: None,
    });
}

impl Executor {
    /// Profiles query execution and returns results with per-operator timing
    /// statistics extracted from the DataFusion physical plan tree.
    pub async fn profile(
        &self,
        plan: crate::query::planner::LogicalPlan,
        params: &HashMap<String, Value>,
    ) -> Result<(Vec<HashMap<String, Value>>, ProfileOutput)> {
        // Generate ExplainOutput first
        let planner =
            crate::query::planner::QueryPlanner::new(self.storage.schema_manager().schema());
        let explain_output = planner.explain_logical_plan(&plan)?;

        let start = Instant::now();

        let prop_manager = self.create_prop_manager();

        // DDL/admin queries don't flow through DataFusion — fall back to
        // single aggregate stat.
        let (results, stats) = if self.is_ddl_or_admin(&plan) {
            let results = self
                .execute_subplan(plan, &prop_manager, params, None)
                .await?;
            let elapsed = start.elapsed();
            let stats = vec![OperatorStats {
                operator: "DDL/Admin Execution".to_string(),
                actual_rows: results.len(),
                time_ms: elapsed.as_secs_f64() * 1000.0,
                memory_bytes: 0,
                index_hits: None,
                index_misses: None,
            }];
            (results, stats)
        } else {
            let (batches, execution_plan) = self
                .execute_datafusion_with_plan(plan, &prop_manager, params)
                .await?;
            let results = self.record_batches_to_rows(batches)?;
            let stats = collect_plan_metrics(&execution_plan);
            (results, stats)
        };

        let total_time = start.elapsed();

        Ok((
            results,
            ProfileOutput {
                explain: explain_output,
                runtime_stats: stats,
                total_time_ms: total_time.as_millis() as u64,
                peak_memory_bytes: 0,
            },
        ))
    }

    fn create_prop_manager(&self) -> uni_store::runtime::property_manager::PropertyManager {
        uni_store::runtime::property_manager::PropertyManager::new(
            self.storage.clone(),
            self.storage.schema_manager_arc(),
            1000,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Accumulator tests ────────────────────────────────────────────

    #[test]
    fn test_accumulator_count_basic() {
        let mut acc = Accumulator::new("COUNT", false);
        acc.update(&Value::Int(1), false);
        acc.update(&Value::Null, false); // null skipped
        acc.update(&Value::Int(2), false);
        assert_eq!(acc.finish(), Value::Int(2));
    }

    #[test]
    fn test_accumulator_count_wildcard() {
        let mut acc = Accumulator::new("COUNT", false);
        acc.update(&Value::Int(1), true);
        acc.update(&Value::Null, true); // wildcard counts nulls
        acc.update(&Value::Int(2), true);
        assert_eq!(acc.finish(), Value::Int(3));
    }

    #[test]
    fn test_accumulator_sum() {
        let mut acc = Accumulator::new("SUM", false);
        acc.update(&Value::Int(10), false);
        acc.update(&Value::Float(2.5), false);
        acc.update(&Value::Null, false); // null skipped
        assert_eq!(acc.finish(), Value::Float(12.5));
    }

    #[test]
    fn test_accumulator_minmax_temporal() {
        // Regression for the `cypher_cross_type_cmp` finding: temporal values
        // shared `cypher_type_rank` 5 and fell through to `Ordering::Equal`, so
        // MIN/MAX kept the first-encountered value instead of the true extreme.
        let dt = |secs: i64| {
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: secs * 1_000_000_000,
                offset_seconds: 0,
                timezone_name: None,
            })
        };
        // 2020, then 2010, then 2030 (unsorted insertion order).
        let y2020 = dt(1_577_836_800);
        let y2010 = dt(1_262_304_000);
        let y2030 = dt(1_893_456_000);

        let mut min_acc = Accumulator::new("MIN", false);
        let mut max_acc = Accumulator::new("MAX", false);
        for v in [&y2020, &y2010, &y2030] {
            min_acc.update(v, false);
            max_acc.update(v, false);
        }
        assert_eq!(
            min_acc.finish(),
            y2010,
            "MIN over dates must return the earliest"
        );
        assert_eq!(
            max_acc.finish(),
            y2030,
            "MAX over dates must return the latest"
        );
    }

    #[test]
    fn test_accumulator_avg() {
        let mut acc = Accumulator::new("AVG", false);
        acc.update(&Value::Int(10), false);
        acc.update(&Value::Int(20), false);
        acc.update(&Value::Int(30), false);
        assert_eq!(acc.finish(), Value::Float(20.0));
    }

    #[test]
    fn test_accumulator_avg_empty() {
        let acc = Accumulator::new("AVG", false);
        assert_eq!(acc.finish(), Value::Null);
    }

    #[test]
    fn test_accumulator_min_max() {
        let mut min_acc = Accumulator::new("MIN", false);
        let mut max_acc = Accumulator::new("MAX", false);
        for v in &[Value::Int(3), Value::Int(1), Value::Int(2)] {
            min_acc.update(v, false);
            max_acc.update(v, false);
        }
        assert_eq!(min_acc.finish(), Value::Int(1));
        assert_eq!(max_acc.finish(), Value::Int(3));
    }

    #[test]
    fn test_accumulator_collect() {
        let mut acc = Accumulator::new("COLLECT", false);
        acc.update(&Value::String("a".into()), false);
        acc.update(&Value::Null, false); // null skipped
        acc.update(&Value::String("b".into()), false);
        assert_eq!(
            acc.finish(),
            Value::List(vec![Value::String("a".into()), Value::String("b".into()),])
        );
    }

    #[test]
    fn test_accumulator_count_distinct() {
        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::String("a".into()), false);
        acc.update(&Value::String("b".into()), false);
        acc.update(&Value::String("a".into()), false); // duplicate
        acc.update(&Value::Null, false); // null skipped
        assert_eq!(acc.finish(), Value::Int(2));
    }

    #[test]
    fn test_accumulator_percentile_empty() {
        let acc = Accumulator::new_with_percentile("PERCENTILEDISC", false, 0.5);
        assert_eq!(acc.finish(), Value::Null);
    }

    // ── compare_values tests ─────────────────────────────────────────

    #[test]
    fn test_compare_values_int_ordering() {
        assert!(Executor::compare_values(&Value::Int(1), &Value::Int(2)).is_lt());
        assert!(Executor::compare_values(&Value::Int(5), &Value::Int(5)).is_eq());
        assert!(Executor::compare_values(&Value::Int(9), &Value::Int(3)).is_gt());
    }

    #[test]
    fn test_compare_values_null_last() {
        // Null should sort after everything
        assert!(Executor::compare_values(&Value::Int(1), &Value::Null).is_lt());
        assert!(Executor::compare_values(&Value::Null, &Value::Int(1)).is_gt());
        assert!(Executor::compare_values(&Value::Null, &Value::Null).is_eq());
    }

    #[test]
    fn test_compare_values_cross_type_rank() {
        // String should sort before Bool which sorts before Int
        assert!(Executor::compare_values(&Value::String("z".into()), &Value::Bool(false)).is_lt());
        assert!(Executor::compare_values(&Value::Bool(true), &Value::Int(1)).is_lt());
    }

    #[test]
    fn test_compare_values_lists() {
        let l1 = Value::List(vec![Value::Int(1), Value::Int(2)]);
        let l2 = Value::List(vec![Value::Int(1), Value::Int(3)]);
        assert!(Executor::compare_values(&l1, &l2).is_lt());
    }

    /// Regression (D5 mirror, core.rs cross-type Int/Float arm) — FIXED. The
    /// comparator used to cast `i64 as f64`, collapsing an integer just above
    /// 2^53 onto the float. It now compares exactly via `cmp_i64_f64`, so
    /// `compare_values(Int(2^53+1), Float(2^53.0))` is `Greater`
    /// (9007199254740993 > 9007199254740992), and the reverse is `Less`.
    #[test]
    fn repro_compare_values_int_float_precision_collapse() {
        let big_int = Value::Int(9_007_199_254_740_993);
        let float_2p53 = Value::Float(9_007_199_254_740_992.0);
        assert!(Executor::compare_values(&big_int, &float_2p53).is_gt());
        assert!(Executor::compare_values(&float_2p53, &big_int).is_lt());
    }

    /// COUNT(DISTINCT) must coerce numerically (1 vs 1.0 count once, matching
    /// `cypher_eq`) while keeping cross-type identity (1 vs '1' vs true count
    /// separately — the old stringifying accumulator collapsed 1 with '1').
    #[test]
    fn test_count_distinct_type_identity() {
        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::Int(1), false);
        acc.update(&Value::Float(1.0), false);
        assert_eq!(acc.finish(), Value::Int(1), "1 and 1.0 count once");

        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::Int(1), false);
        acc.update(&Value::String("1".to_string()), false);
        assert_eq!(acc.finish(), Value::Int(2), "1 and '1' are distinct");

        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::Int(1), false);
        acc.update(&Value::Bool(true), false);
        assert_eq!(acc.finish(), Value::Int(2), "1 and true are distinct");

        // Non-integral and non-finite floats keep float identity; NaN and
        // signed zero follow Value's normalized Hash/Eq (one bucket each).
        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::Float(1.5), false);
        acc.update(&Value::Float(1.5), false);
        acc.update(&Value::Float(f64::NAN), false);
        acc.update(&Value::Float(f64::NAN), false);
        acc.update(&Value::Float(0.0), false);
        acc.update(&Value::Float(-0.0), false);
        assert_eq!(acc.finish(), Value::Int(3), "1.5, NaN, 0 -> 3 buckets");
    }
}
