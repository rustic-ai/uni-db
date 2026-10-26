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
    /// Cypher's running sum, shared with the DataFusion aggregate: exact for
    /// integers, numbers or durations, anything else an error.
    Sum(uni_query_functions::cypher_sum::CypherSum),
    Min(Option<Value>),
    Max(Option<Value>),
    Avg {
        sum: uni_query_functions::cypher_sum::CypherSum,
        count: u64,
    },
    Collect(Vec<Value>),
    /// `AGG(DISTINCT x)`: `inner` folds the first value of each class of
    /// equal values ([`distinct_key`]). Only `count` honored DISTINCT before;
    /// `sum`, `avg`, `min`, `max`, `collect` and the percentiles folded
    /// every row.
    Distinct {
        seen: HashSet<Value>,
        inner: Box<Accumulator>,
    },
    /// `percentileDisc` / `percentileCont`: the inputs as exact numbers,
    /// and the percentile read from the first row that has one
    /// ([`Accumulator::set_percentile`]).
    PercentileDisc {
        values: Vec<Value>,
        percentile: Option<f64>,
    },
    PercentileCont {
        values: Vec<Value>,
        percentile: Option<f64>,
    },
}

/// Collapse an integral finite `Float` to the equivalent `Int`; leave every
/// other value untouched.
///
/// Shared *mechanism* only. The callers below each document their own
/// specification for why they fold this way — they are governed by different
/// rules (openCypher aggregation vs. the MERGE match-key coercion) and are
/// deliberately kept as separate, individually-documented entry points.
pub(crate) fn canonical_numeric_key(val: &Value) -> Value {
    // Exact: `2^63` (float) is not `i64::MAX`, which an
    // `f <= i64::MAX as f64` bound folded it into.
    match val {
        Value::Float(f) => uni_common::f64_to_exact_i64(*f).map_or(Value::Float(*f), Value::Int),
        other => other.clone(),
    }
}

/// Canonical key for `AGG(DISTINCT …)`: equal values share it, at any depth
/// of lists and maps, so `1` and `1.0` count once (matching `cypher_eq`'s
/// numeric equality); every other value keeps its type. The accumulator
/// previously stringified values, which also collapsed `1` with `'1'` — a
/// wrong answer.
fn distinct_key(val: &Value) -> Value {
    uni_query_functions::df_udfs::canonical_cypher_value(val.clone())
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
    // Numbers by exact value: widening the integer to `f64` tied `2^53 + 1`
    // with `2^53.0`, and `i64::MAX` with `2^63`.
    if let Some(ord) = uni_query_functions::expr_eval::cypher_numeric_cmp(a, b) {
        return ord.unwrap_or(Ordering::Equal);
    }
    match (a, b) {
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
        Self::with_percentile(op, distinct, None)
    }

    #[cfg(test)]
    pub(crate) fn new_with_percentile(op: &str, distinct: bool, percentile: f64) -> Self {
        Self::with_percentile(op, distinct, Some(percentile))
    }

    fn with_percentile(op: &str, distinct: bool, percentile: Option<f64>) -> Self {
        let op_upper = op.to_uppercase();
        match op_upper.as_str() {
            _ if distinct => Accumulator::Distinct {
                seen: HashSet::new(),
                inner: Box::new(Self::with_percentile(op, false, percentile)),
            },
            "COUNT" => Accumulator::Count(0),
            "SUM" => Accumulator::Sum(Default::default()),
            "MIN" => Accumulator::Min(None),
            "MAX" => Accumulator::Max(None),
            "AVG" => Accumulator::Avg {
                sum: Default::default(),
                count: 0,
            },
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

    /// Adds one row's value.
    ///
    /// # Errors
    ///
    /// When `sum` or `avg` receives a value that is neither a number nor a
    /// duration, or mixes the two, or a percentile receives a non-number.
    pub(crate) fn update(&mut self, val: &Value, is_wildcard: bool) -> anyhow::Result<()> {
        let name = self.name();
        match self {
            Accumulator::Count(c) => {
                if is_wildcard || !val.is_null() {
                    *c += 1;
                }
            }
            Accumulator::Sum(total) => total.add(val)?,
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
                sum.add(val)?;
                if !val.is_null() {
                    *count += 1;
                }
            }
            Accumulator::Collect(v) => {
                if !val.is_null() {
                    v.push(val.clone());
                }
            }
            // Every aggregate skips NULL, so DISTINCT does not admit it.
            Accumulator::Distinct { seen, inner } => {
                if !val.is_null() && seen.insert(distinct_key(val)) {
                    inner.update(val, is_wildcard)?;
                }
            }
            Accumulator::PercentileDisc { values, .. }
            | Accumulator::PercentileCont { values, .. } => {
                if let Some(n) = uni_query_functions::percentile::read_input(val, name)
                    .map_err(|e| anyhow::anyhow!(e))?
                {
                    values.push(n);
                }
            }
        }
        Ok(())
    }

    /// The Cypher name of a percentile aggregate, for its messages.
    fn name(&self) -> &'static str {
        match self {
            Accumulator::PercentileCont { .. } => "percentileCont",
            Accumulator::Distinct { inner, .. } => inner.name(),
            _ => "percentileDisc",
        }
    }

    /// Whether this aggregate takes a percentile argument it has not read
    /// yet, so the caller evaluates the second argument only while needed.
    pub(crate) fn wants_percentile(&self) -> bool {
        match self {
            Accumulator::PercentileDisc { percentile, .. }
            | Accumulator::PercentileCont { percentile, .. } => percentile.is_none(),
            Accumulator::Distinct { inner, .. } => inner.wants_percentile(),
            _ => false,
        }
    }

    /// Reads one row's percentile argument. The first non-NULL one is the
    /// percentile; nothing else reads it. The fallback executor never read
    /// it, so every percentile here answered the 0th.
    ///
    /// # Errors
    ///
    /// When the percentile is not a number or is outside `[0, 1]`.
    pub(crate) fn set_percentile(&mut self, p: &Value) -> anyhow::Result<()> {
        let name = self.name();
        match self {
            Accumulator::PercentileDisc { percentile, .. }
            | Accumulator::PercentileCont { percentile, .. } => {
                if percentile.is_none() {
                    *percentile = uni_query_functions::percentile::read_percentile(p, name)
                        .map_err(|e| anyhow::anyhow!(e))?;
                }
                Ok(())
            }
            Accumulator::Distinct { inner, .. } => inner.set_percentile(p),
            _ => Ok(()),
        }
    }

    /// The aggregate's value.
    pub(crate) fn finish(&self) -> Value {
        match self {
            Accumulator::Count(c) => Value::Int(*c),
            Accumulator::Sum(total) => total.sum(),
            Accumulator::Min(opt) => opt.as_ref().cloned().unwrap_or(Value::Null),
            Accumulator::Max(opt) => opt.as_ref().cloned().unwrap_or(Value::Null),
            Accumulator::Avg { sum, count } => sum.avg(*count),
            Accumulator::Collect(v) => Value::List(v.clone()),
            Accumulator::Distinct { inner, .. } => inner.finish(),
            Accumulator::PercentileDisc { values, percentile } => {
                uni_query_functions::percentile::percentile_disc(
                    &mut values.clone(),
                    percentile.unwrap_or(0.0),
                )
            }
            Accumulator::PercentileCont { values, percentile } => {
                uni_query_functions::percentile::percentile_cont(
                    &mut values.clone(),
                    percentile.unwrap_or(0.0),
                )
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
    /// The statement's DataFusion memory pool, built on first use and shared
    /// by every physical plan the statement builds.
    ///
    /// Fresh per clone, like `counters`; shared on purpose through
    /// [`Self::share_statement_with`] and [`Self::set_statement_memory`].
    pub(crate) statement_memory: StatementMemory,
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
    /// An enclosing query's deadline, when this executor runs a query on its
    /// behalf (a procedure's inner Cypher). Replaces the fresh
    /// `config.query_timeout` budget: the inner query spends the caller's time.
    pub(crate) inherited_deadline: Option<crate::query::df_graph::DeadlineClock>,
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

/// The memory pool one statement draws on, shared by every executor and
/// physical plan the statement builds.
///
/// A statement can build several physical plans: the executor's own, the
/// sub-plans its fallback interpreter runs, the per-row plans of a mutation,
/// and the inner query of every procedure it calls. Each used to get a pool of
/// its own sized at the full `max_query_memory`, so a statement could hold
/// several times the limit at once. One pool per statement makes the limit a
/// limit, and makes the pool's reserved total the statement's exact reserved
/// memory.
///
/// Built lazily, by the first plan, from that executor's `max_query_memory`.
#[derive(Clone, Default)]
pub struct StatementMemory(
    Arc<std::sync::OnceLock<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>>,
);

impl std::fmt::Debug for StatementMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StatementMemory")
            .field(&self.0.get().map(|p| p.reserved()))
            .finish()
    }
}

impl StatementMemory {
    /// A slot already holding `pool`, for an executor that runs inside a
    /// statement whose pool exists (a procedure's inner query).
    pub fn with_pool(pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>) -> Self {
        let slot = std::sync::OnceLock::new();
        let _ = slot.set(pool);
        Self(Arc::new(slot))
    }

    /// The pool, building it with `init` if no plan has yet.
    pub(crate) fn get_or_init(
        &self,
        init: impl FnOnce() -> Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    ) -> Arc<dyn datafusion::execution::memory_pool::MemoryPool> {
        Arc::clone(self.0.get_or_init(init))
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
            config: self.config.clone(),
            gen_expr_cache: self.gen_expr_cache.clone(),
            procedure_registry: self.procedure_registry.clone(),
            xervo_runtime: self.xervo_runtime.clone(),
            // Fresh warnings per clone — see struct doc.
            warnings: Arc::new(std::sync::Mutex::new(Vec::new())),
            // Fresh counters per clone, same reason.
            counters: Arc::new(uni_store::QueryCounters::new()),
            // Fresh per clone, same reason.
            statement_memory: StatementMemory::default(),
            transaction_l0_override: self.transaction_l0_override.clone(),
            id_reservoir: self.id_reservoir.clone(),
            custom_function_registry: self.custom_function_registry.clone(),
            cancellation_token: self.cancellation_token.clone(),
            inherited_deadline: self.inherited_deadline.clone(),
            prop_manager_arc: self.prop_manager_arc.clone(),
            df_session_template: self.df_session_template.clone(),
            read_snapshot: self.read_snapshot.clone(),
        }
    }
}

impl Executor {
    /// Create a read-only executor backed by the given storage manager.
    pub fn new(storage: Arc<StorageManager>) -> Self {
        // A declaration judges the values a property already holds by the
        // write path's rule; see `uni_store::storage::declared_values`.
        uni_store::storage::declared_values::install_coercion(
            crate::query::executor::write::coerce_declared_value,
        );
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
            config: uni_common::config::UniConfig::default(),
            gen_expr_cache: Arc::new(RwLock::new(HashMap::new())),
            procedure_registry: Some(proc_registry),
            xervo_runtime: None,
            warnings: Arc::new(std::sync::Mutex::new(Vec::new())),
            counters: Arc::new(uni_store::QueryCounters::new()),
            statement_memory: StatementMemory::default(),
            transaction_l0_override: None,
            id_reservoir: None,
            custom_function_registry: None,
            cancellation_token: None,
            inherited_deadline: None,
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

    /// Set the file sandbox configuration for BACKUP and COPY commands.
    ///
    /// Overrides `file_sandbox` of the configuration [`Self::set_config`]
    /// applied; the sandbox is part of that configuration, so a `Uni`
    /// database's executor already carries its `UniConfig::file_sandbox`.
    pub fn set_file_sandbox(&mut self, sandbox: uni_common::config::FileSandboxConfig) {
        self.config.file_sandbox = sandbox;
    }

    /// Apply a runtime configuration to this executor.
    pub fn set_config(&mut self, config: uni_common::config::UniConfig) {
        self.config = config;
    }

    /// Validate a file path against the configured file sandbox.
    ///
    /// The sandbox is read from `config` — the `UniConfig` a database applies
    /// with [`Self::set_config`]. It used to be a separate field that only
    /// `set_file_sandbox` wrote and nothing in the database called, so
    /// `UniConfig::file_sandbox` was never enforced.
    pub(crate) fn validate_path(&self, path: &str) -> Result<std::path::PathBuf> {
        self.config
            .file_sandbox
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

    /// Share an existing counter set with this executor instead of its own.
    ///
    /// An `Executor` allocates a fresh `Arc<QueryCounters>`, which is right when
    /// it *is* the execution. It is wrong when one logical query builds several
    /// executors: each would count into its own set and only one could be
    /// harvested. A Locy evaluation is that case — the ASSUME/ABDUCE path
    /// re-enters with a second executor — so the evaluation owns the counters
    /// and hands the same `Arc` to every executor it builds.
    ///
    /// Prefer this to harvesting each executor and folding with
    /// `QueryCounters::merge_from`: sharing cannot forget a participant, and a
    /// forgotten one is invisible, since a missing count looks exactly like work
    /// that did not happen.
    pub fn set_counters(&mut self, counters: Arc<uni_store::QueryCounters>) {
        self.counters = counters;
    }

    /// Draw on `memory`, another executor's statement pool, instead of a pool
    /// of this executor's own.
    ///
    /// Pair with [`Self::set_counters`] wherever one statement builds several
    /// executors (a Locy evaluation's re-entries): sharing the counters without
    /// the pool would report one statement's peak while enforcing one budget
    /// per executor.
    pub fn set_statement_memory(&mut self, memory: StatementMemory) {
        self.statement_memory = memory;
    }

    /// This executor's statement-memory slot, for handing to another executor
    /// of the same statement.
    pub fn statement_memory(&self) -> StatementMemory {
        self.statement_memory.clone()
    }

    /// Share `other`'s counters and memory pool: make this executor part of
    /// the same statement.
    pub(crate) fn share_statement_with(&mut self, other: &Executor) {
        self.counters = Arc::clone(&other.counters);
        self.statement_memory = other.statement_memory.clone();
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

    /// The property manager reads on this executor's behalf should use: the
    /// shared one when installed, else a fresh one over **live** storage.
    ///
    /// Never one over [`Self::effective_storage`]: a property manager over
    /// version-pinned storage treats the pin as a time-travel snapshot and
    /// skips the transaction's own buffer, so a transaction would stop
    /// reading its own property writes.
    pub(crate) fn live_property_manager(&self) -> Arc<PropertyManager> {
        self.prop_manager_arc.clone().unwrap_or_else(|| {
            Arc::new(PropertyManager::new(
                self.storage.clone(),
                self.storage.schema_manager_arc(),
                1000,
            ))
        })
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

    /// Spend an enclosing query's deadline rather than a fresh budget.
    ///
    /// For a query run on another query's behalf, such as a procedure's inner
    /// Cypher. The clock is live, so idle credit granted to the enclosing
    /// cursor reaches this executor's checks too.
    pub(crate) fn inherit_deadline(&mut self, clock: crate::query::df_graph::DeadlineClock) {
        self.inherited_deadline = Some(clock);
    }

    /// The deadline and cancellation token a procedure host built by this
    /// executor should carry, so the inner queries it runs spend this query's
    /// budget.
    ///
    /// The inherited clock when there is one; otherwise `ctx`'s deadline, which
    /// is fixed because this path does not stream.
    pub(crate) fn procedure_budget(
        &self,
        ctx: Option<&QueryContext>,
    ) -> (
        crate::query::df_graph::DeadlineClock,
        Option<tokio_util::sync::CancellationToken>,
    ) {
        let deadline = self.inherited_deadline.clone().unwrap_or_else(|| {
            crate::query::df_graph::DeadlineClock::fixed(ctx.and_then(|c| c.deadline))
        });
        let token = ctx
            .and_then(|c| c.cancellation_token.clone())
            .or_else(|| self.cancellation_token.clone());
        (deadline, token)
    }

    /// The deadline a `QueryContext` built now should carry.
    fn context_deadline(&self) -> Option<Instant> {
        match &self.inherited_deadline {
            Some(clock) => clock.effective(),
            None => Some(Instant::now() + self.config.query_timeout),
        }
    }

    /// Build a `QueryContext` from the current writer or standalone L0 manager.
    /// When `transaction_l0_override` is set, it is used as the transaction L0 —
    /// this is how private-per-transaction L0 buffers become visible to reads
    /// without requiring the writer lock at tx creation.
    /// The L0 buffers this executor's reads see (current, pending flush and
    /// any transaction-local buffer), or none when it has no L0 at all.
    pub(crate) async fn visible_l0(&self) -> uni_store::runtime::l0_visibility::L0Context {
        self.get_context()
            .await
            .map(|ctx| uni_store::runtime::l0_visibility::L0Context::from_query_context(&ctx))
            .unwrap_or_default()
    }

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
            if let Some(deadline) = self.context_deadline() {
                ctx.set_deadline(deadline);
            }
            if let Some(ref token) = self.cancellation_token {
                ctx.set_cancellation_token(token.clone());
            }
            ctx.set_counters(self.counters.clone());
            // The shared property manager reads live storage; a pinned
            // read's L1 visibility travels with the context.
            ctx.set_l1_visibility(self.effective_storage().pinned_row_visibility());
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
                if let Some(deadline) = self.context_deadline() {
                    ctx.set_deadline(deadline);
                }
                if let Some(ref token) = self.cancellation_token {
                    ctx.set_cancellation_token(token.clone());
                }
                ctx.set_counters(self.counters.clone());
                ctx.set_l1_visibility(self.effective_storage().pinned_row_visibility());
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
    /// Most bytes the query held reserved from its DataFusion memory pools at
    /// once, summed across every physical plan it built (sub-plans included).
    ///
    /// This is *reserved* memory, the quantity `max_query_memory` bounds, not
    /// process RSS: an operator that allocates without reserving through the
    /// pool (#242) does not appear in it. A query whose operators reserve
    /// nothing — a DDL statement, a plain scan returning few rows — reads `0`.
    pub peak_memory_bytes: usize,
    /// Logical planning time in milliseconds (Cypher AST to logical plan).
    ///
    /// Measured by the API layer before execution starts, so it is not part of
    /// [`Self::total_time_ms`].
    #[serde(default)]
    pub logical_planning_ms: f64,
    /// Physical planning time in milliseconds, summed over every plan built.
    ///
    /// Includes the sub-plans built during execution (a correlated subquery's
    /// body once per outer row), so it is part of [`Self::total_time_ms`].
    #[serde(default)]
    pub physical_planning_ms: f64,
    /// Physical plans built: one for the query plus one per execution-time
    /// sub-plan.
    #[serde(default)]
    pub physical_plans: u64,
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
    /// Memory reserved by this operator in bytes.
    ///
    /// Always `0` for now. Reservations are made by named consumers
    /// (`ExternalSorter[0]`, `GraphScanExec[3]`, ...) whose names do not map
    /// one-to-one onto plan nodes — two sorts in one plan register the same
    /// name — so a per-operator figure would be a guess. The query-level
    /// [`ProfileOutput::peak_memory_bytes`] is measured.
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
        let plans_before = self.counters.physical_plans();
        let planning_before = self.counters.planning_nanos();

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
                // Every pool this query built reports into its counters; see
                // `peak_pool`.
                peak_memory_bytes: usize::try_from(self.counters.peak_reserved_bytes())
                    .unwrap_or(usize::MAX),
                // Filled in by the API layer, which owns logical planning.
                logical_planning_ms: 0.0,
                physical_planning_ms: self
                    .counters
                    .planning_nanos()
                    .saturating_sub(planning_before) as f64
                    / 1_000_000.0,
                physical_plans: self.counters.physical_plans().saturating_sub(plans_before),
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

    const TWO_53: i64 = 1 << 53;
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;

    #[test]
    fn canonical_numeric_key_does_not_fold_two_to_the_63() {
        assert_eq!(
            canonical_numeric_key(&Value::Float(TWO_63)),
            Value::Float(TWO_63)
        );
        assert_eq!(
            canonical_numeric_key(&Value::Float(-TWO_63)),
            Value::Int(i64::MIN)
        );
        assert_eq!(canonical_numeric_key(&Value::Float(1.0)), Value::Int(1));
    }

    #[test]
    fn min_max_compare_integers_and_floats_exactly() {
        for values in [
            [Value::Int(TWO_53 + 1), Value::Float(TWO_53 as f64)],
            [Value::Float(TWO_53 as f64), Value::Int(TWO_53 + 1)],
        ] {
            let mut min_acc = Accumulator::new("MIN", false);
            let mut max_acc = Accumulator::new("MAX", false);
            for v in &values {
                min_acc.update(v, false).unwrap();
                max_acc.update(v, false).unwrap();
            }
            assert_eq!(min_acc.finish(), Value::Float(TWO_53 as f64));
            assert_eq!(max_acc.finish(), Value::Int(TWO_53 + 1));
        }
        let mut max_acc = Accumulator::new("MAX", false);
        max_acc.update(&Value::Float(TWO_63), false).unwrap();
        max_acc.update(&Value::Int(i64::MAX), false).unwrap();
        assert_eq!(max_acc.finish(), Value::Float(TWO_63));
    }

    #[test]
    fn distinct_aggregates_fold_the_first_value_of_each_class() {
        let values = [
            Value::Float(1.0),
            Value::Int(1),
            Value::Null,
            Value::Float(1.0),
        ];
        for (op, want) in [
            ("SUM", Value::Float(1.0)),
            ("COUNT", Value::Int(1)),
            ("MIN", Value::Float(1.0)),
            ("COLLECT", Value::List(vec![Value::Float(1.0)])),
        ] {
            let mut acc = Accumulator::new(op, true);
            for v in &values {
                acc.update(v, false).unwrap();
            }
            assert_eq!(acc.finish(), want, "{op}(DISTINCT)");
        }
        let mut acc = Accumulator::new("COUNT", true);
        for v in [Value::Int(i64::MAX), Value::Float(TWO_63)] {
            acc.update(&v, false).unwrap();
        }
        assert_eq!(acc.finish(), Value::Int(2));
    }

    #[test]
    fn test_accumulator_percentile_rejects_a_non_number() {
        for op in ["PERCENTILECONT", "PERCENTILEDISC"] {
            let mut acc = Accumulator::new_with_percentile(op, false, 0.5);
            acc.update(&Value::Int(1), false).unwrap();
            acc.update(&Value::Null, false).unwrap();
            assert!(acc.update(&Value::String("x".into()), false).is_err());
        }
    }

    #[test]
    fn test_accumulator_count_basic() {
        let mut acc = Accumulator::new("COUNT", false);
        acc.update(&Value::Int(1), false).unwrap();
        acc.update(&Value::Null, false).unwrap(); // null skipped
        acc.update(&Value::Int(2), false).unwrap();
        assert_eq!(acc.finish(), Value::Int(2));
    }

    #[test]
    fn test_accumulator_count_wildcard() {
        let mut acc = Accumulator::new("COUNT", false);
        acc.update(&Value::Int(1), true).unwrap();
        acc.update(&Value::Null, true).unwrap(); // wildcard counts nulls
        acc.update(&Value::Int(2), true).unwrap();
        assert_eq!(acc.finish(), Value::Int(3));
    }

    #[test]
    fn test_accumulator_sum() {
        let mut acc = Accumulator::new("SUM", false);
        acc.update(&Value::Int(10), false).unwrap();
        acc.update(&Value::Float(2.5), false).unwrap();
        acc.update(&Value::Null, false).unwrap(); // null skipped
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
            min_acc.update(v, false).unwrap();
            max_acc.update(v, false).unwrap();
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
        acc.update(&Value::Int(10), false).unwrap();
        acc.update(&Value::Int(20), false).unwrap();
        acc.update(&Value::Int(30), false).unwrap();
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
            min_acc.update(v, false).unwrap();
            max_acc.update(v, false).unwrap();
        }
        assert_eq!(min_acc.finish(), Value::Int(1));
        assert_eq!(max_acc.finish(), Value::Int(3));
    }

    #[test]
    fn test_accumulator_collect() {
        let mut acc = Accumulator::new("COLLECT", false);
        acc.update(&Value::String("a".into()), false).unwrap();
        acc.update(&Value::Null, false).unwrap(); // null skipped
        acc.update(&Value::String("b".into()), false).unwrap();
        assert_eq!(
            acc.finish(),
            Value::List(vec![Value::String("a".into()), Value::String("b".into()),])
        );
    }

    #[test]
    fn test_accumulator_count_distinct() {
        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::String("a".into()), false).unwrap();
        acc.update(&Value::String("b".into()), false).unwrap();
        acc.update(&Value::String("a".into()), false).unwrap(); // duplicate
        acc.update(&Value::Null, false).unwrap(); // null skipped
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
        acc.update(&Value::Int(1), false).unwrap();
        acc.update(&Value::Float(1.0), false).unwrap();
        assert_eq!(acc.finish(), Value::Int(1), "1 and 1.0 count once");

        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::Int(1), false).unwrap();
        acc.update(&Value::String("1".to_string()), false).unwrap();
        assert_eq!(acc.finish(), Value::Int(2), "1 and '1' are distinct");

        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::Int(1), false).unwrap();
        acc.update(&Value::Bool(true), false).unwrap();
        assert_eq!(acc.finish(), Value::Int(2), "1 and true are distinct");

        // Non-integral and non-finite floats keep float identity; NaN and
        // signed zero follow Value's normalized Hash/Eq (one bucket each).
        let mut acc = Accumulator::new("COUNT", true);
        acc.update(&Value::Float(1.5), false).unwrap();
        acc.update(&Value::Float(1.5), false).unwrap();
        acc.update(&Value::Float(f64::NAN), false).unwrap();
        acc.update(&Value::Float(f64::NAN), false).unwrap();
        acc.update(&Value::Float(0.0), false).unwrap();
        acc.update(&Value::Float(-0.0), false).unwrap();
        assert_eq!(acc.finish(), Value::Int(3), "1.5, NaN, 0 -> 3 buckets");
    }
}
