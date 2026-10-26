// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::query::WINDOW_FUNCTIONS;
use crate::query::datetime::{classify_temporal, eval_datetime_function, parse_datetime_utc};
use crate::query::expr_eval::{
    eval_binary_op, eval_in_op, eval_scalar_function, eval_vector_similarity,
};
use crate::query::planner::{LogicalPlan, QueryPlanner};
use crate::types::Value;
use anyhow::{Result, anyhow};

/// Convert a `Value` to `chrono::DateTime<Utc>`, handling both `Value::Temporal` and `Value::String`.
fn value_to_datetime_utc(val: &Value) -> Option<chrono::DateTime<chrono::Utc>> {
    match val {
        Value::Temporal(tv) => {
            use uni_common::TemporalValue;
            match tv {
                TemporalValue::DateTime {
                    nanos_since_epoch, ..
                }
                | TemporalValue::LocalDateTime {
                    nanos_since_epoch, ..
                } => Some(chrono::DateTime::from_timestamp_nanos(*nanos_since_epoch)),
                TemporalValue::Date { days_since_epoch } => {
                    chrono::DateTime::from_timestamp(*days_since_epoch as i64 * 86400, 0)
                }
                _ => None,
            }
        }
        Value::String(s) => parse_datetime_utc(s).ok(),
        _ => None,
    }
}
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use metrics;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{ConstraintTarget, ConstraintType, DataType, SchemaManager};
use uni_cypher::ast::{
    BinaryOp, ConstraintTarget as AstConstraintTarget, Expr, MapProjectionItem, Quantifier,
    ShowConstraints, UnaryOp,
};
use uni_store::QueryContext;
use uni_store::cloud::{build_store_from_url, copy_store_prefix, is_cloud_url};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::arrow_convert;

// DataFusion engine imports
use crate::query::df_graph::L0Context;
use crate::query::df_planner::HybridPhysicalPlanner;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::SessionContext;
use parking_lot::RwLock as SyncRwLock;

use arrow_array::RecordBatch;

use super::core::*;

/// Number of system fields on an edge map: `_eid`, `_src`, `_dst`, `_type`, `_type_name`.
const EDGE_SYSTEM_FIELD_COUNT: usize = 5;
/// Number of system fields on a vertex map: `_vid`, `_label`, `_uid`.
const VERTEX_SYSTEM_FIELD_COUNT: usize = 3;

/// Hydrate an entity map (vertex or edge) with properties if not already loaded.
///
/// This is the fallback for pushdown hydration - if the entity only has system fields
/// (indicating pushdown didn't load properties), we load all properties here.
///
/// System field counts:
/// - Edge: 5 fields (_eid, _src, _dst, _type, _type_name)
/// - Vertex: 3 fields (_vid, _label, _uid)
async fn hydrate_entity_if_needed(
    map: &mut HashMap<String, Value>,
    prop_manager: &PropertyManager,
    ctx: Option<&QueryContext>,
) {
    // Which kind of entity this is, and its id, asked once. Reading `_eid` and
    // `_vid` as integers meant a map spelling its id `_id`, or carrying the
    // serde form `"Vid(7)"`, was hydrated as neither — so `n.prop` came back
    // NULL after a pushdown that emitted only the system fields (#234).
    let entity = uni_common::value::entity_ref_from_map(map);

    // An ephemeral entity (`uni.create.vNode` / `vEdge`) has no stored row:
    // whatever it carries is all there is, so there is nothing to hydrate.
    match entity {
        Some(uni_common::value::EntityRef::Vertex(vid)) if vid.is_ephemeral() => return,
        Some(uni_common::value::EntityRef::Edge(eid)) if eid.is_ephemeral() => return,
        _ => {}
    }

    // Check for edge entity
    if let Some(uni_common::value::EntityRef::Edge(eid)) = entity {
        let eid_u64 = eid.as_u64();
        if map.len() <= EDGE_SYSTEM_FIELD_COUNT {
            tracing::debug!(
                "Pushdown fallback: hydrating edge {} at execution time",
                eid_u64
            );
            if let Ok(Some(props)) = prop_manager
                .get_all_edge_props_with_ctx(Eid::from(eid_u64), ctx)
                .await
            {
                for (key, value) in props {
                    map.entry(key).or_insert(value);
                }
            }
        } else {
            tracing::trace!(
                "Pushdown success: edge {} already has {} properties",
                eid_u64,
                map.len() - EDGE_SYSTEM_FIELD_COUNT
            );
        }
        return;
    }

    // Check for vertex entity
    if let Some(uni_common::value::EntityRef::Vertex(vid)) = entity {
        let vid_u64 = vid.as_u64();
        if map.len() <= VERTEX_SYSTEM_FIELD_COUNT {
            tracing::debug!(
                "Pushdown fallback: hydrating vertex {} at execution time",
                vid_u64
            );
            if let Ok(Some(props)) = prop_manager
                .get_all_vertex_props_with_ctx(Vid::from(vid_u64), ctx)
                .await
            {
                for (key, value) in props {
                    map.entry(key).or_insert(value);
                }
            }
        } else {
            tracing::trace!(
                "Pushdown success: vertex {} already has {} properties",
                vid_u64,
                map.len() - VERTEX_SYSTEM_FIELD_COUNT
            );
        }
    }
}

/// Promote a VID-string placeholder variable to a `Map`, draining its
/// dotted system/property columns (`var.<prop>`) into the new map.
///
/// Used by `record_batches_to_rows` for search-procedure outputs, where the
/// bare variable arrives as a VID string rather than a materialized Map.
fn promote_vid_placeholder(row: &mut HashMap<String, Value>, var: &str) {
    let prefix = format!("{}.", var);
    let mut map = HashMap::new();

    let dotted_keys: Vec<String> = row
        .keys()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();

    for key in &dotted_keys {
        let prop_name = &key[prefix.len()..];
        if let Some(val) = row.remove(key) {
            map.insert(prop_name.to_string(), val);
        }
    }

    // Replace the VID-string placeholder with the constructed Map
    row.insert(var.to_string(), Value::Map(map));
}

/// Merge the helper system columns (`var._vid`, `var._labels`, `var._eid`,
/// `var._type`) and remaining USER property columns (`var.<prop>`) into the
/// bare `Map` variable, removing the dotted helper columns from `row`.
///
/// Merging user properties keeps `RETURN var.prop` consistent after a
/// unit-subquery SET refreshes the dotted columns: without it the (now stale)
/// bare entity Struct from the outer scan would override the post-SET values.
/// Internal helpers (`_all_props`, `_src_vid`, …) and `overflow_json` are
/// dropped silently.
fn merge_dotted_columns(row: &mut HashMap<String, Value>, var: &str) {
    // Merge node system fields (_vid, _labels)
    let vid_key = format!("{}._vid", var);
    let labels_key = format!("{}._labels", var);

    let vid_val = row.remove(&vid_key);
    let labels_val = row.remove(&labels_key);

    if let Some(Value::Map(map)) = row.get_mut(var) {
        if let Some(v) = vid_val {
            map.insert("_vid".to_string(), v);
        }
        if let Some(v) = labels_val {
            map.insert("_labels".to_string(), v);
        }
    }

    // Merge edge system fields (_eid, _type, _src_vid, _dst_vid).
    // These are emitted as helper columns by the traverse exec.
    // The structural projection already includes them in the struct,
    // but we still need to remove the dotted helper columns.
    let eid_key = format!("{}._eid", var);
    let type_key = format!("{}._type", var);

    let eid_val = row.remove(&eid_key);
    let type_val = row.remove(&type_key);

    if (eid_val.is_some() || type_val.is_some())
        && let Some(Value::Map(map)) = row.get_mut(var)
    {
        if let Some(v) = eid_val {
            map.entry("_eid".to_string()).or_insert(v);
        }
        if let Some(v) = type_val {
            map.entry("_type".to_string()).or_insert(v);
        }
    }

    // Drain remaining dotted columns, merging surviving USER properties.
    let prefix = format!("{}.", var);
    let dotted_keys: Vec<String> = row
        .keys()
        .filter(|k| k.starts_with(&prefix))
        .cloned()
        .collect();
    for key in dotted_keys {
        let prop_name = key[prefix.len()..].to_string();
        let val = row.remove(&key);
        if uni_common::core::schema::is_reserved_property_name(&prop_name) {
            continue;
        }
        if let (Some(val), Some(Value::Map(map))) = (val, row.get_mut(var)) {
            map.insert(prop_name, val);
        }
    }
}

/// Print every physical plan to stderr when `UNI_DUMP_PHYSICAL` is set.
///
/// `EXPLAIN` shows the *logical* plan, and several defects only become visible
/// in the physical one — #184's is a column that survives into a traversal's
/// projection, which no logical plan shows. Read once rather than per query:
/// `std::env::var` takes a lock on the environment, and this sits in the
/// execution path of every statement.
static DUMP_PHYSICAL_PLAN: std::sync::LazyLock<bool> =
    std::sync::LazyLock::new(|| std::env::var("UNI_DUMP_PHYSICAL").is_ok());

/// The DataFusion session configuration a query runs under.
///
/// `UniConfig::parallelism` becomes `target_partitions` and
/// `UniConfig::execution_batch_size`, when set, becomes `batch_size`; both
/// were previously ignored, so every query ran with DataFusion's defaults
/// whatever the config said. Every place that builds a session must use this,
/// or a knob silently applies to some queries and not others.
pub fn datafusion_session_config(
    config: &uni_common::UniConfig,
) -> datafusion::prelude::SessionConfig {
    let session =
        datafusion::prelude::SessionConfig::new().with_target_partitions(config.parallelism.max(1));
    match config.execution_batch_size {
        Some(rows) => session.with_batch_size(rows.max(1)),
        None => session,
    }
}

impl Executor {
    /// The VID a value denotes, as an entity or as a raw id.
    ///
    /// This is the write path's lenient reader — `SET`, `DELETE` and the
    /// importers all accept an id where an entity would do — so it delegates to
    /// [`uni_common::Value::coerce_vid`] rather than the narrow
    /// `entity_vid`. It is the only one of the five former hand-rolled
    /// extractors that reports a failure instead of returning `None`, and that
    /// is kept: a write that cannot identify its target must not proceed
    /// silently.
    pub(crate) fn vid_from_value(val: &Value) -> Result<Vid> {
        val.coerce_vid()
            .ok_or_else(|| anyhow!("Invalid Vid format: {:?}", val))
    }

    /// Find a node value in the row by VID.
    ///
    /// Scans all values in the row, looking for a node (Map or Node) whose VID
    /// matches the target. Returns the full node value if found, or a bare
    /// `Value::Node` carrying just the identity as fallback.
    fn find_node_by_vid(row: &HashMap<String, Value>, target_vid: Vid) -> Value {
        for val in row.values() {
            if let Ok(vid) = Self::vid_from_value(val)
                && vid == target_vid
            {
                return val.clone();
            }
        }
        // Nothing in the row denotes it, so answer with the identity alone.
        Value::Node(uni_common::Node {
            vid: target_vid,
            labels: Vec::new(),
            properties: HashMap::new(),
        })
    }

    /// Create L0 context, session, and planner for DataFusion execution.
    ///
    /// This is the shared setup for `execute_datafusion` and `execute_merge_read_plan`.
    /// Returns `(session_ctx, planner, prop_manager_arc)`.
    pub async fn create_datafusion_planner(
        &self,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<(
        Arc<SyncRwLock<SessionContext>>,
        HybridPhysicalPlanner,
        Arc<PropertyManager>,
    )> {
        let query_ctx = self.get_context().await;
        let l0_context = match query_ctx {
            Some(ref ctx) => L0Context::from_query_context(ctx),
            None => L0Context::empty(),
        };

        // C2: scans route through the version-pinned storage manager so
        // post-snapshot L1 ROWS are invisible (the row-existence pin), and
        // its adjacency reads are filtered to the pin. The PropertyManager
        // is shared and stays on LIVE storage, so read-your-writes holds (a
        // transaction's own uncommitted edge/vertex properties live in
        // tx_l0, consulted before L1 — e.g. MERGE's edge-property match);
        // its L1 reads are bounded by the pin through the query context's
        // `l1_visibility`, and it bypasses its shared cache while pinned.
        let effective_storage = self.effective_storage();

        // Prefer the shared `Arc<PropertyManager>` installed via
        // `Executor::set_prop_manager` — skips the LRU/Mutex allocation cost
        // (~80 µs/query) of constructing a fresh, immediately-abandoned
        // PropertyManager. Falls back to the legacy fresh-construction path
        // for callers that have not installed one.
        let prop_manager_arc = if let Some(shared) = self.prop_manager_arc.as_ref() {
            shared.clone()
        } else {
            Arc::new(PropertyManager::new(
                self.storage.clone(),
                self.storage.schema_manager_arc(),
                prop_manager.cache_size(),
            ))
        };

        // Two-mode session construction (hot/cold template path, main)
        // fused with plugin-framework scalar/optimizer registration
        // (plugin-fw).
        //
        // Hot path: clone the pre-built `df_session_template` — an O(1)
        // Arc bump on the inner `SessionState`. Skips the ~140 µs cost of
        // `SessionContext::new()` + `register_cypher_udfs()`. The template
        // only has the built-in Cypher UDFs baked in (see
        // `Uni::build` → `register_cypher_udfs`), so it is ONLY safe to
        // reuse when this query needs no per-query dynamic registration:
        //   - no legacy `CustomFunctionRegistry` scalars,
        //   - no plugin-registered optimizer rules,
        //   - no host-level plugin scalars (`Uni::load_*_plugin`),
        //   - no session-scoped plugin scalars.
        //
        // Cold path: construct a fresh, isolated `SessionContext` (folding
        // in any plugin optimizer rules) and register the dynamic scalar
        // sets so they do not leak into the shared template.
        let has_custom_udfs = self
            .custom_function_registry
            .as_ref()
            .is_some_and(|r| !r.is_empty());
        let host_plugin_registry = self
            .procedure_registry
            .as_ref()
            .and_then(|pr| pr.plugin_registry());
        // Optimizer rules come from the host plugin registry (the legacy
        // `CustomFunctionRegistry` only ever carried scalar fns). When no
        // host registry is attached, there are no plugin optimizer rules.
        let optimizer_providers = host_plugin_registry
            .as_ref()
            .map(|pr| pr.optimizer_rules())
            .unwrap_or_default();
        let session_local_registry =
            crate::query::df_udfs_plugin::current_session_plugin_registry();
        let needs_dynamic_registration = has_custom_udfs
            || !optimizer_providers.is_empty()
            || host_plugin_registry.is_some()
            || session_local_registry.is_some();

        // `max_query_memory` reaches DataFusion here, as a memory pool on the
        // runtime environment. The pool cannot be swapped onto an existing
        // `SessionContext`, so it has to be installed wherever one is built —
        // and both places matter, because `needs_dynamic_registration` is true
        // whenever a host plugin registry is attached, which is every database
        // that loaded the built-in plugins. Sizing it here rather than only on
        // the shared template also makes a per-statement `.max_memory()`
        // override real, and makes the budget per query rather than shared
        // across concurrent ones, which is what the setting's name promises.
        let df_runtime = self.query_runtime()?;

        let session = if let (Some(tmpl), false) = (
            self.df_session_template.as_ref(),
            needs_dynamic_registration,
        ) {
            // Hot path: clone the template (Cypher UDFs already registered).
            // A fresh state, not a clone of the template's handle: the query
            // registers its own `_cypher_regex` below, which must not reach
            // the shared template, and carries its own memory pool.
            use datafusion::execution::session_state::SessionStateBuilder;
            let state = SessionStateBuilder::new_from_existing(tmpl.state())
                .with_runtime_env(Arc::clone(&df_runtime))
                .build();
            SessionContext::new_with_state(state)
        } else {
            // Cold path: build a fresh session, optionally with any
            // plugin-registered optimizer rules folded in.
            //
            // M5h: build a `SessionContext` that includes any
            // plugin-registered optimizer rules. The rule chain is
            // snapshotted at session-construction time; reload discipline
            // is M10's problem.
            let session = if optimizer_providers.is_empty() {
                SessionContext::new_with_config_rt(
                    datafusion_session_config(&self.config),
                    Arc::clone(&df_runtime),
                )
            } else {
                use datafusion::execution::session_state::SessionStateBuilder;
                use uni_plugin::traits::operator::OptimizerPhase;
                let mut builder = SessionStateBuilder::new()
                    .with_config(datafusion_session_config(&self.config))
                    .with_default_features()
                    .with_runtime_env(Arc::clone(&df_runtime));
                for provider in optimizer_providers.iter() {
                    match provider.phase() {
                        OptimizerPhase::Logical => {
                            builder = builder.with_optimizer_rule(provider.rule());
                        }
                        OptimizerPhase::Physical => {
                            if let Some(rule) = provider.physical_rule() {
                                builder = builder.with_physical_optimizer_rule(rule);
                            } else {
                                tracing::debug!(
                                    target: "uni.plugin.registry",
                                    "physical-phase provider returned no physical_rule(); skipping"
                                );
                            }
                        }
                        OptimizerPhase::Both => {
                            builder = builder.with_optimizer_rule(provider.rule());
                            if let Some(rule) = provider.physical_rule() {
                                builder = builder.with_physical_optimizer_rule(rule);
                            }
                        }
                        _ => {
                            tracing::debug!(
                                target: "uni.plugin.registry",
                                "skipping optimizer rule for unknown phase"
                            );
                        }
                    }
                }
                let state = builder.build();
                SessionContext::new_with_state(state)
            };
            crate::query::df_udfs::register_cypher_udfs(&session)?;
            // Instance-scope, legacy `CustomFunctionRegistry` shadow path
            // (db.register_function() entries + apoc-core mirrors). Routed
            // through the plugin-registry adapter (plugin-fw).
            if let Some(ref registry) = self.custom_function_registry {
                crate::query::df_udfs_plugin::register_custom_functions_as_plugin_scalars(
                    &session, registry,
                )?;
            }
            // Instance-scope, **host** plugin registry (M8.6 follow-up):
            // `Uni::load_python_plugin` / `Uni::load_rhai_plugin` /
            // `Uni::load_wasm_*` and other host-level plugin-load paths
            // register into `UniInner.plugin_registry`. The executor
            // reaches that registry via the procedure-registry's
            // attached handle (set by `Uni::build` at construction time).
            // Without this branch, scalars added through
            // `Uni::load_*_plugin` were invisible to Cypher's UDF
            // resolution despite being directly invokable via
            // `db.plugin_registry().scalar_fn(...)`.
            if let Some(ref host_pr) = host_plugin_registry {
                crate::query::df_udfs_plugin::register_plugin_scalar_udfs(&session, host_pr)?;
            }
            // Session-scope (M8.6): when the call is inside a
            // `scoped_with_session_plugin_registry` scope (set by a host
            // crate's per-query execution path), register the session's
            // local plugin scalars *last* so they shadow instance entries
            // by name (DataFusion's `register_udf` is last-write-wins).
            if let Some(ref session_local) = session_local_registry {
                crate::query::df_udfs_plugin::register_plugin_scalar_udfs(&session, session_local)?;
            }
            session
        };
        let session_ctx = Arc::new(SyncRwLock::new(session));

        let mut planner = HybridPhysicalPlanner::with_l0_context(
            session_ctx.clone(),
            effective_storage.clone(),
            l0_context,
            prop_manager_arc.clone(),
            effective_storage.schema_manager().schema(),
            params.clone(),
            HashMap::new(),
        );

        // Thread this execution's counters into the graph context every physical
        // operator receives. Must come before the other `with_*` calls only in
        // the sense that `take_graph_ctx` preserves it either way — but it must
        // be here at all, or every operator-level count is dropped.
        planner = planner.with_counters(Some(self.counters.clone()));
        // The statement's pool travels with the graph context, so a procedure's
        // inner query (`QueryProcedureHost::execute_inner_query`) draws on it
        // instead of building a full-size pool of its own per call.
        planner = planner.with_memory_pool(Some(Arc::clone(&df_runtime.memory_pool)));
        planner = planner.with_vector_knn_default_k(self.config.vector_knn_default_k);
        // The deadline and token live on the `QueryContext` built above; without
        // this hop they never reach a physical operator and every
        // `graph_ctx.check_timeout()` below is a no-op. See #207.
        if let Some(ref ctx) = query_ctx {
            planner = match &self.inherited_deadline {
                // Live, so the operators see credit the enclosing cursor earns
                // while this query runs.
                Some(clock) => {
                    planner.with_inherited_deadline(clock, ctx.cancellation_token.clone())
                }
                None => planner
                    .with_deadline_and_cancellation(ctx.deadline, ctx.cancellation_token.clone()),
            };
        }
        planner = planner.with_algo_registry(self.algo_registry.clone());
        if let Some(ref registry) = self.procedure_registry {
            planner = planner.with_procedure_registry(registry.clone());
        }
        // M5b follow-up #4: thread the host's plugin registry through to
        // the physical planner so `plan_vector_knn` can consult
        // `register_index_handle` for plugin-supplied vector index
        // handles. The procedure registry's attached plugin registry is
        // the host's actual instance (set by `Uni::build`, and what
        // `db.plugin_registry()` returns to user code). The legacy
        // `CustomFunctionRegistry` only ever carried scalar fns (no index
        // handles / optimizer rules), so it is not a source here.
        let host_plugin_registry = self
            .procedure_registry
            .as_ref()
            .and_then(|r| r.plugin_registry());
        if let Some(registry) = host_plugin_registry {
            planner = planner.with_plugin_registry(registry);
        }
        if let Some(ref xervo_runtime) = self.xervo_runtime {
            planner = planner.with_xervo_runtime(xervo_runtime.clone());
        }
        // FU-1 / M11 #6: thread the outer transaction's writer handle
        // through so declared `WRITE`-mode procedures invoked from
        // this query can run their Cypher bodies via the write-enabled
        // inner-query host.
        if let Some(writer) = &self.writer {
            planner = planner.with_writer(Arc::clone(writer));
        }

        // `=~` evaluates under this query's backtrack limit and budget. The
        // UDF instance carries them because DataFusion runs partitions on
        // spawned tasks, which no task-local reaches.
        let budget = planner.graph_ctx().budget();
        let interrupt: uni_query_functions::expr_eval::RegexInterrupt =
            Arc::new(move || budget.check().err().map(|e| e.to_string()));
        session_ctx.read().register_udf(
            uni_query_functions::df_udfs::create_cypher_regex_udf_with_runtime(Arc::new(
                uni_query_functions::expr_eval::RegexRuntime::new(
                    self.config.regex_backtrack_limit,
                    Some(interrupt),
                ),
            )),
        );

        Ok((session_ctx, planner, prop_manager_arc))
    }

    /// The DataFusion runtime environment for one physical plan, whose memory
    /// pool caps execution at `max_query_memory` bytes (unbounded when `0`).
    ///
    /// `GreedyMemoryPool` rather than `FairSpillPool`, but **not** because
    /// nothing can spill. This comment used to say a disk-spill path was not
    /// configured; that is false and was already disproved by #202, whose own
    /// evidence is an `ExternalSorter` asking for 5.1 GB on LDBC IC9 *with a
    /// disk manager available the whole time*. `RuntimeEnvBuilder` defaults to
    /// `DiskManagerMode::OsTmpDirectory`, so DataFusion's own buffering
    /// operators can spill and do. `disk_manager_default_is_a_real_directory`
    /// pins that, since the choice below rests on it.
    ///
    /// The reason to keep the greedy pool is a different one, and it is a
    /// judgement rather than a measurement — no benchmark has compared the two
    /// here. `FairSpillPool` holds back a share to guarantee spilling consumers
    /// room to spill *into*. That protects the operators that reserve, and the
    /// ones that dominate this system's peak memory do not reserve at all
    /// (#242): a graph scan builds one whole `RecordBatch` through
    /// `MutableArrayData` without asking the pool. Reserving headroom would
    /// therefore shrink the budget for the DataFusion operators that do ask,
    /// while protecting nothing from the allocations that actually end the
    /// process.
    ///
    /// That trade inverts once #242 makes first-party operators reserve and
    /// #214/#240 let the scan emit incrementally — a buffering operator can
    /// only spill *between* inputs, and today it is handed one indivisible
    /// batch. Revisit the pool then, not before.
    ///
    /// The bound is real but partial, and deliberately so: a pool only accounts
    /// allocations that *reserve* through it. An operator that builds an Arrow
    /// buffer directly — `MutableArrayData`, the allocation behind #184 — never
    /// asks the pool and is still unbounded. This makes the limit real where
    /// DataFusion cooperates; it is not a fix for that crash.
    ///
    /// The pool is always wrapped in a [`PeakTrackingPool`], including when the
    /// limit is disabled and the inner pool is unbounded: that is what lets
    /// `PROFILE` report a peak for every query rather than only for bounded
    /// ones. The wrapper adds one relaxed atomic add (plus a `fetch_max`) per
    /// reservation change, which operators make per batch, not per row.
    ///
    /// Everything but the pool comes from one process-wide base environment:
    /// the disk manager and DataFusion's file caches are shared, so a query
    /// pays for two `Arc`s and a pool rather than for a fresh disk manager and
    /// cache set.
    ///
    /// [`PeakTrackingPool`]: super::peak_pool::PeakTrackingPool
    pub(super) fn query_runtime(
        &self,
    ) -> Result<Arc<datafusion::execution::runtime_env::RuntimeEnv>> {
        use datafusion::execution::memory_pool::{
            GreedyMemoryPool, MemoryPool, UnboundedMemoryPool,
        };
        use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

        static BASE: std::sync::OnceLock<Arc<RuntimeEnv>> = std::sync::OnceLock::new();
        let base = match BASE.get() {
            Some(base) => base,
            None => {
                let built = RuntimeEnvBuilder::new().build_arc()?;
                BASE.get_or_init(|| built)
            }
        };

        // One pool per statement, not per plan: see `StatementMemory`.
        let pool = self.statement_memory.get_or_init(|| {
            let limit = self.config.max_query_memory;
            let inner: Arc<dyn MemoryPool> = if limit == 0 {
                Arc::new(UnboundedMemoryPool::default())
            } else {
                Arc::new(GreedyMemoryPool::new(limit))
            };
            Arc::new(super::peak_pool::PeakTrackingPool::new(
                inner,
                Arc::clone(&self.counters),
            ))
        });
        Ok(RuntimeEnvBuilder::from_runtime_env(base)
            .with_memory_pool(pool)
            .build_arc()?)
    }

    /// Execute a DataFusion physical plan and collect all result batches.
    ///
    /// Unbounded: no deadline and no cancellation are observed. Prefer
    /// [`Self::collect_batches_checked`] wherever a `GraphExecutionContext` is
    /// at hand — which is every query path inside this crate.
    pub fn collect_batches(
        session_ctx: &Arc<SyncRwLock<SessionContext>>,
        execution_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) -> BoxFuture<'_, Result<Vec<RecordBatch>>> {
        Self::drain_plan(session_ctx, execution_plan, None)
    }

    /// [`Self::collect_batches`], checking the query's deadline and
    /// cancellation token at every batch boundary.
    ///
    /// The unchecked form used `try_collect()`, which consumes the whole stream
    /// in one await. Nothing between the first batch and the last could observe
    /// a timeout, so a long-running query ran to completion and only *then*
    /// discovered it was over budget — the outer `tokio::time::timeout` returns
    /// an error to the caller while the abandoned work keeps burning CPU and
    /// memory on its task. LDBC IC12 returned after 571 s against a 120 s
    /// budget this way.
    ///
    /// The bound this restores is per batch, not per instant: an operator that
    /// never yields between two batches still cannot be preempted, so a single
    /// long-running operator remains unbounded. That is a real remaining gap
    /// and needs interruption inside the operators; what this fixes is the case
    /// where the query *does* yield and nothing was looking.
    pub fn collect_batches_checked(
        session_ctx: &Arc<SyncRwLock<SessionContext>>,
        execution_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        graph_ctx: &Arc<crate::query::df_graph::GraphExecutionContext>,
    ) -> BoxFuture<'static, Result<Vec<RecordBatch>>> {
        let session_ctx = Arc::clone(session_ctx);
        let graph_ctx = Arc::clone(graph_ctx);
        Box::pin(
            async move { Self::drain_plan(&session_ctx, execution_plan, Some(&graph_ctx)).await },
        )
    }

    /// Shared drain loop for both collectors.
    fn drain_plan<'a>(
        session_ctx: &'a Arc<SyncRwLock<SessionContext>>,
        execution_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        graph_ctx: Option<&'a Arc<crate::query::df_graph::GraphExecutionContext>>,
    ) -> BoxFuture<'a, Result<Vec<RecordBatch>>> {
        Box::pin(async move {
            use futures::StreamExt;

            let task_ctx = session_ctx.read().task_ctx();
            if *DUMP_PHYSICAL_PLAN {
                eprintln!(
                    "{}",
                    datafusion::physical_plan::displayable(execution_plan.as_ref()).indent(true)
                );
            }
            let partition_count = execution_plan.output_partitioning().partition_count();
            let mut all_batches = Vec::new();
            for partition in 0..partition_count {
                let mut stream = execution_plan.execute(partition, task_ctx.clone())?;
                loop {
                    // Before awaiting the next batch, so a query already over
                    // budget does not start more work.
                    if let Some(ctx) = graph_ctx {
                        ctx.check_timeout()?;
                    }
                    match stream.next().await {
                        Some(batch) => all_batches.push(batch?),
                        None => break,
                    }
                }
            }
            Ok(all_batches)
        })
    }

    /// Drive a physical plan as a stream, yielding each batch as it is produced.
    ///
    /// The streaming sibling of [`Self::drain_plan`]. The two differ in exactly
    /// one thing: what happens to a batch once it arrives. `drain_plan` pushes
    /// it into a `Vec`; this hands it to the caller.
    ///
    /// That accumulation was the whole of #240. DataFusion's execution is
    /// already incremental and `GraphScanExec` already slices its output to
    /// `batch_size`, so the plan below genuinely yields many batches. Collecting
    /// them is what made the query layer single-shot, and with it the cursor's
    /// per-item memory check, which could only fire once the whole result was
    /// resident in its most expensive form.
    ///
    /// Partitions are drained in order rather than concurrently, matching
    /// `drain_plan`, so row order is identical between the two.
    ///
    /// # Errors
    ///
    /// Yields the failure and then ends. A failed partition ends the whole
    /// stream: a caller that has seen an error must not go on to receive rows
    /// from a later partition as though the query had succeeded.
    fn stream_plan(
        session_ctx: &Arc<SyncRwLock<SessionContext>>,
        execution_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        graph_ctx: Arc<crate::query::df_graph::GraphExecutionContext>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        let task_ctx = session_ctx.read().task_ctx();
        if *DUMP_PHYSICAL_PLAN {
            eprintln!(
                "{}",
                datafusion::physical_plan::displayable(execution_plan.as_ref()).indent(true)
            );
        }
        let partition_count = execution_plan.output_partitioning().partition_count();

        struct DrainState {
            plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            task_ctx: Arc<datafusion::execution::TaskContext>,
            graph_ctx: Arc<crate::query::df_graph::GraphExecutionContext>,
            next_partition: usize,
            partition_count: usize,
            current: Option<datafusion::execution::SendableRecordBatchStream>,
            /// When the last batch was handed out, if one has been.
            ///
            /// The gap between that moment and the next poll is the consumer
            /// thinking, not the query running, so it is credited back to the
            /// deadline rather than spent against it.
            yielded_at: Option<Instant>,
        }

        /// End the stream after the item about to be yielded.
        ///
        /// Spelled once so no error path can leave a later partition reachable.
        fn halt(state: &mut DrainState) {
            state.current = None;
            state.next_partition = state.partition_count;
        }

        let state = DrainState {
            plan: execution_plan,
            task_ctx,
            graph_ctx,
            next_partition: 0,
            partition_count,
            current: None,
            yielded_at: None,
        };

        stream::unfold(state, |mut state| async move {
            // Everything since the previous batch left this loop was the
            // consumer's time. Credit it before any deadline is consulted.
            //
            // This over-credits slightly: row conversion above this operator
            // also happens in that window and is genuinely the query's work.
            // Erring toward the consumer is the right direction — the failure
            // it prevents is a correct query killed for being read slowly,
            // while the failure it admits is a runaway query living a little
            // longer than its budget, which the per-poll ceiling in the cursor
            // still bounds.
            if let Some(handed_out) = state.yielded_at.take() {
                state.graph_ctx.credit_idle_time(handed_out.elapsed());
            }
            loop {
                if state.current.is_none() {
                    if state.next_partition >= state.partition_count {
                        return None;
                    }
                    match state
                        .plan
                        .execute(state.next_partition, state.task_ctx.clone())
                    {
                        Ok(partition_stream) => {
                            state.next_partition += 1;
                            state.current = Some(partition_stream);
                        }
                        Err(e) => {
                            halt(&mut state);
                            return Some((Err(e.into()), state));
                        }
                    }
                }

                // Before awaiting the next batch, so a query already over
                // budget does not start more work. Mirrors `drain_plan`.
                if let Err(e) = state.graph_ctx.check_timeout() {
                    halt(&mut state);
                    return Some((Err(e), state));
                }

                let Some(partition_stream) = state.current.as_mut() else {
                    // Unreachable: the block above either set `current` or
                    // returned. End the stream rather than assume.
                    halt(&mut state);
                    return None;
                };

                match partition_stream.next().await {
                    Some(Ok(batch)) => {
                        state.yielded_at = Some(Instant::now());
                        return Some((Ok(batch), state));
                    }
                    Some(Err(e)) => {
                        halt(&mut state);
                        return Some((Err(e.into()), state));
                    }
                    // Partition exhausted; fall through to the next one.
                    None => state.current = None,
                }
            }
        })
        .boxed()
    }

    /// Executes a query using the DataFusion-based engine.
    ///
    /// Uses `HybridPhysicalPlanner` which produces DataFusion `ExecutionPlan`
    /// trees with custom graph operators for graph-specific operations.
    pub async fn execute_datafusion(
        &self,
        plan: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<RecordBatch>> {
        let (batches, _plan) = self
            .execute_datafusion_with_plan(plan, prop_manager, params)
            .await?;
        Ok(batches)
    }

    /// Build the physical plan for `plan`, ready to execute.
    ///
    /// Shared by the collecting and the streaming execution paths so the two
    /// cannot drift. Everything up to and including physical planning is
    /// identical between them; only what becomes of the batches differs.
    ///
    /// The planner is returned rather than dropped because it owns the
    /// interning scope that handles in the result batches point into. A caller
    /// must keep it alive until those handles have been materialized.
    ///
    /// # Errors
    ///
    /// Returns an error when the session context cannot be built, when the plan
    /// writes but no `Writer` is configured, or when physical planning fails.
    async fn prepare_datafusion_execution(
        &self,
        plan: &LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<(
        Arc<SyncRwLock<SessionContext>>,
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        HybridPhysicalPlanner,
    )> {
        let (session_ctx, mut planner, prop_manager_arc) =
            self.create_datafusion_planner(prop_manager, params).await?;

        // Build MutationContext when the plan contains write operations
        if Self::contains_write_operations(plan) {
            let writer = self
                .writer
                .as_ref()
                .ok_or_else(|| anyhow!("Write operations require a Writer"))?
                .clone();
            let query_ctx = self.get_context().await;

            debug_assert!(
                query_ctx.is_some(),
                "BUG: query_ctx is None for write operation"
            );

            let mutation_ctx = Arc::new(crate::query::df_graph::MutationContext {
                // Shares *this statement's* counters rather than taking the
                // fresh set `Executor::clone` installs. That freshness is
                // deliberate and must stay — the write path clones a cached
                // executor template, and a shared handle would spill one
                // query's counts into the next one's result — but it also meant
                // every scan a mutation performed counted into a set nobody
                // harvests. A batched MERGE whose per-row plans scanned a label
                // 400 times reported the rows its *outer* MATCH examined and
                // nothing else, so the counter read the same for a sixteen-
                // second query as for a sub-second one.
                // The memory pool is shared for the same reason: a per-row plan
                // with a pool of its own would get a second full budget.
                executor: {
                    let mut e = self.clone();
                    e.share_statement_with(self);
                    e
                },
                writer,
                prop_manager: prop_manager_arc,
                params: params.clone(),
                query_ctx,
                tx_l0_override: self.transaction_l0_override.clone(),
            });
            planner = planner.with_mutation_context(mutation_ctx);
            tracing::debug!(
                plan_type = Self::get_plan_type(plan),
                "Mutation routed to DataFusion engine"
            );
        }

        let execution_plan = planner.plan(plan)?;
        Ok((session_ctx, execution_plan, planner))
    }

    /// Execute a query through DataFusion, yielding batches as they are produced.
    ///
    /// The streaming counterpart of [`Self::execute_datafusion`]. Handles are
    /// materialized per batch and warnings are harvested when the stream ends,
    /// which is the same order the collecting path uses — that path simply has
    /// only one moment at which "the end" happens.
    ///
    /// The planner rides in the stream's state because it owns the interning
    /// scope the batches' handles point into. Dropping it when this function
    /// returns would leave the first batch pointing into a dead scope.
    ///
    /// A consumer that drops the stream early forfeits the warnings, which is
    /// the same bargain it makes with the rest of the unread result.
    ///
    /// # Errors
    ///
    /// Returns an error if planning fails. An execution failure is yielded by
    /// the returned stream, which then ends.
    pub(crate) async fn execute_datafusion_stream(
        &self,
        plan: LogicalPlan,
        prop_manager: Arc<PropertyManager>,
        params: HashMap<String, Value>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let (session_ctx, execution_plan, planner) = self
            .prepare_datafusion_execution(&plan, &prop_manager, &params)
            .await?;
        let graph_ctx = Arc::clone(planner.graph_ctx());

        // Materialize per batch, before it is handed out — the collecting path
        // does the same thing to the whole vector at once.
        let materialized = Self::stream_plan(&session_ctx, execution_plan, graph_ctx)
            .map(|batch| {
                let mut one = [batch?];
                crate::query::df_graph::common::materialize_handles_in_batches(&mut one)?;
                let [batch] = one;
                Ok(batch)
            })
            .boxed();

        Ok(stream::unfold(
            (materialized, Some(planner), Arc::clone(&self.warnings)),
            |(mut inner, mut planner, warnings)| async move {
                if let Some(item) = inner.next().await {
                    return Some((item, (inner, planner, warnings)));
                }
                // End of stream: harvest before the planner is dropped.
                if let Some(planner) = planner.take() {
                    let graph_warnings = planner.graph_ctx().take_warnings();
                    if !graph_warnings.is_empty()
                        && let Ok(mut w) = warnings.lock()
                    {
                        w.extend(graph_warnings);
                    }
                }
                None
            },
        )
        .boxed())
    }

    /// Executes a query using the DataFusion-based engine, returning both
    /// result batches and the physical execution plan.
    ///
    /// The returned `Arc<dyn ExecutionPlan>` can be walked to extract per-operator
    /// metrics (e.g., `output_rows`, `elapsed_compute`) that DataFusion's
    /// `BaselineMetrics` recorded during execution.
    pub async fn execute_datafusion_with_plan(
        &self,
        plan: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<(
        Vec<RecordBatch>,
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    )> {
        let (session_ctx, execution_plan, planner) = self
            .prepare_datafusion_execution(&plan, prop_manager, params)
            .await?;
        let plan_clone = Arc::clone(&execution_plan);
        let result =
            Self::collect_batches_checked(&session_ctx, execution_plan, planner.graph_ctx()).await;

        // Harvest warnings from the graph execution context after query completion.
        let graph_warnings = planner.graph_ctx().take_warnings();
        if !graph_warnings.is_empty()
            && let Ok(mut w) = self.warnings.lock()
        {
            w.extend(graph_warnings);
        }

        // Results outlive this function, and the planner owning the interning
        // scope does not — so any handle still in the batches has to become
        // real bytes before they leave.
        let mut batches = result?;
        crate::query::df_graph::common::materialize_handles_in_batches(&mut batches)?;
        Ok((batches, plan_clone))
    }

    /// Execute a MERGE read sub-plan through the DataFusion engine.
    ///
    /// Plans and executes the MERGE pattern match using flat columnar output
    /// (no structural projections), then groups dotted columns (`a._vid`,
    /// `a._labels`, etc.) into per-variable Maps for downstream MERGE logic.
    pub(crate) async fn execute_merge_read_plan(
        &self,
        plan: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        merge_variables: Vec<String>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let (session_ctx, planner, _prop_manager_arc) =
            self.create_datafusion_planner(prop_manager, params).await?;

        // Plan with full property access ("*") for all merge variables so that
        // ON MATCH SET / ON CREATE SET have complete entity Maps to work with.
        let extra: HashMap<String, HashSet<String>> = merge_variables
            .iter()
            .map(|v| (v.clone(), ["*".to_string()].into_iter().collect()))
            .collect();
        let execution_plan = planner.plan_with_properties(&plan, extra)?;
        let all_batches =
            Self::collect_batches_checked(&session_ctx, execution_plan, planner.graph_ctx())
                .await?;

        // Convert to flat rows (dotted column names like "a._vid", "b._labels")
        let flat_rows = self.record_batches_to_rows(all_batches)?;

        // Group dotted columns into per-variable Maps for MERGE's match logic.
        // E.g., {"a._vid": 0, "a._labels": ["A"]} → {"a": Map({"_vid": 0, "_labels": ["A"]})}
        let rows = flat_rows
            .into_iter()
            .map(|mut row| {
                for var in &merge_variables {
                    // Skip if already materialized (e.g., by record_batches_to_rows)
                    if row.contains_key(var) {
                        continue;
                    }
                    let prefix = format!("{}.", var);
                    let dotted_keys: Vec<String> = row
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .cloned()
                        .collect();
                    if !dotted_keys.is_empty() {
                        let mut map = HashMap::new();
                        for key in dotted_keys {
                            let prop_name = key[prefix.len()..].to_string();
                            if let Some(val) = row.remove(&key) {
                                map.insert(prop_name, val);
                            }
                        }
                        row.insert(var.clone(), Value::Map(map));
                    }
                }
                row
            })
            .collect();

        Ok(rows)
    }

    /// Converts DataFusion RecordBatches to row-based HashMap format.
    ///
    /// Handles special metadata on fields:
    /// - `cv_encoded=true`: Parse the string value as JSON to restore original type
    ///
    /// Also normalizes path structures to user-facing format (converts _vid to _id).
    pub(crate) fn record_batches_to_rows(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut rows = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();
            let schema = batch.schema();

            for row_idx in 0..num_rows {
                let mut row = HashMap::new();

                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    // Recover the Uni DataType so LargeBinary/struct columns decode
                    // correctly. Raw `Bytes` columns are tagged with `uni_raw_bytes` at
                    // scan time (scan.rs `property_field`) because `Bytes`, `CypherValue`,
                    // and `Duration` all map to Arrow `LargeBinary` and are otherwise
                    // indistinguishable here; without this hint a raw `Bytes` value would
                    // be run through the CypherValue codec and corrupted (issue #93).
                    let data_type = if field
                        .metadata()
                        .get("uni_raw_bytes")
                        .is_some_and(|v| v == "true")
                    {
                        Some(&uni_common::DataType::Bytes)
                    } else if uni_common::core::schema::is_datetime_struct(field.data_type()) {
                        Some(&uni_common::DataType::DateTime)
                    } else if uni_common::core::schema::is_time_struct(field.data_type()) {
                        Some(&uni_common::DataType::Time)
                    } else {
                        None
                    };
                    let mut value =
                        arrow_convert::arrow_to_value(column.as_ref(), row_idx, data_type)?
                            .canonical_entity();

                    // Check if this field contains JSON-encoded values (e.g., from UNWIND)
                    // Parse JSON string to restore the original type
                    if field
                        .metadata()
                        .get("cv_encoded")
                        .is_some_and(|v| v == "true")
                        && let Value::String(s) = &value
                        && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s)
                    {
                        value = Value::from(parsed);
                    }

                    // Normalize path structures to user-facing format
                    value = Self::normalize_path_if_needed(value);

                    row.insert(field.name().clone(), value);
                }

                // Merge system fields into bare variable maps.
                // The projection step emits helper columns like "n._vid" and "n._labels"
                // alongside the materialized "n" column (a Map of user properties).
                // Here we merge those system fields into the map and remove the helpers.
                //
                // For search procedures (vector/FTS), the bare variable may be a VID
                // string placeholder rather than a Map. In that case, promote it to a
                // Map so we can merge system fields and properties into it.
                let bare_vars: Vec<String> = row
                    .keys()
                    .filter(|k| !k.contains('.') && matches!(row.get(*k), Some(Value::Map(_))))
                    .cloned()
                    .collect();

                // Detect VID-placeholder variables that have system columns
                // (e.g., "node" is String("1") but "node._vid" exists).
                // Promote these to Maps so system fields can be merged in.
                let vid_placeholder_vars: Vec<String> = row
                    .keys()
                    .filter(|k| {
                        !k.contains('.')
                            && matches!(row.get(*k), Some(Value::String(_)))
                            && row.contains_key(&format!("{}._vid", k))
                    })
                    .cloned()
                    .collect();

                for var in &vid_placeholder_vars {
                    promote_vid_placeholder(&mut row, var);
                }

                for var in &bare_vars {
                    merge_dotted_columns(&mut row, var);
                }

                rows.push(row);
            }
        }

        Ok(rows)
    }

    /// Normalize a value if it's a path structure, converting internal format to user-facing format.
    ///
    /// This only normalizes path structures (objects with "nodes" and "relationships" arrays).
    /// Other values are returned unchanged to avoid interfering with query execution.
    fn normalize_path_if_needed(value: Value) -> Value {
        match value {
            Value::Map(map)
                if map.contains_key("nodes")
                    && (map.contains_key("relationships") || map.contains_key("edges")) =>
            {
                Self::normalize_path_map(map)
            }
            other => other,
        }
    }

    /// Normalize a path map object.
    fn normalize_path_map(mut map: HashMap<String, Value>) -> Value {
        // Normalize nodes array
        if let Some(Value::List(nodes)) = map.remove("nodes") {
            let normalized_nodes: Vec<Value> = nodes
                .into_iter()
                .map(|n| {
                    if let Value::Map(node_map) = n {
                        Self::normalize_path_node_map(node_map)
                    } else {
                        n
                    }
                })
                .collect();
            map.insert("nodes".to_string(), Value::List(normalized_nodes));
        }

        // Normalize relationships array (may be called "relationships" or "edges")
        let rels_key = if map.contains_key("relationships") {
            "relationships"
        } else {
            "edges"
        };
        if let Some(Value::List(rels)) = map.remove(rels_key) {
            let normalized_rels: Vec<Value> = rels
                .into_iter()
                .map(|r| {
                    if let Value::Map(rel_map) = r {
                        Self::normalize_path_edge_map(rel_map)
                    } else {
                        r
                    }
                })
                .collect();
            map.insert("relationships".to_string(), Value::List(normalized_rels));
        }

        Value::Map(map)
    }

    /// Convert a Value to its string representation for path normalization.
    fn value_to_id_string(val: Value) -> String {
        match val {
            Value::Int(n) => n.to_string(),
            Value::Float(n) => n.to_string(),
            Value::String(s) => s,
            other => other.to_string(),
        }
    }

    /// Move a map entry from `src_key` to `dst_key`, converting the value to a string.
    /// When `src_key == dst_key`, this simply stringifies the value in place.
    fn stringify_map_field(map: &mut HashMap<String, Value>, src_key: &str, dst_key: &str) {
        if let Some(val) = map.remove(src_key) {
            map.insert(
                dst_key.to_string(),
                Value::String(Self::value_to_id_string(val)),
            );
        }
    }

    /// Ensure the "properties" field is a non-null map.
    fn ensure_properties_map(map: &mut HashMap<String, Value>) {
        match map.get("properties") {
            Some(props) if !props.is_null() => {}
            _ => {
                map.insert("properties".to_string(), Value::Map(HashMap::new()));
            }
        }
    }

    /// Normalize a node within a path to user-facing format.
    fn normalize_path_node_map(mut map: HashMap<String, Value>) -> Value {
        Self::stringify_map_field(&mut map, "_vid", "_id");
        Self::ensure_properties_map(&mut map);
        Value::Map(map)
    }

    /// Normalize an edge within a path to user-facing format.
    fn normalize_path_edge_map(mut map: HashMap<String, Value>) -> Value {
        Self::stringify_map_field(&mut map, "_eid", "_id");
        Self::stringify_map_field(&mut map, "_src", "_src");
        Self::stringify_map_field(&mut map, "_dst", "_dst");

        if let Some(type_name) = map.remove("_type_name") {
            map.insert("_type".to_string(), type_name);
        }

        Self::ensure_properties_map(&mut map);
        Value::Map(map)
    }

    #[instrument(
        skip(self, prop_manager, params),
        fields(rows_returned, duration_ms),
        level = "info"
    )]
    pub fn execute<'a>(
        &'a self,
        plan: LogicalPlan,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
    ) -> BoxFuture<'a, Result<Vec<HashMap<String, Value>>>> {
        Box::pin(async move {
            let query_type = Self::get_plan_type(&plan);
            let ctx = self.get_context().await;
            let start = Instant::now();

            // Route DDL/Admin queries to the fallback executor.
            // All other queries (including similar_to) flow through DataFusion.
            let res = if self.is_ddl_or_admin(&plan) {
                self.execute_subplan(plan, prop_manager, params, ctx.as_ref())
                    .await
            } else {
                let batches = self.execute_datafusion(plan, prop_manager, params).await?;
                self.record_batches_to_rows(batches)
            };

            let duration = start.elapsed();
            metrics::histogram!("uni_query_duration_seconds", "query_type" => query_type)
                .record(duration.as_secs_f64());

            tracing::Span::current().record("duration_ms", duration.as_millis());
            match &res {
                Ok(rows) => {
                    tracing::Span::current().record("rows_returned", rows.len());
                    metrics::counter!("uni_query_rows_returned_total", "query_type" => query_type)
                        .increment(rows.len() as u64);
                }
                Err(e) => {
                    let error_type = if e.to_string().contains("timed out") {
                        "timeout"
                    } else if e.to_string().contains("syntax") {
                        "syntax"
                    } else {
                        "execution"
                    };
                    metrics::counter!("uni_query_errors_total", "query_type" => query_type, "error_type" => error_type).increment(1);
                }
            }

            res
        })
    }

    fn get_plan_type(plan: &LogicalPlan) -> &'static str {
        match plan {
            LogicalPlan::Scan { .. } => "read_scan",
            LogicalPlan::FusedIndexScan { .. } => "read_fused_index_scan",
            LogicalPlan::FusedIndexScanWrapped { .. } => "read_fused_index_scan_wrapped",
            LogicalPlan::MultiplicityInsensitive { .. } => "read_multiplicity_insensitive",
            LogicalPlan::ExtIdLookup { .. } => "read_extid_lookup",
            LogicalPlan::Traverse { .. } => "read_traverse",
            LogicalPlan::TraverseMainByType { .. } => "read_traverse_main",
            LogicalPlan::ScanAll { .. } => "read_scan_all",
            LogicalPlan::ScanMainByLabels { .. } => "read_scan_main",
            LogicalPlan::VectorKnn { .. } => "read_vector",
            LogicalPlan::Create { .. } | LogicalPlan::CreateBatch { .. } => "write_create",
            LogicalPlan::Merge { .. } => "write_merge",
            LogicalPlan::Delete { .. } => "write_delete",
            LogicalPlan::Set { .. } => "write_set",
            LogicalPlan::Remove { .. } => "write_remove",
            LogicalPlan::ProcedureCall { .. } => "call",
            LogicalPlan::Backup { .. } => "backup",
            _ => "other",
        }
    }

    /// Return all direct child plan references from a `LogicalPlan`.
    ///
    /// This centralizes the variant→children mapping so that recursive walkers
    /// (e.g., `contains_write_operations`) can delegate the
    /// "recurse into children" logic instead of duplicating the match arms.
    ///
    /// Note: `Foreach` returns only its `input`; the `body: Vec<LogicalPlan>`
    /// is not included because it requires special iteration. Callers that
    /// need to inspect the body should handle `Foreach` before falling through.
    ///
    /// The match is intentionally exhaustive — do NOT add a `_ => vec![]` arm;
    /// a new `LogicalPlan` variant should fail to compile here until it is
    /// classified as child-bearing or leaf. This walker previously ended in
    /// such a wildcard, which silently made `FusedIndexScanWrapped` and the
    /// five `Locy*` operators look like leaves. That matters because
    /// `plan_children` drives `is_ddl_or_admin` (which routes a plan to the
    /// DataFusion engine or the row-fallback executor) and
    /// `contains_write_operations` (which decides whether a `MutationContext`
    /// is built) — the same failure mode as issue #131, where a variant missing
    /// from a plan walker silently degraded an equi-join to a quadratic
    /// `CrossJoinExec`. The five sibling walkers in `df_planner.rs` carry this
    /// guarantee already; this one was the exception.
    fn plan_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
        match plan {
            // Single-input wrappers
            LogicalPlan::Project { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::MultiplicityInsensitive { input }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Unwind { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Create { input, .. }
            | LogicalPlan::CreateBatch { input, .. }
            | LogicalPlan::Set { input, .. }
            | LogicalPlan::Remove { input, .. }
            | LogicalPlan::Delete { input, .. }
            | LogicalPlan::Merge { input, .. }
            | LogicalPlan::Foreach { input, .. }
            | LogicalPlan::Traverse { input, .. }
            | LogicalPlan::TraverseMainByType { input, .. }
            | LogicalPlan::BindZeroLengthPath { input, .. }
            | LogicalPlan::BindPath { input, .. }
            | LogicalPlan::ShortestPath { input, .. }
            | LogicalPlan::AllShortestPaths { input, .. }
            | LogicalPlan::Explain { plan: input, .. } => vec![input.as_ref()],

            // Two-input wrappers
            LogicalPlan::Apply {
                input, subquery, ..
            }
            | LogicalPlan::SubqueryCall { input, subquery } => {
                vec![input.as_ref(), subquery.as_ref()]
            }
            LogicalPlan::Union { left, right, .. } | LogicalPlan::CrossJoin { left, right } => {
                vec![left.as_ref(), right.as_ref()]
            }
            LogicalPlan::RecursiveCTE {
                initial, recursive, ..
            } => vec![initial.as_ref(), recursive.as_ref()],
            LogicalPlan::QuantifiedPattern {
                input,
                pattern_plan,
                ..
            } => vec![input.as_ref(), pattern_plan.as_ref()],

            // Observability wrapper around a fusion-eligible operator
            // (`VectorKnn`, `InvertedIndexLookup`, or a `ProcedureCall` to
            // `uni.search` / `uni.vector.query` / `uni.fts.query` /
            // `uni.sparse.query`). Runtime behaviour is identical to the
            // unwrapped node, so `inner` is a genuine child: treating it as a
            // leaf hid the wrapped operator from every consumer of this walker.
            LogicalPlan::FusedIndexScanWrapped { inner, .. } => vec![inner.as_ref()],

            // Locy post-fixpoint operators. Reachable only as a
            // `LocyClausePlan.body` beneath `LocyProgram`, never from a root
            // plan — so these arms are inert today and exist to keep the match
            // honest rather than to change any current answer.
            LogicalPlan::LocyFold { input, .. }
            | LogicalPlan::LocyBestBy { input, .. }
            | LogicalPlan::LocyPriority { input, .. }
            | LogicalPlan::LocyProject { input, .. }
            | LogicalPlan::LocyModelInvoke { input, .. }
            | LogicalPlan::LocyGenerate { input, .. } => vec![input.as_ref()],

            // ── Leaves ───────────────────────────────────────────────────
            //
            // `LocyProgram` belongs here deliberately. Its clause bodies hang
            // off `strata → rules → clauses → body`, not a direct field, so
            // yielding them would *change* what callers see rather than harden
            // it: `contains_write_operations` and `is_ddl_or_admin` would begin
            // descending into Locy rule bodies, and `is_ddl_or_admin` is the
            // sole gate routing a plan to the row-fallback executor, whose Locy
            // arms are `unreachable!()`. Do not "complete" this arm.
            LogicalPlan::Scan { .. }
            | LogicalPlan::ScanAll { .. }
            | LogicalPlan::ScanMainByLabels { .. }
            | LogicalPlan::ExtIdLookup { .. }
            | LogicalPlan::FusedIndexScan { .. }
            | LogicalPlan::VectorKnn { .. }
            | LogicalPlan::InvertedIndexLookup { .. }
            | LogicalPlan::ProcedureCall { .. }
            | LogicalPlan::LocyProgram { .. }
            | LogicalPlan::LocyDerivedScan { .. }
            | LogicalPlan::Empty
            | LogicalPlan::OuterRow { .. }
            | LogicalPlan::CreateVectorIndex { .. }
            | LogicalPlan::CreateSparseIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateScalarIndex { .. }
            | LogicalPlan::CreateJsonFtsIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::ShowIndexes { .. }
            | LogicalPlan::Backup { .. }
            | LogicalPlan::CopyTo { .. }
            | LogicalPlan::CopyFrom { .. }
            | LogicalPlan::ShowDatabase
            | LogicalPlan::ShowConfig
            | LogicalPlan::ShowStatistics
            | LogicalPlan::Vacuum
            | LogicalPlan::Checkpoint
            | LogicalPlan::CreateLabel(_)
            | LogicalPlan::CreateEdgeType(_)
            | LogicalPlan::AlterLabel(_)
            | LogicalPlan::AlterEdgeType(_)
            | LogicalPlan::DropLabel(_)
            | LogicalPlan::DropEdgeType(_)
            | LogicalPlan::CreateConstraint(_)
            | LogicalPlan::DropConstraint(_)
            | LogicalPlan::ShowConstraints(_) => vec![],
        }
    }

    /// Check if a plan is a DDL or admin operation that should skip DataFusion.
    ///
    /// These operations don't produce data streams and aren't supported by the
    /// DataFusion planner. Recurses through wrapper nodes (`Project`, `Sort`,
    /// `Limit`, etc.) to detect DDL/admin operations nested inside read
    /// wrappers (e.g. `CALL procedure(...) YIELD x RETURN x`).
    pub(crate) fn is_ddl_or_admin(&self, plan: &LogicalPlan) -> bool {
        match plan {
            // DDL / schema operations
            LogicalPlan::CreateLabel(_)
            | LogicalPlan::CreateEdgeType(_)
            | LogicalPlan::AlterLabel(_)
            | LogicalPlan::AlterEdgeType(_)
            | LogicalPlan::DropLabel(_)
            | LogicalPlan::DropEdgeType(_)
            | LogicalPlan::CreateConstraint(_)
            | LogicalPlan::DropConstraint(_)
            | LogicalPlan::ShowConstraints(_) => true,

            // Index operations
            LogicalPlan::CreateVectorIndex { .. }
            | LogicalPlan::CreateSparseIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateScalarIndex { .. }
            | LogicalPlan::CreateJsonFtsIndex { .. }
            | LogicalPlan::DropIndex { .. }
            | LogicalPlan::ShowIndexes { .. } => true,

            // Admin / utility operations
            LogicalPlan::ShowDatabase
            | LogicalPlan::ShowConfig
            | LogicalPlan::ShowStatistics
            | LogicalPlan::Vacuum
            | LogicalPlan::Checkpoint
            | LogicalPlan::CopyTo { .. }
            | LogicalPlan::CopyFrom { .. }
            | LogicalPlan::Backup { .. }
            | LogicalPlan::Explain { .. } => true,

            // Procedure calls: DF-eligible procedures go through DataFusion,
            // everything else (DDL, admin, unknown) stays on fallback.
            LogicalPlan::ProcedureCall { procedure_name, .. } => {
                !self.is_df_eligible_procedure(procedure_name)
            }

            // A procedure called with an input row (`MATCH … CALL p(…) YIELD …`).
            // The fallback executor reads the rows around it without entity
            // semantics — `RETURN a` gave the VID, `labels(a)` and an edge's
            // properties NULL — and ran the call once over an empty input, so
            // any procedure the DataFusion operator can dispatch runs there.
            // Only the procedures the fallback alone implements stay here.
            LogicalPlan::Apply {
                input, subquery, ..
            } => {
                self.is_ddl_or_admin(input)
                    || match subquery.as_ref() {
                        LogicalPlan::ProcedureCall { procedure_name, .. } => {
                            !self.is_df_eligible_procedure(procedure_name)
                                && !self.is_df_dispatchable_procedure(procedure_name)
                        }
                        other => self.is_ddl_or_admin(other),
                    }
            }

            // Recurse through children using plan_children
            _ => Self::plan_children(plan)
                .iter()
                .any(|child| self.is_ddl_or_admin(child)),
        }
    }

    /// Returns `true` if the procedure is a read-only, data-producing procedure
    /// that can be executed through the DataFusion engine.
    ///
    /// Eligibility qualifies a `CALL` for the DataFusion plan path from three
    /// sources (plugin-compute proposal §6, DF-3):
    ///
    /// 1. A fixed set of **built-in DF-native procedures** (`uni.schema.*`,
    ///    `uni.vector.query`, `uni.fts.query`, `uni.sparse.query`, `uni.search`,
    ///    `uni.create.v*`).
    /// 2. **First-party graph-algorithm procedures** under the reserved
    ///    `uni.algo.*` namespace. These are DF-native algorithm adapters
    ///    registered by the built-in `AlgorithmRegistry` as *procedures* (149
    ///    of them share one `ProcedureSignature` code path). Third parties
    ///    cannot register under the reserved `uni` plugin namespace, so this
    ///    prefix is a first-party shortcut, not a squatting vector — the
    ///    DF-3 concern (a third party reaching the DF path by *name*) is closed
    ///    by source (3), which is the actual registration-driven flip.
    /// 3. **The DF-3 registration-driven flip:** any registered *algorithm
    ///    provider* (typically third-party `myco.algo.*`) that declares
    ///    `df_composable` in its
    ///    [`AlgorithmSignature`](uni_plugin::traits::algorithm::AlgorithmSignature).
    ///    Previously a third-party algorithm provider could reach the DF path
    ///    only by squatting the `uni.algo.` prefix; now it declares composability
    ///    and is a first-class plan node, while a non-declaring one stays on the
    ///    row path.
    ///
    /// Unknown procedures default to the row-based fallback executor (safe for TCK
    /// test procedures, future DDL, and admin) — the fallback stays a correctness
    /// twin (DF-2), so a wrong answer here only costs the vectorized path.
    fn is_df_eligible_procedure(&self, name: &str) -> bool {
        // (1) Built-in DF-native procedures (not registrable algorithm providers).
        if matches!(
            name,
            "uni.schema.labels"
                | "uni.schema.edgeTypes"
                | "uni.schema.relationshipTypes"
                | "uni.schema.indexes"
                | "uni.schema.constraints"
                | "uni.schema.labelInfo"
                | "uni.vector.query"
                | "uni.fts.query"
                | "uni.sparse.query"
                | "uni.search"
                // M5g — `uni.create.vNode` produces a typed Node yield
                // via the planner-level node-shape expansion in
                // `GraphProcedureCallExec`; only the DataFusion path
                // honours that, so route it there explicitly. (vEdge
                // emits a self-contained Struct column and works on
                // either path — list both for symmetry.)
                | "uni.create.vNode"
                | "uni.create.vEdge"
        ) {
            return true;
        }
        // (2) First-party graph-algorithm procedures (reserved namespace).
        if name.starts_with("uni.algo.") {
            return true;
        }
        // (3) Registration-driven (DF-3): a provider that declares df_composable.
        self.procedure_registry
            .as_ref()
            .and_then(|reg| reg.resolve_user_algorithm(name))
            .is_some_and(|entry| entry.provider.signature().df_composable)
    }

    /// Whether the DataFusion procedure operator can dispatch `name`.
    ///
    /// `GraphProcedureCallExec` resolves a procedure through the same three
    /// registries the fallback consults after its built-in admin and DDL
    /// procedures: plugin procedures, algorithm providers, and the TCK mock
    /// registry. A name none of them holds is one of those built-ins, or
    /// unknown.
    fn is_df_dispatchable_procedure(&self, name: &str) -> bool {
        self.procedure_registry.as_ref().is_some_and(|reg| {
            reg.resolve_user_procedure(name).is_some()
                || reg.resolve_user_algorithm(name).is_some()
                || reg.get(name).is_some()
        })
    }

    /// Check if a plan contains write/mutation operations anywhere in the tree.
    ///
    /// Write operations (`CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`, `FOREACH`)
    /// are used to determine when a MutationContext needs to be built for DataFusion.
    /// This recurses through read-only wrapper nodes to detect writes nested inside
    /// projections (e.g. `CREATE (n:Person) RETURN n` produces `Project { Create { ... } }`).
    fn contains_write_operations(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Create { .. }
            | LogicalPlan::CreateBatch { .. }
            | LogicalPlan::Merge { .. }
            | LogicalPlan::Delete { .. }
            | LogicalPlan::Set { .. }
            | LogicalPlan::Remove { .. }
            | LogicalPlan::Foreach { .. } => true,
            _ => Self::plan_children(plan)
                .iter()
                .any(|child| Self::contains_write_operations(child)),
        }
    }

    /// Executes a query as a stream of result batches.
    ///
    /// Routes DDL/Admin through the fallback executor and everything else
    /// through DataFusion.
    ///
    /// Genuinely incremental for DataFusion plans: one item per physical batch,
    /// so a per-item guard above it — the cursor's memory ceiling in
    /// `uni::api::impl_query` — watches the result grow rather than meeting it
    /// whole. This was `stream::once` over a fully collected `Vec`, which made
    /// every such guard a post-hoc report of an allocation already paid for
    /// (#240).
    ///
    /// DDL and admin plans stay single-item. They materialize by nature and
    /// there is nothing to stream.
    pub fn execute_stream(
        self,
        plan: LogicalPlan,
        prop_manager: Arc<PropertyManager>,
        params: HashMap<String, Value>,
    ) -> BoxStream<'static, Result<Vec<HashMap<String, Value>>>> {
        let this = self;
        let this_for_ctx = this.clone();

        let ctx_stream = stream::once(async move { this_for_ctx.get_context().await });

        ctx_stream
            .flat_map(move |ctx| {
                let plan = plan.clone();
                let this = this.clone();
                let prop_manager = prop_manager.clone();
                let params = params.clone();

                if this.is_ddl_or_admin(&plan) {
                    let fut = async move {
                        this.execute_subplan(plan, &prop_manager, &params, ctx.as_ref())
                            .await
                    };
                    return stream::once(fut).boxed();
                }

                let rows_from = this.clone();
                stream::once(async move {
                    this.execute_datafusion_stream(plan, prop_manager, params)
                        .await
                })
                .flat_map(|opened| match opened {
                    Ok(batches) => batches,
                    Err(e) => stream::once(async move { Err(e) }).boxed(),
                })
                .map(move |batch| rows_from.record_batches_to_rows(vec![batch?]))
                .boxed()
            })
            .boxed()
    }

    pub(crate) fn evaluate_expr<'a>(
        &'a self,
        expr: &'a Expr,
        row: &'a HashMap<String, Value>,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> BoxFuture<'a, Result<Value>> {
        let this = self;
        Box::pin(async move {
            // First check if the expression itself is already pre-computed in the row
            let repr = expr.to_string_repr();
            if let Some(val) = row.get(&repr) {
                return Ok(val.clone());
            }

            match expr {
                Expr::PatternComprehension { .. } => {
                    // Handled by DataFusion path via PatternComprehensionExecExpr
                    Err(anyhow::anyhow!(
                        "Pattern comprehensions are handled by DataFusion executor"
                    ))
                }
                Expr::CollectSubquery(query) => {
                    // The twin of `CountSubquery` below: plan the body with the
                    // current row's variables in scope so a correlated
                    // reference resolves, run it, and gather the single column
                    // it returns.
                    let planner = QueryPlanner::new(this.storage.schema_manager().schema());
                    let vars_in_scope: Vec<String> = row.keys().cloned().collect();
                    let plan = planner
                        .plan_with_scope(*query.clone(), vars_in_scope)
                        .map_err(|e| anyhow!("COLLECT subquery planning failed: {e}"))?;

                    let mut sub_params = params.clone();
                    sub_params.extend(row.clone());
                    let results = this
                        .execute(plan, prop_manager, &sub_params)
                        .await
                        .map_err(|e| anyhow!("COLLECT subquery execution failed: {e}"))?;

                    let mut collected = Vec::with_capacity(results.len());
                    for mut result_row in results {
                        // openCypher requires the body to return exactly one
                        // column; anything else is a query error rather than an
                        // arbitrary pick.
                        if result_row.len() != 1 {
                            return Err(anyhow!(
                                "COLLECT {{ … }} must return exactly one column, got {}",
                                result_row.len()
                            ));
                        }
                        let (_, value) = result_row.drain().next().expect("one column");
                        collected.push(value);
                    }
                    Ok(Value::List(collected))
                }
                Expr::Variable(name) => {
                    if let Some(val) = row.get(name) {
                        Ok(val.clone())
                    } else if let Some(vid_val) = row.get(&format!("{}._vid", name)) {
                        // Fallback: scan results may have system columns like "d._vid"
                        // without a materialized "d" Map. Return the VID so Property
                        // evaluation can fetch properties from storage.
                        Ok(vid_val.clone())
                    } else {
                        Ok(params.get(name).cloned().unwrap_or(Value::Null))
                    }
                }
                // A parameter the caller did not supply is an error, never a
                // NULL: a write would store or overwrite with that NULL.
                Expr::Parameter(name) => params.get(name).cloned().ok_or_else(|| {
                    anyhow!(uni_common::UniError::parameter_missing_message([name]))
                }),
                Expr::Property(var_expr, prop_name) => {
                    // Fast path: if the base is a Variable, try flat-key lookup first.
                    // DataFusion scan results use flat keys like "d.embedding" rather than
                    // nested maps, so "d.embedding" won't be found via Variable("d") -> Property.
                    if let Expr::Variable(var_name) = var_expr.as_ref() {
                        let flat_key = format!("{}.{}", var_name, prop_name);
                        if let Some(val) = row.get(flat_key.as_str()) {
                            return Ok(val.clone());
                        }
                    }

                    let base_val = this
                        .evaluate_expr(var_expr, row, prop_manager, params, ctx)
                        .await?;

                    // Handle system properties _vid and _id directly
                    if (prop_name == "_vid" || prop_name == "_id")
                        && let Ok(vid) = Self::vid_from_value(&base_val)
                    {
                        return Ok(Value::Int(vid.as_u64() as i64));
                    }

                    // Handle Value::Node - access properties directly or via prop manager
                    if let Value::Node(node) = &base_val {
                        // Handle system properties
                        if prop_name == "_vid" || prop_name == "_id" {
                            return Ok(Value::Int(node.vid.as_u64() as i64));
                        }
                        if prop_name == "_labels" {
                            return Ok(Value::List(
                                node.labels
                                    .iter()
                                    .map(|l| Value::String(l.clone()))
                                    .collect(),
                            ));
                        }
                        // Check in-memory properties first
                        if let Some(val) = node.properties.get(prop_name.as_str()) {
                            return Ok(val.clone());
                        }
                        // Fallback to storage lookup
                        if let Ok(val) = prop_manager
                            .get_vertex_prop_with_ctx(node.vid, prop_name, ctx)
                            .await
                        {
                            return Ok(val);
                        }
                        return Ok(Value::Null);
                    }

                    // Handle Value::Edge - access properties directly or via prop manager
                    if let Value::Edge(edge) = &base_val {
                        // Handle system properties
                        if prop_name == "_eid" || prop_name == "_id" {
                            return Ok(Value::Int(edge.eid.as_u64() as i64));
                        }
                        if prop_name == "_type" {
                            return Ok(Value::String(edge.edge_type.clone()));
                        }
                        if prop_name == "_src" {
                            return Ok(Value::Int(edge.src.as_u64() as i64));
                        }
                        if prop_name == "_dst" {
                            return Ok(Value::Int(edge.dst.as_u64() as i64));
                        }
                        // Check in-memory properties first
                        if let Some(val) = edge.properties.get(prop_name.as_str()) {
                            return Ok(val.clone());
                        }
                        // Fallback to storage lookup
                        if let Ok(val) = prop_manager.get_edge_prop(edge.eid, prop_name, ctx).await
                        {
                            return Ok(val);
                        }
                        return Ok(Value::Null);
                    }

                    // If base_val is an object (node/edge), check its properties first
                    // This handles properties from CREATE/MERGE that may not be persisted yet
                    if let Value::Map(map) = &base_val {
                        // First check top-level (for system properties like _id, _label, etc.)
                        if let Some(val) = map.get(prop_name.as_str()) {
                            return Ok(val.clone());
                        }
                        // Then the entity's property container (`_all_props`, or a
                        // serialised entity's `properties`)
                        if let Some(props) = uni_common::value::entity_property_container(map)
                            && let Some(val) = props.get(prop_name.as_str())
                        {
                            return Ok(val.clone());
                        }
                        // Fallback to a storage lookup, whichever encoding carries
                        // the identity. `_id` used to be read only as a *string*,
                        // so a map spelling its id `_id` as an integer resolved to
                        // neither vertex nor edge and the whole property access
                        // returned NULL — a wrong answer with no error (#234).
                        match uni_common::value::entity_ref_from_map(map) {
                            Some(uni_common::value::EntityRef::Vertex(vid)) => {
                                if let Ok(val) = prop_manager
                                    .get_vertex_prop_with_ctx(vid, prop_name, ctx)
                                    .await
                                {
                                    return Ok(val);
                                }
                            }
                            Some(uni_common::value::EntityRef::Edge(eid)) => {
                                if let Ok(val) =
                                    prop_manager.get_edge_prop(eid, prop_name, ctx).await
                                {
                                    return Ok(val);
                                }
                            }
                            None => {}
                        }
                        return Ok(Value::Null);
                    }

                    // If base_val is just a VID, fetch from property manager
                    if let Ok(vid) = Self::vid_from_value(&base_val) {
                        return prop_manager
                            .get_vertex_prop_with_ctx(vid, prop_name, ctx)
                            .await;
                    }

                    if base_val.is_null() {
                        return Ok(Value::Null);
                    }

                    // Check if base_val is a temporal value and prop_name is a temporal accessor
                    {
                        use crate::query::datetime::{
                            eval_duration_accessor, eval_temporal_accessor, is_duration_accessor,
                            is_duration_string, is_temporal_accessor, is_temporal_string,
                        };

                        // Handle Value::Temporal directly (no string parsing needed)
                        if let Value::Temporal(tv) = &base_val {
                            if matches!(tv, uni_common::TemporalValue::Duration { .. }) {
                                if is_duration_accessor(prop_name) {
                                    // Convert to string for the existing accessor logic
                                    return eval_duration_accessor(
                                        &base_val.to_string(),
                                        prop_name,
                                    );
                                }
                            } else if is_temporal_accessor(prop_name) {
                                return eval_temporal_accessor(&base_val.to_string(), prop_name);
                            }
                        }

                        // Handle Value::String temporal (backward compat)
                        if let Value::String(s) = &base_val {
                            if is_temporal_string(s) && is_temporal_accessor(prop_name) {
                                return eval_temporal_accessor(s, prop_name);
                            }
                            if is_duration_string(s) && is_duration_accessor(prop_name) {
                                return eval_duration_accessor(s, prop_name);
                            }
                        }
                    }

                    Err(anyhow!(
                        "Cannot access property '{}' on {:?}",
                        prop_name,
                        base_val
                    ))
                }
                Expr::ArrayIndex {
                    array: arr_expr,
                    index: idx_expr,
                } => {
                    let arr_val = this
                        .evaluate_expr(arr_expr, row, prop_manager, params, ctx)
                        .await?;
                    let idx_val = this
                        .evaluate_expr(idx_expr, row, prop_manager, params, ctx)
                        .await?;

                    if let Value::List(arr) = &arr_val {
                        // Handle signed indices (allow negative)
                        if let Some(i) = idx_val.as_i64() {
                            let idx = if i < 0 {
                                // Negative index: -1 = last element, -2 = second to last, etc.
                                let positive_idx = arr.len() as i64 + i;
                                if positive_idx < 0 {
                                    return Ok(Value::Null); // Out of bounds
                                }
                                positive_idx as usize
                            } else {
                                i as usize
                            };
                            if idx < arr.len() {
                                return Ok(arr[idx].clone());
                            }
                            return Ok(Value::Null);
                        } else if idx_val.is_null() {
                            return Ok(Value::Null);
                        } else {
                            return Err(anyhow::anyhow!(
                                "TypeError: InvalidArgumentType - list index must be an integer, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    // A declared `VECTOR` reads back as `Value::Vector`; it
                    // indexes like the list of floats it was written as.
                    if let Value::Vector(vec) = &arr_val {
                        if let Some(i) = idx_val.as_i64() {
                            let idx = if i < 0 { vec.len() as i64 + i } else { i };
                            return Ok(usize::try_from(idx)
                                .ok()
                                .and_then(|i| vec.get(i))
                                .map_or(Value::Null, |f| Value::Float(f64::from(*f))));
                        } else if idx_val.is_null() {
                            return Ok(Value::Null);
                        }
                        return Err(anyhow::anyhow!(
                            "TypeError: InvalidArgumentType - list index must be an integer, got: {:?}",
                            idx_val
                        ));
                    }
                    if let Value::Map(map) = &arr_val {
                        if let Some(key) = idx_val.as_str() {
                            return Ok(map.get(key).cloned().unwrap_or(Value::Null));
                        } else if !idx_val.is_null() {
                            return Err(anyhow::anyhow!(
                                "TypeError: InvalidArgumentValue - Map index must be a string, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    // Handle bracket access on Node: n['name'] returns property
                    if let Value::Node(node) = &arr_val {
                        if let Some(key) = idx_val.as_str() {
                            // Check in-memory properties first
                            if let Some(val) = node.properties.get(key) {
                                return Ok(val.clone());
                            }
                            // Fallback to property manager
                            if let Ok(val) = prop_manager
                                .get_vertex_prop_with_ctx(node.vid, key, ctx)
                                .await
                            {
                                return Ok(val);
                            }
                            return Ok(Value::Null);
                        } else if !idx_val.is_null() {
                            return Err(anyhow::anyhow!(
                                "TypeError: Node index must be a string, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    // Handle bracket access on Edge: e['property'] returns property
                    if let Value::Edge(edge) = &arr_val {
                        if let Some(key) = idx_val.as_str() {
                            // Check in-memory properties first
                            if let Some(val) = edge.properties.get(key) {
                                return Ok(val.clone());
                            }
                            // Fallback to property manager
                            if let Ok(val) = prop_manager.get_edge_prop(edge.eid, key, ctx).await {
                                return Ok(val);
                            }
                            return Ok(Value::Null);
                        } else if !idx_val.is_null() {
                            return Err(anyhow::anyhow!(
                                "TypeError: Edge index must be a string, got: {:?}",
                                idx_val
                            ));
                        }
                    }
                    // Handle bracket access on VID (integer): n['name'] where n is a VID
                    if let Ok(vid) = Self::vid_from_value(&arr_val)
                        && let Some(key) = idx_val.as_str()
                    {
                        if let Ok(val) = prop_manager.get_vertex_prop_with_ctx(vid, key, ctx).await
                        {
                            return Ok(val);
                        }
                        return Ok(Value::Null);
                    }
                    if arr_val.is_null() {
                        return Ok(Value::Null);
                    }
                    Err(anyhow!(
                        "TypeError: InvalidArgumentType - cannot index into {:?}",
                        arr_val
                    ))
                }
                Expr::ArraySlice { array, start, end } => {
                    let mut arr_val = this
                        .evaluate_expr(array, row, prop_manager, params, ctx)
                        .await?;
                    // A `VECTOR` slices like its list of floats, into a list.
                    if let Value::Vector(vec) = &arr_val {
                        arr_val =
                            Value::List(vec.iter().map(|f| Value::Float(f64::from(*f))).collect());
                    }

                    if let Value::List(arr) = &arr_val {
                        let len = arr.len();

                        // Evaluate start index (default to 0), null → null result
                        let start_idx = if let Some(s) = start {
                            let v = this
                                .evaluate_expr(s, row, prop_manager, params, ctx)
                                .await?;
                            if v.is_null() {
                                return Ok(Value::Null);
                            }
                            let raw = v.as_i64().unwrap_or(0);
                            if raw < 0 {
                                (len as i64 + raw).max(0) as usize
                            } else {
                                (raw as usize).min(len)
                            }
                        } else {
                            0
                        };

                        // Evaluate end index (default to length), null → null result
                        let end_idx = if let Some(e) = end {
                            let v = this
                                .evaluate_expr(e, row, prop_manager, params, ctx)
                                .await?;
                            if v.is_null() {
                                return Ok(Value::Null);
                            }
                            let raw = v.as_i64().unwrap_or(len as i64);
                            if raw < 0 {
                                (len as i64 + raw).max(0) as usize
                            } else {
                                (raw as usize).min(len)
                            }
                        } else {
                            len
                        };

                        // Return sliced array
                        if start_idx >= end_idx {
                            return Ok(Value::List(vec![]));
                        }
                        let end_idx = end_idx.min(len);
                        return Ok(Value::List(arr[start_idx..end_idx].to_vec()));
                    }

                    if arr_val.is_null() {
                        return Ok(Value::Null);
                    }
                    Err(anyhow!("Cannot slice {:?}", arr_val))
                }
                Expr::Literal(lit) => Ok(lit.to_value()),
                Expr::List(items) => {
                    let mut vals = Vec::new();
                    for item in items {
                        vals.push(
                            this.evaluate_expr(item, row, prop_manager, params, ctx)
                                .await?,
                        );
                    }
                    Ok(Value::List(vals))
                }
                Expr::Map(items) => {
                    let mut map = HashMap::new();
                    for (key, value_expr) in items {
                        let val = this
                            .evaluate_expr(value_expr, row, prop_manager, params, ctx)
                            .await?;
                        map.insert(key.clone(), val);
                    }
                    Ok(Value::Map(map))
                }
                Expr::Exists { query, .. } => {
                    // Plan and execute subquery; failures return false (pattern doesn't match)
                    let planner = QueryPlanner::new(this.storage.schema_manager().schema());
                    let vars_in_scope: Vec<String> = row.keys().cloned().collect();

                    match planner.plan_with_scope(*query.clone(), vars_in_scope) {
                        Ok(plan) => {
                            let mut sub_params = params.clone();
                            sub_params.extend(row.clone());

                            match this.execute(plan, prop_manager, &sub_params).await {
                                Ok(results) => Ok(Value::Bool(!results.is_empty())),
                                Err(e) => {
                                    log::debug!("EXISTS subquery execution failed: {}", e);
                                    Ok(Value::Bool(false))
                                }
                            }
                        }
                        Err(e) => {
                            log::debug!("EXISTS subquery planning failed: {}", e);
                            Ok(Value::Bool(false))
                        }
                    }
                }
                Expr::CountSubquery(query) => {
                    // Similar to Exists but returns count
                    let planner = QueryPlanner::new(this.storage.schema_manager().schema());

                    let vars_in_scope: Vec<String> = row.keys().cloned().collect();

                    match planner.plan_with_scope(*query.clone(), vars_in_scope) {
                        Ok(plan) => {
                            let mut sub_params = params.clone();
                            sub_params.extend(row.clone());

                            match this.execute(plan, prop_manager, &sub_params).await {
                                Ok(results) => Ok(Value::from(results.len() as i64)),
                                Err(e) => Err(anyhow!("Subquery execution failed: {}", e)),
                            }
                        }
                        Err(e) => Err(anyhow!("Subquery planning failed: {}", e)),
                    }
                }
                Expr::Quantifier {
                    quantifier,
                    variable,
                    list,
                    predicate,
                } => {
                    // Quantifier expression evaluation (ALL/ANY/SINGLE/NONE)
                    //
                    // This is the primary execution path for quantifiers because DataFusion
                    // does not support lambda functions yet. Queries with quantifiers attempt
                    // DataFusion translation first, fail (see df_expr.rs:289), then fall back
                    // to this fallback executor path.
                    //
                    // This is intentional design - we get correct semantics with row-by-row
                    // evaluation until DataFusion adds lambda support.
                    //
                    // See: https://github.com/apache/datafusion/issues/14205

                    // Evaluate the list expression
                    let list_val = this
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    // Handle null propagation
                    if list_val.is_null() {
                        return Ok(Value::Null);
                    }

                    // Convert to array
                    let items = match list_val {
                        Value::List(arr) => arr,
                        _ => return Err(anyhow!("Quantifier expects a list, got: {:?}", list_val)),
                    };

                    // Evaluate predicate for each item
                    let mut satisfied_count = 0;
                    for item in &items {
                        // Create new row with bound variable
                        let mut item_row = row.clone();
                        item_row.insert(variable.clone(), item.clone());

                        // Evaluate predicate with bound variable
                        let pred_result = this
                            .evaluate_expr(predicate, &item_row, prop_manager, params, ctx)
                            .await?;

                        // Check if predicate is satisfied
                        if let Value::Bool(true) = pred_result {
                            satisfied_count += 1;
                        }
                    }

                    // Return based on quantifier type
                    let result = match quantifier {
                        Quantifier::All => satisfied_count == items.len(),
                        Quantifier::Any => satisfied_count > 0,
                        Quantifier::Single => satisfied_count == 1,
                        Quantifier::None => satisfied_count == 0,
                    };

                    Ok(Value::Bool(result))
                }
                Expr::ListComprehension {
                    variable,
                    list,
                    where_clause,
                    map_expr,
                } => {
                    // List comprehension evaluation: [x IN list WHERE pred | expr]
                    //
                    // Similar to quantifiers, this requires lambda-like evaluation
                    // which DataFusion doesn't support yet. This is the primary execution path.

                    // Evaluate the list expression
                    let list_val = this
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    // Handle null propagation
                    if list_val.is_null() {
                        return Ok(Value::Null);
                    }

                    // Convert to array
                    let items = match list_val {
                        Value::List(arr) => arr,
                        _ => {
                            return Err(anyhow!(
                                "List comprehension expects a list, got: {:?}",
                                list_val
                            ));
                        }
                    };

                    // Collect mapped values
                    let mut results = Vec::new();
                    for item in &items {
                        // Create new row with bound variable
                        let mut item_row = row.clone();
                        item_row.insert(variable.clone(), item.clone());

                        // Apply WHERE filter if present
                        if let Some(predicate) = where_clause {
                            let pred_result = this
                                .evaluate_expr(predicate, &item_row, prop_manager, params, ctx)
                                .await?;

                            // Skip items that don't match the filter
                            if !matches!(pred_result, Value::Bool(true)) {
                                continue;
                            }
                        }

                        // Apply map expression
                        let mapped_val = this
                            .evaluate_expr(map_expr, &item_row, prop_manager, params, ctx)
                            .await?;
                        results.push(mapped_val);
                    }

                    Ok(Value::List(results))
                }
                Expr::BinaryOp { left, op, right } => {
                    // Short-circuit evaluation for AND/OR
                    match op {
                        BinaryOp::And => {
                            let l_val = this
                                .evaluate_expr(left, row, prop_manager, params, ctx)
                                .await?;
                            // Short-circuit: if left is false, don't evaluate right
                            if let Some(false) = l_val.as_bool() {
                                return Ok(Value::Bool(false));
                            }
                            let r_val = this
                                .evaluate_expr(right, row, prop_manager, params, ctx)
                                .await?;
                            eval_binary_op(&l_val, op, &r_val)
                        }
                        BinaryOp::Or => {
                            let l_val = this
                                .evaluate_expr(left, row, prop_manager, params, ctx)
                                .await?;
                            // Short-circuit: if left is true, don't evaluate right
                            if let Some(true) = l_val.as_bool() {
                                return Ok(Value::Bool(true));
                            }
                            let r_val = this
                                .evaluate_expr(right, row, prop_manager, params, ctx)
                                .await?;
                            eval_binary_op(&l_val, op, &r_val)
                        }
                        BinaryOp::Regex => {
                            let l_val = this
                                .evaluate_expr(left, row, prop_manager, params, ctx)
                                .await?;
                            let r_val = this
                                .evaluate_expr(right, row, prop_manager, params, ctx)
                                .await?;
                            // `=~` under this query's backtrack limit, deadline
                            // and cancellation.
                            let deadline = ctx.and_then(|c| c.deadline);
                            let token = ctx.and_then(|c| c.cancellation_token.clone());
                            let interrupt: uni_query_functions::expr_eval::RegexInterrupt =
                                Arc::new(move || {
                                    crate::query::df_graph::common::check_deadline(
                                        token.as_ref(),
                                        deadline,
                                    )
                                    .err()
                                    .map(|e| e.to_string())
                                });
                            let runtime =
                                Arc::new(uni_query_functions::expr_eval::RegexRuntime::new(
                                    this.config.regex_backtrack_limit,
                                    Some(interrupt),
                                ));
                            uni_query_functions::expr_eval::REGEX_RUNTIME
                                .sync_scope(runtime, || eval_binary_op(&l_val, op, &r_val))
                        }
                        _ => {
                            // For all other operators, evaluate both sides
                            let l_val = this
                                .evaluate_expr(left, row, prop_manager, params, ctx)
                                .await?;
                            let r_val = this
                                .evaluate_expr(right, row, prop_manager, params, ctx)
                                .await?;
                            eval_binary_op(&l_val, op, &r_val)
                        }
                    }
                }
                Expr::In { expr, list } => {
                    let l_val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    let r_val = this
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;
                    eval_in_op(&l_val, &r_val)
                }
                Expr::UnaryOp { op, expr } => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    match op {
                        UnaryOp::Not => {
                            // Three-valued logic: NOT null = null
                            match val.as_bool() {
                                Some(b) => Ok(Value::Bool(!b)),
                                None if val.is_null() => Ok(Value::Null),
                                None => Err(anyhow!(
                                    "InvalidArgumentType: NOT requires a boolean argument"
                                )),
                            }
                        }
                        UnaryOp::Neg => {
                            // `-null` is null, like every other arithmetic
                            // operator on a null operand.
                            if val.is_null() {
                                Ok(Value::Null)
                            } else if let Some(i) = val.as_i64() {
                                i.checked_neg().map(Value::Int).ok_or_else(|| {
                                    anyhow!("ArithmeticError: integer overflow negating {i}")
                                })
                            } else if let Some(f) = val.as_f64() {
                                Ok(Value::Float(-f))
                            } else {
                                Err(anyhow!("Cannot negate non-numeric value: {:?}", val))
                            }
                        }
                    }
                }
                Expr::IsNull(expr) => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    Ok(Value::Bool(val.is_null()))
                }
                Expr::IsNotNull(expr) => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    Ok(Value::Bool(!val.is_null()))
                }
                Expr::IsUnique(_) => {
                    // IS UNIQUE is only valid in constraint definitions, not in query expressions
                    Err(anyhow!(
                        "IS UNIQUE can only be used in constraint definitions"
                    ))
                }
                Expr::Case {
                    expr,
                    when_then,
                    else_expr,
                } => {
                    if let Some(base_expr) = expr {
                        let base_val = this
                            .evaluate_expr(base_expr, row, prop_manager, params, ctx)
                            .await?;
                        for (w, t) in when_then {
                            let w_val = this
                                .evaluate_expr(w, row, prop_manager, params, ctx)
                                .await?;
                            if base_val == w_val {
                                return this.evaluate_expr(t, row, prop_manager, params, ctx).await;
                            }
                        }
                    } else {
                        for (w, t) in when_then {
                            let w_val = this
                                .evaluate_expr(w, row, prop_manager, params, ctx)
                                .await?;
                            if w_val.as_bool() == Some(true) {
                                return this.evaluate_expr(t, row, prop_manager, params, ctx).await;
                            }
                        }
                    }
                    if let Some(e) = else_expr {
                        return this.evaluate_expr(e, row, prop_manager, params, ctx).await;
                    }
                    Ok(Value::Null)
                }
                Expr::Wildcard => Ok(Value::Null),
                Expr::FunctionCall { name, args, .. } => {
                    // `id`, `elementId`, `labels` and `properties` are
                    // `eval_scalar_function`'s, through the generic path below,
                    // so every row evaluator answers them alike. `type()`
                    // stays here: an edge carried by its type id is named
                    // through the schema.
                    // Special case: type() returns the relationship type name
                    if name.eq_ignore_ascii_case("TYPE") {
                        if args.len() != 1 {
                            return Err(anyhow!("type() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        // Through the accessor. Reading `_type` out of a map
                        // by hand meant a native `Value::Edge` — every edge the
                        // write path now binds — fell straight through to
                        // `Null`, silently.
                        match val.edge_type_ref() {
                            Some(uni_common::value::EdgeTypeRef::Name(name)) => {
                                return Ok(Value::String(name));
                            }
                            Some(uni_common::value::EdgeTypeRef::Id(type_id)) => {
                                if let Some(name) = this
                                    .storage
                                    .schema_manager()
                                    .edge_type_name_by_id_unified(type_id)
                                {
                                    return Ok(Value::String(name));
                                }
                            }
                            None => {}
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: startNode() returns the start node of a relationship
                    if name.eq_ignore_ascii_case("STARTNODE") {
                        if args.len() != 1 {
                            return Err(anyhow!("startNode() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        // A nested `_startNode` is a whole node the plan
                        // already resolved; prefer it over re-deriving one.
                        if let Value::Map(map) = &val
                            && let Some(start_node) = map.get("_startNode")
                        {
                            return Ok(start_node.clone());
                        }
                        if let Some((Some(src), _)) = val.edge_endpoints() {
                            return Ok(Self::find_node_by_vid(row, src));
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: endNode() returns the end node of a relationship
                    if name.eq_ignore_ascii_case("ENDNODE") {
                        if args.len() != 1 {
                            return Err(anyhow!("endNode() requires exactly 1 argument"));
                        }
                        let val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Map(map) = &val
                            && let Some(end_node) = map.get("_endNode")
                        {
                            return Ok(end_node.clone());
                        }
                        if let Some((_, Some(dst))) = val.edge_endpoints() {
                            return Ok(Self::find_node_by_vid(row, dst));
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: hasLabel() checks if a node has a specific label
                    // Used for WHERE n:Label predicates
                    if name.eq_ignore_ascii_case("HASLABEL") {
                        if args.len() != 2 {
                            return Err(anyhow!("hasLabel() requires exactly 2 arguments"));
                        }
                        let node_val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let label_val = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?;

                        let label_to_check = label_val.as_str().ok_or_else(|| {
                            anyhow!("Second argument to hasLabel must be a string")
                        })?;

                        // Both arms this replaced matched `Value::Map` and read
                        // `_labels` by hand — the first even claimed to "handle
                        // proper Value::Node", which it did not: a natively
                        // encoded node fell to the catch-all and answered
                        // `false`.
                        let has_label = node_val
                            .entity_labels()
                            .is_some_and(|labels| labels.iter().any(|l| l == label_to_check));
                        return Ok(Value::Bool(has_label));
                    }

                    // Quantifier functions (ANY/ALL/NONE/SINGLE) as function calls are not supported.
                    // These should be parsed as Expr::Quantifier instead.
                    if matches!(
                        name.to_uppercase().as_str(),
                        "ANY" | "ALL" | "NONE" | "SINGLE"
                    ) {
                        return Err(anyhow!(
                            "{}() with list comprehensions is not yet supported. Use MATCH with WHERE instead.",
                            name.to_lowercase()
                        ));
                    }

                    // Special case: COALESCE needs short-circuit evaluation
                    if name.eq_ignore_ascii_case("COALESCE") {
                        for arg in args {
                            let val = this
                                .evaluate_expr(arg, row, prop_manager, params, ctx)
                                .await?;
                            if !val.is_null() {
                                return Ok(val);
                            }
                        }
                        return Ok(Value::Null);
                    }

                    // Special case: vector_similarity has dedicated implementation
                    if name.eq_ignore_ascii_case("vector_similarity") {
                        if args.len() != 2 {
                            return Err(anyhow!("vector_similarity takes 2 arguments"));
                        }
                        let v1 = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let v2 = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?;
                        return eval_vector_similarity(&v1, &v2);
                    }

                    // Special case: uni.validAt handles node fetching
                    if name.eq_ignore_ascii_case("uni.temporal.validAt")
                        || name.eq_ignore_ascii_case("uni.validAt")
                        || name.eq_ignore_ascii_case("validAt")
                    {
                        if args.len() != 4 {
                            return Err(anyhow!("validAt requires 4 arguments"));
                        }
                        let node_val = this
                            .evaluate_expr(&args[0], row, prop_manager, params, ctx)
                            .await?;
                        let start_prop = this
                            .evaluate_expr(&args[1], row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("start_prop must be string"))?
                            .to_string();
                        let end_prop = this
                            .evaluate_expr(&args[2], row, prop_manager, params, ctx)
                            .await?
                            .as_str()
                            .ok_or(anyhow!("end_prop must be string"))?
                            .to_string();
                        let time_val = this
                            .evaluate_expr(&args[3], row, prop_manager, params, ctx)
                            .await?;

                        let query_time = value_to_datetime_utc(&time_val).ok_or_else(|| {
                            anyhow!("time argument must be a datetime value or string")
                        })?;

                        // Fetch temporal property values - supports both vertices and edges
                        let valid_from_val: Option<Value> =
                            if let Ok(vid) = Self::vid_from_value(&node_val) {
                                // Vertex case - VID string format
                                prop_manager
                                    .get_vertex_prop_with_ctx(vid, &start_prop, ctx)
                                    .await
                                    .ok()
                            } else if let Some(eid) = node_val.entity_eid() {
                                // Edge case. This used to be reachable only through the
                                // `Value::Map` arm below, so a native `Value::Edge`
                                // matched nothing and fell to the `false` return —
                                // `validAt` silently filtered out every row whose
                                // relationship happened to be materialised natively
                                // rather than as a map (#234).
                                prop_manager.get_edge_prop(eid, &start_prop, ctx).await.ok()
                            } else if let Value::Map(map) = &node_val {
                                // Inline object - property embedded directly. Entity
                                // maps are handled above, by the vid/eid accessors.
                                map.get(&start_prop).cloned()
                            } else {
                                return Ok(Value::Bool(false));
                            };

                        let valid_from = match valid_from_val {
                            Some(ref v) => match value_to_datetime_utc(v) {
                                Some(dt) => dt,
                                None if v.is_null() => return Ok(Value::Bool(false)),
                                None => {
                                    return Err(anyhow!(
                                        "Property {} must be a datetime value or string",
                                        start_prop
                                    ));
                                }
                            },
                            None => return Ok(Value::Bool(false)),
                        };

                        let valid_to_val: Option<Value> =
                            if let Ok(vid) = Self::vid_from_value(&node_val) {
                                // Vertex case - VID string format
                                prop_manager
                                    .get_vertex_prop_with_ctx(vid, &end_prop, ctx)
                                    .await
                                    .ok()
                            } else if let Some(eid) = node_val.entity_eid() {
                                // Edge case. This used to be reachable only through the
                                // `Value::Map` arm below, so a native `Value::Edge`
                                // matched nothing and fell to the `false` return —
                                // `validAt` silently filtered out every row whose
                                // relationship happened to be materialised natively
                                // rather than as a map (#234).
                                prop_manager.get_edge_prop(eid, &end_prop, ctx).await.ok()
                            } else if let Value::Map(map) = &node_val {
                                // Inline object - property embedded directly. Entity
                                // maps are handled above, by the vid/eid accessors.
                                map.get(&end_prop).cloned()
                            } else {
                                return Ok(Value::Bool(false));
                            };

                        let valid_to = match valid_to_val {
                            Some(ref v) => match value_to_datetime_utc(v) {
                                Some(dt) => Some(dt),
                                None if v.is_null() => None,
                                None => {
                                    return Err(anyhow!(
                                        "Property {} must be a datetime value or null",
                                        end_prop
                                    ));
                                }
                            },
                            None => None,
                        };

                        let is_valid = valid_from <= query_time
                            && valid_to.map(|vt| query_time < vt).unwrap_or(true);
                        return Ok(Value::Bool(is_valid));
                    }

                    // For all other functions, evaluate arguments then call helper
                    let mut evaluated_args = Vec::with_capacity(args.len());
                    for arg in args {
                        let mut val = this
                            .evaluate_expr(arg, row, prop_manager, params, ctx)
                            .await?;

                        // Eagerly hydrate edge/vertex maps if pushdown hydration didn't load properties.
                        // Functions like validAt() need access to properties like valid_from/valid_to.
                        if let Value::Map(ref mut map) = val {
                            hydrate_entity_if_needed(map, prop_manager, ctx).await;
                        }

                        evaluated_args.push(val);
                    }
                    eval_scalar_function(
                        name,
                        &evaluated_args,
                        self.custom_function_registry.as_deref(),
                    )
                }
                Expr::Reduce {
                    accumulator,
                    init,
                    variable,
                    list,
                    expr,
                } => {
                    let mut acc = self
                        .evaluate_expr(init, row, prop_manager, params, ctx)
                        .await?;
                    let list_val = self
                        .evaluate_expr(list, row, prop_manager, params, ctx)
                        .await?;

                    if let Value::List(items) = list_val {
                        for item in items {
                            // Create a temporary scope/row with accumulator and variable
                            // For simplicity in fallback executor, we can construct a new row map
                            // merging current row + new variables.
                            let mut scope = row.clone();
                            scope.insert(accumulator.clone(), acc.clone());
                            scope.insert(variable.clone(), item);

                            acc = self
                                .evaluate_expr(expr, &scope, prop_manager, params, ctx)
                                .await?;
                        }
                    } else {
                        return Err(anyhow!("REDUCE list argument must evaluate to a list"));
                    }
                    Ok(acc)
                }
                Expr::ValidAt { .. } => {
                    // VALID_AT should have been transformed to a function call in the planner
                    Err(anyhow!(
                        "VALID_AT expression should have been transformed to function call in planner"
                    ))
                }

                Expr::LabelCheck { expr, labels } => {
                    let val = this
                        .evaluate_expr(expr, row, prop_manager, params, ctx)
                        .await?;
                    // Both encodings, through the accessors. This matched on
                    // `Value::Map` alone, so `WHERE n:Person` against a natively
                    // encoded node fell to the catch-all and answered `false` —
                    // a predicate that silently excludes every row.
                    if matches!(val, Value::Null) {
                        return Ok(Value::Null);
                    }
                    if let Some(node_labels) = val.entity_labels() {
                        let has_all = labels
                            .iter()
                            .all(|want| node_labels.iter().any(|have| have == want));
                        return Ok(Value::Bool(has_all));
                    }
                    if let Some(edge_type) = val.edge_type_ref() {
                        // An edge has exactly one type, so more than one label
                        // can never all match.
                        if labels.len() != 1 {
                            return Ok(Value::Bool(false));
                        }
                        let name = match edge_type {
                            uni_common::value::EdgeTypeRef::Name(n) => Some(n),
                            uni_common::value::EdgeTypeRef::Id(id) => this
                                .storage
                                .schema_manager()
                                .edge_type_name_by_id_unified(id),
                        };
                        return Ok(Value::Bool(name.as_deref() == Some(labels[0].as_str())));
                    }
                    Ok(Value::Bool(false))
                }

                Expr::MapProjection { base, items } => {
                    let base_value = this
                        .evaluate_expr(base, row, prop_manager, params, ctx)
                        .await?;

                    // Extract properties from the base object
                    let properties = match &base_value {
                        Value::Map(map) => map,
                        _ => {
                            return Err(anyhow!(
                                "Map projection requires object, got {:?}",
                                base_value
                            ));
                        }
                    };

                    let mut result_map = HashMap::new();

                    for item in items {
                        match item {
                            MapProjectionItem::Property(prop) => {
                                if let Some(value) = properties.get(prop.as_str()) {
                                    result_map.insert(prop.clone(), value.clone());
                                }
                            }
                            MapProjectionItem::AllProperties => {
                                // Include all properties except the engine's reserved fields
                                for (key, value) in properties.iter() {
                                    if !uni_common::core::schema::is_reserved_property_name(key) {
                                        result_map.insert(key.clone(), value.clone());
                                    }
                                }
                            }
                            MapProjectionItem::LiteralEntry(key, expr) => {
                                let value = this
                                    .evaluate_expr(expr, row, prop_manager, params, ctx)
                                    .await?;
                                result_map.insert(key.clone(), value);
                            }
                            MapProjectionItem::Variable(var_name) => {
                                // Variable selector: include the value of the variable in the result
                                // e.g., person{.name, friend} includes the value of 'friend' variable
                                if let Some(value) = row.get(var_name.as_str()) {
                                    result_map.insert(var_name.clone(), value.clone());
                                }
                            }
                        }
                    }

                    Ok(Value::Map(result_map))
                }
            }
        })
    }

    pub(crate) fn execute_subplan<'a>(
        &'a self,
        plan: LogicalPlan,
        prop_manager: &'a PropertyManager,
        params: &'a HashMap<String, Value>,
        ctx: Option<&'a QueryContext>,
    ) -> BoxFuture<'a, Result<Vec<HashMap<String, Value>>>> {
        Box::pin(async move {
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }
            match plan {
                LogicalPlan::Union { left, right, all } => {
                    self.execute_union(left, right, all, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::CreateVectorIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = self.storage.index_manager();
                    idx_mgr.create_vector_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateSparseIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    // `create_sparse_vector_index` registers the index, backfills any
                    // already-flushed rows via the backend, handles the empty
                    // (create-before-ingest) case, and persists the schema.
                    let idx_mgr = self.storage.index_manager();
                    idx_mgr.create_sparse_vector_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateFullTextIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = self.storage.index_manager();
                    idx_mgr.create_fts_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateScalarIndex {
                    mut config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }

                    // Check for expression indexes - create generated columns
                    let mut modified_properties = Vec::new();

                    for prop in &config.properties {
                        // Heuristic: if contains '(' and ')', it's an expression
                        if prop.contains('(') && prop.contains(')') {
                            let gen_col = SchemaManager::generated_column_name(prop);

                            // Add generated property to schema
                            let sm = self.storage.schema_manager_arc();
                            if let Err(e) = sm.add_generated_property(
                                &config.label,
                                &gen_col,
                                DataType::String, // Default type for expressions
                                prop.clone(),
                            ) {
                                log::warn!("Failed to add generated property (might exist): {}", e);
                            }

                            modified_properties.push(gen_col);
                        } else {
                            // Simple property - use as-is
                            modified_properties.push(prop.clone());
                        }
                    }

                    config.properties = modified_properties;

                    let idx_mgr = self.storage.index_manager();
                    idx_mgr.create_scalar_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateJsonFtsIndex {
                    config,
                    if_not_exists,
                } => {
                    if if_not_exists && self.index_exists_by_name(&config.name) {
                        return Ok(vec![]);
                    }
                    let idx_mgr = self.storage.index_manager();
                    idx_mgr.create_json_fts_index(config).await?;
                    Ok(vec![])
                }
                LogicalPlan::ShowDatabase => Ok(self.execute_show_database()),
                LogicalPlan::ShowConfig => Ok(self.execute_show_config()),
                LogicalPlan::ShowStatistics => self.execute_show_statistics().await,
                LogicalPlan::Vacuum => {
                    self.execute_vacuum().await?;
                    Ok(vec![])
                }
                LogicalPlan::Checkpoint => {
                    self.execute_checkpoint().await?;
                    Ok(vec![])
                }
                LogicalPlan::CopyTo {
                    label,
                    path,
                    format,
                    options,
                } => {
                    let count = self
                        .execute_copy_to(&label, &path, &format, &options, prop_manager, params)
                        .await?;
                    let mut result = HashMap::new();
                    result.insert("count".to_string(), Value::Int(count as i64));
                    Ok(vec![result])
                }
                LogicalPlan::CopyFrom {
                    label,
                    path,
                    format,
                    options,
                } => {
                    let count = self
                        .execute_copy_from(&label, &path, &format, &options)
                        .await?;
                    let mut result = HashMap::new();
                    result.insert("count".to_string(), Value::Int(count as i64));
                    Ok(vec![result])
                }
                LogicalPlan::CreateLabel(clause) => {
                    self.execute_create_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateEdgeType(clause) => {
                    self.execute_create_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::AlterLabel(clause) => {
                    self.execute_alter_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::AlterEdgeType(clause) => {
                    self.execute_alter_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropLabel(clause) => {
                    self.execute_drop_label(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropEdgeType(clause) => {
                    self.execute_drop_edge_type(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::CreateConstraint(clause) => {
                    self.execute_create_constraint(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::DropConstraint(clause) => {
                    self.execute_drop_constraint(clause).await?;
                    Ok(vec![])
                }
                LogicalPlan::ShowConstraints(clause) => Ok(self.execute_show_constraints(clause)),
                LogicalPlan::DropIndex { name, if_exists } => {
                    // #233: this matched the error's rendered STRING, so any
                    // genuine drop failure whose message happens to contain
                    // "not found" was swallowed by `IF EXISTS`. Ask the schema
                    // whether the index exists instead of inferring it from
                    // error text.
                    if if_exists && self.storage.schema_manager().get_index(&name).is_none() {
                        return Ok(vec![]);
                    }
                    let idx_mgr = self.storage.index_manager();
                    idx_mgr.drop_index(&name).await?;
                    Ok(vec![])
                }
                LogicalPlan::ShowIndexes { filter } => {
                    Ok(self.execute_show_indexes(filter.as_deref()))
                }
                // Scan/traverse nodes: delegate to DataFusion for data access,
                // then convert results to HashMaps for the fallback executor.
                LogicalPlan::Scan { .. }
                | LogicalPlan::FusedIndexScan { .. }
                | LogicalPlan::FusedIndexScanWrapped { .. }
                | LogicalPlan::ExtIdLookup { .. }
                | LogicalPlan::ScanAll { .. }
                | LogicalPlan::ScanMainByLabels { .. }
                | LogicalPlan::Traverse { .. }
                | LogicalPlan::TraverseMainByType { .. } => {
                    let batches = self.execute_datafusion(plan, prop_manager, params).await?;
                    self.record_batches_to_rows(batches)
                }
                LogicalPlan::Filter {
                    input,
                    predicate,
                    optional_variables,
                } => {
                    let input_matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;

                    tracing::debug!(
                        "Filter: Evaluating predicate {:?} on {} input rows, optional_vars={:?}",
                        predicate,
                        input_matches.len(),
                        optional_variables
                    );

                    // For OPTIONAL MATCH with WHERE: we need LEFT OUTER JOIN semantics.
                    // Group rows by non-optional variables, apply filter, and ensure
                    // at least one row per group (with NULLs if filter removes all).
                    if !optional_variables.is_empty() {
                        // Helper to check if a key belongs to an optional variable.
                        // Keys can be "var" or "var.field" (e.g., "m" or "m._vid").
                        let is_optional_key = |k: &str| -> bool {
                            optional_variables.contains(k)
                                || optional_variables
                                    .iter()
                                    .any(|var| k.starts_with(&format!("{}.", var)))
                        };

                        // Helper to check if a key is internal (should not affect grouping)
                        let is_internal_key =
                            |k: &str| -> bool { k.starts_with("__") || k.starts_with("_") };

                        // Compute the key (non-optional, non-internal variables) for grouping
                        let non_optional_vars: Vec<String> = input_matches
                            .first()
                            .map(|row| {
                                row.keys()
                                    .filter(|k| !is_optional_key(k) && !is_internal_key(k))
                                    .cloned()
                                    .collect()
                            })
                            .unwrap_or_default();

                        // Group rows by their non-optional variable values
                        let mut groups: std::collections::HashMap<
                            Vec<u8>,
                            Vec<HashMap<String, Value>>,
                        > = std::collections::HashMap::new();

                        for row in &input_matches {
                            // Create a key from non-optional variable values
                            let key: Vec<u8> = non_optional_vars
                                .iter()
                                .map(|var| {
                                    row.get(var).map(|v| format!("{v:?}")).unwrap_or_default()
                                })
                                .collect::<Vec<_>>()
                                .join("|")
                                .into_bytes();

                            groups.entry(key).or_default().push(row.clone());
                        }

                        let mut filtered = Vec::new();
                        for (_key, group_rows) in groups {
                            let mut group_passed = Vec::new();

                            for row in &group_rows {
                                // If optional variables are already NULL, preserve the row
                                let has_null_optional = optional_variables.iter().any(|var| {
                                    // Check both "var" and "var._vid" style keys
                                    let direct_null =
                                        matches!(row.get(var), Some(Value::Null) | None);
                                    let prefixed_null = row
                                        .keys()
                                        .filter(|k| k.starts_with(&format!("{}.", var)))
                                        .any(|k| matches!(row.get(k), Some(Value::Null)));
                                    direct_null || prefixed_null
                                });

                                if has_null_optional {
                                    group_passed.push(row.clone());
                                    continue;
                                }

                                let res = self
                                    .evaluate_expr(&predicate, row, prop_manager, params, ctx)
                                    .await?;

                                if res.as_bool().unwrap_or(false) {
                                    group_passed.push(row.clone());
                                }
                            }

                            if group_passed.is_empty() {
                                // No rows passed - emit one row with NULLs for optional variables
                                // Use the first row's non-optional values as a template
                                if let Some(template) = group_rows.first() {
                                    let mut null_row = HashMap::new();
                                    for (k, v) in template {
                                        if is_optional_key(k) {
                                            null_row.insert(k.clone(), Value::Null);
                                        } else {
                                            null_row.insert(k.clone(), v.clone());
                                        }
                                    }
                                    filtered.push(null_row);
                                }
                            } else {
                                filtered.extend(group_passed);
                            }
                        }

                        tracing::debug!(
                            "Filter (OPTIONAL): {} input rows -> {} output rows",
                            input_matches.len(),
                            filtered.len()
                        );

                        return Ok(filtered);
                    }

                    // Standard filter for non-OPTIONAL MATCH
                    let mut filtered = Vec::new();
                    for row in input_matches.iter() {
                        let res = self
                            .evaluate_expr(&predicate, row, prop_manager, params, ctx)
                            .await?;

                        let passes = res.as_bool().unwrap_or(false);

                        if passes {
                            filtered.push(row.clone());
                        }
                    }

                    tracing::debug!(
                        "Filter: {} input rows -> {} output rows",
                        input_matches.len(),
                        filtered.len()
                    );

                    Ok(filtered)
                }
                LogicalPlan::ProcedureCall {
                    procedure_name,
                    arguments,
                    yield_items,
                } => {
                    let yield_names: Vec<String> =
                        yield_items.iter().map(|(n, _)| n.clone()).collect();
                    let results = self
                        .execute_procedure(
                            &procedure_name,
                            &arguments,
                            &yield_names,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await?;

                    // Handle aliasing: collect all original values first, then
                    // build the aliased row in one pass. This avoids issues when
                    // an alias matches another yield item's original name (e.g.,
                    // YIELD a AS b, b AS d — renaming "a" to "b" must not
                    // clobber the original "b" before it is renamed to "d").
                    let has_aliases = yield_items.iter().any(|(_, a)| a.is_some());
                    if !has_aliases {
                        // No aliases (includes YIELD * which produces empty yield_items) —
                        // pass through the procedure output rows unchanged.
                        Ok(results)
                    } else {
                        let mut aliased_results = Vec::with_capacity(results.len());
                        for row in results {
                            let mut new_row = HashMap::new();
                            for (name, alias) in &yield_items {
                                let col_name = alias.as_ref().unwrap_or(name);
                                let val = row.get(name).cloned().unwrap_or(Value::Null);
                                new_row.insert(col_name.clone(), val);
                            }
                            aliased_results.push(new_row);
                        }
                        Ok(aliased_results)
                    }
                }
                LogicalPlan::VectorKnn { .. } => {
                    unreachable!("VectorKnn is handled by DataFusion engine")
                }
                LogicalPlan::InvertedIndexLookup { .. } => {
                    unreachable!("InvertedIndexLookup is handled by DataFusion engine")
                }
                LogicalPlan::Sort { input, order_by } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_sort(rows, &order_by, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Limit { input, skip, fetch } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let skip = skip.unwrap_or(0);
                    let take = fetch.unwrap_or(usize::MAX);
                    Ok(rows.into_iter().skip(skip).take(take).collect())
                }
                LogicalPlan::Aggregate {
                    input,
                    group_by,
                    aggregates,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_aggregate(rows, &group_by, &aggregates, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Window {
                    input,
                    window_exprs,
                } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_window(rows, &window_exprs, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Project { input, projections } => {
                    let matches = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_project(matches, &projections, prop_manager, params, ctx)
                        .await
                }
                // A planning hint only; the rows are the input's.
                LogicalPlan::MultiplicityInsensitive { input } => {
                    self.execute_subplan(*input, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Distinct { input } => {
                    let rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    let mut seen = std::collections::HashSet::new();
                    let mut result = Vec::new();
                    for row in rows {
                        let key = Self::canonical_row_key(&row);
                        if seen.insert(key) {
                            result.push(row);
                        }
                    }
                    Ok(result)
                }
                LogicalPlan::Unwind {
                    input,
                    expr,
                    variable,
                } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_unwind(input_rows, &expr, &variable, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Apply {
                    input,
                    subquery,
                    input_filter,
                } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    self.execute_apply(
                        input_rows,
                        &subquery,
                        input_filter.as_ref(),
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::SubqueryCall { input, subquery } => {
                    let input_rows = self
                        .execute_subplan(*input, prop_manager, params, ctx)
                        .await?;
                    // Execute subquery for each input row (correlated)
                    // No input_filter for CALL { }
                    self.execute_apply(input_rows, &subquery, None, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::RecursiveCTE {
                    cte_name,
                    initial,
                    recursive,
                } => {
                    self.execute_recursive_cte(
                        &cte_name,
                        *initial,
                        *recursive,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await
                }
                LogicalPlan::CrossJoin { left, right } => {
                    self.execute_cross_join(left, right, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::Set { .. }
                | LogicalPlan::Remove { .. }
                | LogicalPlan::Merge { .. }
                | LogicalPlan::Create { .. }
                | LogicalPlan::CreateBatch { .. } => {
                    unreachable!("mutations are handled by DataFusion engine")
                }
                LogicalPlan::Delete { .. } => {
                    unreachable!("mutations are handled by DataFusion engine")
                }
                LogicalPlan::Backup {
                    destination,
                    options,
                } => self.execute_backup(&destination, &options).await,
                LogicalPlan::Explain { plan } => {
                    let plan_str = format!("{:#?}", plan);
                    let mut row = HashMap::new();
                    row.insert("plan".to_string(), Value::String(plan_str));
                    Ok(vec![row])
                }
                LogicalPlan::ShortestPath { .. } => {
                    unreachable!("ShortestPath is handled by DataFusion engine")
                }
                LogicalPlan::AllShortestPaths { .. } => {
                    unreachable!("AllShortestPaths is handled by DataFusion engine")
                }
                LogicalPlan::Foreach { .. } => {
                    unreachable!("mutations are handled by DataFusion engine")
                }
                LogicalPlan::Empty => Ok(vec![HashMap::new()]),
                // This executor runs no correlated operator, so nothing binds an
                // import to an outer row: each is its same-named parameter.
                LogicalPlan::OuterRow { variables } => {
                    let projections = variables
                        .iter()
                        .map(|v| (Expr::Parameter(v.name.clone()), Some(v.name.clone())))
                        .collect();
                    let project = LogicalPlan::Project {
                        input: Box::new(LogicalPlan::Empty),
                        projections,
                    };
                    self.execute_subplan(project, prop_manager, params, ctx)
                        .await
                }
                LogicalPlan::BindZeroLengthPath { .. } => {
                    unreachable!("BindZeroLengthPath is handled by DataFusion engine")
                }
                LogicalPlan::BindPath { .. } => {
                    unreachable!("BindPath is handled by DataFusion engine")
                }
                LogicalPlan::QuantifiedPattern { .. } => {
                    unreachable!("QuantifiedPattern is handled by DataFusion engine")
                }
                LogicalPlan::LocyProgram { .. }
                | LogicalPlan::LocyFold { .. }
                | LogicalPlan::LocyBestBy { .. }
                | LogicalPlan::LocyPriority { .. }
                | LogicalPlan::LocyDerivedScan { .. }
                | LogicalPlan::LocyProject { .. }
                | LogicalPlan::LocyModelInvoke { .. }
                | LogicalPlan::LocyGenerate { .. } => {
                    unreachable!("Locy operators are handled by DataFusion engine")
                }
            }
        })
    }

    /// Execute a single plan from a FOREACH body with the given scope.
    ///
    /// Used by the DataFusion ForeachExec operator to delegate body clause
    /// execution back to the executor.
    #[expect(clippy::too_many_arguments)]
    pub(crate) async fn execute_foreach_body_plan(
        &self,
        plan: LogicalPlan,
        scope: &mut HashMap<String, Value>,
        writer: &uni_store::runtime::writer::Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        match plan {
            LogicalPlan::Set { items, .. } => {
                self.execute_set_items_locked(
                    &items,
                    scope,
                    writer,
                    prop_manager,
                    params,
                    ctx,
                    tx_l0,
                    &crate::query::df_graph::mutation_common::Prefetch::default(),
                )
                .await?;
            }
            LogicalPlan::Remove { items, .. } => {
                self.execute_remove_items_locked(
                    &items,
                    scope,
                    writer,
                    prop_manager,
                    ctx,
                    tx_l0,
                    &crate::query::df_graph::mutation_common::Prefetch::default(),
                )
                .await?;
            }
            LogicalPlan::Delete { items, detach, .. } => {
                for expr in &items {
                    let val = self
                        .evaluate_expr(expr, scope, prop_manager, params, ctx)
                        .await?;
                    self.execute_delete_item_locked(&val, detach, writer, tx_l0)
                        .await?;
                }
            }
            LogicalPlan::Create { pattern, .. } => {
                let (prepared, temp_vars) = Self::prepare_pattern_for_path_binding(&pattern);
                self.execute_create_binding_paths(
                    &prepared,
                    &temp_vars,
                    scope,
                    writer,
                    prop_manager,
                    params,
                    ctx,
                    tx_l0,
                )
                .await?;
            }
            LogicalPlan::CreateBatch { patterns, .. } => {
                for pattern in &patterns {
                    let (prepared, temp_vars) = Self::prepare_pattern_for_path_binding(pattern);
                    self.execute_create_binding_paths(
                        &prepared,
                        &temp_vars,
                        scope,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0,
                    )
                    .await?;
                }
            }
            LogicalPlan::Merge {
                pattern,
                on_match: _,
                on_create,
                ..
            } => {
                // Fold ON CREATE SET so a NOT-NULL property set only by
                // ON CREATE SET passes create-time validation (RC4); the
                // post-create SET below settles the final values.
                let seed_props = self
                    .on_create_seed_props(on_create.as_ref(), scope, prop_manager, params, ctx)
                    .await?;
                self.execute_create_pattern(
                    &pattern,
                    scope,
                    writer,
                    prop_manager,
                    params,
                    ctx,
                    tx_l0,
                    Some(&seed_props),
                )
                .await?;
                if let Some(on_create_clause) = on_create {
                    self.execute_set_items_locked(
                        &on_create_clause.items,
                        scope,
                        writer,
                        prop_manager,
                        params,
                        ctx,
                        tx_l0,
                        &crate::query::df_graph::mutation_common::Prefetch::default(),
                    )
                    .await?;
                }
            }
            LogicalPlan::Foreach {
                variable,
                list,
                body,
                ..
            } => {
                let list_val = self
                    .evaluate_expr(&list, scope, prop_manager, params, ctx)
                    .await?;
                let items = match list_val {
                    Value::List(arr) => arr,
                    Value::Null => return Ok(()),
                    _ => return Err(anyhow!("FOREACH requires a list")),
                };
                // Cloned once, not per item: a nested FOREACH accumulates
                // across its own iterations for the same reason the outer one
                // does. Re-cloning per item made `FOREACH (i IN [1,2] |
                // FOREACH (j IN [1,2,3] | SET n.seen = n.seen + 1))` leave
                // `seen = 1` rather than 6.
                let mut nested_scope = scope.clone();
                for item in items {
                    nested_scope.insert(variable.clone(), item);
                    for nested_plan in &body {
                        Box::pin(self.execute_foreach_body_plan(
                            nested_plan.clone(),
                            &mut nested_scope,
                            writer,
                            prop_manager,
                            params,
                            ctx,
                            tx_l0,
                        ))
                        .await?;
                    }
                }
            }
            _ => {
                return Err(anyhow!(
                    "Unsupported operation in FOREACH body: only SET, REMOVE, DELETE, CREATE, MERGE, and nested FOREACH are allowed"
                ));
            }
        }
        Ok(())
    }

    fn canonical_row_key(row: &HashMap<String, Value>) -> String {
        let mut pairs: Vec<_> = row.iter().collect();
        pairs.sort_by_key(|(k, _)| *k);

        pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={}", Self::canonical_value_key(v)))
            .collect::<Vec<_>>()
            .join("|")
    }

    fn canonical_value_key(v: &Value) -> String {
        match v {
            Value::Null => "null".to_string(),
            Value::Bool(b) => format!("b:{b}"),
            Value::Int(i) => format!("n:{i}"),
            Value::Float(f) => {
                if f.is_nan() {
                    "nan".to_string()
                } else if f.is_infinite() {
                    if f.is_sign_positive() {
                        "inf:+".to_string()
                    } else {
                        "inf:-".to_string()
                    }
                } else if let Some(i) = uni_common::f64_to_exact_i64(*f) {
                    // Exact: the bound `f <= i64::MAX as f64` admitted `2^63`,
                    // which the cast saturated into `i64::MAX`'s key.
                    format!("n:{i}")
                } else {
                    format!("f:{f}")
                }
            }
            Value::String(s) => {
                if let Some(k) = Self::temporal_string_key(s) {
                    format!("temporal:{k}")
                } else {
                    format!("s:{s}")
                }
            }
            Value::Bytes(b) => format!("bytes:{:?}", b),
            Value::List(items) => format!(
                "list:[{}]",
                items
                    .iter()
                    .map(Self::canonical_value_key)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Value::Map(map) => {
                let mut pairs: Vec<_> = map.iter().collect();
                pairs.sort_by_key(|(k, _)| *k);
                format!(
                    "map:{{{}}}",
                    pairs
                        .into_iter()
                        .map(|(k, v)| format!("{k}:{}", Self::canonical_value_key(v)))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            Value::Node(n) => {
                let mut labels = n.labels.clone();
                labels.sort();
                format!(
                    "node:{}:{}:{}",
                    n.vid.as_u64(),
                    labels.join(":"),
                    Self::canonical_value_key(&Value::Map(n.properties.clone()))
                )
            }
            Value::Edge(e) => format!(
                "edge:{}:{}:{}:{}:{}",
                e.eid.as_u64(),
                e.edge_type,
                e.src.as_u64(),
                e.dst.as_u64(),
                Self::canonical_value_key(&Value::Map(e.properties.clone()))
            ),
            Value::Path(p) => format!(
                "path:nodes=[{}];edges=[{}]",
                p.nodes
                    .iter()
                    .map(|n| Self::canonical_value_key(&Value::Node(n.clone())))
                    .collect::<Vec<_>>()
                    .join(","),
                p.edges
                    .iter()
                    .map(|e| Self::canonical_value_key(&Value::Edge(e.clone())))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Value::Vector(vs) => format!("vec:{:?}", vs),
            Value::Temporal(t) => format!("temporal:{}", Self::canonical_temporal_key(t)),
            _ => format!("{v:?}"),
        }
    }

    fn canonical_temporal_key(t: &uni_common::TemporalValue) -> String {
        match t {
            uni_common::TemporalValue::Date { days_since_epoch } => {
                format!("date:{days_since_epoch}")
            }
            uni_common::TemporalValue::LocalTime {
                nanos_since_midnight,
            } => format!("localtime:{nanos_since_midnight}"),
            uni_common::TemporalValue::Time {
                nanos_since_midnight,
                offset_seconds,
            } => {
                let utc_nanos = *nanos_since_midnight - (*offset_seconds as i64 * 1_000_000_000);
                format!("time:{utc_nanos}")
            }
            uni_common::TemporalValue::LocalDateTime { nanos_since_epoch } => {
                format!("localdatetime:{nanos_since_epoch}")
            }
            uni_common::TemporalValue::DateTime {
                nanos_since_epoch, ..
            } => format!("datetime:{nanos_since_epoch}"),
            uni_common::TemporalValue::Duration {
                months,
                days,
                nanos,
            } => format!("duration:{months}:{days}:{nanos}"),
            uni_common::TemporalValue::Btic { lo, hi, meta } => {
                format!("btic:{lo}:{hi}:{meta}")
            }
        }
    }

    fn temporal_string_key(s: &str) -> Option<String> {
        let fn_name = match classify_temporal(s)? {
            uni_common::TemporalType::Date => "DATE",
            uni_common::TemporalType::LocalTime => "LOCALTIME",
            uni_common::TemporalType::Time => "TIME",
            uni_common::TemporalType::LocalDateTime => "LOCALDATETIME",
            uni_common::TemporalType::DateTime => "DATETIME",
            uni_common::TemporalType::Duration => "DURATION",
            uni_common::TemporalType::Btic => return None, // BTIC uses dedicated parsing
        };
        match eval_datetime_function(fn_name, &[Value::String(s.to_string())]).ok()? {
            Value::Temporal(tv) => Some(Self::canonical_temporal_key(&tv)),
            _ => None,
        }
    }

    /// Execute aggregate operation: GROUP BY + aggregate functions.
    /// Interval for timeout checks in aggregate loops.
    pub(crate) const AGGREGATE_TIMEOUT_CHECK_INTERVAL: usize = 1000;

    pub(crate) async fn execute_aggregate(
        &self,
        rows: Vec<HashMap<String, Value>>,
        group_by: &[Expr],
        aggregates: &[Expr],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before aggregation
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        let mut groups: HashMap<String, (Vec<Value>, Vec<Accumulator>)> = HashMap::new();

        // Cypher semantics: aggregation without grouping keys returns one row even
        // on empty input (e.g. `RETURN count(*)`, `RETURN avg(x)`).
        if rows.is_empty() {
            if group_by.is_empty() {
                let accs = Self::create_accumulators(aggregates);
                let row = Self::build_aggregate_result(group_by, aggregates, &[], &accs);
                return Ok(vec![row]);
            }
            return Ok(vec![]);
        }

        for (idx, row) in rows.into_iter().enumerate() {
            // Periodic timeout check during aggregation
            if idx.is_multiple_of(Self::AGGREGATE_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            let key_vals = self
                .evaluate_group_keys(group_by, &row, prop_manager, params, ctx)
                .await?;
            // Build a canonical key so grouping follows Cypher value semantics
            // (e.g. temporal equality by instant, numeric normalization where applicable).
            let key_str = format!(
                "[{}]",
                key_vals
                    .iter()
                    .map(Self::canonical_value_key)
                    .collect::<Vec<_>>()
                    .join(",")
            );

            let entry = groups
                .entry(key_str)
                .or_insert_with(|| (key_vals, Self::create_accumulators(aggregates)));

            self.update_accumulators(&mut entry.1, aggregates, &row, prop_manager, params, ctx)
                .await?;
        }

        let results = groups
            .values()
            .map(|(k_vals, accs)| Self::build_aggregate_result(group_by, aggregates, k_vals, accs))
            .collect();

        Ok(results)
    }

    pub(crate) async fn execute_window(
        &self,
        mut rows: Vec<HashMap<String, Value>>,
        window_exprs: &[Expr],
        _prop_manager: &PropertyManager,
        _params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before window computation
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        // If no rows or no window expressions, return as-is
        if rows.is_empty() || window_exprs.is_empty() {
            return Ok(rows);
        }

        // Process each window function expression
        for window_expr in window_exprs {
            // Extract window function details
            let Expr::FunctionCall {
                name,
                args,
                window_spec: Some(window_spec),
                ..
            } = window_expr
            else {
                return Err(anyhow!(
                    "Window expression must be a FunctionCall with OVER clause: {:?}",
                    window_expr
                ));
            };

            let name_upper = name.to_uppercase();

            // Validate it's a supported window function
            if !WINDOW_FUNCTIONS.contains(&name_upper.as_str()) {
                return Err(anyhow!(
                    "Unsupported window function: {}. Supported functions: {}",
                    name,
                    WINDOW_FUNCTIONS.join(", ")
                ));
            }

            // Build partition groups based on PARTITION BY clause
            let mut partition_map: HashMap<Vec<Value>, Vec<usize>> = HashMap::new();

            for (row_idx, row) in rows.iter().enumerate() {
                // Evaluate partition key
                let partition_key: Vec<Value> = if window_spec.partition_by.is_empty() {
                    // No partitioning - all rows in one partition
                    vec![]
                } else {
                    window_spec
                        .partition_by
                        .iter()
                        .map(|expr| self.evaluate_simple_expr(expr, row))
                        .collect::<Result<Vec<_>>>()?
                };

                partition_map
                    .entry(partition_key)
                    .or_default()
                    .push(row_idx);
            }

            // Process each partition
            for row_indices in partition_map.values_mut() {
                // Sort rows within partition by ORDER BY clause
                if !window_spec.order_by.is_empty() {
                    row_indices.sort_by(|&a, &b| {
                        for sort_item in &window_spec.order_by {
                            let val_a = self.evaluate_simple_expr(&sort_item.expr, &rows[a]);
                            let val_b = self.evaluate_simple_expr(&sort_item.expr, &rows[b]);

                            if let (Ok(va), Ok(vb)) = (val_a, val_b) {
                                let cmp = Executor::compare_values(&va, &vb);
                                let cmp = if sort_item.ascending {
                                    cmp
                                } else {
                                    cmp.reverse()
                                };
                                if cmp != std::cmp::Ordering::Equal {
                                    return cmp;
                                }
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }

                // Compute window function values for this partition
                for (position, &row_idx) in row_indices.iter().enumerate() {
                    let window_value = match name_upper.as_str() {
                        "ROW_NUMBER" => Value::from((position + 1) as i64),
                        "RANK" => {
                            // RANK: position (1-indexed) of first row in group of tied rows
                            let rank = if position == 0 {
                                1i64
                            } else {
                                let prev_row_idx = row_indices[position - 1];
                                let same_as_prev = self.rows_have_same_sort_keys(
                                    &window_spec.order_by,
                                    &rows,
                                    row_idx,
                                    prev_row_idx,
                                );

                                if same_as_prev {
                                    // Walk backwards to find where this group started
                                    let mut group_start = position - 1;
                                    while group_start > 0 {
                                        let curr_idx = row_indices[group_start];
                                        let prev_idx = row_indices[group_start - 1];
                                        if !self.rows_have_same_sort_keys(
                                            &window_spec.order_by,
                                            &rows,
                                            curr_idx,
                                            prev_idx,
                                        ) {
                                            break;
                                        }
                                        group_start -= 1;
                                    }
                                    (group_start + 1) as i64
                                } else {
                                    (position + 1) as i64
                                }
                            };
                            Value::from(rank)
                        }
                        "DENSE_RANK" => {
                            // Dense rank: continuous ranking without gaps
                            let mut dense_rank = 1i64;
                            for i in 0..position {
                                let curr_idx = row_indices[i + 1];
                                let prev_idx = row_indices[i];
                                if !self.rows_have_same_sort_keys(
                                    &window_spec.order_by,
                                    &rows,
                                    curr_idx,
                                    prev_idx,
                                ) {
                                    dense_rank += 1;
                                }
                            }
                            Value::from(dense_rank)
                        }
                        "LAG" => {
                            let (value_expr, offset, default_value) =
                                self.extract_lag_lead_params("LAG", args, &rows[row_idx])?;

                            if position >= offset {
                                let target_idx = row_indices[position - offset];
                                self.evaluate_simple_expr(value_expr, &rows[target_idx])?
                            } else {
                                default_value
                            }
                        }
                        "LEAD" => {
                            let (value_expr, offset, default_value) =
                                self.extract_lag_lead_params("LEAD", args, &rows[row_idx])?;

                            if position + offset < row_indices.len() {
                                let target_idx = row_indices[position + offset];
                                self.evaluate_simple_expr(value_expr, &rows[target_idx])?
                            } else {
                                default_value
                            }
                        }
                        "NTILE" => {
                            // Extract num_buckets argument: NTILE(num_buckets)
                            let num_buckets_expr = args.first().ok_or_else(|| {
                                anyhow!("NTILE requires 1 argument: NTILE(num_buckets)")
                            })?;
                            let num_buckets_val =
                                self.evaluate_simple_expr(num_buckets_expr, &rows[row_idx])?;
                            let num_buckets = num_buckets_val.as_i64().ok_or_else(|| {
                                anyhow!(
                                    "NTILE argument must be an integer, got: {:?}",
                                    num_buckets_val
                                )
                            })?;

                            if num_buckets <= 0 {
                                return Err(anyhow!(
                                    "NTILE bucket count must be positive, got: {}",
                                    num_buckets
                                ));
                            }

                            let num_buckets = num_buckets as usize;
                            let partition_size = row_indices.len();

                            // Calculate bucket assignment using standard algorithm
                            // For N rows and B buckets:
                            // - Base size: N / B
                            // - Extra rows: N % B (go to first buckets)
                            let base_size = partition_size / num_buckets;
                            let extra_rows = partition_size % num_buckets;

                            // Determine bucket for current row
                            let bucket = if position < extra_rows * (base_size + 1) {
                                // Row is in one of the larger buckets (first 'extra_rows' buckets)
                                position / (base_size + 1) + 1
                            } else {
                                // Row is in one of the normal-sized buckets
                                let adjusted_position = position - extra_rows * (base_size + 1);
                                extra_rows + (adjusted_position / base_size) + 1
                            };

                            Value::from(bucket as i64)
                        }
                        "FIRST_VALUE" => {
                            // FIRST_VALUE returns the value of the expression from the first row in the window frame
                            let value_expr = args.first().ok_or_else(|| {
                                anyhow!("FIRST_VALUE requires 1 argument: FIRST_VALUE(expr)")
                            })?;

                            // Get the first row in the partition (after ordering)
                            if row_indices.is_empty() {
                                Value::Null
                            } else {
                                let first_idx = row_indices[0];
                                self.evaluate_simple_expr(value_expr, &rows[first_idx])?
                            }
                        }
                        "LAST_VALUE" => {
                            // LAST_VALUE returns the value of the expression from the last row in the window frame
                            let value_expr = args.first().ok_or_else(|| {
                                anyhow!("LAST_VALUE requires 1 argument: LAST_VALUE(expr)")
                            })?;

                            // Get the last row in the partition (after ordering)
                            if row_indices.is_empty() {
                                Value::Null
                            } else {
                                let last_idx = row_indices[row_indices.len() - 1];
                                self.evaluate_simple_expr(value_expr, &rows[last_idx])?
                            }
                        }
                        "NTH_VALUE" => {
                            // NTH_VALUE returns the value of the expression from the nth row in the window frame
                            if args.len() != 2 {
                                return Err(anyhow!(
                                    "NTH_VALUE requires 2 arguments: NTH_VALUE(expr, n)"
                                ));
                            }

                            let value_expr = &args[0];
                            let n_expr = &args[1];

                            let n_val = self.evaluate_simple_expr(n_expr, &rows[row_idx])?;
                            let n = n_val.as_i64().ok_or_else(|| {
                                anyhow!(
                                    "NTH_VALUE second argument must be an integer, got: {:?}",
                                    n_val
                                )
                            })?;

                            if n <= 0 {
                                return Err(anyhow!(
                                    "NTH_VALUE position must be positive, got: {}",
                                    n
                                ));
                            }

                            let nth_index = (n - 1) as usize; // Convert 1-based to 0-based
                            if nth_index < row_indices.len() {
                                let nth_idx = row_indices[nth_index];
                                self.evaluate_simple_expr(value_expr, &rows[nth_idx])?
                            } else {
                                Value::Null
                            }
                        }
                        _ => unreachable!("Window function {} already validated", name),
                    };

                    // Add window function result to row
                    // Use the window expression's string representation as the column name
                    let col_name = window_expr.to_string_repr();
                    rows[row_idx].insert(col_name, window_value);
                }
            }
        }

        Ok(rows)
    }

    /// Helper to evaluate simple expressions for window function sorting/partitioning.
    ///
    /// Uses `&self` for consistency with other evaluation methods, though it only
    /// recurses for property access.
    fn evaluate_simple_expr(&self, expr: &Expr, row: &HashMap<String, Value>) -> Result<Value> {
        match expr {
            Expr::Variable(name) => row
                .get(name)
                .cloned()
                .ok_or_else(|| anyhow!("Variable not found: {}", name)),
            Expr::Property(base, prop) => {
                let base_val = self.evaluate_simple_expr(base, row)?;
                if let Value::Map(map) = base_val {
                    map.get(prop)
                        .cloned()
                        .ok_or_else(|| anyhow!("Property not found: {}", prop))
                } else {
                    Err(anyhow!("Cannot access property on non-object"))
                }
            }
            Expr::Literal(lit) => Ok(lit.to_value()),
            _ => Err(anyhow!(
                "Unsupported expression in window function: {:?}",
                expr
            )),
        }
    }

    /// Check if two rows have matching sort keys for ranking functions.
    fn rows_have_same_sort_keys(
        &self,
        order_by: &[uni_cypher::ast::SortItem],
        rows: &[HashMap<String, Value>],
        idx_a: usize,
        idx_b: usize,
    ) -> bool {
        order_by.iter().all(|sort_item| {
            let val_a = self.evaluate_simple_expr(&sort_item.expr, &rows[idx_a]);
            let val_b = self.evaluate_simple_expr(&sort_item.expr, &rows[idx_b]);
            matches!((val_a, val_b), (Ok(a), Ok(b)) if a == b)
        })
    }

    /// Extract offset and default value for LAG/LEAD window functions.
    fn extract_lag_lead_params<'a>(
        &self,
        func_name: &str,
        args: &'a [Expr],
        row: &HashMap<String, Value>,
    ) -> Result<(&'a Expr, usize, Value)> {
        let value_expr = args.first().ok_or_else(|| {
            anyhow!(
                "{} requires at least 1 argument: {}(expr [, offset [, default]])",
                func_name,
                func_name
            )
        })?;

        let offset = if let Some(offset_expr) = args.get(1) {
            let offset_val = self.evaluate_simple_expr(offset_expr, row)?;
            offset_val.as_i64().ok_or_else(|| {
                anyhow!(
                    "{} offset must be an integer, got: {:?}",
                    func_name,
                    offset_val
                )
            })? as usize
        } else {
            1
        };

        let default_value = if let Some(default_expr) = args.get(2) {
            self.evaluate_simple_expr(default_expr, row)?
        } else {
            Value::Null
        };

        Ok((value_expr, offset, default_value))
    }

    /// Evaluate group-by key expressions for a row.
    pub(crate) async fn evaluate_group_keys(
        &self,
        group_by: &[Expr],
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<Value>> {
        let mut key_vals = Vec::new();
        for expr in group_by {
            key_vals.push(
                self.evaluate_expr(expr, row, prop_manager, params, ctx)
                    .await?,
            );
        }
        Ok(key_vals)
    }

    /// Update accumulators with values from the current row.
    pub(crate) async fn update_accumulators(
        &self,
        accs: &mut [Accumulator],
        aggregates: &[Expr],
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for (i, agg_expr) in aggregates.iter().enumerate() {
            if let Expr::FunctionCall { args, .. } = agg_expr {
                let is_wildcard = args.is_empty() || matches!(args[0], Expr::Wildcard);
                let val = if is_wildcard {
                    Value::Null
                } else {
                    self.evaluate_expr(&args[0], row, prop_manager, params, ctx)
                        .await?
                };
                if accs[i].wants_percentile()
                    && let Some(p) = args.get(1)
                {
                    let p = self
                        .evaluate_expr(p, row, prop_manager, params, ctx)
                        .await?;
                    accs[i].set_percentile(&p)?;
                }
                accs[i].update(&val, is_wildcard)?;
            }
        }
        Ok(())
    }

    /// Execute sort operation with ORDER BY clauses.
    pub(crate) async fn execute_recursive_cte(
        &self,
        cte_name: &str,
        initial: LogicalPlan,
        recursive: LogicalPlan,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use std::collections::HashSet;

        // Helper to create a stable key for cycle detection.
        // Uses sorted keys to ensure consistent ordering.
        pub(crate) fn row_key(row: &HashMap<String, Value>) -> String {
            let mut pairs: Vec<_> = row.iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(b.0));
            format!("{pairs:?}")
        }

        // 1. Execute Anchor
        let mut working_table = self
            .execute_subplan(initial, prop_manager, params, ctx)
            .await?;
        let mut result_table = working_table.clone();

        // Track seen rows for cycle detection
        let mut seen: HashSet<String> = working_table.iter().map(row_key).collect();

        // 2. Loop
        // Safety: Max iterations to prevent infinite loop
        let max_iterations = self.config.max_recursive_cte_iterations;
        for _iteration in 0..max_iterations {
            // CWE-400: Check timeout at each iteration to prevent resource exhaustion
            if let Some(ctx) = ctx {
                ctx.check_timeout()?;
            }

            if working_table.is_empty() {
                break;
            }

            // Bind working table to CTE name in params
            let working_val = Value::List(
                working_table
                    .iter()
                    .map(|row| {
                        if row.len() == 1 {
                            row.values().next().unwrap().clone()
                        } else {
                            Value::Map(row.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        }
                    })
                    .collect(),
            );

            let mut next_params = params.clone();
            next_params.insert(cte_name.to_string(), working_val);

            // Execute recursive part
            let next_result = self
                .execute_subplan(recursive.clone(), prop_manager, &next_params, ctx)
                .await?;

            if next_result.is_empty() {
                break;
            }

            // Filter out already-seen rows (cycle detection)
            let new_rows: Vec<_> = next_result
                .into_iter()
                .filter(|row| {
                    let key = row_key(row);
                    seen.insert(key) // Returns false if already present
                })
                .collect();

            if new_rows.is_empty() {
                // All results were cycles - terminate
                break;
            }

            result_table.extend(new_rows.clone());
            working_table = new_rows;
        }

        // Output accumulated results as a variable
        let final_list = Value::List(
            result_table
                .into_iter()
                .map(|row| {
                    // If the CTE returns a single column and we want to treat it as a list of values?
                    // E.g. WITH RECURSIVE r AS (RETURN 1 UNION RETURN 2) -> [1, 2] or [{expr:1}, {expr:2}]?
                    // Cypher LISTs usually contain values.
                    // If the row has 1 column, maybe unwrap?
                    // But SQL CTEs are tables.
                    // Let's stick to List<Map> for consistency with how we pass it in.
                    // UNLESS the user extracts it.
                    // My parser test `MATCH (n) WHERE n IN hierarchy` implies `hierarchy` contains Nodes.
                    // If `row` contains `root` (Node), then `hierarchy` should be `[Node, Node]`.
                    // If row has multiple cols, `[ {a:1, b:2}, ... ]`.
                    // If row has 1 col, users expect `[val, val]`.
                    if row.len() == 1 {
                        row.values().next().unwrap().clone()
                    } else {
                        Value::Map(row.into_iter().collect())
                    }
                })
                .collect(),
        );

        let mut final_row = HashMap::new();
        final_row.insert(cte_name.to_string(), final_list);
        Ok(vec![final_row])
    }

    /// Interval for timeout checks in sort loops.
    const SORT_TIMEOUT_CHECK_INTERVAL: usize = 1000;

    pub(crate) async fn execute_sort(
        &self,
        rows: Vec<HashMap<String, Value>>,
        order_by: &[uni_cypher::ast::SortItem],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // CWE-400: Check timeout before potentially expensive sort
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        let mut rows_with_keys = Vec::with_capacity(rows.len());
        for (idx, row) in rows.into_iter().enumerate() {
            // Periodic timeout check during key extraction
            if idx.is_multiple_of(Self::SORT_TIMEOUT_CHECK_INTERVAL)
                && let Some(ctx) = ctx
            {
                ctx.check_timeout()?;
            }

            let mut keys = Vec::new();
            for item in order_by {
                let val = row
                    .get(&item.expr.to_string_repr())
                    .cloned()
                    .unwrap_or(Value::Null);
                let val = if val.is_null() {
                    self.evaluate_expr(&item.expr, &row, prop_manager, params, ctx)
                        .await
                        .unwrap_or(Value::Null)
                } else {
                    val
                };
                keys.push(val);
            }
            rows_with_keys.push((row, keys));
        }

        // Check timeout again before synchronous sort (can't be interrupted)
        if let Some(ctx) = ctx {
            ctx.check_timeout()?;
        }

        rows_with_keys.sort_by(|a, b| Self::compare_sort_keys(&a.1, &b.1, order_by));

        Ok(rows_with_keys.into_iter().map(|(r, _)| r).collect())
    }

    /// Create accumulators for aggregate expressions.
    pub(crate) fn create_accumulators(aggregates: &[Expr]) -> Vec<Accumulator> {
        aggregates
            .iter()
            .map(|expr| {
                if let Expr::FunctionCall { name, distinct, .. } = expr {
                    Accumulator::new(name, *distinct)
                } else {
                    Accumulator::new("COUNT", false)
                }
            })
            .collect()
    }

    /// Build result row from group-by keys and accumulators.
    pub(crate) fn build_aggregate_result(
        group_by: &[Expr],
        aggregates: &[Expr],
        key_vals: &[Value],
        accs: &[Accumulator],
    ) -> HashMap<String, Value> {
        let mut res_row = HashMap::new();
        for (i, expr) in group_by.iter().enumerate() {
            res_row.insert(expr.to_string_repr(), key_vals[i].clone());
        }
        for (i, expr) in aggregates.iter().enumerate() {
            // Use aggregate_column_name to ensure consistency with planner
            let col_name = crate::query::planner::aggregate_column_name(expr);
            res_row.insert(col_name, accs[i].finish());
        }
        res_row
    }

    /// Compare and return ordering for sort operation.
    pub(crate) fn compare_sort_keys(
        a_keys: &[Value],
        b_keys: &[Value],
        order_by: &[uni_cypher::ast::SortItem],
    ) -> std::cmp::Ordering {
        for (i, item) in order_by.iter().enumerate() {
            let order = Self::compare_values(&a_keys[i], &b_keys[i]);
            if order != std::cmp::Ordering::Equal {
                return if item.ascending {
                    order
                } else {
                    order.reverse()
                };
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Executes BACKUP command to local or cloud storage.
    ///
    /// Supports both local filesystem paths and cloud URLs (s3://, gs://, az://).
    pub(crate) async fn execute_backup(
        &self,
        destination: &str,
        _options: &HashMap<String, Value>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // 1. Flush L0
        if let Some(writer_arc) = &self.writer {
            let writer: &uni_store::Writer = writer_arc.as_ref();
            writer.flush_to_l1(None).await?;
        }

        // 2. Snapshot
        let snapshot_manager = self.storage.snapshot_manager();
        let snapshot = snapshot_manager
            .load_latest_snapshot()
            .await?
            .ok_or_else(|| anyhow!("No snapshot found"))?;

        // 3. Copy files - cloud or local path
        if is_cloud_url(destination) {
            self.backup_to_cloud(destination, &snapshot.snapshot_id)
                .await?;
        } else {
            // Validate local destination path against sandbox
            let validated_dest = self.validate_path(destination)?;
            self.backup_to_local(&validated_dest, &snapshot.snapshot_id)
                .await?;
        }

        let mut res = HashMap::new();
        res.insert(
            "status".to_string(),
            Value::String("Backup completed".to_string()),
        );
        res.insert(
            "snapshot_id".to_string(),
            Value::String(snapshot.snapshot_id),
        );
        Ok(vec![res])
    }

    /// Backs up database to a local filesystem destination.
    async fn backup_to_local(&self, dest_path: &std::path::Path, _snapshot_id: &str) -> Result<()> {
        let source_path = std::path::Path::new(self.storage.base_path());

        if !dest_path.exists() {
            std::fs::create_dir_all(dest_path)?;
        }

        // Recursive copy (local to local)
        if source_path.exists() {
            Self::copy_dir_all(source_path, dest_path)?;
        }

        // Copy schema to destination/catalog/schema.json
        let schema_manager = self.storage.schema_manager();
        let dest_catalog = dest_path.join("catalog");
        if !dest_catalog.exists() {
            std::fs::create_dir_all(&dest_catalog)?;
        }

        let schema_content = serde_json::to_string_pretty(&schema_manager.schema())?;
        std::fs::write(dest_catalog.join("schema.json"), schema_content)?;

        Ok(())
    }

    /// Backs up database to a cloud storage destination.
    ///
    /// Streams data from source to destination, supporting cross-cloud backups.
    async fn backup_to_cloud(&self, dest_url: &str, _snapshot_id: &str) -> Result<()> {
        use object_store::ObjectStore;
        use object_store::ObjectStoreExt;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjPath;

        let (dest_store, dest_prefix) = build_store_from_url(dest_url)?;
        let source_path = std::path::Path::new(self.storage.base_path());

        // Create local store for source, coerced to dyn ObjectStore
        let src_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(source_path)?);

        // Copy catalog/ directory
        let catalog_src = ObjPath::from("catalog");
        let catalog_dst = if dest_prefix.as_ref().is_empty() {
            ObjPath::from("catalog")
        } else {
            ObjPath::from(format!("{}/catalog", dest_prefix.as_ref()))
        };
        copy_store_prefix(&src_store, &dest_store, &catalog_src, &catalog_dst).await?;

        // Copy storage/ directory
        let storage_src = ObjPath::from("storage");
        let storage_dst = if dest_prefix.as_ref().is_empty() {
            ObjPath::from("storage")
        } else {
            ObjPath::from(format!("{}/storage", dest_prefix.as_ref()))
        };
        copy_store_prefix(&src_store, &dest_store, &storage_src, &storage_dst).await?;

        // Ensure schema is present at canonical catalog location.
        let schema_manager = self.storage.schema_manager();
        let schema_content = serde_json::to_string_pretty(&schema_manager.schema())?;
        let schema_path = if dest_prefix.as_ref().is_empty() {
            ObjPath::from("catalog/schema.json")
        } else {
            ObjPath::from(format!("{}/catalog/schema.json", dest_prefix.as_ref()))
        };
        dest_store
            .put(&schema_path, bytes::Bytes::from(schema_content).into())
            .await?;

        Ok(())
    }

    /// Maximum directory depth for backup operations.
    ///
    /// **CWE-674 (Uncontrolled Recursion)**: Prevents stack overflow from
    /// excessively deep directory structures.
    const MAX_BACKUP_DEPTH: usize = 100;

    /// Maximum file count for backup operations.
    ///
    /// **CWE-400 (Resource Consumption)**: Prevents disk exhaustion and
    /// long-running operations from malicious or unexpectedly large directories.
    const MAX_BACKUP_FILES: usize = 100_000;

    /// Recursively copies a directory with security limits.
    ///
    /// # Security
    ///
    /// - **CWE-674**: Depth limit prevents stack overflow
    /// - **CWE-400**: File count limit prevents resource exhaustion
    /// - **Symlink handling**: Symlinks are skipped to prevent loop attacks
    pub(crate) fn copy_dir_all(
        src: &std::path::Path,
        dst: &std::path::Path,
    ) -> std::io::Result<()> {
        let mut file_count = 0usize;
        Self::copy_dir_all_impl(src, dst, 0, &mut file_count)
    }

    /// Internal implementation with depth and file count tracking.
    pub(crate) fn copy_dir_all_impl(
        src: &std::path::Path,
        dst: &std::path::Path,
        depth: usize,
        file_count: &mut usize,
    ) -> std::io::Result<()> {
        if depth >= Self::MAX_BACKUP_DEPTH {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Maximum backup depth {} exceeded at {:?}",
                    Self::MAX_BACKUP_DEPTH,
                    src
                ),
            ));
        }

        std::fs::create_dir_all(dst)?;

        for entry in std::fs::read_dir(src)? {
            if *file_count >= Self::MAX_BACKUP_FILES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "Maximum backup file count {} exceeded",
                        Self::MAX_BACKUP_FILES
                    ),
                ));
            }
            *file_count += 1;

            let entry = entry?;
            let metadata = entry.metadata()?;

            // Skip symlinks to prevent loops and traversal attacks
            if metadata.file_type().is_symlink() {
                // Silently skip - logging would require tracing dependency
                continue;
            }

            let dst_path = dst.join(entry.file_name());
            if metadata.is_dir() {
                Self::copy_dir_all_impl(&entry.path(), &dst_path, depth + 1, file_count)?;
            } else {
                std::fs::copy(entry.path(), dst_path)?;
            }
        }
        Ok(())
    }

    /// Execute a projection operation.
    pub(crate) async fn execute_project(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        projections: &[(Expr, Option<String>)],
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut results = Vec::new();
        for m in input_rows {
            let mut row = HashMap::new();
            for (expr, alias) in projections {
                let val = self
                    .evaluate_expr(expr, &m, prop_manager, params, ctx)
                    .await?;
                let name = alias.clone().unwrap_or_else(|| expr.to_string_repr());
                row.insert(name, val);
            }
            results.push(row);
        }
        Ok(results)
    }

    /// Execute an UNWIND operation.
    pub(crate) async fn execute_unwind(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        expr: &Expr,
        variable: &str,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut results = Vec::new();
        for row in input_rows {
            let val = self
                .evaluate_expr(expr, &row, prop_manager, params, ctx)
                .await?;
            if let Value::List(items) = val {
                for item in items {
                    let mut new_row = row.clone();
                    new_row.insert(variable.to_string(), item);
                    results.push(new_row);
                }
            }
        }
        Ok(results)
    }

    /// Execute an APPLY (correlated subquery) operation.
    pub(crate) async fn execute_apply(
        &self,
        input_rows: Vec<HashMap<String, Value>>,
        subquery: &LogicalPlan,
        input_filter: Option<&Expr>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut filtered_rows = input_rows;

        if let Some(filter) = input_filter {
            let mut filtered = Vec::new();
            for row in filtered_rows {
                let res = self
                    .evaluate_expr(filter, &row, prop_manager, params, ctx)
                    .await?;
                if res.as_bool().unwrap_or(false) {
                    filtered.push(row);
                }
            }
            filtered_rows = filtered;
        }

        // No input rows, no output rows: the subquery runs once per input row.
        // A standalone CALL is planned as the procedure itself, not an Apply.
        if filtered_rows.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for row in filtered_rows {
            let mut sub_params = params.clone();
            sub_params.extend(row.clone());

            let sub_rows = self
                .execute_subplan(subquery.clone(), prop_manager, &sub_params, ctx)
                .await?;

            for sub_row in sub_rows {
                let mut new_row = row.clone();
                new_row.extend(sub_row);
                results.push(new_row);
            }
        }
        Ok(results)
    }

    /// Execute SHOW INDEXES command.
    pub(crate) fn execute_show_indexes(&self, filter: Option<&str>) -> Vec<HashMap<String, Value>> {
        let schema = self.storage.schema_manager().schema();
        let mut rows = Vec::new();
        for idx in &schema.indexes {
            let (name, type_str, details) = match idx {
                uni_common::core::schema::IndexDefinition::Vector(c) => (
                    c.name.clone(),
                    "VECTOR",
                    format!("{:?} on {}.{}", c.index_type, c.label, c.property),
                ),
                uni_common::core::schema::IndexDefinition::FullText(c) => (
                    c.name.clone(),
                    "FULLTEXT",
                    format!("on {}:{:?}", c.label, c.properties),
                ),
                uni_common::core::schema::IndexDefinition::Scalar(cfg) => (
                    cfg.name.clone(),
                    "SCALAR",
                    format!(":{}({:?})", cfg.label, cfg.properties),
                ),
                _ => ("UNKNOWN".to_string(), "UNKNOWN", "".to_string()),
            };

            if let Some(f) = filter
                && f != type_str
            {
                continue;
            }

            let mut row = HashMap::new();
            row.insert("name".to_string(), Value::String(name));
            row.insert("type".to_string(), Value::String(type_str.to_string()));
            row.insert("details".to_string(), Value::String(details));
            rows.push(row);
        }
        rows
    }

    pub(crate) fn execute_show_database(&self) -> Vec<HashMap<String, Value>> {
        let mut row = HashMap::new();
        row.insert("name".to_string(), Value::String("uni".to_string()));
        // Could add storage path, etc.
        vec![row]
    }

    pub(crate) fn execute_show_config(&self) -> Vec<HashMap<String, Value>> {
        // Placeholder as we don't easy access to config struct from here
        vec![]
    }

    pub(crate) async fn execute_show_statistics(&self) -> Result<Vec<HashMap<String, Value>>> {
        let snapshot = self
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?;
        let mut results = Vec::new();

        if let Some(snap) = snapshot {
            for (label, s) in &snap.vertices {
                let mut row = HashMap::new();
                row.insert("type".to_string(), Value::String("Label".to_string()));
                row.insert("name".to_string(), Value::String(label.clone()));
                row.insert("count".to_string(), Value::Int(s.count as i64));
                results.push(row);
            }
            for (edge, s) in &snap.edges {
                let mut row = HashMap::new();
                row.insert("type".to_string(), Value::String("Edge".to_string()));
                row.insert("name".to_string(), Value::String(edge.clone()));
                row.insert("count".to_string(), Value::Int(s.count as i64));
                results.push(row);
            }
        }

        Ok(results)
    }

    pub(crate) fn execute_show_constraints(
        &self,
        clause: ShowConstraints,
    ) -> Vec<HashMap<String, Value>> {
        let schema = self.storage.schema_manager().schema();
        let mut rows = Vec::new();
        for c in &schema.constraints {
            if let Some(target) = &clause.target {
                match (target, &c.target) {
                    (AstConstraintTarget::Label(l1), ConstraintTarget::Label(l2)) if l1 == l2 => {}
                    (AstConstraintTarget::EdgeType(e1), ConstraintTarget::EdgeType(e2))
                        if e1 == e2 => {}
                    _ => continue,
                }
            }

            let mut row = HashMap::new();
            row.insert("name".to_string(), Value::String(c.name.clone()));
            let type_str = match c.constraint_type {
                ConstraintType::Unique { .. } => "UNIQUE",
                ConstraintType::Exists { .. } => "EXISTS",
                ConstraintType::Check { .. } => "CHECK",
                ConstraintType::NodeKey { .. } => "NODE KEY",
                _ => "UNKNOWN",
            };
            row.insert("type".to_string(), Value::String(type_str.to_string()));

            let target_str = match &c.target {
                ConstraintTarget::Label(l) => format!("(:{})", l),
                ConstraintTarget::EdgeType(e) => format!("[:{}]", e),
                _ => "UNKNOWN".to_string(),
            };
            row.insert("target".to_string(), Value::String(target_str));

            rows.push(row);
        }
        rows
    }

    /// Execute a MERGE operation.
    pub(crate) async fn execute_cross_join(
        &self,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let left_rows = self
            .execute_subplan(*left, prop_manager, params, ctx)
            .await?;
        let right_rows = self
            .execute_subplan(*right, prop_manager, params, ctx)
            .await?;

        let mut results = Vec::new();
        for l in &left_rows {
            for r in &right_rows {
                let mut combined = l.clone();
                combined.extend(r.clone());
                results.push(combined);
            }
        }
        Ok(results)
    }

    /// Execute a UNION operation with optional deduplication.
    pub(crate) async fn execute_union(
        &self,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let mut left_rows = self
            .execute_subplan(*left, prop_manager, params, ctx)
            .await?;
        let mut right_rows = self
            .execute_subplan(*right, prop_manager, params, ctx)
            .await?;

        left_rows.append(&mut right_rows);

        if !all {
            // Sorting keys fixes map order, but `format!("{:?}")` still renders
            // each `Value` via `Debug`, and a `Value::Map`-valued cell nests a
            // `HashMap` whose `Debug` order is instance-dependent — so equal
            // rows could survive UNION dedup. `canonical_row_key` encodes both
            // keys and values structurally and deterministically.
            let mut seen = HashSet::new();
            left_rows.retain(|row| seen.insert(Self::canonical_row_key(row)));
        }
        Ok(left_rows)
    }

    /// Check if an index with the given name exists.
    pub(crate) fn index_exists_by_name(&self, name: &str) -> bool {
        let schema = self.storage.schema_manager().schema();
        schema.indexes.iter().any(|idx| match idx {
            uni_common::core::schema::IndexDefinition::Vector(c) => c.name == name,
            uni_common::core::schema::IndexDefinition::FullText(c) => c.name == name,
            uni_common::core::schema::IndexDefinition::Scalar(c) => c.name == name,
            _ => false,
        })
    }

    pub(crate) async fn detach_delete_vertex(
        &self,
        vid: Vid,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        self.detach_incident_edges(&[vid], writer, tx_l0).await
    }

    /// Delete every incident edge (both directions) of `vids`.
    ///
    /// The shared core of [`Self::detach_delete_vertex`] (the N=1 case) and
    /// [`Self::batch_detach_delete_vertices`].
    ///
    /// Deletes edges ONLY, never the vertices themselves. That split is
    /// load-bearing: `execute_delete_vertex` calls the N=1 path and then
    /// deletes the vertex itself, so folding `writer.delete_vertex` in here
    /// would delete it twice.
    async fn detach_incident_edges(
        &self,
        vids: &[Vid],
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        let (out_graph, in_graph) = self.batch_load_incident_edges(vids, writer).await?;

        let mut deleted: HashSet<Eid> = HashSet::new();
        for edge in out_graph.edges().chain(in_graph.edges()) {
            if deleted.insert(edge.eid) {
                writer
                    .delete_edge(edge.eid, edge.src_vid, edge.dst_vid, edge.edge_type, tx_l0)
                    .await?;
            }
        }
        // Edges created earlier in this transaction are not in the subgraph
        // above; delete them explicitly rather than relying on the vertex
        // deletion's cascade, so they are counted and hooked like any other.
        for (eid, src, dst, edge_type) in tx_incident_edges(vids, tx_l0) {
            if deleted.insert(eid) {
                writer.delete_edge(eid, src, dst, edge_type, tx_l0).await?;
            }
        }

        Ok(())
    }

    /// Load outgoing + incoming subgraphs (1-hop) for a batch of vertices
    /// in two scans — one per direction. Shared infrastructure for
    /// `batch_detach_delete_vertices` (which deletes the loaded edges) and
    /// `batch_check_vertices_have_no_edges` (which errors if any
    /// non-tombstoned edges exist).
    ///
    /// Returns raw `WorkingGraph`s with **no tombstone filtering**;
    /// callers that care about same-batch edge deletes (e.g., the
    /// non-detach check that runs after edges have been individually
    /// DELETEd) layer the filter on top.
    async fn batch_load_incident_edges(
        &self,
        vids: &[Vid],
        writer: &Writer,
    ) -> Result<(
        uni_store::runtime::working_graph::WorkingGraph,
        uni_store::runtime::working_graph::WorkingGraph,
    )> {
        let schema = self.storage.schema_manager().schema();
        let edge_type_ids: Vec<u32> = schema.all_edge_type_ids();
        let l0 = Some(writer.l0_manager.get_current());

        let out_graph = self
            .storage
            .load_subgraph_cached(
                vids,
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Outgoing,
                l0.clone(),
            )
            .await?;
        let in_graph = self
            .storage
            .load_subgraph_cached(
                vids,
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Incoming,
                l0,
            )
            .await?;
        Ok((out_graph, in_graph))
    }

    /// Batch detach-delete: load subgraphs for all VIDs at once, then delete edges and vertices.
    pub(crate) async fn batch_detach_delete_vertices(
        &self,
        vids: &[Vid],
        labels_per_vid: Vec<Option<Vec<String>>>,
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        self.detach_incident_edges(vids, writer, tx_l0).await?;

        for (vid, labels) in vids.iter().zip(labels_per_vid) {
            writer.delete_vertex(*vid, labels, tx_l0).await?;
        }

        Ok(())
    }

    /// Batch non-detach DELETE check: verify NO incident edges exist for
    /// any VID in `vids`, modulo tombstones from the writer's L0 and the
    /// tx-local L0 (same set the per-VID `check_vertex_has_no_edges`
    /// consults at write.rs:2650-2673).
    ///
    /// Why this exists: the per-VID `check_vertex_has_no_edges` does two
    /// `load_subgraph_cached(&[vid], ...)` calls per vertex (outgoing +
    /// incoming). At N vertices that's 2N subgraph loads. The batched
    /// path collapses to 2 loads regardless of N.
    ///
    /// Tombstone visibility: detach (which deletes everything) doesn't
    /// need the tombstone filter; non-detach does, because the caller
    /// may have already DELETEd some incident edges in the same
    /// statement (e.g. `MATCH (a)-[r]->(b) DELETE r, a` — edges are
    /// deleted before nodes per mutation_common.rs:720-724, so by the
    /// time this check runs, `r`'s eid is in `tx_l0.tombstones` and
    /// must be excluded from the "still has edges" set).
    pub(crate) async fn batch_check_vertices_have_no_edges(
        &self,
        vids: &[Vid],
        writer: &Writer,
        tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
    ) -> Result<()> {
        if vids.is_empty() {
            return Ok(());
        }

        let tombstoned_eids = collect_tombstoned_eids(writer, tx_l0);

        let (out_graph, in_graph) = self.batch_load_incident_edges(vids, writer).await?;

        let loaded = out_graph
            .edges()
            .chain(in_graph.edges())
            .map(|e| (e.eid, e.src_vid));
        // `batch_load_incident_edges` reads the adjacency CSR and overlay,
        // which a transaction's own edges never reach until commit. Without
        // these, a node could be deleted in the transaction that connected
        // it, and the deletion's cascade silently took the edge with it.
        let in_tx = tx_incident_edges(vids, tx_l0)
            .into_iter()
            .map(|(eid, src, _, _)| (eid, src));
        for (eid, src) in loaded.chain(in_tx) {
            if !tombstoned_eids.contains(&eid) {
                return Err(anyhow!(
                    "ConstraintVerificationFailed: DeleteConnectedNode - Cannot delete node {}, because it still has relationships. To delete the node and its relationships, use DETACH DELETE.",
                    src
                ));
            }
        }
        Ok(())
    }
}

/// Live edges in the transaction-local L0 with an endpoint in `vids`, as
/// `(eid, src, dst, edge_type)`.
///
/// A transaction's edges live only in its private buffer until commit — they
/// are not in the adjacency overlay the subgraph loads read — so anything
/// asking "which relationships does this node have right now" must add them.
fn tx_incident_edges(
    vids: &[Vid],
    tx_l0: Option<&Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>>,
) -> Vec<(Eid, Vid, Vid, u32)> {
    let Some(tx) = tx_l0 else {
        return Vec::new();
    };
    let wanted: HashSet<Vid> = vids.iter().copied().collect();
    let guard = tx.read();
    guard
        .edge_endpoints
        .iter()
        .filter(|(eid, (src, dst, _))| {
            (wanted.contains(src) || wanted.contains(dst)) && !guard.tombstones.contains_key(eid)
        })
        .map(|(eid, (src, dst, edge_type))| (*eid, *src, *dst, *edge_type))
        .collect()
}

#[cfg(test)]
mod plan_children_tests {
    use super::*;
    use crate::query::planner::FusionKind;
    use parking_lot::RwLock;

    /// `FusedIndexScanWrapped` carries a real child plan.
    ///
    /// Before the wildcard was removed this returned `vec![]`, so every
    /// consumer of `plan_children` — `is_ddl_or_admin` (engine routing) and
    /// `contains_write_operations` (`MutationContext` construction) — saw a
    /// fusion-wrapped operator as a leaf and never inspected what it wrapped.
    #[test]
    fn plan_children_descends_into_fused_index_scan_wrapped() {
        let plan = LogicalPlan::FusedIndexScanWrapped {
            inner: Box::new(LogicalPlan::Empty),
            kind: FusionKind::BtreeUnion,
        };
        let children = Executor::plan_children(&plan);
        assert_eq!(children.len(), 1, "the wrapped operator must be a child");
        assert!(matches!(children[0], LogicalPlan::Empty));
    }

    /// The five Locy post-fixpoint operators likewise carry an `input`.
    #[test]
    fn plan_children_descends_into_locy_operators() {
        let plan = LogicalPlan::LocyFold {
            input: Box::new(LogicalPlan::Empty),
            key_columns: vec![],
            fold_bindings: vec![],
            strict_probability_domain: false,
            probability_epsilon: 0.0,
        };
        let children = Executor::plan_children(&plan);
        assert_eq!(children.len(), 1);
        assert!(matches!(children[0], LogicalPlan::Empty));
    }

    /// Inverse guard: a Locy *scan* is a leaf and must stay one.
    ///
    /// `LocyProgram` is the load-bearing case of this rule — its clause bodies
    /// hang off `strata → rules → clauses → body` rather than a direct field,
    /// so yielding them would make `is_ddl_or_admin` recurse into Locy rule
    /// bodies, and that predicate is the sole gate sending a plan to the
    /// row-fallback executor, whose Locy arms are `unreachable!()`. A wrong
    /// `true` there is a panic, not a wrong answer.
    ///
    /// It is not constructed here: it carries sixteen fields including three
    /// `Arc`-wrapped registries, and building one would test the constructor
    /// rather than the walker. `LocyDerivedScan` shares the arm and the
    /// property, and the arm's comment carries the reasoning for both.
    #[test]
    fn plan_children_treats_a_locy_scan_as_a_leaf() {
        let plan = LogicalPlan::LocyDerivedScan {
            scan_index: 0,
            rule_name: "r".to_string(),
            data: Arc::new(RwLock::new(Vec::new())),
            schema: Arc::new(arrow_schema::Schema::empty()),
        };
        assert!(
            Executor::plan_children(&plan).is_empty(),
            "Locy scans must remain leaves — see the arm's comment"
        );
    }
}
