// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.algo.*` adapter wrapping the static
//! [`uni_algo::algo::AlgorithmRegistry`].
//!
//! One `AlgorithmProcedureAdapter` is registered per algorithm at startup
//! time. The adapter stores the algorithm's [`AlgoProcedure`] handle
//! directly, plus a cached [`ProcedureSignature`] derived from the
//! algorithm's signature. At invocation time the adapter downcasts the
//! [`ProcedureContext::host`] to [`QueryProcedureHost`], rebuilds an
//! [`AlgoContext`] from the host snapshot (mirroring the legacy
//! `execute_algo_procedure` in `procedure_call.rs`), drives the algo
//! stream to completion, and converts rows into a single Arrow
//! [`RecordBatch`].
//!
//! This is the M4 transitional adapter. M5c.1 will replace each wrapped
//! algorithm with a proper `AlgorithmProvider` implementation, and the
//! static `AlgorithmRegistry` will retire.
//!
//! **M5c.2 (2026-05-24):** the adapter now discriminates between two
//! call shapes by inspecting the JSON shape of `args[0]`:
//!
//! - **Legacy** — `args[0]` is a JSON `Array` (the `nodeLabels` list).
//!   The full positional arg vector is forwarded to
//!   `AlgoProcedure::execute` unchanged. The first call per `QName`
//!   per process emits a `tracing::warn!` flagged "legacy-arity";
//!   subsequent calls are silent.
//! - **V2 `(graphRef, config)`** — `args[0]` is a JSON `Object`. The
//!   adapter parses `graphRef` via [`uni_algo::parse_graph_ref`] into a
//!   [`ProjectionInput`], synthesises the algorithm's internal positional
//!   arg vector from `Native` variants (label/edge lists + per-optional-
//!   arg lookups into `config`), and forwards to the same
//!   `AlgoProcedure::execute`. `Cypher` runs the inner queries through
//!   `QueryProcedureHost::execute_inner_query`; `Named` resolves the
//!   stored projection via the per-`StorageManager` `ProjectionStore`.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use uni_algo::algo::AlgorithmRegistry;
use uni_algo::algo::procedures::{AlgoContext, AlgoProcedure, AlgoResultRow, ValueType};
use uni_algo::{ProjectionInput, parse_graph_ref};
use uni_common::Value;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

use crate::procedures_plugin::host_args::arg;
use crate::query::df_graph::common::exec_err;
use crate::query::df_graph::procedure_call::{
    algo_yield_field, build_typed_column, json_to_value, value_type_to_arrow,
};
use crate::query::executor::procedure_host::QueryProcedureHost;

// Rust guideline compliant

/// `ProcedurePlugin` wrapping a single [`AlgoProcedure`] from the
/// static `AlgorithmRegistry`.
struct AlgorithmProcedureAdapter {
    proc: Arc<dyn AlgoProcedure>,
    /// Cached `uni-plugin` signature derived once at register time.
    signature: ProcedureSignature,
    /// Cached `uni-algo` yields (for `AlgoResultRow` column index
    /// lookup at row-projection time).
    algo_yields: Vec<(&'static str, ValueType)>,
}

impl std::fmt::Debug for AlgorithmProcedureAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmProcedureAdapter")
            .field("name", &self.proc.name())
            .finish()
    }
}

impl ProcedurePlugin for AlgorithmProcedureAdapter {
    fn signature(&self) -> &ProcedureSignature {
        &self.signature
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .and_then(|h| h.as_any().downcast_ref::<QueryProcedureHost>())
            .ok_or_else(|| {
                FnError::new(
                    0x701,
                    "uni.algo.*: requires QueryProcedureHost (host not bound on ProcedureContext)",
                )
            })?;

        // ColumnarValue → serde_json::Value, matching the legacy
        // `execute_algo_procedure` conversion. Scalars convert
        // directly; complex types (List/Map/etc) are encoded as
        // LargeBinary JSON by `procedure_call::value_to_columnar` and
        // are deserialized here back to JSON values so algos that take
        // `Vec<String>` (label lists, etc.) see the proper shape.
        let raw_args: Vec<serde_json::Value> = args
            .iter()
            .map(|cv| match cv {
                ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b)))
                | ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => {
                    // An argument that will not decode is malformed, not null.
                    // JSON `null` is a legal argument value, so substituting it
                    // let the procedure run on an argument the caller never
                    // passed (#233 class) — `graphRef` in particular would fall
                    // through to `V2Plan::Direct` and report a signature error
                    // naming the wrong cause.
                    serde_json::from_slice::<serde_json::Value>(b).map_err(|e| {
                        FnError::new(0x821, format!("procedure argument decode failed: {e}"))
                    })
                }
                ColumnarValue::Scalar(s) => Ok(serde_json::Value::from(scalar_value_to_uni_value(
                    s.clone(),
                ))),
                ColumnarValue::Array(_) => Ok(serde_json::Value::Null),
            })
            .collect::<Result<_, FnError>>()?;

        // M5c.5: only the V2 `(graphRef, config)` shape is accepted at
        // the public Cypher entry. A JSON-Object first arg is parsed as
        // `graphRef`; anything else (positional vid args for procedures
        // like `shortestPath`, or empty arg lists) falls through to
        // `V2Plan::Direct` and is validated by the procedure's own
        // signature. The legacy `(['L'], ['E'], ...)` array form is
        // gone — callers must use the map shape.
        let plan = match raw_args.first() {
            Some(v) if v.is_object() => {
                let projection = parse_graph_ref(v)
                    .map_err(|e| FnError::new(0x820, format!("graphRef parse: {e}")))?;
                let config = raw_args
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
                let store = crate::projection_store::for_storage(host.storage());
                V2Plan::from_projection(
                    self.proc.name(),
                    self.proc.as_ref(),
                    projection,
                    config,
                    &store,
                )?
            }
            _ => V2Plan::Direct(raw_args),
        };

        // Build AlgoContext with L0 visibility, mirroring
        // `procedure_call.rs::execute_algo_procedure` lines 1244-1258.
        let l0_mgr = host.l0_context().read_view();
        let algo_ctx = AlgoContext::new(Arc::clone(host.storage()), l0_mgr)
            .with_property_manager(host.property_manager().cloned());

        let proc = Arc::clone(&self.proc);
        let algo_yields = self.algo_yields.clone();
        let plugin_yields = self.signature.yields.clone();
        let host = host.clone();
        let algo_name = self.proc.name().to_owned();

        // Drive the algo stream off-thread via a tokio task and pipe
        // RecordBatches into a one-shot channel; wrap as a
        // RecordBatchStreamAdapter for DataFusion.
        let out_schema = Arc::new(Schema::new(plugin_yields.clone()));

        let stream = futures::stream::once(async move {
            let mut algo_stream = match plan {
                V2Plan::Direct(args) => {
                    if proc.wants_native_terminals() {
                        // Cypher-path family: algorithm builds its own
                        // projection from the edge-type schema or
                        // per-call terminals. No projection arg.
                        proc.execute_with_native_terminals(algo_ctx, args)
                    } else {
                        // Standard `(nodeLabels, edgeTypes, …)` shape:
                        // host materialises the projection here and
                        // routes through the V2 entry point.
                        let projection =
                            uni_algo::algo::procedure_template::build_projection_from_direct_args(
                                proc.as_ref(),
                                &algo_ctx,
                                &args,
                            )
                            .await
                            .map_err(|e| {
                                datafusion::error::DataFusionError::Execution(format!(
                                    "{algo_name}: Direct projection build failed: {e}"
                                ))
                            })?;
                        proc.execute_with_projection(algo_ctx, args, projection)
                    }
                }
                V2Plan::Cypher {
                    node_query,
                    edge_query,
                    weight_column,
                    include_reverse,
                    args,
                } => {
                    // Materialise the projection by running the two
                    // inner queries against the outer L0 snapshot.
                    let inner_params = std::collections::HashMap::new();
                    let node_rows = host
                        .execute_inner_query(
                            &node_query,
                            &inner_params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "{algo_name}: Cypher projection node query failed: {e}"
                            ))
                        })?;
                    let edge_rows = host
                        .execute_inner_query(
                            &edge_query,
                            &inner_params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "{algo_name}: Cypher projection edge query failed: {e}"
                            ))
                        })?;
                    let projection = uni_algo::algo::projection::GraphProjection::from_rows(
                        &node_rows,
                        &edge_rows,
                        weight_column.as_deref(),
                        include_reverse,
                    )
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{algo_name}: Cypher projection schema: {e}"
                        ))
                    })?;
                    proc.execute_with_projection(algo_ctx, args, projection)
                }
                V2Plan::Named { projection, args } => {
                    // ProjectionStore returned an Arc<GraphProjection>;
                    // try_unwrap so we move the projection into the
                    // algorithm when no other reader holds it, else
                    // clone the inner contents.
                    let owned = Arc::try_unwrap(projection).unwrap_or_else(|arc| (*arc).clone());
                    proc.execute_with_projection(algo_ctx, args, owned)
                }
            };
            let mut rows: Vec<AlgoResultRow> = Vec::new();
            while let Some(row_res) = algo_stream.next().await {
                if rows.len().is_multiple_of(1000) {
                    host.check_timeout().map_err(exec_err)?;
                }
                let row = row_res.map_err(exec_err)?;
                rows.push(row);
            }
            build_algo_record_batch(&rows, &algo_yields, &plugin_yields)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Dispatch shape for the V2 `(graphRef, config)` adapter path.
///
/// Native and legacy resolve to [`V2Plan::Direct`] (just forward the
/// positional vector to `AlgoProcedure::execute`); Cypher carries the
/// inner queries so the async stream can materialise the projection
/// before calling [`AlgoProcedure::execute_with_projection`]; Named
/// carries the stored projection looked up from `ProjectionStore`.
enum V2Plan {
    /// Positional arg vector ready for `AlgoProcedure::execute`.
    Direct(Vec<serde_json::Value>),
    /// Cypher projection — inner queries deferred to the async path.
    Cypher {
        node_query: String,
        edge_query: String,
        weight_column: Option<String>,
        include_reverse: bool,
        /// Argument vector for `execute_with_projection`: positions 0
        /// and 1 are empty arrays (placeholders for `nodeLabels` /
        /// `edgeTypes`); positions 2.. are the algorithm-specific
        /// args drawn from `config`.
        args: Vec<serde_json::Value>,
    },
    /// Pre-materialised projection from `ProjectionStore`.
    Named {
        /// Projection looked up from the store.
        projection: Arc<uni_algo::algo::GraphProjection>,
        /// Argument vector for `execute_with_projection`.
        args: Vec<serde_json::Value>,
    },
}

impl V2Plan {
    /// Decide which `V2Plan` variant a parsed [`ProjectionInput`] maps
    /// to and prepare the positional argument vector the algorithm
    /// will see.
    fn from_projection(
        algo_name: &str,
        proc: &dyn AlgoProcedure,
        projection: ProjectionInput,
        config: serde_json::Value,
        store: &crate::projection_store::ProjectionStore,
    ) -> Result<Self, FnError> {
        use serde_json::Value as J;

        let mut config_obj = match config {
            J::Object(m) => m,
            J::Null => serde_json::Map::new(),
            other => {
                return Err(FnError::new(
                    0x820,
                    format!("config must be a Map, got {other}"),
                ));
            }
        };

        match projection {
            ProjectionInput::Native {
                node_labels,
                edge_types,
                weight_property,
                include_reverse: _,
            } => {
                if let Some(weight) = weight_property
                    && !config_obj.contains_key("weightProperty")
                {
                    config_obj.insert("weightProperty".to_owned(), J::String(weight));
                }
                Ok(V2Plan::Direct(build_legacy_arg_vec(
                    proc,
                    node_labels,
                    edge_types,
                    &config_obj,
                )))
            }
            ProjectionInput::Cypher {
                node_query,
                edge_query,
                weight_column,
                include_reverse,
            } => {
                // `graphRef.weightColumn` mirrors into the algorithm's
                // `weightProperty` config key for algos that read it.
                if let Some(col) = &weight_column
                    && !config_obj.contains_key("weightProperty")
                {
                    config_obj.insert("weightProperty".to_owned(), J::String(col.clone()));
                }
                // Build the arg vector with empty label/edge placeholders;
                // `execute_with_projection` ignores positions 0/1 entirely.
                let args = build_legacy_arg_vec(proc, Vec::new(), Vec::new(), &config_obj);
                Ok(V2Plan::Cypher {
                    node_query,
                    edge_query,
                    weight_column,
                    include_reverse,
                    args,
                })
            }
            ProjectionInput::Named { name } => {
                let entry = store.get(&name).ok_or_else(|| {
                    FnError::new(
                        0x822,
                        format!(
                            "{algo_name}: no projection named `{name}` \
                             in the ProjectionStore; call `uni.graph.project` first"
                        ),
                    )
                })?;
                let args = build_legacy_arg_vec(proc, Vec::new(), Vec::new(), &config_obj);
                Ok(V2Plan::Named {
                    projection: entry.projection,
                    args,
                })
            }
        }
    }
}

/// Assemble the positional `Vec<serde_json::Value>` for an algorithm
/// from the canonical 2-arg `(graphRef Native, config)` shape.
///
/// Walks the algorithm's *native* `uni-algo::ProcedureSignature`
/// (`proc.signature()`) so we can read each optional arg's declared
/// default. We compute the highest optional slot that `config` actually
/// supplies; the result is truncated to that length so any unsupplied
/// optionals beyond it stay missing and `validate_args` substitutes
/// them from `signature.optional_args` defaults.
fn build_legacy_arg_vec(
    proc: &dyn AlgoProcedure,
    node_labels: Vec<String>,
    edge_types: Vec<String>,
    config: &serde_json::Map<String, serde_json::Value>,
) -> Vec<serde_json::Value> {
    use serde_json::Value as J;
    let sig = proc.signature();
    let req = &sig.args; // required (always [nodeLabels, relationshipTypes])
    let opt = &sig.optional_args; // optional, in declared order

    let last_supplied = opt
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, (n, _, _))| config.contains_key(*n).then_some(i));

    let mut out: Vec<J> = Vec::with_capacity(req.len() + opt.len());
    out.push(J::Array(node_labels.into_iter().map(J::String).collect()));
    out.push(J::Array(edge_types.into_iter().map(J::String).collect()));
    let Some(end) = last_supplied else {
        return out; // bare 2-slot internal vector — no optional args supplied.
    };
    for (name, _ty, default) in opt.iter().take(end + 1) {
        let v = config
            .get(*name)
            .cloned()
            .unwrap_or_else(|| default.clone());
        out.push(v);
    }
    out
}

/// Convert a DataFusion `ScalarValue` into a `uni_common::Value` for the
/// algo crate. Covers the primitives the algo CALL signatures use; falls
/// back to `Null` for other shapes (algos don't currently take complex
/// arg types).
fn scalar_value_to_uni_value(sv: ScalarValue) -> Value {
    match sv {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(b)) => Value::Bool(b),
        ScalarValue::Int64(Some(i)) => Value::Int(i),
        ScalarValue::Int32(Some(i)) => Value::Int(i64::from(i)),
        ScalarValue::UInt64(Some(u)) => i64::try_from(u).map(Value::Int).unwrap_or(Value::Null),
        ScalarValue::Float64(Some(f)) => Value::Float(f),
        ScalarValue::Float32(Some(f)) => Value::Float(f64::from(f)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Value::String(s),
        ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => Value::Bytes(b),
        _ => Value::Null,
    }
}

/// Project the algorithm's `AlgoResultRow`s onto the plugin's declared
/// Arrow schema. Mirrors `procedure_call.rs::build_algo_batch` but
/// operates on the plugin's `signature.yields` (Arrow `Field`s) directly
/// rather than the legacy yield-name-and-alias pairs.
fn build_algo_record_batch(
    rows: &[AlgoResultRow],
    algo_yields: &[(&'static str, ValueType)],
    plugin_yields: &[Field],
) -> Result<RecordBatch, datafusion::error::DataFusionError> {
    let out_schema = Arc::new(Schema::new(plugin_yields.to_vec()));
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(out_schema));
    }

    let num_rows = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(plugin_yields.len());

    for (idx, field) in plugin_yields.iter().enumerate() {
        // The plugin schema is built 1:1 from the algo schema, so column
        // `idx` corresponds to `algo_yields[idx]`. Defensive: fall back
        // to a name lookup if the orders ever drift.
        let algo_idx = if idx < algo_yields.len() && algo_yields[idx].0 == field.name() {
            idx
        } else {
            algo_yields
                .iter()
                .position(|(n, _)| *n == field.name())
                .unwrap_or(usize::MAX)
        };

        // A list, map or path yield is a `LargeBinary` CypherValue column and
        // takes the value itself; `json_to_value` would flatten it to text.
        let structured = matches!(field.data_type(), DataType::LargeBinary);
        let uni_values: Vec<Value> = rows
            .iter()
            .map(|row| {
                if algo_idx == usize::MAX || algo_idx >= row.values.len() {
                    Value::Null
                } else if structured {
                    Value::from(row.values[algo_idx].clone())
                } else {
                    json_to_value(&row.values[algo_idx])
                }
            })
            .collect();

        let values_iter = uni_values.iter().map(Some);
        columns.push(build_typed_column(values_iter, num_rows, field.data_type()));
    }

    RecordBatch::try_new(out_schema, columns).map_err(crate::query::df_graph::common::arrow_err)
}

/// Build a `uni-plugin` `ProcedureSignature` from an algorithm's
/// `uni-algo` signature. The args list combines required + optional.
fn build_plugin_signature(
    proc: &dyn AlgoProcedure,
) -> (ProcedureSignature, Vec<(&'static str, ValueType)>) {
    let algo_sig = proc.signature();

    let mut args: Vec<NamedArgType> =
        Vec::with_capacity(algo_sig.args.len() + algo_sig.optional_args.len());
    for (name, vt) in &algo_sig.args {
        args.push(arg(name, ArgType::Primitive(value_type_to_arrow(vt)), ""));
    }
    for (name, vt, default) in &algo_sig.optional_args {
        args.push(NamedArgType {
            name: smol_str::SmolStr::new(*name),
            ty: ArgType::Primitive(value_type_to_arrow(vt)),
            default: serde_json_to_scalar(default, vt),
            doc: String::new(),
        });
    }

    let yields: Vec<Field> = algo_sig
        .yields
        .iter()
        .map(|(name, vt)| algo_yield_field(name, vt))
        .collect();

    let plugin_sig = ProcedureSignature {
        args,
        yields,
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: format!("uni.{} (algorithm adapter)", proc.name()),
    };
    (plugin_sig, algo_sig.yields)
}

/// Best-effort conversion of an algo optional-arg default to a
/// DataFusion `ScalarValue`. Returns `None` if the shape doesn't map
/// cleanly; callers requesting the optional arg without supplying it
/// will then see `Null` and the algo's own `validate_args` re-applies
/// the default from `signature().optional_args`.
fn serde_json_to_scalar(v: &serde_json::Value, vt: &ValueType) -> Option<ScalarValue> {
    match (v, vt) {
        (serde_json::Value::Null, _) => Some(ScalarValue::Null),
        (serde_json::Value::Bool(b), _) => Some(ScalarValue::Boolean(Some(*b))),
        (serde_json::Value::Number(n), ValueType::Int) => {
            n.as_i64().map(|i| ScalarValue::Int64(Some(i)))
        }
        (serde_json::Value::Number(n), _) => n.as_f64().map(|f| ScalarValue::Float64(Some(f))),
        (serde_json::Value::String(s), _) => Some(ScalarValue::Utf8(Some(s.clone()))),
        _ => None,
    }
}

/// Run a registered [`AlgorithmProvider`] and return its result stream.
///
/// Builds an [`AlgorithmHostBridge`] carrying the algorithm's storage, L0
/// snapshot, and the owning plugin's effective capabilities, then invokes
/// `provider.run`. The returned `SendableRecordBatchStream` is `'static`
/// (the provider extracts owned handles from the host before returning),
/// so the transient bridge dropped here does not invalidate it.
///
/// Shared by both CALL dispatch paths (the planned df_graph path and the
/// simple executor fallback), each of which adapts the stream to its own
/// output shape.
///
/// # Errors
///
/// Returns [`FnError`] if the provider cannot start — including when the
/// owning plugin lacks the `HostQuery` capability the bridge's
/// `project` gate requires.
/// Resolves a Cypher/Named `graphRef` for a guest GraphCompute algorithm by
/// reusing the native `uni.algo.*` projection machinery (issue #151 P3). It is
/// injected into the `AlgorithmHostBridge`, which cannot reach query execution
/// or the projection store across the `uni-query → uni-plugin-builtin` edge.
struct GuestGraphResolver {
    host: QueryProcedureHost,
}

impl uni_plugin_builtin::algorithms::bridge::GraphProjectionResolver for GuestGraphResolver {
    fn resolve(
        &self,
        graph_ref: serde_json::Value,
    ) -> futures::future::BoxFuture<
        'static,
        Result<Arc<uni_algo::algo::projection::GraphProjection>, FnError>,
    > {
        let host = self.host.clone();
        Box::pin(async move {
            match parse_graph_ref(&graph_ref)
                .map_err(|e| FnError::new(0x820, format!("graphRef parse: {e}")))?
            {
                ProjectionInput::Cypher {
                    node_query,
                    edge_query,
                    weight_column,
                    include_reverse,
                } => {
                    let params = std::collections::HashMap::new();
                    let node_rows = host
                        .execute_inner_query(
                            &node_query,
                            &params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| FnError::new(0x803, format!("Cypher node query: {e}")))?;
                    let edge_rows = host
                        .execute_inner_query(
                            &edge_query,
                            &params,
                            uni_plugin::traits::procedure::ProcedureMode::Read,
                        )
                        .await
                        .map_err(|e| FnError::new(0x803, format!("Cypher edge query: {e}")))?;
                    let projection = uni_algo::algo::projection::GraphProjection::from_rows(
                        &node_rows,
                        &edge_rows,
                        weight_column.as_deref(),
                        include_reverse,
                    )
                    .map_err(|e| FnError::new(0x803, format!("Cypher projection schema: {e}")))?;
                    Ok(Arc::new(projection))
                }
                ProjectionInput::Named { name } => {
                    let store = crate::projection_store::for_storage(host.storage());
                    let entry = store.get(&name).ok_or_else(|| {
                        FnError::new(0x803, format!("named projection `{name}` not found"))
                    })?;
                    Ok(entry.projection)
                }
                ProjectionInput::Native { .. } => Err(FnError::new(
                    0x820,
                    "Native graphRef is handled by the spec scan, not the guest resolver",
                )),
            }
        })
    }
}

/// Build a resolver for guest Cypher/Named projections from a query host.
pub(crate) fn guest_graph_resolver(
    host: QueryProcedureHost,
) -> Arc<dyn uni_plugin_builtin::algorithms::bridge::GraphProjectionResolver> {
    Arc::new(GuestGraphResolver { host })
}

/// If the trailing config object is a Cypher/Named `graphRef` (bearing
/// `nodeQuery`/`edgeQuery`/`name`) rather than a Native scoping object, extract
/// it so the bridge resolves it through the injected resolver (issue #151 P3).
fn extract_query_graph_ref(config_json: &str) -> Option<serde_json::Value> {
    let args: Vec<serde_json::Value> = serde_json::from_str(config_json).ok()?;
    let obj = args.last()?.as_object()?;
    uni_plugin::traits::algorithm::GraphProjectionSpec::is_query_graph_ref(obj)
        .then(|| args.last().cloned())
        .flatten()
}

/// Whether any **named scope** on the trailing config object is Cypher/Named.
///
/// The primary projection and each scope choose their mode independently, so a
/// Native primary with a Cypher scope still needs the resolver installed. Without
/// this check that scope would fall through to a Native storage scan of an empty
/// spec and silently project the wrong graph — the drift the shared
/// `is_query_graph_ref` predicate exists to prevent.
fn has_query_scope(config_json: &str) -> bool {
    let Ok(args) = serde_json::from_str::<Vec<serde_json::Value>>(config_json) else {
        return false;
    };
    args.last()
        .and_then(serde_json::Value::as_object)
        .and_then(|o| o.get("scopes"))
        .and_then(serde_json::Value::as_object)
        .is_some_and(|scopes| {
            scopes
                .values()
                .filter_map(serde_json::Value::as_object)
                .any(uni_plugin::traits::algorithm::GraphProjectionSpec::is_query_graph_ref)
        })
}

pub(crate) fn run_algorithm_provider(
    entry: &uni_plugin::registry::AlgorithmEntry,
    storage: Arc<uni_store::storage::manager::StorageManager>,
    l0_manager: Option<Arc<uni_store::runtime::L0Manager>>,
    property_manager: Arc<uni_store::runtime::property_manager::PropertyManager>,
    config_json: &str,
    resolver: Option<Arc<dyn uni_plugin_builtin::algorithms::bridge::GraphProjectionResolver>>,
) -> Result<SendableRecordBatchStream, FnError> {
    use uni_plugin::traits::algorithm::AlgorithmContext;

    let mut bridge = uni_plugin_builtin::algorithms::bridge::host_bridge_from_storage(
        storage,
        l0_manager,
        entry.effective_caps.clone(),
    )
    .with_property_manager(Some(property_manager));
    // Cypher/Named graphRef (issue #151 P3): route projection through the
    // injected resolver instead of the Native storage scan. If no resolver is
    // available on this execution path, fail clearly rather than silently
    // projecting the whole graph.
    let primary_ref = extract_query_graph_ref(config_json);
    if primary_ref.is_some() || has_query_scope(config_json) {
        let resolver = resolver.ok_or_else(|| {
            FnError::new(
                0x820,
                "Cypher/Named graph projection is not available on this execution path",
            )
        })?;
        // A Cypher *scope* under a Native primary installs the resolver without a
        // primary ref, so the primary still takes the storage-scan path.
        bridge = match primary_ref {
            Some(graph_ref) => bridge.with_graph_resolver(resolver, graph_ref),
            None => bridge.with_resolver(resolver),
        };
    }
    // Host-side arg validation/coercion (proposal §4.6 / D7): a provider that
    // declares typed `args` gets arity + type checking and default-filling here,
    // before it runs; providers on the legacy untyped contract (empty `args`)
    // see `config_json` pass through unchanged.
    let coerced = entry.provider.signature().coerce_config_json(config_json)?;
    let ctx = AlgorithmContext::new(&coerced).with_host(&bridge);
    entry.provider.run(ctx)
}

/// Register every `uni.algo.*` adapter from `algo_registry` into `r`.
///
/// One `ProcedurePlugin` registration per entry; the adapter retains an
/// `Arc` to the algorithm so the registry itself can be dropped after
/// registration without invalidating the plugins.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is already
/// taken in the underlying plugin registry.
pub fn register_into(
    r: &mut PluginRegistrar<'_>,
    algo_registry: &Arc<AlgorithmRegistry>,
) -> Result<(), PluginError> {
    for name in algo_registry.list() {
        let Some(proc) = algo_registry.get(name) else {
            continue;
        };
        let (signature, algo_yields) = build_plugin_signature(&*proc);
        let suffix = name.strip_prefix("uni.").unwrap_or(name).to_owned();
        let adapter = Arc::new(AlgorithmProcedureAdapter {
            proc,
            signature: signature.clone(),
            algo_yields,
        });
        r.procedure(QName::new("uni", suffix), signature, adapter)?;
    }
    Ok(())
}
