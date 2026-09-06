//! Bridge wiring `uni_algo::AlgoProcedure` into
//! `uni_plugin::AlgorithmProvider`.
//!
//! The bridge implements `AlgorithmProvider::run` by:
//! 1. Parsing `config_json` into `Vec<serde_json::Value>` args (the
//!    shape the algo expects from `CALL`).
//! 2. Downcasting `AlgorithmContext::host` to [`AlgorithmHostBridge`] to
//!    recover the concrete `AlgoContext` (StorageManager + L0Manager).
//! 3. Driving the algorithm's `AlgoResultRow` stream to completion and
//!    collecting it into a single Arrow `RecordBatch` matching the
//!    declared `AlgorithmSignature::output_fields`.
//!
//! When no host is bound, the bridge returns an
//! `Unbound` error code so the caller can supply the host on retry.
//
// Rust guideline compliant

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use futures::future::BoxFuture;
use uni_algo::algo::procedures::{AlgoContext, AlgoProcedure, AlgoResultRow, ValueType};
use uni_algo::algo::projection::{GraphProjection, ProjectionBuilder};
use uni_common::core::id::Vid;
use uni_plugin::traits::algorithm::{
    AlgorithmContext, AlgorithmHost, AlgorithmProvider, AlgorithmSignature, GraphProjectionSpec,
    GraphScopeSpec, GraphView,
};
use uni_plugin::{Capability, CapabilitySet, FnError};

/// Read-only [`GraphView`] backed by a materialized [`GraphProjection`].
///
/// Slot-indexed accessors delegate directly to the projection's dense CSR
/// arrays; see [`GraphView`] for the panic contract on weights / reverse.
pub struct GraphViewImpl(Arc<GraphProjection>);

impl std::fmt::Debug for GraphViewImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphViewImpl")
            .field("vertex_count", &self.0.vertex_count())
            .field("edge_count", &self.0.edge_count())
            .finish_non_exhaustive()
    }
}

impl GraphView for GraphViewImpl {
    fn vertex_count(&self) -> usize {
        self.0.vertex_count()
    }
    fn edge_count(&self) -> usize {
        self.0.edge_count()
    }
    fn out_neighbors(&self, slot: u32) -> &[u32] {
        self.0.out_neighbors(slot)
    }
    fn out_degree(&self, slot: u32) -> u32 {
        self.0.out_degree(slot)
    }
    fn in_neighbors(&self, slot: u32) -> &[u32] {
        self.0.in_neighbors(slot)
    }
    fn in_degree(&self, slot: u32) -> u32 {
        self.0.in_degree(slot)
    }
    fn has_reverse(&self) -> bool {
        self.0.has_reverse()
    }
    fn out_weight(&self, slot: u32, edge_idx: usize) -> f64 {
        self.0.out_weight(slot, edge_idx)
    }
    fn has_weights(&self) -> bool {
        self.0.has_weights()
    }
    fn to_vid(&self, slot: u32) -> Vid {
        self.0.to_vid(slot)
    }
    fn to_slot(&self, vid: Vid) -> Option<u32> {
        self.0.to_slot(vid)
    }
    fn vertices(&self) -> Box<dyn Iterator<Item = (u32, Vid)> + '_> {
        Box::new(self.0.vertices())
    }
}

/// Bridge host that surfaces `StorageManager` + optional `L0Manager`
/// to plugin algorithms through [`AlgorithmHost`].
///
/// Provides [`AlgorithmHost::project`] (gated on [`Capability::HostQuery`])
/// as the stable topology-access path, and retains
/// [`AlgorithmHost::as_any`] for the legacy downcast used by
/// [`AlgoProviderBridge`].
pub struct AlgorithmHostBridge {
    /// The concrete algo context the wrapped procedures need.
    pub algo_ctx: AlgoContext,
    /// Effective capabilities of the plugin owning the running algorithm.
    pub effective_caps: CapabilitySet,
    /// Cypher/Named `graphRef` + its resolver (issue #151 P3). When both are set,
    /// `project_for_graph_compute` resolves the projection through the injected
    /// resolver instead of scanning storage from the (empty) Native spec.
    resolver: Option<Arc<dyn GraphProjectionResolver>>,
    graph_ref: Option<serde_json::Value>,
}

/// Resolves a Cypher/Named `graphRef` into a materialized [`GraphProjection`].
///
/// Defined here (uni-plugin-builtin) so a uni-query type can implement it and be
/// injected into the bridge (issue #151 P3): the bridge cannot reach query
/// execution or the projection store across the `uni-query → uni-plugin-builtin`
/// dependency edge, so the query-side machinery is supplied by inversion.
pub trait GraphProjectionResolver: Send + Sync {
    /// Materialize the projection named by `graph_ref` (a Cypher or Named
    /// graphRef object). Runs in the bridge's async context.
    fn resolve(
        &self,
        graph_ref: serde_json::Value,
    ) -> BoxFuture<'static, Result<Arc<GraphProjection>, FnError>>;
}

impl std::fmt::Debug for AlgorithmHostBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmHostBridge")
            .field("effective_caps", &self.effective_caps)
            .field("has_graph_ref", &self.graph_ref.is_some())
            .finish_non_exhaustive()
    }
}

impl AlgorithmHostBridge {
    /// Construct a host bridge from an [`AlgoContext`] and effective caps.
    #[must_use]
    pub fn new(algo_ctx: AlgoContext, effective_caps: CapabilitySet) -> Self {
        Self {
            algo_ctx,
            effective_caps,
            resolver: None,
            graph_ref: None,
        }
    }

    /// Attach a Cypher/Named `graphRef` and the resolver that materializes it
    /// (issue #151 P3). When set, `project_for_graph_compute` resolves through
    /// `resolver` rather than scanning storage.
    #[must_use]
    pub fn with_graph_resolver(
        mut self,
        resolver: Arc<dyn GraphProjectionResolver>,
        graph_ref: serde_json::Value,
    ) -> Self {
        self.resolver = Some(resolver);
        self.graph_ref = Some(graph_ref);
        self
    }

    /// Attach the resolver without a primary `graphRef`.
    ///
    /// Needed when the primary projection is Native but a **named scope** is
    /// Cypher/Named: the resolver must be present for the scope, while the
    /// primary still takes the storage-scan path. Without this the scope would
    /// silently fall through to a Native scan of an empty spec.
    #[must_use]
    pub fn with_resolver(mut self, resolver: Arc<dyn GraphProjectionResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Whether the effective `HostQuery` grant names a restricting scope (a
    /// non-empty prefix list that is not the universal `**`/`*` wildcard).
    fn host_query_scope_restricted(&self) -> bool {
        let scopes: Vec<String> = self
            .effective_caps
            .iter()
            .find_map(|c| match c {
                Capability::HostQuery { scopes, .. } => {
                    Some(scopes.iter().map(ToString::to_string).collect())
                }
                _ => None,
            })
            .unwrap_or_default();
        !scopes.is_empty() && !scopes.iter().any(|p| p == "**" || p == "*")
    }

    /// Builds a concrete projection for GraphCompute kernels, gated on caps.
    ///
    /// Unlike [`AlgorithmHost::project`] (which yields an opaque
    /// `Arc<dyn GraphView>`), this returns the concrete `Arc<GraphProjection>`
    /// an [`AlgoSession`](crate::algorithms::graph_compute::AlgoSession) binds.
    /// It enforces the two orthogonal gates of the proposal (§4.6):
    /// [`Capability::GraphCompute`] for the kernel surface and
    /// [`Capability::HostQuery`] for the data read.
    ///
    /// # Errors
    /// Returns `0x86C` if `GraphCompute` is not granted, `0x804` if `HostQuery`
    /// is not granted, or `0x803` if the projection build fails.
    pub fn project_for_graph_compute(
        &self,
        spec: &GraphProjectionSpec,
    ) -> BoxFuture<'static, Result<Arc<GraphProjection>, FnError>> {
        self.project_scope(spec, self.graph_ref.clone())
    }

    /// Projects one scope, taking its Cypher/Named `graphRef` per call.
    ///
    /// [`Self::project_for_graph_compute`] reads the bridge's single stored
    /// `graph_ref`, which is right for the primary projection but cannot express
    /// a `scopes` map where each entry may be Native or Cypher independently.
    /// This takes the ref as an argument instead; passing `None` forces the
    /// Native storage scan even when the bridge holds a ref for the primary.
    ///
    /// Returns a `'static` future that clones everything it needs, so N scopes
    /// can be built by collecting N futures before entering the result stream —
    /// which is what the loader adapters do, so no borrow of the host escapes.
    ///
    /// # Errors
    /// As [`Self::project_for_graph_compute`].
    pub fn project_scope(
        &self,
        spec: &GraphProjectionSpec,
        graph_ref: Option<serde_json::Value>,
    ) -> BoxFuture<'static, Result<Arc<GraphProjection>, FnError>> {
        if !self
            .effective_caps
            .contains_variant(&Capability::GraphCompute)
        {
            return Box::pin(async {
                Err(FnError::new(
                    crate::algorithms::graph_compute::error::CAPABILITY_DENIED,
                    "GraphCompute: capability `graph-compute` not granted",
                ))
            });
        }
        if !self
            .effective_caps
            .contains_variant(&Capability::HostQuery {
                read_only: false,
                scopes: Vec::new(),
            })
        {
            return Box::pin(async {
                Err(FnError::new(
                    0x804,
                    "GraphCompute: `project` additionally requires `HostQuery`",
                ))
            });
        }

        // Cypher/Named graphRef (issue #151 P3): resolved by the injected
        // uni-query resolver in this async context, since the bridge cannot reach
        // query execution or the projection store. A restricting HostQuery scope
        // cannot be checked against a query-defined subgraph, so reject
        // fail-closed and require an unscoped grant.
        if let (Some(resolver), Some(graph_ref)) = (self.resolver.clone(), graph_ref) {
            let restricted = self.host_query_scope_restricted();
            return Box::pin(async move {
                if restricted {
                    return Err(FnError::new(
                        0x804,
                        "GraphCompute: a Cypher/Named projection requires an unscoped HostQuery \
                         grant (a restricting scope cannot gate a query-defined subgraph)",
                    ));
                }
                resolver.resolve(graph_ref).await
            });
        }

        // Enforce the HostQuery scope restriction (E5): when the grant names
        // scopes (label / edge-type prefixes), every projected label and edge
        // type must match one — a plugin scoped to `Person` cannot project the
        // whole graph. An empty scope list is unrestricted (the default).
        let scope_prefixes: Vec<String> = self
            .effective_caps
            .iter()
            .find_map(|c| match c {
                Capability::HostQuery { scopes, .. } => {
                    Some(scopes.iter().map(ToString::to_string).collect())
                }
                _ => None,
            })
            .unwrap_or_default();
        // A `**`/`*` scope (the default `HostQuery` grant, and what the Python
        // `"HostQuery"` grant string parses to) is unrestricted; only a narrower
        // prefix list actually gates which labels a guest may project.
        let scope_restricted =
            !scope_prefixes.is_empty() && !scope_prefixes.iter().any(|p| p == "**" || p == "*");

        // Fail-loud (G9): projecting the whole graph must be a deliberate choice,
        // not the silent default. Regardless of grant width, an unscoped
        // projection (empty node_labels AND edge_types) requires either explicit
        // nodeLabels/edgeTypes or an explicit `projectAll: true`. The #151 guard
        // below only fired under a *restricted* scope, so under the default `**`
        // grant an unscoped projection used to silently pull in every declared
        // label — corrupting index-keyed kernels when unrelated data (e.g. a
        // coexisting MCTS search tree) shares the store. First-party providers
        // opt into the whole graph by setting `project_all`; guests must pass
        // `projectAll: true` in their config on purpose.
        if spec.node_labels.is_empty() && spec.edge_types.is_empty() && !spec.project_all {
            return Box::pin(async {
                Err(FnError::new(
                    0x804,
                    "GraphCompute: an unscoped projection is not allowed; name \
                     nodeLabels/edgeTypes explicitly, or set projectAll:true to \
                     deliberately project the whole graph",
                ))
            });
        }
        if scope_restricted {
            // Fail-closed (issue #151): a whole-graph projection under a restricted
            // HostQuery grant defeats the scope, so it is rejected even when the
            // caller opted in via `projectAll` — the opt-in cannot override a
            // restricting scope. The guest must name in-scope labels / edge types.
            if spec.node_labels.is_empty() && spec.edge_types.is_empty() {
                let scopes = scope_prefixes.join(", ");
                return Box::pin(async move {
                    Err(FnError::new(
                        0x804,
                        format!(
                            "GraphCompute: an unscoped projection is not allowed under restricted \
                             HostQuery scopes [{scopes}]; name nodeLabels/edgeTypes explicitly \
                             (projectAll does not override a restricting scope)"
                        ),
                    ))
                });
            }
            let in_scope = |name: &str| scope_prefixes.iter().any(|p| name.starts_with(p.as_str()));
            let denied = spec
                .node_labels
                .iter()
                .chain(spec.edge_types.iter())
                .find(|name| !in_scope(name));
            if let Some(name) = denied {
                let name = name.clone();
                let scopes = scope_prefixes.join(", ");
                return Box::pin(async move {
                    Err(FnError::new(
                        0x804,
                        format!(
                            "GraphCompute: `{name}` is outside the granted HostQuery scopes [{scopes}]"
                        ),
                    ))
                });
            }
        }
        // G11: a whole-graph projection (no nodeLabels/edgeTypes named) must not
        // silently omit vertices whose label is present in storage/L0 but absent
        // from the schema — uni-db permits schemaless labels via `CREATE (:X)`
        // without a prior `schema().label("X")`, and the projection enumerates
        // only *declared* labels. Detect the drift and fail loud so "the whole
        // graph" cannot quietly drop them. A *scoped* projection is exempt: it
        // names exactly what it wants, and a named-but-undeclared label already
        // errors in ProjectionBuilder::resolve_ids.
        if spec.node_labels.is_empty() && spec.edge_types.is_empty() {
            let schema = self.algo_ctx.storage.schema_manager().schema();
            let declared: std::collections::HashSet<String> =
                schema.labels.keys().cloned().collect();
            // Sorted, deduped union of physically-present vertex labels: flushed
            // (storage index) + unflushed (every live L0 generation).
            let mut present: std::collections::BTreeSet<String> = self
                .algo_ctx
                .storage
                .physical_vertex_label_names()
                .into_iter()
                .collect();
            if let Some(l0) = &self.algo_ctx.l0_manager {
                let mut bufs = l0.get_pending_flush();
                bufs.push(l0.get_current());
                for buf in bufs {
                    present.extend(buf.read().label_to_vids.keys().cloned());
                }
            }
            let undeclared: Vec<String> = present
                .into_iter()
                .filter(|n| !declared.contains(n))
                .collect();
            if !undeclared.is_empty() {
                let names = undeclared.join(", ");
                return Box::pin(async move {
                    Err(FnError::new(
                        0x804,
                        format!(
                            "GraphCompute: a whole-graph projection would silently omit vertices \
                             with undeclared (schemaless) label(s) [{names}]; declare them via \
                             schema().label(...) or name nodeLabels explicitly"
                        ),
                    ))
                });
            }
        }
        let storage = Arc::clone(&self.algo_ctx.storage);
        let l0 = self.algo_ctx.l0_manager.as_ref().map(Arc::clone);
        let spec = spec.clone();
        Box::pin(async move {
            let projection = build_projection(storage, l0, spec, "GraphCompute project").await?;
            Ok(Arc::new(projection))
        })
    }

    /// Reads the per-invocation GraphCompute work and arena-byte caps.
    ///
    /// Uses a plugin's declared [`Capability::GraphComputeWork`] /
    /// [`Capability::GraphComputeArenaBytes`] quota when present, otherwise the
    /// pinned defaults (proposal §12). The work cap is returned verbatim (a
    /// `Some(w)` grant survives capability attenuation unchanged, test G-3) and
    /// resolved against the size-derived default by
    /// [`WorkBudget::resolve`](crate::algorithms::graph_compute::WorkBudget::resolve),
    /// where an explicit grant is authoritative and may *raise* the ceiling
    /// (proposal §9). The work grant and the arena-byte cap are independent
    /// dimensions (test G-6).
    #[must_use]
    pub fn graph_compute_caps(&self) -> (Option<u64>, usize) {
        let mut work = None;
        let mut arena = crate::algorithms::graph_compute::DEFAULT_ARENA_MAX_BYTES;
        for cap in self.effective_caps.iter() {
            match cap {
                Capability::GraphComputeWork(w) => work = Some(*w),
                Capability::GraphComputeArenaBytes(b) => {
                    arena = usize::try_from(*b).unwrap_or(usize::MAX);
                }
                _ => {}
            }
        }
        (work, arena)
    }

    /// Reads the per-invocation wall-clock deadline for a GraphCompute guest.
    ///
    /// Returns the plugin's declared [`Capability::WallClockMillisPerCall`] grant
    /// in milliseconds, if present. A loader uses it to arm its watchdog /
    /// deadline; absent, the loader applies its own default.
    #[must_use]
    pub fn graph_compute_deadline_ms(&self) -> Option<u64> {
        self.effective_caps.iter().find_map(|cap| match cap {
            Capability::WallClockMillisPerCall(ms) => Some(*ms),
            _ => None,
        })
    }
}

impl AlgorithmHost for AlgorithmHostBridge {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn project(
        &self,
        spec: &GraphProjectionSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn GraphView>, FnError>> {
        // Gate topology access on `HostQuery` (variant match; payload
        // attenuation is applied at registration-time intersection).
        if !self
            .effective_caps
            .contains_variant(&Capability::HostQuery {
                read_only: false,
                scopes: Vec::new(),
            })
        {
            return Box::pin(async {
                Err(FnError::new(
                    0x804,
                    "AlgorithmHost::project: capability `HostQuery` not granted",
                ))
            });
        }

        // Clone owned inputs into the `'static` future so it can be moved
        // into the stream a provider returns from the synchronous `run`.
        let storage = Arc::clone(&self.algo_ctx.storage);
        let l0 = self.algo_ctx.l0_manager.as_ref().map(Arc::clone);
        let spec = spec.clone();

        Box::pin(async move {
            let projection = build_projection(storage, l0, spec, "AlgorithmHost::project").await?;
            Ok(Arc::new(GraphViewImpl(Arc::new(projection))) as Arc<dyn GraphView>)
        })
    }
}

/// Materializes a [`GraphProjection`] from a [`GraphProjectionSpec`].
///
/// Shared by [`AlgorithmHostBridge::project_scope`] and the
/// [`AlgorithmHost::project`] impl; `err_ctx` prefixes the build-failure
/// message so each entry point keeps its own wording.
async fn build_projection(
    storage: Arc<uni_store::storage::manager::StorageManager>,
    l0: Option<Arc<uni_store::runtime::L0Manager>>,
    spec: GraphProjectionSpec,
    err_ctx: &'static str,
) -> Result<GraphProjection, FnError> {
    let node_labels: Vec<&str> = spec.node_labels.iter().map(String::as_str).collect();
    let edge_types: Vec<&str> = spec.edge_types.iter().map(String::as_str).collect();
    let node_props: Vec<&str> = spec.node_properties.iter().map(String::as_str).collect();
    let edge_props: Vec<&str> = spec.edge_properties.iter().map(String::as_str).collect();
    let mut builder = ProjectionBuilder::new(storage)
        .l0_manager(l0)
        .node_labels(&node_labels)
        .edge_types(&edge_types)
        .include_reverse(spec.include_reverse)
        .node_properties(&node_props)
        .edge_properties(&edge_props);
    if let Some(prop) = spec.weight_property.as_deref() {
        builder = builder.weight_property(prop);
    }
    builder
        .build()
        .await
        .map_err(|e| FnError::new(0x803, format!("{err_ctx} build failed: {e}")))
}

/// Provider wrapping a single [`AlgoProcedure`].
pub struct AlgoProviderBridge {
    proc: Arc<dyn AlgoProcedure>,
    signature: AlgorithmSignature,
    yields: Vec<(&'static str, ValueType)>,
}

impl std::fmt::Debug for AlgoProviderBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgoProviderBridge")
            .field("name", &self.proc.name())
            .finish_non_exhaustive()
    }
}

impl AlgoProviderBridge {
    /// Wrap an `AlgoProcedure` as an `AlgorithmProvider`.
    #[must_use]
    pub fn new(proc: Arc<dyn AlgoProcedure>) -> Self {
        let sig = proc.signature();
        let output_fields: Vec<Field> = sig
            .yields
            .iter()
            .map(|(n, vt)| Field::new((*n).to_owned(), value_type_to_arrow(vt), true))
            .collect();
        let signature = AlgorithmSignature {
            output_fields,
            docs: format!("uni.{} (algorithm)", proc.name()),
            ..Default::default()
        };
        Self {
            proc,
            signature,
            yields: sig.yields,
        }
    }
}

impl AlgorithmProvider for AlgoProviderBridge {
    fn signature(&self) -> &AlgorithmSignature {
        &self.signature
    }

    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError> {
        let host = ctx
            .host
            .ok_or_else(|| FnError::new(0x800, "AlgoProviderBridge: host unbound"))?;
        let bridge = host
            .as_any()
            .downcast_ref::<AlgorithmHostBridge>()
            .ok_or_else(|| {
                FnError::new(0x801, "AlgoProviderBridge: host is not AlgorithmHostBridge")
            })?;

        let args: Vec<serde_json::Value> = if ctx.config_json.is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(ctx.config_json)
                .map_err(|e| FnError::new(0x802, format!("config_json parse: {e}")))?
        };

        // Clone what we need into the async stream; the wrapped
        // `AlgoContext` is `!Clone`, but `StorageManager` / `L0Manager`
        // inside are `Arc`, so we rebuild a fresh `AlgoContext` from
        // their clones.
        let algo_ctx = AlgoContext::new(
            Arc::clone(&bridge.algo_ctx.storage),
            bridge.algo_ctx.l0_manager.as_ref().map(Arc::clone),
        );
        let proc = Arc::clone(&self.proc);
        let yields = self.yields.clone();
        let fields = self.signature.output_fields.clone();
        let out_schema = Arc::new(Schema::new(fields.clone()));

        let stream = futures::stream::once(async move {
            // Same dispatch logic as `uni-query`'s V2Plan::Direct
            // branch: route cypher-path algos through
            // `execute_with_native_terminals`; everything else builds
            // a projection from `(nodeLabels, edgeTypes, …)` args and
            // takes the projection-aware entry point.
            let mut algo_stream = if proc.wants_native_terminals() {
                proc.execute_with_native_terminals(algo_ctx, args)
            } else {
                let projection =
                    uni_algo::algo::procedure_template::build_projection_from_direct_args(
                        proc.as_ref(),
                        &algo_ctx,
                        &args,
                    )
                    .await
                    .map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "AlgoProviderBridge projection build failed: {e}"
                        ))
                    })?;
                proc.execute_with_projection(algo_ctx, args, projection)
            };
            let mut rows: Vec<AlgoResultRow> = Vec::new();
            while let Some(row_res) = algo_stream.next().await {
                let row = row_res
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
                rows.push(row);
            }
            build_record_batch(&rows, &yields, &fields)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

fn value_type_to_arrow(vt: &ValueType) -> DataType {
    match vt {
        ValueType::Int => DataType::Int64,
        ValueType::Float => DataType::Float64,
        ValueType::String => DataType::Utf8,
        ValueType::Bool => DataType::Boolean,
        ValueType::List | ValueType::Map | ValueType::Path => DataType::LargeBinary,
        ValueType::Node => DataType::Int64,
        ValueType::Relationship => DataType::Int64,
        ValueType::Any => DataType::Utf8,
    }
}

fn build_record_batch(
    rows: &[AlgoResultRow],
    yields: &[(&'static str, ValueType)],
    fields: &[Field],
) -> Result<RecordBatch, datafusion::error::DataFusionError> {
    use arrow_array::{BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray};
    let schema = Arc::new(Schema::new(fields.to_vec()));
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for (idx, (_name, vt)) in yields.iter().enumerate() {
        let col: ArrayRef = match vt {
            ValueType::Int | ValueType::Node | ValueType::Relationship => {
                let v: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| {
                        r.values
                            .get(idx)
                            .and_then(|x| x.as_i64().or_else(|| x.as_u64().map(|u| u as i64)))
                    })
                    .collect();
                Arc::new(Int64Array::from(v))
            }
            ValueType::Float => {
                let v: Vec<Option<f64>> = rows
                    .iter()
                    .map(|r| r.values.get(idx).and_then(|x| x.as_f64()))
                    .collect();
                Arc::new(Float64Array::from(v))
            }
            ValueType::Bool => {
                let v: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| r.values.get(idx).and_then(|x| x.as_bool()))
                    .collect();
                Arc::new(BooleanArray::from(v))
            }
            ValueType::String | ValueType::Any => {
                // #233 Tier 1: a JSON null used to render as the literal
                // string "null" here, while the Int/Float/Bool arms correctly
                // produce a NULL cell. `to_string` stays as the fallback for
                // genuinely non-string values, which is the documented
                // stringify behaviour for `Any`.
                let v: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| {
                        r.values.get(idx).and_then(|x| {
                            if x.is_null() {
                                None
                            } else {
                                Some(
                                    x.as_str()
                                        .map(str::to_owned)
                                        .unwrap_or_else(|| x.to_string()),
                                )
                            }
                        })
                    })
                    .collect();
                Arc::new(StringArray::from(v))
            }
            ValueType::List | ValueType::Map | ValueType::Path => {
                // #233 Tier 1: `unwrap_or_default()` wrote a ZERO-LENGTH blob
                // when serialization failed (`serde_json` refuses NaN and
                // +/-inf), so a list or map that could not be encoded decoded
                // downstream as an empty one rather than surfacing.
                let v: Vec<Option<Vec<u8>>> = rows
                    .iter()
                    .map(|r| {
                        r.values
                            .get(idx)
                            .map(|x| {
                                serde_json::to_vec(x).map_err(|e| {
                                    datafusion::error::DataFusionError::Execution(format!(
                                        "cannot encode a {vt:?} yield value: {e}"
                                    ))
                                })
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<Option<Vec<u8>>>, _>>()?;
                Arc::new(LargeBinaryArray::from_iter(v.iter().map(|o| o.as_deref())))
            }
        };
        cols.push(col);
    }
    RecordBatch::try_new(schema, cols)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Helper: build an `AlgorithmHostBridge` from `StorageManager` + L0.
///
/// Hosts use this when constructing an `AlgorithmContext`. `effective_caps`
/// carries the owning plugin's grants so [`AlgorithmHost::project`] can gate
/// topology access on [`Capability::HostQuery`].
#[must_use]
pub fn host_bridge_from_storage(
    storage: Arc<uni_store::storage::manager::StorageManager>,
    l0: Option<Arc<uni_store::runtime::L0Manager>>,
    effective_caps: CapabilitySet,
) -> AlgorithmHostBridge {
    AlgorithmHostBridge::new(AlgoContext::new(storage, l0), effective_caps)
}

/// The primary projection plus every pre-declared named scope for one CALL.
///
/// Parsed once from the trailing config object and consumed identically by all
/// four loader adapters. Sharing this rather than hand-writing the same steps per
/// loader is deliberate: the guest shims drifting from the host contract is the
/// defect class this subsystem has hit most, and four copies of "parse config,
/// build futures, bind primary first" is exactly how it happens.
#[derive(Debug)]
pub struct ProjectionPlan {
    /// Knobs for the primary projection — the one `emit` keys its `nodeId` to.
    pub primary: GraphProjectionSpec,
    /// Cypher/Named `graphRef` for the primary, if it named one.
    pub primary_graph_ref: Option<serde_json::Value>,
    /// Named scopes, in declaration order.
    pub scopes: Vec<GraphScopeSpec>,
}

impl ProjectionPlan {
    /// Strips the trailing projection-config object from `args` and parses it.
    ///
    /// With no config object, yields the loader default (`include_reverse: true`,
    /// so the In-direction kernels work) and no scopes.
    ///
    /// # Errors
    /// Returns `0x86E` when a `scopes` map is malformed — an unnamed scope, a
    /// non-object scope value, or a scope called `graph`.
    pub fn take_from_args(args: &mut Vec<serde_json::Value>) -> Result<Self, FnError> {
        let Some(cfg) = GraphProjectionSpec::take_config_from_args(args) else {
            return Ok(Self {
                primary: GraphProjectionSpec {
                    include_reverse: true,
                    ..GraphProjectionSpec::default()
                },
                primary_graph_ref: None,
                scopes: Vec::new(),
            });
        };
        let scopes = GraphProjectionSpec::scopes_from_config_object(&cfg).map_err(|e| {
            FnError::new(crate::algorithms::graph_compute::error::ARG_VALIDATION, e)
        })?;
        let primary_graph_ref = GraphProjectionSpec::is_query_graph_ref(&cfg)
            .then(|| serde_json::Value::Object(cfg.clone()));
        Ok(Self {
            primary: GraphProjectionSpec::from_config_object(&cfg),
            primary_graph_ref,
            scopes,
        })
    }
}

impl std::fmt::Debug for BoundProjections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundProjections")
            .field("vertices", &self.primary.vertex_count())
            .field(
                "scopes",
                &self.named.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// Every projection a CALL needs, built and bound.
///
/// Returned by [`build_projections`] after all the futures resolve.
pub struct BoundProjections {
    /// The primary projection.
    pub primary: Arc<GraphProjection>,
    /// Named scopes in declaration order, paired with their projections.
    pub named: Vec<(String, Arc<GraphProjection>)>,
}

impl BoundProjections {
    /// Total vertices across every projection, for sizing the work budget.
    #[must_use]
    pub fn total_vertices(&self) -> u64 {
        self.named
            .iter()
            .map(|(_, g)| g.vertex_count() as u64)
            .sum::<u64>()
            + self.primary.vertex_count() as u64
    }

    /// Total edges across every projection, for sizing the work budget.
    #[must_use]
    pub fn total_edges(&self) -> u64 {
        self.named
            .iter()
            .map(|(_, g)| g.edge_count() as u64)
            .sum::<u64>()
            + self.primary.edge_count() as u64
    }
}

/// Builds the futures for every projection in `plan`, ready to be awaited.
///
/// Called **before** the adapter enters its result stream, so no borrow of the
/// host escapes into the `'static` future — the same reason the single-projection
/// path built its future early. Awaiting is sequential inside
/// [`await_projections`]: `ProjectionBuilder::build` scans storage, and running N
/// of those concurrently buys little while making the work accounting racy.
#[must_use]
pub fn build_projections(bridge: &AlgorithmHostBridge, plan: &ProjectionPlan) -> ProjectionFutures {
    ProjectionFutures {
        primary: bridge.project_scope(&plan.primary, plan.primary_graph_ref.clone()),
        named: plan
            .scopes
            .iter()
            .map(|s| {
                (
                    s.name.clone(),
                    bridge.project_scope(&s.spec, s.graph_ref.clone()),
                )
            })
            .collect(),
    }
}

impl std::fmt::Debug for ProjectionFutures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionFutures")
            .field(
                "scopes",
                &self.named.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// A projection being built.
type PendingProjection = BoxFuture<'static, Result<Arc<GraphProjection>, FnError>>;

/// Pending projections for one CALL, awaited by [`await_projections`].
pub struct ProjectionFutures {
    primary: PendingProjection,
    named: Vec<(String, PendingProjection)>,
}

/// Awaits every projection, naming the scope that failed.
///
/// # Errors
/// Propagates the first projection failure, prefixed with the scope name so a
/// broken scope is not reported as if the primary projection failed.
pub async fn await_projections(futures: ProjectionFutures) -> Result<BoundProjections, FnError> {
    let primary = futures.primary.await?;
    let mut named = Vec::with_capacity(futures.named.len());
    for (name, fut) in futures.named {
        let g = fut
            .await
            .map_err(|e| FnError::new(e.code, format!("graph scope `{name}`: {}", e.message)))?;
        named.push((name, g));
    }
    Ok(BoundProjections { primary, named })
}
