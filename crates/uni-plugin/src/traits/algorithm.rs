//! Graph algorithm plugins.
//!
//! Two surfaces: [`AlgorithmProvider`] for black-box algorithms (the
//! existing `uni-algo` library style), and [`GraphView`] — the stable,
//! read-only topology API a provider obtains from its [`AlgorithmHost`]
//! via [`AlgorithmHost::project`] to walk the graph without depending on
//! `uni-store` / `uni-algo` types.

use std::sync::Arc;

use datafusion::execution::SendableRecordBatchStream;
use futures::future::BoxFuture;
use uni_common::core::id::Vid;

use crate::errors::FnError;

/// Static signature of an algorithm.
///
/// `args` and `slices` are additive, defaulted fields (construct with
/// `..Default::default()`): existing providers that leave them empty keep the
/// legacy untyped `config_json` contract unchanged. A provider that declares
/// `args` opts into host-side arity/type validation before it runs (proposal
/// §4.6 / decision D7); a provider that declares `slices` opts into load-time
/// capability-slice version negotiation (proposal §4.3 / decision D6).
#[derive(Clone, Debug, Default)]
pub struct AlgorithmSignature {
    /// Output column schema.
    pub output_fields: Vec<arrow_schema::Field>,
    /// Markdown docs.
    pub docs: String,
    /// Declared positional arguments, in call order.
    ///
    /// Empty (the default) preserves the legacy behavior: arguments arrive as a
    /// raw positional `config_json` array the provider parses itself. When
    /// non-empty, the host validates arity and coerces each positional argument
    /// against the declared [`NamedArgType`](crate::traits::procedure::NamedArgType) before the provider runs, filling
    /// omitted trailing arguments from their declared defaults.
    pub args: Vec<crate::traits::procedure::NamedArgType>,
    /// Required capability slices, checked at load time.
    ///
    /// Empty (the default) means the algorithm targets only the always-present
    /// `graph-compute@1` surface. A declared [`SliceReq`] whose version the host
    /// does not provide fails the load with a clear error (`0x86A`) rather than a
    /// mysterious runtime "unknown kernel op" trap.
    pub slices: Vec<SliceReq>,
    /// Whether a `CALL` of this algorithm may be planned as a first-class
    /// DataFusion `ExecutionPlan` node (the vectorized path), rather than only
    /// through the row-based interpreter (proposal §6, DF-3).
    ///
    /// `false` (the default) preserves the row path. A provider sets this `true`
    /// to *declare* it composes correctly as a leaf/source plan node — its `run`
    /// returns a well-formed `RecordBatch` stream matching `output_fields`, and it
    /// consumes MATCH-bound arguments via `outer_values` rather than a child plan.
    /// This replaces the previous name-prefix allowlist (`uni.algo.*`): eligibility
    /// is now **registration-driven**, so a third-party `myco.algo.*` provider that
    /// declares `df_composable` is a first-class plan node like the first-party
    /// ones, and a `uni.algo.`-named provider that does *not* declare it no longer
    /// gets the DF path by prefix alone.
    pub df_composable: bool,
}

/// A required capability-slice version an algorithm declares in its signature.
///
/// The host checks each requirement against the slices it actually implements
/// (today only `graph-compute@1`) when the algorithm loads, refusing a mismatch
/// up front (proposal §4.3 / decision D6). Adding a slice or bumping a version is
/// a forward-compatible, additive change: an algorithm that declares no slices is
/// grandfathered onto the base surface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SliceReq {
    /// The capability-slice name, e.g. `"graph-compute"`.
    pub slice: smol_str::SmolStr,
    /// The minimum slice version the algorithm requires.
    pub version: u16,
}

/// The capability slices this host implements, for load-time negotiation.
///
/// A guest algorithm's declared [`SliceReq`]s are checked against this table
/// when it loads. The host implements `graph-compute@1` (the coarse read-only
/// kernels over a projection) and `graph-arena@1` (mutable session-local
/// structure, proposal §5.1). A future slice is added here in lockstep with its
/// kernels so negotiation stays a pure lookup (proposal §4.3 / §10).
pub const HOST_CAPABILITY_SLICES: &[(&str, u16)] = &[("graph-compute", 1), ("graph-arena", 1)];

impl AlgorithmSignature {
    /// Validates the declared capability slices against `host_slices`.
    ///
    /// Each requirement must be met by a host slice of the same name whose
    /// version is at least the requested one. Pass [`HOST_CAPABILITY_SLICES`] for
    /// the production surface.
    ///
    /// # Errors
    /// Returns `0x86A` (`SliceVersionMismatch`) naming the first requirement the
    /// host cannot satisfy (proposal §4.3 / §12, decision D6).
    pub fn check_slices(&self, host_slices: &[(&str, u16)]) -> Result<(), FnError> {
        for req in &self.slices {
            let satisfied = host_slices
                .iter()
                .any(|(name, ver)| *name == req.slice.as_str() && *ver >= req.version);
            if !satisfied {
                return Err(FnError::new(
                    0x86A,
                    format!(
                        "algorithm requires capability slice `{}@{}` the host does not provide",
                        req.slice, req.version
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Validates and normalizes a positional `config_json` array against `args`.
    ///
    /// When `args` is empty this is a no-op returning `config_json` unchanged, so
    /// providers on the legacy untyped contract are unaffected. Otherwise it
    /// parses the positional JSON array and, per declared argument: rejects a
    /// present value whose JSON kind is incompatible with the declared
    /// [`ArgType`](crate::traits::scalar::ArgType), errors on a missing argument
    /// that has no default, and appends the declared default for an omitted
    /// trailing argument. Extra positional arguments beyond the declared arity
    /// are rejected. The returned JSON array is what the provider then parses, so
    /// it observes defaults already filled in.
    ///
    /// # Errors
    /// Returns `0x86E` (argument arity/type violation) with a message naming the
    /// offending argument (proposal §4.6, decision D7).
    pub fn coerce_config_json(&self, config_json: &str) -> Result<String, FnError> {
        use crate::traits::scalar::ArgType;

        if self.args.is_empty() {
            return Ok(config_json.to_owned());
        }
        let mut provided: Vec<serde_json::Value> = if config_json.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(config_json)
                .map_err(|e| FnError::new(0x86E, format!("bad positional config json: {e}")))?
        };
        if provided.len() > self.args.len() {
            return Err(FnError::new(
                0x86E,
                format!(
                    "too many arguments: got {}, expected at most {}",
                    provided.len(),
                    self.args.len()
                ),
            ));
        }
        let mut out = Vec::with_capacity(self.args.len());
        for (i, arg) in self.args.iter().enumerate() {
            match provided.get_mut(i) {
                Some(value) => {
                    let value = std::mem::replace(value, serde_json::Value::Null);
                    // A `CypherValue` argument is opaque and accepts any JSON.
                    if !matches!(arg.ty, ArgType::CypherValue)
                        && !json_matches_argtype(&value, &arg.ty)
                    {
                        return Err(FnError::new(
                            0x86E,
                            format!("argument `{}` (position {i}) has the wrong type", arg.name),
                        ));
                    }
                    out.push(value);
                }
                None => match &arg.default {
                    Some(default) => out.push(scalar_default_to_json(default)),
                    None => {
                        return Err(FnError::new(
                            0x86E,
                            format!("missing required argument `{}` (position {i})", arg.name),
                        ));
                    }
                },
            }
        }
        serde_json::to_string(&out)
            .map_err(|e| FnError::new(0x86E, format!("re-encoding coerced config: {e}")))
    }
}

/// Whether a JSON value is compatible with a declared primitive/vector arg type.
fn json_matches_argtype(value: &serde_json::Value, ty: &crate::traits::scalar::ArgType) -> bool {
    use arrow_schema::DataType;

    use crate::traits::scalar::ArgType;
    match ty {
        ArgType::CypherValue => true,
        ArgType::Vector { .. } => value.is_array(),
        ArgType::Variadic(inner) => json_matches_argtype(value, inner),
        ArgType::Primitive(dt) => match dt {
            DataType::Boolean => value.is_boolean(),
            DataType::Utf8 | DataType::LargeUtf8 => value.is_string(),
            DataType::Float16 | DataType::Float32 | DataType::Float64 => value.is_number(),
            d if d.is_integer() => value.is_i64() || value.is_u64(),
            // Unknown/opaque primitive: don't reject, defer to the provider.
            _ => true,
        },
    }
}

/// Renders a declared [`ScalarValue`](datafusion::scalar::ScalarValue) default as
/// JSON to append for an omitted trailing argument.
fn scalar_default_to_json(default: &datafusion::scalar::ScalarValue) -> serde_json::Value {
    use datafusion::scalar::ScalarValue;

    match default {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(Some(b)) => serde_json::Value::Bool(*b),
        ScalarValue::Float32(Some(x)) => serde_json::json!(*x),
        ScalarValue::Float64(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int8(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int16(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int32(Some(x)) => serde_json::json!(*x),
        ScalarValue::Int64(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt8(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt16(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt32(Some(x)) => serde_json::json!(*x),
        ScalarValue::UInt64(Some(x)) => serde_json::json!(*x),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            serde_json::Value::String(s.clone())
        }
        // Any other/None scalar defaults to JSON null.
        _ => serde_json::Value::Null,
    }
}

/// Per-invocation context passed to an [`AlgorithmProvider`].
///
/// `host` is an opaque [`AlgorithmHost`] callback the host populates
/// when invoking the algorithm. Algorithms that need a concrete
/// graph-projection / storage handle downcast through `host` rather
/// than depend on `uni-store` / `uni-algo` types directly — this keeps
/// `uni-plugin` free of upward dependencies.
#[non_exhaustive]
pub struct AlgorithmContext<'a> {
    /// JSON-serialized algorithm configuration.
    pub config_json: &'a str,
    /// Optional opaque host handle. `None` when no host is bound — the
    /// algorithm may fall back to a config-only path or surface an
    /// `Unbound` error.
    pub host: Option<&'a dyn AlgorithmHost>,
}

impl std::fmt::Debug for AlgorithmContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmContext")
            .field("config_json", &self.config_json)
            .field("host_bound", &self.host.is_some())
            .finish()
    }
}

impl<'a> AlgorithmContext<'a> {
    /// Construct an `AlgorithmContext` with no host bound.
    #[must_use]
    pub fn new(config_json: &'a str) -> Self {
        Self {
            config_json,
            host: None,
        }
    }

    /// Attach a host handle.
    #[must_use]
    pub fn with_host(mut self, host: &'a dyn AlgorithmHost) -> Self {
        self.host = Some(host);
        self
    }
}

/// Host callback surfacing graph access to plugin algorithms.
///
/// A provider's [`AlgorithmProvider::run`] receives an [`AlgorithmHost`]
/// through its [`AlgorithmContext`] and calls [`AlgorithmHost::project`]
/// to materialize a [`GraphView`] over the requested subgraph. Hosts
/// (e.g. `uni-plugin-builtin`) implement `project` by building a
/// projection from their `StorageManager` / `L0Manager`; the
/// [`AlgorithmHost::as_any`] downcast hook remains for hosts that expose
/// additional concrete state. This keeps `uni-plugin` free of upward
/// dependencies on `uni-store` / `uni-algo`.
pub trait AlgorithmHost: Send + Sync {
    /// Downcast hook — bridges implement this to expose the concrete
    /// host type.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Materialize a read-only [`GraphView`] over the subgraph named by
    /// `spec`.
    ///
    /// The returned future is `'static` (owns its inputs) so a provider
    /// can move it into the stream it returns from the synchronous
    /// [`AlgorithmProvider::run`] and `.await` it there. The default
    /// implementation reports that the host offers no graph access;
    /// graph-capable hosts override it.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the host offers no graph access, the
    /// caller lacks the required capability (e.g. `HostQuery`), or the
    /// projection cannot be built.
    fn project(
        &self,
        spec: &GraphProjectionSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn GraphView>, FnError>> {
        let _ = spec;
        Box::pin(async {
            Err(FnError::new(
                0x805,
                "AlgorithmHost: project() is not supported by this host",
            ))
        })
    }
}

/// Selects which subgraph an [`AlgorithmHost::project`] call materializes.
///
/// Naming `node_labels` / `edge_types` scopes the projection to exactly those.
/// Leaving BOTH empty means "the whole graph", but that is only honored when
/// [`Self::project_all`] is `true` — otherwise the projection fails loud (G9),
/// so an unscoped projection can never *silently* pull in unrelated labels (e.g.
/// a coexisting search tree) and corrupt an index-keyed kernel. `weight_property`
/// names an edge property to expose through [`GraphView::out_weight`];
/// `include_reverse` requests inbound adjacency ([`GraphView::in_neighbors`]).
#[derive(Clone, Debug, Default)]
pub struct GraphProjectionSpec {
    /// Vertex labels to include; empty (with [`Self::project_all`]) selects every label.
    pub node_labels: Vec<String>,
    /// Edge types to include; empty (with [`Self::project_all`]) selects every type.
    pub edge_types: Vec<String>,
    /// Edge property surfaced as the traversal weight, if any.
    pub weight_property: Option<String>,
    /// Whether to also build inbound adjacency.
    pub include_reverse: bool,
    /// Vertex properties to materialize into per-vertex `[V]` tensors the guest
    /// reads by name via `gc.node_property` (issue #151).
    pub node_properties: Vec<String>,
    /// Edge properties to materialize into per-edge `[E]` tensors the guest
    /// reads by name via `gc.edge_property` (issue #151).
    pub edge_properties: Vec<String>,
    /// Deliberate opt-in to projecting the *whole* graph when neither
    /// `node_labels` nor `edge_types` is named (G9). With both empty and this
    /// `false`, [`AlgorithmHost::project`] fails loud instead of silently
    /// pulling in every schema label/edge-type. First-party providers set this
    /// to preserve the ergonomic whole-graph default; guest loaders must pass
    /// `projectAll: true` in their config to project everything on purpose.
    pub project_all: bool,
}

impl GraphProjectionSpec {
    /// Parses the Native-mode projection knobs from a guest/procedure config
    /// object. This is the single source of truth for `nodeLabels` /
    /// `edgeTypes` (accepting the `relationshipTypes` alias) / `weightProperty`
    /// / `includeReverse` — the native providers and all four guest loaders
    /// funnel through it so the knob names cannot drift.
    ///
    /// `includeReverse` defaults to `true` (inbound adjacency is built unless
    /// the caller opts out), matching the graphRef contract in `uni-algo` and
    /// keeping In-direction kernels (WCC / k-core / HITS) working. Unknown keys
    /// are ignored and malformed values fall back to the field default: this is
    /// a best-effort projection hint, not a strict schema.
    #[must_use]
    pub fn from_config_object(cfg: &serde_json::Map<String, serde_json::Value>) -> Self {
        fn string_array(v: &serde_json::Value) -> Vec<String> {
            // #233 Tier 1: a bare string (`edgeTypes: "KNOWS"` rather than
            // `["KNOWS"]`) yielded an EMPTY vec, and an empty scoping list
            // means "no restriction" — so the projection silently widened to
            // every edge type and PageRank / WCC scored a different graph
            // than the caller asked for. A scalar string is the realistic
            // mistake, so accept it as a one-element list.
            if let Some(one) = v.as_str() {
                return vec![one.to_owned()];
            }
            let Some(arr) = v.as_array() else {
                tracing::warn!(
                    value = %v,
                    "projection scoping value is neither a string nor an array; ignoring it, \
                     which leaves the projection UNSCOPED for this key",
                );
                return Vec::new();
            };
            arr.iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect()
        }

        let node_labels = cfg.get("nodeLabels").map(string_array).unwrap_or_default();
        // `relationshipTypes` is the openCypher-flavored alias the native
        // procedures already accept as a synonym for `edgeTypes`.
        let edge_types = cfg
            .get("edgeTypes")
            .or_else(|| cfg.get("relationshipTypes"))
            .map(string_array)
            .unwrap_or_default();
        let weight_property = cfg
            .get("weightProperty")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let include_reverse = cfg
            .get("includeReverse")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        let node_properties = cfg
            .get("nodeProperties")
            .map(string_array)
            .unwrap_or_default();
        let edge_properties = cfg
            .get("edgeProperties")
            .map(string_array)
            .unwrap_or_default();
        // G9: explicit opt-in to a whole-graph projection. Absent/false means an
        // unscoped projection is rejected fail-loud by the bridge.
        let project_all = cfg
            .get("projectAll")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        Self {
            node_labels,
            edge_types,
            weight_property,
            include_reverse,
            node_properties,
            edge_properties,
            project_all,
        }
    }

    /// Keys that mark a trailing CALL argument as a projection-config object
    /// (a "graphRef") rather than a guest algorithm argument. Covers the
    /// Native knobs, the P2 property-tensor knobs, and the P3 Cypher/Named
    /// graphRef knobs so the trailing object is stripped from the guest's
    /// arguments consistently across every projection mode.
    pub const CONFIG_KEYS: &'static [&'static str] = &[
        "nodeLabels",
        "edgeTypes",
        "relationshipTypes",
        "weightProperty",
        "includeReverse",
        "nodeProperties",
        "edgeProperties",
        "projectAll",
        "nodeQuery",
        "edgeQuery",
        "weightColumn",
        "name",
        "scopes",
    ];

    /// Keys that mark a config object as a **Cypher/Named** `graphRef` rather
    /// than a Native label/edge-type scoping object.
    ///
    /// Single-sourced so the query layer's graphRef sniffing and the per-scope
    /// parsing below cannot drift: a scope routed to the Native storage scan
    /// when it names a Cypher query would silently project the wrong graph.
    pub const QUERY_CONFIG_KEYS: &'static [&'static str] = &["nodeQuery", "edgeQuery", "name"];

    /// Whether a config object names a Cypher/Named projection.
    #[must_use]
    pub fn is_query_graph_ref(cfg: &serde_json::Map<String, serde_json::Value>) -> bool {
        Self::QUERY_CONFIG_KEYS.iter().any(|k| cfg.contains_key(*k))
    }

    /// Parses the `scopes` map into pre-declared named projections.
    ///
    /// A guest that needs more than one view of the store declares them at the
    /// CALL site rather than projecting on demand, because projection is the one
    /// thing a guest must not be able to trigger in a loop:
    ///
    /// ```cypher
    /// CALL myplugin.compare([], {
    ///   nodeLabels: ['Cell'], edgeTypes: ['ADJ'],
    ///   scopes: {
    ///     agg:  {nodeLabels: ['Cell'], edgeTypes: ['AGGREGATES']},
    ///     flow: {nodeQuery: 'MATCH (c:Cell) RETURN id(c) AS id'}
    ///   }
    /// })
    /// ```
    ///
    /// The outer object stays the *primary* projection — the one `emit` keys its
    /// `nodeId` column to. Each scope value is parsed by the same
    /// [`Self::from_config_object`], so every Native knob works per scope; a
    /// scope bearing a Cypher/Named key is carried through verbatim for the
    /// resolver instead.
    ///
    /// # Errors
    /// Returns a message naming the offending scope when `scopes` is not an
    /// object, a scope name is empty, a scope value is not an object, or a scope
    /// is named `graph` (which would shadow the primary handle's own accessor).
    pub fn scopes_from_config_object(
        cfg: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<Vec<GraphScopeSpec>, String> {
        let Some(raw) = cfg.get("scopes") else {
            return Ok(Vec::new());
        };
        let map = raw
            .as_object()
            .ok_or_else(|| "`scopes` must be an object of {name: projection-config}".to_string())?;
        let mut out = Vec::with_capacity(map.len());
        for (name, value) in map {
            if name.is_empty() {
                return Err("a scope name must not be empty".to_string());
            }
            if name == "graph" {
                return Err(
                    "`graph` is not a valid scope name: it is the primary projection, \
                     reached with `gc.graph()` rather than `gc.graph_named(..)`"
                        .to_string(),
                );
            }
            let obj = value.as_object().ok_or_else(|| {
                format!("scope `{name}` must be a projection-config object, got {value}")
            })?;
            let graph_ref = Self::is_query_graph_ref(obj).then(|| value.clone());
            out.push(GraphScopeSpec {
                name: name.clone(),
                spec: Self::from_config_object(obj),
                graph_ref,
            });
        }
        Ok(out)
    }

    /// Rejects a `scopes` map on an algorithm that does not consume one.
    ///
    /// `scopes` joined [`Self::CONFIG_KEYS`] so a scopes-only object is stripped
    /// from the guest's positional arguments. That stripping applies to *every*
    /// algorithm, but only the guest loader adapters build the declared
    /// projections — so without this a first-party provider would accept a
    /// `scopes` map, project nothing, and run as if it had never been asked.
    /// Silently ignoring a projection the caller asked for is the same failure
    /// the unscoped-projection change (G9) made loud; this keeps it loud.
    ///
    /// # Errors
    /// Returns `0x86E` naming `algorithm` when `cfg` carries a non-empty `scopes`.
    pub fn reject_scopes(
        cfg: &serde_json::Map<String, serde_json::Value>,
        algorithm: &str,
    ) -> Result<(), FnError> {
        let has_scopes = cfg
            .get("scopes")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|m| !m.is_empty());
        if !has_scopes {
            return Ok(());
        }
        Err(FnError::new(
            0x86E,
            format!(
                "{algorithm} does not take named `scopes`: it runs a fixed algorithm over \
                 one projection. Named scopes are a guest-authored-algorithm feature -- \
                 the guest is what decides which scope to read."
            ),
        ))
    }

    /// Like [`Self::take_from_args`] but also returns the raw config object.
    ///
    /// [`Self::take_from_args`] discards the object after parsing, which is fine
    /// for the Native knobs but loses `scopes` (whose values must be re-parsed
    /// per scope, and whose Cypher entries must survive verbatim).
    #[must_use]
    pub fn take_config_from_args(
        args: &mut Vec<serde_json::Value>,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        let is_config = args
            .last()
            .and_then(serde_json::Value::as_object)
            .is_some_and(|o| Self::CONFIG_KEYS.iter().any(|k| o.contains_key(*k)));
        if !is_config {
            return None;
        }
        match args.pop() {
            Some(serde_json::Value::Object(cfg)) => Some(cfg),
            _ => None,
        }
    }

    /// If the last element of `args` is a JSON object bearing at least one
    /// [`Self::CONFIG_KEYS`] key, removes it from `args` and returns the parsed
    /// Native spec; otherwise leaves `args` untouched and returns `None`.
    ///
    /// The Native-spec half of the "trailing object is the projection config"
    /// convention. The guest loaders now go through
    /// [`ProjectionPlan`](../../../uni_plugin_builtin/algorithms/bridge/struct.ProjectionPlan.html),
    /// which needs the raw object to parse `scopes`; this remains for callers
    /// that want only the Native knobs. Both share
    /// [`Self::take_config_from_args`], so the recognition rule cannot drift.
    /// In P3 Cypher/Named
    /// mode the returned Native spec is empty (the query/name keys are unknown
    /// to [`Self::from_config_object`]) and is ignored by the bridge in favor of
    /// the pre-built projection — but the object is still stripped here so it
    /// never reaches the guest function.
    #[must_use]
    pub fn take_from_args(args: &mut Vec<serde_json::Value>) -> Option<Self> {
        Self::take_config_from_args(args).map(|cfg| Self::from_config_object(&cfg))
    }
}

/// One pre-declared named projection from a CALL-site `scopes` map.
///
/// Named scopes are how a guest algorithm reaches more than one view of the
/// store. They are declared at the call site and built by the host *before* the
/// guest runs, which is the point: a guest that could project on demand could
/// project in a loop, and projection is `O(V+E)` storage work that the native
/// work meter does not govern.
#[derive(Clone, Debug)]
pub struct GraphScopeSpec {
    /// The name the guest passes to `graph_named`.
    pub name: String,
    /// Native knobs for this scope, parsed by [`GraphProjectionSpec::from_config_object`].
    pub spec: GraphProjectionSpec,
    /// The raw scope object when it names a Cypher/Named projection, to be
    /// resolved through the host's injected resolver rather than scanned.
    pub graph_ref: Option<serde_json::Value>,
}

/// Every graph-projection knob, enumerated so the guest/native surface contract
/// is checked at compile time: adding a knob forces a classification in
/// [`ProjectionKnob::reach`] and a key in [`ProjectionKnob::config_key`] (both
/// wildcard-free `match`es), mirroring the capability
/// `every_variant_classified_exactly_once` exhaustiveness test. This is the
/// anti-drift guard for the guest/native gap issue #151 exposed. See
/// `docs/proposals/graphcompute_projection_parity_2026-07-19.md` §4.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionKnob {
    /// Scope to these vertex labels (`nodeLabels`).
    NodeLabels,
    /// Scope to these edge types (`edgeTypes` / `relationshipTypes`).
    EdgeTypes,
    /// Bind an edge property as the traversal weight (`weightProperty`).
    WeightProperty,
    /// Also build inbound adjacency (`includeReverse`).
    IncludeReverse,
    /// Materialize per-vertex property tensors (`nodeProperties`).
    NodeProperties,
    /// Materialize per-edge property tensors (`edgeProperties`).
    EdgeProperties,
    /// Cypher-mode node selection query (`nodeQuery`).
    CypherNodeQuery,
    /// Cypher-mode edge selection query (`edgeQuery`).
    CypherEdgeQuery,
    /// Cypher-mode weight column (`weightColumn`).
    CypherWeightColumn,
    /// Named pre-registered projection (`name`).
    NamedGraph,
}

/// How a projection knob is reachable from a guest algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KnobReach {
    /// Parsed by the shared [`GraphProjectionSpec::from_config_object`] — the
    /// Native + property-tensor knobs.
    GuestNative,
    /// Resolved by the uni-query projection seam from a Cypher/Named graphRef.
    GuestQuerySeam,
    /// Host/native-only — not expressible by a guest (none today; reserved for
    /// future knobs such as orientation or parallel-edge aggregation).
    HostOnly,
}

impl ProjectionKnob {
    /// Every knob, the single source of truth the exhaustiveness test iterates.
    pub const ALL: &'static [ProjectionKnob] = &[
        ProjectionKnob::NodeLabels,
        ProjectionKnob::EdgeTypes,
        ProjectionKnob::WeightProperty,
        ProjectionKnob::IncludeReverse,
        ProjectionKnob::NodeProperties,
        ProjectionKnob::EdgeProperties,
        ProjectionKnob::CypherNodeQuery,
        ProjectionKnob::CypherEdgeQuery,
        ProjectionKnob::CypherWeightColumn,
        ProjectionKnob::NamedGraph,
    ];

    /// The graphRef config key for this knob. Wildcard-free: a new variant fails
    /// to compile here until it is given a key.
    #[must_use]
    pub fn config_key(self) -> &'static str {
        match self {
            ProjectionKnob::NodeLabels => "nodeLabels",
            ProjectionKnob::EdgeTypes => "edgeTypes",
            ProjectionKnob::WeightProperty => "weightProperty",
            ProjectionKnob::IncludeReverse => "includeReverse",
            ProjectionKnob::NodeProperties => "nodeProperties",
            ProjectionKnob::EdgeProperties => "edgeProperties",
            ProjectionKnob::CypherNodeQuery => "nodeQuery",
            ProjectionKnob::CypherEdgeQuery => "edgeQuery",
            ProjectionKnob::CypherWeightColumn => "weightColumn",
            ProjectionKnob::NamedGraph => "name",
        }
    }

    /// How a guest reaches this knob. Wildcard-free: a new variant fails to
    /// compile here until it is classified.
    #[must_use]
    pub fn reach(self) -> KnobReach {
        match self {
            ProjectionKnob::NodeLabels
            | ProjectionKnob::EdgeTypes
            | ProjectionKnob::WeightProperty
            | ProjectionKnob::IncludeReverse
            | ProjectionKnob::NodeProperties
            | ProjectionKnob::EdgeProperties => KnobReach::GuestNative,
            ProjectionKnob::CypherNodeQuery
            | ProjectionKnob::CypherEdgeQuery
            | ProjectionKnob::CypherWeightColumn
            | ProjectionKnob::NamedGraph => KnobReach::GuestQuerySeam,
        }
    }
}

#[cfg(test)]
mod projection_knob_contract {
    use super::{GraphProjectionSpec, KnobReach, ProjectionKnob};

    #[test]
    fn every_projection_knob_is_classified_and_keyed() {
        // A knob added to the enum makes `reach`/`config_key` non-exhaustive
        // (compile error) and must be appended to `ALL`. Keys must be unique and
        // recognized as graphRef markers so the adapters strip them from guest
        // args.
        let mut keys = std::collections::HashSet::new();
        for knob in ProjectionKnob::ALL {
            let key = knob.config_key();
            assert!(keys.insert(key), "duplicate projection config key {key}");
            assert!(
                GraphProjectionSpec::CONFIG_KEYS.contains(&key),
                "{key} missing from GraphProjectionSpec::CONFIG_KEYS"
            );
            let _ = knob.reach(); // total by construction
        }
    }

    #[test]
    fn guest_native_knobs_round_trip_through_the_shared_parser() {
        // Every GuestNative knob must actually be honored by from_config_object,
        // so a knob can't be declared guest-reachable yet silently unparsed —
        // the precise failure that produced issue #151.
        for &knob in ProjectionKnob::ALL {
            if knob.reach() != KnobReach::GuestNative {
                continue;
            }
            let key = knob.config_key();
            let sample = match knob {
                ProjectionKnob::IncludeReverse => serde_json::json!(false),
                ProjectionKnob::WeightProperty => serde_json::json!("w"),
                _ => serde_json::json!(["X"]),
            };
            let mut obj = serde_json::Map::new();
            obj.insert(key.to_string(), sample);
            let spec = GraphProjectionSpec::from_config_object(&obj);
            let honored = match knob {
                ProjectionKnob::NodeLabels => spec.node_labels == ["X"],
                ProjectionKnob::EdgeTypes => spec.edge_types == ["X"],
                ProjectionKnob::WeightProperty => spec.weight_property.as_deref() == Some("w"),
                ProjectionKnob::IncludeReverse => !spec.include_reverse,
                ProjectionKnob::NodeProperties => spec.node_properties == ["X"],
                ProjectionKnob::EdgeProperties => spec.edge_properties == ["X"],
                _ => unreachable!("only GuestNative knobs reach here"),
            };
            assert!(honored, "from_config_object did not honor `{key}`");
        }
    }
}

/// Stable, read-only topology view handed to a plugin algorithm.
///
/// Vertices are addressed by dense `u32` slots (`0..vertex_count`);
/// [`GraphView::to_vid`] / [`GraphView::to_slot`] translate to and from
/// external [`Vid`]s at the boundary. Neighbor accessors return neighbor
/// *slots*, not vids. A `GraphView` reflects the subgraph named by the
/// [`GraphProjectionSpec`] that produced it and does not observe later
/// writes.
///
/// # Panics
///
/// [`GraphView::out_weight`] panics unless [`GraphView::has_weights`] is
/// `true`, and [`GraphView::in_neighbors`] / [`GraphView::in_degree`]
/// panic unless [`GraphView::has_reverse`] is `true`. Guard with those
/// predicates before calling.
pub trait GraphView: Send + Sync {
    /// Number of vertices; valid slots are `0..vertex_count`.
    fn vertex_count(&self) -> usize;

    /// Total number of outbound edges.
    fn edge_count(&self) -> usize;

    /// Outbound neighbor slots of `slot`.
    fn out_neighbors(&self, slot: u32) -> &[u32];

    /// Number of outbound edges from `slot`.
    fn out_degree(&self, slot: u32) -> u32;

    /// Inbound neighbor slots of `slot`.
    ///
    /// # Panics
    ///
    /// Panics unless [`GraphView::has_reverse`] is `true`.
    fn in_neighbors(&self, slot: u32) -> &[u32];

    /// Number of inbound edges into `slot`.
    ///
    /// # Panics
    ///
    /// Panics unless [`GraphView::has_reverse`] is `true`.
    fn in_degree(&self, slot: u32) -> u32;

    /// Whether inbound adjacency is available.
    fn has_reverse(&self) -> bool;

    /// Weight of the `edge_idx`-th outbound edge of `slot`.
    ///
    /// `edge_idx` indexes into [`GraphView::out_neighbors`] of `slot`.
    ///
    /// # Panics
    ///
    /// Panics unless [`GraphView::has_weights`] is `true`.
    fn out_weight(&self, slot: u32, edge_idx: usize) -> f64;

    /// Whether edge weights are available.
    fn has_weights(&self) -> bool;

    /// Translate a dense slot to its external [`Vid`].
    fn to_vid(&self, slot: u32) -> Vid;

    /// Translate an external [`Vid`] to its dense slot, if present.
    fn to_slot(&self, vid: Vid) -> Option<u32>;

    /// Iterate over every `(slot, vid)` pair in the view.
    fn vertices(&self) -> Box<dyn Iterator<Item = (u32, Vid)> + '_>;
}

/// A black-box graph algorithm.
///
/// The trait is intentionally minimal: a signature describing the output,
/// plus a `run` method returning a streaming `RecordBatch` sequence. The
/// algorithm is responsible for fetching graph data via host APIs (out of
/// scope of this trait — `uni-algo` will provide a `GraphView` abstraction
/// the host adapter passes via `AlgorithmContext` once those APIs are
/// available).
pub trait AlgorithmProvider: Send + Sync {
    /// Static signature.
    fn signature(&self) -> &AlgorithmSignature;

    /// Execute the algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the algorithm cannot be started; per-batch
    /// failures are signaled via `Err` items in the returned stream.
    fn run(&self, ctx: AlgorithmContext<'_>) -> Result<SendableRecordBatchStream, FnError>;
}

#[cfg(test)]
mod tests {

    use super::GraphProjectionSpec;

    /// A scalar `edgeTypes` must scope the projection, not silently widen it.
    ///
    /// #233 Tier 1. `string_array` only accepted an array, so
    /// `edgeTypes: "KNOWS"` produced an EMPTY `edge_types` — and an empty
    /// scoping list means "no restriction", so the projection silently
    /// widened to every edge type and PageRank / WCC scored a different graph
    /// than the caller asked for. The failure is invisible because the
    /// algorithm still returns plausible numbers.
    #[test]
    fn a_scalar_edge_types_scopes_rather_than_widening_the_projection() {
        let cfg: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"edgeTypes": "KNOWS"}"#).expect("config parses");
        let spec = GraphProjectionSpec::from_config_object(&cfg);
        assert_eq!(
            spec.edge_types,
            vec!["KNOWS".to_owned()],
            "a bare string must scope to that one type; an empty list means NO restriction, \
             which silently projects every edge type"
        );
    }

    /// The array form keeps working, and the alias is still honoured.
    #[test]
    fn an_array_edge_types_still_scopes_as_before() {
        let cfg: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"relationshipTypes": ["KNOWS", "LIKES"]}"#)
                .expect("config parses");
        let spec = GraphProjectionSpec::from_config_object(&cfg);
        assert_eq!(
            spec.edge_types,
            vec!["KNOWS".to_owned(), "LIKES".to_owned()],
            "control: the array form and the relationshipTypes alias are unaffected"
        );
    }
    use arrow_schema::DataType;
    use datafusion::scalar::ScalarValue;

    use super::{AlgorithmSignature, HOST_CAPABILITY_SLICES, SliceReq};
    use crate::traits::procedure::NamedArgType;
    use crate::traits::scalar::ArgType;

    fn sig_with(args: Vec<NamedArgType>, slices: Vec<SliceReq>) -> AlgorithmSignature {
        AlgorithmSignature {
            args,
            slices,
            ..Default::default()
        }
    }

    fn arg(name: &str, ty: ArgType, default: Option<ScalarValue>) -> NamedArgType {
        NamedArgType {
            name: name.into(),
            ty,
            default,
            doc: String::new(),
        }
    }

    fn cfg(json: &str) -> serde_json::Map<String, serde_json::Value> {
        match serde_json::from_str(json).expect("valid json") {
            serde_json::Value::Object(o) => o,
            other => panic!("expected an object, got {other}"),
        }
    }

    /// A `scopes`-only object must still be recognised as the projection config.
    ///
    /// If `scopes` were missing from `CONFIG_KEYS`, this object would not be
    /// stripped and would arrive at the guest as a positional argument — a
    /// silent arity shift rather than an error.
    #[test]
    fn a_scopes_only_object_is_recognised_as_the_projection_config() {
        let mut args: Vec<serde_json::Value> =
            serde_json::from_str(r#"[1, {"scopes": {"agg": {"nodeLabels": ["N"]}}}]"#)
                .expect("valid json");
        let spec = super::GraphProjectionSpec::take_from_args(&mut args);
        assert!(
            spec.is_some(),
            "the trailing object must be taken as config"
        );
        assert_eq!(args.len(), 1, "only the guest's own arg may remain");
    }

    /// Each scope is parsed by the same Native parser as the primary, and a
    /// Cypher/Named scope is carried through verbatim for the resolver.
    #[test]
    fn scopes_parse_per_scope_and_keep_their_mode() {
        let scopes = super::GraphProjectionSpec::scopes_from_config_object(&cfg(r#"{"scopes": {
                 "agg": {"nodeLabels": ["Cell"], "edgeTypes": ["AGG"], "weightProperty": "w"},
                 "flow": {"nodeQuery": "MATCH (c) RETURN id(c) AS id"}
               }}"#))
        .expect("well-formed scopes");
        assert_eq!(scopes.len(), 2);

        let agg = scopes.iter().find(|s| s.name == "agg").expect("agg");
        assert_eq!(agg.spec.node_labels, vec!["Cell".to_string()]);
        assert_eq!(agg.spec.weight_property.as_deref(), Some("w"));
        assert!(
            agg.graph_ref.is_none(),
            "a Native scope must not be routed to the resolver"
        );

        let flow = scopes.iter().find(|s| s.name == "flow").expect("flow");
        assert!(
            flow.graph_ref.is_some(),
            "a Cypher scope must reach the resolver verbatim"
        );
    }

    /// No `scopes` key is not an error — it is the ordinary single-graph CALL.
    #[test]
    fn an_absent_scopes_key_yields_no_scopes() {
        let scopes =
            super::GraphProjectionSpec::scopes_from_config_object(&cfg(r#"{"nodeLabels": ["N"]}"#))
                .expect("no scopes is fine");
        assert!(scopes.is_empty());
    }

    /// Malformed scope maps are named, not silently dropped.
    #[test]
    fn a_malformed_scopes_map_is_rejected_with_the_offending_name() {
        for (json, needle) in [
            (r#"{"scopes": ["agg"]}"#, "must be an object"),
            (r#"{"scopes": {"agg": 7}}"#, "agg"),
            (r#"{"scopes": {"": {}}}"#, "must not be empty"),
            // `graph` would shadow the primary handle's own accessor.
            (r#"{"scopes": {"graph": {}}}"#, "primary projection"),
        ] {
            let err = super::GraphProjectionSpec::scopes_from_config_object(&cfg(json))
                .expect_err("must be rejected");
            assert!(
                err.contains(needle),
                "error for {json} must mention `{needle}`, got: {err}"
            );
        }
    }

    #[test]
    fn check_slices_accepts_available_and_rejects_missing() {
        // graph-compute@1 is the host surface; @1 passes, @2 and unknown fail 0x86A.
        let ok = sig_with(
            vec![],
            vec![SliceReq {
                slice: "graph-compute".into(),
                version: 1,
            }],
        );
        assert!(ok.check_slices(HOST_CAPABILITY_SLICES).is_ok());

        let too_new = sig_with(
            vec![],
            vec![SliceReq {
                slice: "graph-compute".into(),
                version: 2,
            }],
        );
        let err = too_new
            .check_slices(HOST_CAPABILITY_SLICES)
            .expect_err("graph-compute@2 must be refused");
        assert_eq!(err.code, 0x86A, "slice mismatch is 0x86A");

        let unknown = sig_with(
            vec![],
            vec![SliceReq {
                slice: "tensor-compute".into(),
                version: 1,
            }],
        );
        assert_eq!(
            unknown
                .check_slices(HOST_CAPABILITY_SLICES)
                .unwrap_err()
                .code,
            0x86A
        );

        // No declared slices is grandfathered onto the base surface.
        assert!(
            sig_with(vec![], vec![])
                .check_slices(HOST_CAPABILITY_SLICES)
                .is_ok()
        );
    }

    #[test]
    fn coerce_config_passes_through_when_untyped() {
        // Empty `args` preserves the legacy raw contract byte-for-byte.
        let s = sig_with(vec![], vec![]);
        assert_eq!(s.coerce_config_json("[1, 2, 3]").unwrap(), "[1, 2, 3]");
    }

    #[test]
    fn coerce_config_fills_defaults_and_validates() {
        let s = sig_with(
            vec![
                arg("src", ArgType::CypherValue, None),
                arg(
                    "alpha",
                    ArgType::Primitive(DataType::Float64),
                    Some(ScalarValue::Float64(Some(0.85))),
                ),
            ],
            vec![],
        );

        // A single provided arg fills the omitted `alpha` default.
        let out = s.coerce_config_json("[5]").unwrap();
        let arr: Vec<serde_json::Value> = serde_json::from_str(&out).unwrap();
        assert_eq!(arr.len(), 2, "the omitted default is appended");
        assert_eq!(arr[0], serde_json::json!(5));
        assert!((arr[1].as_f64().unwrap() - 0.85).abs() < 1e-12);

        // A missing required arg is rejected.
        let err = s.coerce_config_json("[]").expect_err("src is required");
        assert_eq!(err.code, 0x86E);

        // A wrong-typed alpha (string, not number) is rejected.
        let err = s
            .coerce_config_json(r#"[5, "not-a-number"]"#)
            .expect_err("alpha must be numeric");
        assert_eq!(err.code, 0x86E);

        // Too many positional args is rejected.
        assert_eq!(s.coerce_config_json("[5, 0.9, 1]").unwrap_err().code, 0x86E);

        // A CypherValue arg accepts an array (the `sourceVids` shape).
        let arr_src = s.coerce_config_json("[[1, 2, 3], 0.9]").unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&arr_src).unwrap();
        assert!(parsed[0].is_array(), "CypherValue accepts an array");
    }
}
