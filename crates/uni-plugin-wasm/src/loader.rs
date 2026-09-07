//! `WasmLoader` — top-level entry point for loading WASM Component
//! Model plugins.
//!
//! Two-pass dance per proposal §5.6:
//!
//! 1. Build engine with no caps; instantiate the component; call the
//!    `manifest` export to learn what caps the plugin needs.
//! 2. Intersect declared ∩ host grants; rebuild the engine with
//!    epoch-interruption + fuel metering enabled per the plugin
//!    manifest's resource limits; instantiate with the cap-gated
//!    Linker; call `register` to learn the qnames; for each entry
//!    construct an adapter (currently `ComponentScalarFn`; aggregate
//!    and procedure adapters land in M6b.2) and push it into the
//!    `PluginRegistrar`.
//!
//! The actual `wasmtime::Engine` + `Component` + `Linker<HostState>`
//! plumbing lives in this file; the linker construction details are
//! in [`crate::linker`].

// Rust guideline compliant

use std::sync::Arc;

use serde::Deserialize;
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};

use crate::adapter::ComponentScalarFn;
use crate::adapter_aggregate::ComponentAggregateFn;
use crate::adapter_procedure::ComponentProcedure;
use crate::bindings::aggregate::{AggregatePlugin, AggregatePluginPre};
use crate::bindings::algorithm::{AlgorithmPlugin as AlgorithmPluginBindings, AlgorithmPluginPre};
use crate::bindings::procedure::{ProcedurePlugin as ProcedurePluginBindings, ProcedurePluginPre};
use crate::bindings::scalar::{ScalarPlugin, ScalarPluginPre};
use crate::error::WasmError;
use crate::host_state::HostState;
use crate::pool::WasmInstancePool;

/// CM plugin manifest in canonical JSON form (the plugin's
/// `manifest` export's payload). Mirrors proposal §14 and the Extism
/// manifest shape — same fields, different ABI host.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentManifest {
    /// Reverse-DNS plugin id.
    pub id: String,
    /// Semver string.
    pub version: String,
    /// Component Model ABI range (e.g., `"^1.2"`).
    #[serde(default)]
    pub abi: Option<String>,
    /// Capabilities the plugin declares it needs. Each is a bare name
    /// (`"network"`) or a structured object with attenuation patterns
    /// (`{"kind":"network","allow":[...]}`) — see [`uni_plugin::ManifestCapability`].
    #[serde(default)]
    pub capabilities: Vec<uni_plugin::ManifestCapability>,
    /// Determinism class.
    #[serde(default)]
    pub determinism: Option<String>,
    /// Free-form human description.
    #[serde(default)]
    pub description: Option<String>,
    /// Per-call wasmtime fuel limit.
    #[serde(default)]
    pub fuel_per_call: Option<u64>,
    /// Maximum linear-memory pages (one page = 64 KiB).
    #[serde(default)]
    pub memory_max_pages: Option<u32>,
    /// Wall-clock per-call timeout in milliseconds.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl ComponentManifest {
    /// The declared capabilities as a rich [`uni_plugin::CapabilitySet`].
    #[must_use]
    pub fn declared_capability_set(&self) -> uni_plugin::CapabilitySet {
        uni_plugin::CapabilitySet::from_manifest(self.capabilities.iter().cloned())
    }
}

/// Wire-level scalar signature shipped by a plugin's `register` export.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireFnSignature {
    /// Argument types in `WireArgType` form.
    pub args: Vec<WireArgType>,
    /// Return type.
    pub returns: WireArgType,
    /// Volatility — `"immutable"`, `"stable"`, or `"volatile"`.
    #[serde(default = "default_volatility")]
    pub volatility: String,
    /// Null handling — `"propagate"` (default) or `"user_handled"`.
    #[serde(default = "default_null_handling")]
    pub null_handling: String,
}

fn default_volatility() -> String {
    "immutable".to_owned()
}
fn default_null_handling() -> String {
    "propagate".to_owned()
}
fn default_proc_mode() -> String {
    "read".to_owned()
}

// Shared with the Extism loader — see `uni_plugin::wire_manifest`. This
// loader's copy used to omit `Vector` and `Variadic`, so a manifest declaring
// either loaded over Extism and failed to parse here.
pub use uni_plugin::wire_manifest::WireArgType;

/// One registration entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RegistrationEntry {
    /// A Cypher scalar function.
    Scalar {
        /// Fully-qualified name.
        qname: String,
        /// Signature.
        signature: WireFnSignature,
    },
    /// A Cypher aggregate function.
    Aggregate {
        /// Fully-qualified name.
        qname: String,
        /// Per-row input + return types.
        signature: WireFnSignature,
        /// Opaque per-partition state type — typically
        /// `{"kind":"primitive","arrow":"binary"}`.
        state: WireArgType,
    },
    /// A Cypher procedure.
    Procedure {
        /// Fully-qualified name.
        qname: String,
        /// Argument types.
        args: Vec<WireArgType>,
        /// Yielded column types.
        yields: Vec<WireArgType>,
        /// Mode — `"read"`, `"write"`, `"schema"`, or `"dbms"`.
        #[serde(default = "default_proc_mode")]
        mode: String,
    },
    /// A GraphCompute algorithm driving the coarse kernels via `host-graph`.
    Algorithm {
        /// Fully-qualified name.
        qname: String,
        /// Argument types (excluding the injected session id).
        args: Vec<WireArgType>,
        /// Yielded columns as `"name:type"` strings (e.g. `"score:float"`).
        yields: Vec<String>,
    },
}

/// Top-level `register` export payload.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationManifest {
    /// Every qname provided by the plugin.
    pub entries: Vec<RegistrationEntry>,
}

/// Outcome of [`WasmLoader::prepare`] — everything the host needs to
/// decide whether to instantiate the component (and what to plumb in).
#[derive(Clone)]
pub struct PreparedComponent {
    /// Parsed manifest.
    pub manifest: ComponentManifest,
    /// Granted ∩ declared capabilities (rich, with attenuation patterns) —
    /// what the plugin actually gets. Threaded into [`HostState`] so
    /// capability-gated host fns can enforce call-time attenuation.
    pub effective: uni_plugin::CapabilitySet,
    /// Declared-but-not-granted capability variants — used for diagnostics.
    pub denied_capabilities: Vec<String>,
    /// HTTP egress backing `host-net`, carried so pool factories (and the
    /// `ComponentPlugin` re-register path) can install it on each `HostState`.
    pub http: Option<Arc<dyn uni_plugin::HttpEgress>>,
    /// GraphCompute session registry backing `host-graph`, installed on each
    /// [`HostState`] so a guest algorithm's `graph-call` resolves its session.
    pub graph: Option<uni_plugin_builtin::algorithms::graph_compute::SharedRegistry>,
}

impl std::fmt::Debug for PreparedComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedComponent")
            .field("manifest", &self.manifest)
            .field("effective", &self.effective)
            .field("denied_capabilities", &self.denied_capabilities)
            .field("http", &self.http.is_some())
            .finish()
    }
}

/// A fresh, single-use CM scalar instance.
///
/// Wraps a freshly-built wasmtime `Store<HostState>` and the typed
/// `ScalarPlugin` binding. Built per acquire from the cached
/// `ScalarPluginPre` (see `build_pool`) and dropped after one
/// invocation, so guest state never leaks across calls and a trapped
/// store is discarded rather than reused. The store arrives already
/// armed (full fuel, fresh epoch deadline) from `fresh_store`, so the
/// invoke methods don't re-arm it.
pub struct ScalarPluginInstance {
    store: Store<HostState>,
    bindings: ScalarPlugin,
    #[expect(
        dead_code,
        reason = "carried for parity with the other surfaces; the fresh store is armed at build time"
    )]
    limits: EffectiveLimits,
}

impl std::fmt::Debug for ScalarPluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScalarPluginInstance")
            .finish_non_exhaustive()
    }
}

/// A plugin-returned `fn-error` record. Each `wit-bindgen` world emits its own
/// structurally-identical type; this trait lets [`map_call`] format any of them
/// uniformly.
trait WasmCallErr {
    fn code(&self) -> u32;
    fn message(&self) -> &str;
    fn retryable(&self) -> bool;
}

macro_rules! impl_wasm_call_err {
    ($ty:ty) => {
        impl WasmCallErr for $ty {
            fn code(&self) -> u32 {
                self.code
            }
            fn message(&self) -> &str {
                &self.message
            }
            fn retryable(&self) -> bool {
                self.retryable
            }
        }
    };
}
impl_wasm_call_err!(crate::bindings::scalar::FnError);
impl_wasm_call_err!(crate::bindings::aggregate::FnError);
impl_wasm_call_err!(crate::bindings::procedure::FnError);
impl_wasm_call_err!(crate::bindings::algorithm::uni::plugin::types::FnError);

/// Collapse a typed export call's nested result into our error model.
///
/// `Ok(Ok(bytes))` is the success path; `Ok(Err(fn_err))` is a plugin-returned
/// fn-error; the outer `Err` is a wasmtime trap. Resource-limit traps (fuel
/// exhaustion, epoch/wall-clock interrupt) classify as
/// [`WasmError::ResourceLimit`]; everything else as [`WasmError::Invoke`],
/// tagged with `label` (the export name).
fn map_call<E: WasmCallErr>(
    label: &str,
    result: Result<Result<Vec<u8>, E>, wasmtime::Error>,
) -> Result<Vec<u8>, WasmError> {
    match result {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(fn_err)) => Err(WasmError::Invoke(format!(
            "{label} fn-error code={} retryable={}: {}",
            fn_err.code(),
            fn_err.retryable(),
            fn_err.message()
        ))),
        Err(e) => Err(classify_trap(label, &e)),
    }
}

/// Classify a wasmtime trap: resource-limit traps get their own variant so
/// callers can distinguish "plugin exceeded its budget" from "plugin bug".
fn classify_trap(label: &str, e: &wasmtime::Error) -> WasmError {
    if let Some(trap) = e.downcast_ref::<wasmtime::Trap>() {
        match trap {
            wasmtime::Trap::OutOfFuel => {
                return WasmError::ResourceLimit(format!(
                    "{label}: fuel exhausted (fuel_per_call budget)"
                ));
            }
            wasmtime::Trap::Interrupt => {
                return WasmError::ResourceLimit(format!(
                    "{label}: wall-clock timeout exceeded (timeout_ms budget)"
                ));
            }
            _ => {}
        }
    }
    WasmError::Invoke(format!("{label} trap: {e}"))
}

impl ScalarPluginInstance {
    /// Call the plugin's `invoke-scalar` export.
    ///
    /// # Errors
    ///
    /// - [`WasmError::Invoke`] if the underlying wasmtime call traps or
    ///   the plugin returns a fn-error.
    pub fn invoke_scalar(&mut self, qname: &str, ipc: &[u8]) -> Result<Vec<u8>, WasmError> {
        let result = self
            .bindings
            .call_invoke_scalar(&mut self.store, qname, ipc);
        map_call("invoke-scalar", result)
    }

    /// Call the plugin's `manifest` export.
    fn read_manifest(&mut self) -> Result<ComponentManifest, WasmError> {
        let s = self
            .bindings
            .call_manifest(&mut self.store)
            .map_err(|e| WasmError::Instantiate(format!("call manifest: {e}")))?;
        serde_json::from_str(&s)
            .map_err(|e| WasmError::InvalidWasm(format!("manifest json parse: {e}")))
    }

    /// Call the plugin's `register` export.
    fn read_register(&mut self) -> Result<RegistrationManifest, WasmError> {
        let s = self
            .bindings
            .call_register(&mut self.store)
            .map_err(|e| WasmError::Instantiate(format!("call register: {e}")))?;
        serde_json::from_str(&s)
            .map_err(|e| WasmError::InvalidWasm(format!("register json parse: {e}")))
    }
}

/// A fresh, single-use instance for the `aggregate-plugin` world.
///
/// Built per acquire from the cached `AggregatePluginPre`; the store is
/// armed at build time. Aggregate exports are stateless across host
/// calls — running accumulator state is threaded by the host as
/// `state: list<u8>` in→out (see [`crate::adapter_aggregate`]) — so a
/// fresh instance per `agg-*` call is correct, not just safe.
pub struct AggregatePluginInstance {
    store: Store<HostState>,
    bindings: AggregatePlugin,
    #[expect(
        dead_code,
        reason = "carried for parity with the other surfaces; the fresh store is armed at build time"
    )]
    limits: EffectiveLimits,
}

impl std::fmt::Debug for AggregatePluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregatePluginInstance")
            .finish_non_exhaustive()
    }
}

impl AggregatePluginInstance {
    /// Call `agg-new`.
    pub fn agg_new(&mut self, qname: &str) -> Result<Vec<u8>, WasmError> {
        map_call(
            "agg-new",
            self.bindings.call_agg_new(&mut self.store, qname),
        )
    }

    /// Call `agg-update`.
    pub fn agg_update(
        &mut self,
        qname: &str,
        state: &[u8],
        values_ipc: &[u8],
    ) -> Result<Vec<u8>, WasmError> {
        map_call(
            "agg-update",
            self.bindings
                .call_agg_update(&mut self.store, qname, state, values_ipc),
        )
    }

    /// Call `agg-merge`.
    pub fn agg_merge(
        &mut self,
        qname: &str,
        state: &[u8],
        other_states_ipc: &[u8],
    ) -> Result<Vec<u8>, WasmError> {
        map_call(
            "agg-merge",
            self.bindings
                .call_agg_merge(&mut self.store, qname, state, other_states_ipc),
        )
    }

    /// Call `agg-evaluate`.
    pub fn agg_evaluate(&mut self, qname: &str, state: &[u8]) -> Result<Vec<u8>, WasmError> {
        map_call(
            "agg-evaluate",
            self.bindings
                .call_agg_evaluate(&mut self.store, qname, state),
        )
    }
}

/// A fresh, single-use instance for the `procedure-plugin` world.
///
/// Built per acquire from the cached `ProcedurePluginPre`; the store is
/// armed at build time and the instance is dropped after one call.
pub struct ProcedurePluginInstance {
    store: Store<HostState>,
    bindings: ProcedurePluginBindings,
    #[expect(
        dead_code,
        reason = "carried for parity with the other surfaces; the fresh store is armed at build time"
    )]
    limits: EffectiveLimits,
}

impl std::fmt::Debug for ProcedurePluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcedurePluginInstance")
            .finish_non_exhaustive()
    }
}

impl ProcedurePluginInstance {
    /// Call `invoke-procedure`.
    pub fn invoke_procedure(&mut self, qname: &str, args_ipc: &[u8]) -> Result<Vec<u8>, WasmError> {
        map_call(
            "invoke-procedure",
            self.bindings
                .call_invoke_procedure(&mut self.store, qname, args_ipc),
        )
    }
}

/// A fresh, single-use instance for the `algorithm-plugin` world.
pub struct AlgorithmPluginInstance {
    store: Store<HostState>,
    bindings: AlgorithmPluginBindings,
    #[expect(
        dead_code,
        reason = "carried for parity with the other surfaces; store armed at build time"
    )]
    limits: EffectiveLimits,
}

impl std::fmt::Debug for AlgorithmPluginInstance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlgorithmPluginInstance")
            .finish_non_exhaustive()
    }
}

impl AlgorithmPluginInstance {
    /// Call `invoke-algorithm`.
    pub fn invoke_algorithm(&mut self, qname: &str, args_ipc: &[u8]) -> Result<Vec<u8>, WasmError> {
        map_call(
            "invoke-algorithm",
            self.bindings
                .call_invoke_algorithm(&mut self.store, qname, args_ipc),
        )
    }
}

/// Top-level WASM Component Model plugin loader.
#[derive(Default)]
pub struct WasmLoader {
    /// Optional HTTP egress backing the `host-net` interface. Threaded into
    /// each instance's [`HostState`]; the linker only exposes `host-net` when
    /// the plugin is granted `Capability::Network`.
    http: Option<Arc<dyn uni_plugin::HttpEgress>>,
    /// Optional GraphCompute session registry backing `host-graph`.
    graph: Option<uni_plugin_builtin::algorithms::graph_compute::SharedRegistry>,
}

impl std::fmt::Debug for WasmLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmLoader")
            .field("http", &self.http.is_some())
            .finish()
    }
}

impl WasmLoader {
    /// Construct a fresh loader.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach an HTTP egress backing the `host-net` interface (builder style).
    #[must_use]
    pub fn with_http(mut self, http: Arc<dyn uni_plugin::HttpEgress>) -> Self {
        self.http = Some(http);
        self
    }

    /// Attach a GraphCompute session registry backing `host-graph` (builder).
    #[must_use]
    pub fn with_graph(
        mut self,
        graph: uni_plugin_builtin::algorithms::graph_compute::SharedRegistry,
    ) -> Self {
        self.graph = Some(graph);
        self
    }

    /// Build the pass-1 bootstrap `PreparedComponent` used to instantiate a
    /// component far enough to read its `manifest` export.
    ///
    /// The manifest is empty (unknown until pass 1), and `effective` is the
    /// host's *offered* grants (not empty) so a plugin importing a
    /// capability-gated interface (e.g. host-net) can link. This is the
    /// tightest safe upper bound — the plugin can never exceed what the host
    /// offered, and the execution pool (pass 2) further restricts to
    /// `declared ∩ grants`. A plugin importing an interface the host did NOT
    /// offer still fails here at instantiate (link absence).
    fn bootstrap_prepared(&self, host_grants: &uni_plugin::CapabilitySet) -> PreparedComponent {
        PreparedComponent {
            manifest: ComponentManifest {
                id: String::new(),
                version: String::new(),
                abi: None,
                capabilities: Vec::new(),
                determinism: None,
                description: None,
                fuel_per_call: None,
                memory_max_pages: None,
                timeout_ms: None,
            },
            effective: host_grants.clone(),
            denied_capabilities: Vec::new(),
            http: self.http.clone(),
            graph: self.graph.clone(),
        }
    }

    /// Parse a CM-plugin manifest and intersect declared/granted
    /// capabilities. Deterministic — no wasmtime instantiation.
    ///
    /// # Errors
    ///
    /// - [`WasmError::InvalidWasm`] if the JSON doesn't parse.
    pub fn prepare(
        &self,
        manifest_json: &[u8],
        grants: &uni_plugin::CapabilitySet,
    ) -> Result<PreparedComponent, WasmError> {
        let manifest: ComponentManifest = serde_json::from_slice(manifest_json)
            .map_err(|e| WasmError::InvalidWasm(format!("manifest json parse: {e}")))?;
        Ok(self.prepare_parsed(manifest, grants))
    }

    /// Intersect declared/granted capabilities for an already-parsed
    /// manifest, skipping the JSON round-trip.
    ///
    /// [`Self::load`] reads the manifest export off a bootstrap instance
    /// (parsed `ComponentManifest`), then needs the cap-intersection
    /// result. The previous implementation re-serialized the parsed
    /// struct to JSON and called [`Self::prepare`] which deserialized it
    /// straight back — a wasteful round-trip whose only purpose was
    /// reusing the cap-intersection loop. This entry point preserves the
    /// loop and skips the (de)serialization.
    pub fn prepare_parsed(
        &self,
        manifest: ComponentManifest,
        grants: &uni_plugin::CapabilitySet,
    ) -> PreparedComponent {
        let declared = manifest.declared_capability_set();
        // Effective = declared ∩ granted (retains per-variant attenuation).
        let effective = declared.intersect(grants);
        // Declared variants the host did not grant — diagnostics only. Shared
        // variant-aware derivation via `CapabilitySet::denied_against`.
        let denied: Vec<String> = declared
            .denied_against(&effective)
            .iter()
            .map(|c| format!("{c:?}"))
            .collect();
        PreparedComponent {
            manifest,
            effective,
            denied_capabilities: denied,
            http: self.http.clone(),
            graph: self.graph.clone(),
        }
    }

    /// Instantiate a CM plugin into a fresh `ScalarPluginInstance`.
    ///
    /// Used directly only by tests; production code goes through
    /// [`Self::load`] which two-passes the manifest negotiation.
    ///
    /// # Errors
    ///
    /// - [`WasmError::InvalidWasm`] on Component compilation failure.
    /// - [`WasmError::Instantiate`] on linker / instantiation failure.
    pub fn instantiate(
        &self,
        bytes: &[u8],
        prepared: &PreparedComponent,
    ) -> Result<ScalarPluginInstance, WasmError> {
        let limits = EffectiveLimits::resolve(&prepared.manifest);
        let engine = build_engine(&limits)?;
        let component = Component::from_binary(&engine, bytes)
            .map_err(|e| WasmError::InvalidWasm(format!("component compile: {e}")))?;
        let linker: Linker<HostState> =
            select_linker_for_manifest(&engine, &prepared.manifest, &prepared.effective)?;
        let mut store = Store::new(
            &engine,
            HostState::new(prepared.effective.clone(), prepared.http.clone())
                .with_graph(prepared.graph.clone()),
        );
        apply_resource_limits(&mut store, &limits);
        let bindings = ScalarPlugin::instantiate(&mut store, &component, &linker)
            .map_err(|e| WasmError::Instantiate(format!("scalar-plugin instantiate: {e}")))?;
        Ok(ScalarPluginInstance {
            store,
            bindings,
            limits,
        })
    }

    /// End-to-end load: read manifest, intersect with host grants,
    /// rebuild with effective caps, read register export, register
    /// scalar adapters with the supplied registrar.
    ///
    /// # Errors
    ///
    /// See [`Self::instantiate`] + manifest / register parse failures.
    pub fn load(
        &self,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
        registrar: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<LoadOutcome, WasmError> {
        // Pass 1 — minimal prepared state (offered grants, empty manifest),
        // instantiate to read manifest.
        let bootstrap = self.bootstrap_prepared(host_grants);
        let mut bootstrap_inst = self.instantiate(bytes, &bootstrap)?;
        let parsed_manifest = bootstrap_inst.read_manifest()?;
        drop(bootstrap_inst);

        // Rewrite the registrar's plugin id to match the manifest —
        // caller supplies a placeholder (e.g., "wasm.loading") because
        // the canonical id is unknown until pass 1.
        registrar.set_plugin_id(uni_plugin::PluginId::new(parsed_manifest.id.clone()));

        // Pass 2 — intersect caps, rebuild engine with limits. We
        // already have the parsed manifest from pass 1; route through
        // `prepare_parsed` to avoid a JSON re-serialize / re-parse
        // round-trip.
        let prepared = self.prepare_parsed(parsed_manifest, host_grants);

        // Build the pool. Factory captures owned bytes + prepared.
        let pool = build_scalar_pool(bytes, &prepared)?;

        // Use one warm instance to read the register export.
        let registration = read_registration(&pool)?;

        let names = apply_registration(bytes, &prepared, &pool, registration, registrar)?;

        Ok(LoadOutcome {
            plugin_id: prepared.manifest.id.clone(),
            version: prepared.manifest.version.clone(),
            effective_capabilities: capability_names(&prepared.effective),
            denied_capabilities: prepared.denied_capabilities,
            scalars_registered: names.scalars,
            aggregates_registered: names.aggregates,
            procedures_registered: names.procedures,
            algorithms_registered: names.algorithms,
            pool,
        })
    }

    /// Load a component and present it as a [`uni_plugin::Plugin`].
    ///
    /// Unlike [`Self::load`] — which registers adapters directly into a
    /// caller-supplied registrar — this returns a self-contained `Plugin`
    /// whose [`uni_plugin::Plugin::manifest`] is synthesized from the
    /// component's manifest and whose [`uni_plugin::Plugin::register`] replays
    /// the component's `register` entries. It is the bridge the conformance
    /// harness (`uni_plugin_conformance::WasmConformanceLoader`) needs to run
    /// the same probe suite against a real component as against a live-Rust
    /// plugin.
    ///
    /// The returned plugin owns the warm scalar pool plus the component bytes
    /// and negotiated capabilities, so `register` can rebuild
    /// aggregate/procedure pools and is safely re-runnable (the conformance
    /// idempotency probe registers twice).
    ///
    /// # Errors
    ///
    /// See [`Self::load`] — manifest / register parse + instantiation failures,
    /// plus [`WasmError::InvalidWasm`] if the manifest version is not semver.
    pub fn load_as_plugin(
        &self,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
    ) -> Result<Box<dyn uni_plugin::Plugin + Send + Sync>, WasmError> {
        // Pass 1 — instantiate with the offered grants to read the manifest
        // export.
        let bootstrap = self.bootstrap_prepared(host_grants);
        let mut bootstrap_inst = self.instantiate(bytes, &bootstrap)?;
        let parsed_manifest = bootstrap_inst.read_manifest()?;
        drop(bootstrap_inst);

        // Pass 2 — intersect caps, build the scalar pool, read register.
        let prepared = self.prepare_parsed(parsed_manifest, host_grants);
        let scalar_pool = build_scalar_pool(bytes, &prepared)?;
        let registration = read_registration(&scalar_pool)?;
        let manifest = synthesize_plugin_manifest(&prepared.manifest, &registration)?;
        Ok(Box::new(ComponentPlugin {
            manifest,
            bytes: bytes.to_vec(),
            prepared,
            scalar_pool,
            registration,
        }))
    }
}

/// Outcome of a successful [`WasmLoader::load`].
pub struct LoadOutcome {
    /// Reverse-DNS plugin id from the manifest.
    pub plugin_id: String,
    /// Plugin version from the manifest.
    pub version: String,
    /// Capabilities granted (declared ∩ host).
    pub effective_capabilities: Vec<String>,
    /// Capabilities denied (declared but not granted).
    pub denied_capabilities: Vec<String>,
    /// Qnames registered as scalar fns.
    pub scalars_registered: Vec<String>,
    /// Qnames registered as aggregate fns.
    pub aggregates_registered: Vec<String>,
    /// Qnames registered as procedures.
    pub procedures_registered: Vec<String>,
    /// Qnames registered as graph-compute algorithms.
    pub algorithms_registered: Vec<String>,
    /// Scalar-plugin instance pool — `None` if no scalars registered.
    pub pool: Arc<WasmInstancePool<ScalarPluginInstance>>,
}

impl std::fmt::Debug for LoadOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadOutcome")
            .field("plugin_id", &self.plugin_id)
            .field("version", &self.version)
            .field("effective_capabilities", &self.effective_capabilities)
            .field("denied_capabilities", &self.denied_capabilities)
            .field("scalars_registered", &self.scalars_registered)
            .field("aggregates_registered", &self.aggregates_registered)
            .field("procedures_registered", &self.procedures_registered)
            .field("algorithms_registered", &self.algorithms_registered)
            .finish_non_exhaustive()
    }
}

/// Render a capability set as variant-name strings for the `LoadOutcome`
/// reporting surface (diagnostics only).
fn capability_names(caps: &uni_plugin::CapabilitySet) -> Vec<String> {
    caps.iter().map(|c| format!("{c:?}")).collect()
}

/// Pick the right per-major scalar linker for `manifest.abi`.
///
/// Bridges the loader to [`crate::multi_version::SUPPORTED_MAJORS`]. A
/// missing `abi` field defaults to v1 for backward compatibility with
/// the M6b cutover's single-major linker — early plugins predate the
/// multi-version surface.
fn select_linker_for_manifest(
    engine: &Engine,
    manifest: &ComponentManifest,
    effective_caps: &uni_plugin::CapabilitySet,
) -> Result<Linker<HostState>, WasmError> {
    use crate::linker::{build_scalar_linker_v1, build_scalar_linker_v2};
    use crate::multi_version::{SUPPORTED_MAJORS, major_for_abi};

    let Some(abi_str) = manifest.abi.as_deref() else {
        return build_scalar_linker_v1(engine, effective_caps);
    };
    let abi = uni_plugin::AbiRange::parse(abi_str)
        .map_err(|e| WasmError::InvalidWasm(format!("manifest abi parse: {e}")))?;
    match major_for_abi(&abi)? {
        1 => build_scalar_linker_v1(engine, effective_caps),
        2 => build_scalar_linker_v2(engine, effective_caps),
        _ => Err(WasmError::AbiUnsupported {
            requested: abi_str.to_owned(),
            supported: SUPPORTED_MAJORS.to_vec(),
        }),
    }
}

/// Host-imposed default wall-clock budget per export call when the plugin
/// manifest does not declare `timeout_ms`. A plugin needing longer calls
/// must declare its own (larger) value.
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Host-imposed default linear-memory cap (in 64 KiB wasm pages, = 1 GiB)
/// when the plugin manifest does not declare `memory_max_pages`.
pub const DEFAULT_MEMORY_MAX_PAGES: u32 = 16_384;

/// Granularity of the per-engine epoch ticker. Wall-clock timeouts are
/// enforced to within roughly one tick.
const EPOCH_TICK_MS: u64 = 50;

/// Resource limits actually enforced on a plugin instance: the manifest's
/// declared values with host floors applied.
///
/// `timeout_ms` and `memory_max_pages` always resolve (host defaults when
/// undeclared) so a plugin that declares nothing can neither hang the
/// executor nor grow memory without bound. `fuel_per_call` stays
/// declaration-only — fuel costs are opaque to plugin authors, so a host
/// default would mis-budget legitimate plugins; the wall-clock timeout is
/// the universal guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveLimits {
    /// Wall-clock budget per export call, in milliseconds.
    pub timeout_ms: u64,
    /// Linear-memory cap, in 64 KiB wasm pages.
    pub memory_max_pages: u32,
    /// Fuel budget per export call; `None` disables fuel metering.
    pub fuel_per_call: Option<u64>,
}

impl EffectiveLimits {
    /// Resolve a manifest's declared limits against the host floors.
    #[must_use]
    pub fn resolve(manifest: &ComponentManifest) -> Self {
        Self {
            timeout_ms: manifest.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS),
            memory_max_pages: manifest
                .memory_max_pages
                .unwrap_or(DEFAULT_MEMORY_MAX_PAGES),
            fuel_per_call: manifest.fuel_per_call,
        }
    }

    /// Epoch ticks corresponding to `timeout_ms` at the ticker granularity.
    fn deadline_ticks(&self) -> u64 {
        self.timeout_ms.div_ceil(EPOCH_TICK_MS).max(1)
    }
}

fn build_engine(limits: &EffectiveLimits) -> Result<Engine, WasmError> {
    let mut cfg = Config::new();
    cfg.wasm_component_model(true);
    if limits.fuel_per_call.is_some() {
        cfg.consume_fuel(true);
    }
    // Wall-clock timeout is always enforced (host default when undeclared).
    cfg.epoch_interruption(true);
    let engine =
        Engine::new(&cfg).map_err(|e| WasmError::Instantiate(format!("engine config: {e}")))?;

    // Per-engine epoch ticker (the canonical wasmtime pattern): a thread
    // holding only a weak engine handle bumps the epoch every tick; a call
    // whose store deadline elapses traps with `Trap::Interrupt`. The thread
    // exits on its own once the engine is dropped (upgrade fails), so short-
    // lived bootstrap engines don't leak threads.
    let weak = engine.weak();
    let spawned = std::thread::Builder::new()
        .name("uni-wasm-epoch-ticker".to_owned())
        .spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(EPOCH_TICK_MS));
                match weak.upgrade() {
                    Some(engine) => engine.increment_epoch(),
                    None => break,
                }
            }
        });
    if let Err(e) = spawned {
        return Err(WasmError::Instantiate(format!(
            "failed to spawn epoch ticker thread: {e}"
        )));
    }
    Ok(engine)
}

fn apply_resource_limits(store: &mut Store<HostState>, limits: &EffectiveLimits) {
    // Linear-memory cap: enforced by wasmtime's store limiter. The
    // `StoreLimits` value must live in the store data so the limiter
    // closure can borrow it.
    store.data_mut().limits = wasmtime::StoreLimitsBuilder::new()
        .memory_size(limits.memory_max_pages as usize * 65_536)
        .build();
    store.limiter(|state| &mut state.limits);
    reset_call_limits(store, limits);
}

/// Re-arm the per-call budgets before every export call.
///
/// The epoch deadline counts down with each ticker increment and fuel is
/// consumed cumulatively, so both must be reset per call — otherwise long-
/// lived pooled instances would spend one call's budget across many calls.
fn reset_call_limits(store: &mut Store<HostState>, limits: &EffectiveLimits) {
    store.set_epoch_deadline(limits.deadline_ticks());
    if let Some(fuel) = limits.fuel_per_call {
        // Unreachable as the code stands: `build_engine(&limits)` enables
        // `consume_fuel` iff `limits.fuel_per_call.is_some()`, and every store
        // is built from the same `limits`, so this arm only runs on a
        // fuel-enabled engine. It is `expect` rather than `let _` because it
        // is one refactor away from silently dropping the cap — a plugin would
        // then run unmetered instead of trapping as `ResourceLimit`, with
        // nothing to see. Plugins over budget trap out of fuel; the host
        // surfaces that as `WasmError::ResourceLimit`.
        store
            .set_fuel(fuel)
            .expect("engine was built with consume_fuel enabled for this limits set");
    }
}

/// A fresh, cap-limited `Store<HostState>` for one invoke.
///
/// The per-invoke security boundary: every call gets its own `Store` so
/// guest linear memory / globals / WASI context start clean and a trapped
/// store is dropped, never reused (proposal §5.6 + architecture review
/// findings #2 / #3). Engine + epoch deadline + fuel are all (re)armed
/// here because a brand-new store starts with full fuel and a fresh epoch
/// deadline anyway.
fn fresh_store(
    engine: &Engine,
    prepared: &PreparedComponent,
    limits: &EffectiveLimits,
) -> Store<HostState> {
    let mut store = Store::new(
        engine,
        HostState::new(prepared.effective.clone(), prepared.http.clone())
            .with_graph(prepared.graph.clone()),
    );
    apply_resource_limits(&mut store, limits);
    store
}

/// Generic CM-plugin instance-cache factory.
///
/// Caches the heavy artifacts **once** at load time — the wasmtime
/// `Engine`, the compiled `Component`, and the surface-specific
/// `bindgen!`-generated `*Pre` (an `InstancePre<HostState>` wrapper) —
/// then hands the instance cache a cheap factory that, per acquire,
/// builds a fresh `Store<HostState>` and calls `pre.instantiate(&mut
/// store)`. Re-instantiation per invoke is what gives each call clean
/// guest state (a persistent store would leak it).
///
/// The per-surface builders supply two closures:
///
/// - `build_pre` — runs once: `linker.instantiate_pre(component)` →
///   `SurfacePre::new(pre)`, returning a `Clone` `*Pre` value.
/// - `instantiate` — runs per acquire: takes the fresh `Store` by value
///   plus the cached `*Pre`, calls `pre.instantiate(&mut store)`, and
///   packs both into the surface-specific instance struct (which owns
///   its store for the duration of the call).
fn build_pool<I, P, BP, MK>(
    bytes: &[u8],
    prepared: &PreparedComponent,
    build_pre: BP,
    instantiate: MK,
) -> Result<Arc<WasmInstancePool<I>>, WasmError>
where
    I: Send + 'static,
    P: Clone + Send + Sync + 'static,
    BP: FnOnce(&Component, &Linker<HostState>) -> Result<P, WasmError>,
    MK: Fn(&P, Store<HostState>, EffectiveLimits) -> Result<I, WasmError> + Send + Sync + 'static,
{
    // Compile + link once; cache the artifact for cheap per-invoke
    // instantiation.
    let limits = EffectiveLimits::resolve(&prepared.manifest);
    let engine = build_engine(&limits)?;
    let component = Component::from_binary(&engine, bytes)
        .map_err(|e| WasmError::InvalidWasm(format!("component compile: {e}")))?;
    let linker: Linker<HostState> =
        select_linker_for_manifest(&engine, &prepared.manifest, &prepared.effective)?;
    let pre = build_pre(&component, &linker)?;

    let prepared_owned: Arc<PreparedComponent> = Arc::new(prepared.clone());
    let engine_owned = Arc::new(engine);
    let instantiate = Arc::new(instantiate);

    let factory = {
        let prepared = Arc::clone(&prepared_owned);
        let engine = Arc::clone(&engine_owned);
        let instantiate = Arc::clone(&instantiate);
        move || -> Result<I, WasmError> {
            let store = fresh_store(&engine, &prepared, &limits);
            instantiate(&pre, store, limits)
        }
    };

    let pool = WasmInstancePool::new(crate::pool::PoolConfig::default(), factory)?;
    Ok(Arc::new(pool))
}

/// Parse a wire qname into a [`uni_plugin::QName`], tagging a parse failure
/// as [`WasmError::InvalidWasm`].
fn parse_qname(qname: &str) -> Result<uni_plugin::QName, WasmError> {
    uni_plugin::QName::parse(qname)
        .map_err(|e| WasmError::InvalidWasm(format!("invalid qname `{qname}`: {e}")))
}

/// Qnames registered by [`apply_registration`], grouped by surface.
struct RegisteredQNames {
    scalars: Vec<String>,
    aggregates: Vec<String>,
    procedures: Vec<String>,
    algorithms: Vec<String>,
}

/// Replay a parsed `register` manifest into `registrar`.
///
/// Constructs one adapter per entry from `scalar_pool` and from
/// aggregate/procedure pools built lazily from `bytes` + `prepared`. Shared by
/// [`WasmLoader::load`] and [`ComponentPlugin::register`] so both register
/// identically — no probe behaves differently against the two paths.
fn apply_registration(
    bytes: &[u8],
    prepared: &PreparedComponent,
    scalar_pool: &Arc<WasmInstancePool<ScalarPluginInstance>>,
    registration: RegistrationManifest,
    registrar: &mut uni_plugin::PluginRegistrar<'_>,
) -> Result<RegisteredQNames, WasmError> {
    let mut scalars = Vec::new();
    let mut aggregates = Vec::new();
    let mut procedures = Vec::new();
    let mut algorithms = Vec::new();
    let mut agg_pool: Option<Arc<WasmInstancePool<AggregatePluginInstance>>> = None;
    let mut proc_pool: Option<Arc<WasmInstancePool<ProcedurePluginInstance>>> = None;
    let mut algo_pool: Option<Arc<WasmInstancePool<AlgorithmPluginInstance>>> = None;

    for entry in registration.entries {
        match entry {
            RegistrationEntry::Scalar { qname, signature } => {
                let parsed_qname = parse_qname(&qname)?;
                let sig = wire_fn_sig_to_internal(&signature)?;
                let adapter = Arc::new(ComponentScalarFn::new(
                    Arc::clone(scalar_pool),
                    parsed_qname.clone(),
                    sig.clone(),
                ));
                registrar
                    .scalar_fn(parsed_qname, sig, adapter)
                    .map_err(|e| {
                        WasmError::Internal(format!("registrar.scalar_fn `{qname}`: {e}"))
                    })?;
                scalars.push(qname);
            }
            RegistrationEntry::Aggregate {
                qname,
                signature,
                state,
            } => {
                let parsed_qname = parse_qname(&qname)?;
                let sig = wire_agg_sig_to_internal(&signature, &state)?;
                let pool_ref = match &agg_pool {
                    Some(p) => Arc::clone(p),
                    None => {
                        let p = build_aggregate_pool(bytes, prepared)?;
                        agg_pool = Some(Arc::clone(&p));
                        p
                    }
                };
                let adapter = Arc::new(ComponentAggregateFn::new(
                    pool_ref,
                    parsed_qname.clone(),
                    sig.clone(),
                ));
                registrar
                    .aggregate_fn(parsed_qname, sig, adapter)
                    .map_err(|e| {
                        WasmError::Internal(format!("registrar.aggregate_fn `{qname}`: {e}"))
                    })?;
                aggregates.push(qname);
            }
            RegistrationEntry::Procedure {
                qname,
                args,
                yields,
                mode,
            } => {
                let parsed_qname = parse_qname(&qname)?;
                let sig = wire_proc_sig_to_internal(&args, &yields, &mode)?;
                let pool_ref = match &proc_pool {
                    Some(p) => Arc::clone(p),
                    None => {
                        let p = build_procedure_pool(bytes, prepared)?;
                        proc_pool = Some(Arc::clone(&p));
                        p
                    }
                };
                let adapter = Arc::new(ComponentProcedure::new(
                    pool_ref,
                    parsed_qname.clone(),
                    sig.clone(),
                ));
                registrar
                    .procedure(parsed_qname, sig, adapter)
                    .map_err(|e| {
                        WasmError::Internal(format!("registrar.procedure `{qname}`: {e}"))
                    })?;
                procedures.push(qname);
            }
            RegistrationEntry::Algorithm {
                qname,
                args,
                yields,
            } => {
                let parsed_qname = parse_qname(&qname)?;
                let registry = prepared.graph.clone().ok_or_else(|| {
                    WasmError::Internal(format!(
                        "algorithm `{qname}` needs a GraphCompute registry \
                         (call WasmLoader::with_graph)"
                    ))
                })?;
                let sig = build_algorithm_signature(&args, &yields)?;
                let pool_ref = match &algo_pool {
                    Some(p) => Arc::clone(p),
                    None => {
                        let p = build_algorithm_pool(bytes, prepared)?;
                        algo_pool = Some(Arc::clone(&p));
                        p
                    }
                };
                let adapter = Arc::new(crate::adapter_algorithm::ComponentAlgorithm::new(
                    pool_ref,
                    registry,
                    parsed_qname.clone(),
                    sig,
                ));
                registrar.algorithm(parsed_qname, adapter).map_err(|e| {
                    WasmError::Internal(format!("registrar.algorithm `{qname}`: {e}"))
                })?;
                algorithms.push(qname);
            }
        }
    }

    Ok(RegisteredQNames {
        scalars,
        aggregates,
        procedures,
        algorithms,
    })
}

/// Synthesize a [`uni_plugin::PluginManifest`] from a component manifest.
///
/// The declared `CapabilitySet` includes the extension capabilities implied by
/// the `register` entries (`Capability::ScalarFn` for a scalar, etc.), so a
/// registrar built from this manifest permits exactly the registrations the
/// component will perform — which is what the conformance registration probes
/// rely on. ABI defaults to `^1` when the component omits it.
///
/// # Errors
///
/// Returns [`WasmError::InvalidWasm`] if the version is not valid semver or the
/// ABI range is malformed.
fn synthesize_plugin_manifest(
    component: &ComponentManifest,
    registration: &RegistrationManifest,
) -> Result<uni_plugin::PluginManifest, WasmError> {
    use uni_plugin::{
        AbiRange, Capability, CapabilitySet, Determinism, PluginId, ProvidedSurfaces, Scope,
        SideEffects,
    };

    let version = semver::Version::parse(&component.version).map_err(|e| {
        WasmError::InvalidWasm(format!("manifest version `{}`: {e}", component.version))
    })?;
    let abi = AbiRange::parse(component.abi.as_deref().unwrap_or("^1"))
        .map_err(|e| WasmError::InvalidWasm(format!("manifest abi: {e}")))?;

    let mut capabilities = CapabilitySet::new();
    let mut side_effects = SideEffects::ReadOnly;
    for entry in &registration.entries {
        match entry {
            RegistrationEntry::Scalar { .. } => {
                capabilities.insert(Capability::ScalarFn);
            }
            RegistrationEntry::Aggregate { .. } => {
                capabilities.insert(Capability::AggregateFn);
            }
            RegistrationEntry::Procedure { mode, .. } => {
                capabilities.insert(Capability::Procedure);
                match mode.as_str() {
                    "write" => {
                        capabilities.insert(Capability::ProcedureWrites);
                        side_effects = SideEffects::Writes;
                    }
                    "schema" => {
                        capabilities.insert(Capability::ProcedureSchema);
                        side_effects = SideEffects::Writes;
                    }
                    "dbms" => {
                        capabilities.insert(Capability::ProcedureDbms);
                    }
                    _ => {}
                }
            }
            RegistrationEntry::Algorithm { .. } => {
                capabilities.insert(Capability::Algorithm);
            }
        }
    }

    let determinism = match component.determinism.as_deref() {
        Some("pure") => Determinism::Pure,
        Some("session-scoped" | "session_scoped") => Determinism::SessionScoped,
        _ => Determinism::Nondeterministic,
    };

    Ok(uni_plugin::PluginManifest {
        id: PluginId::new(component.id.clone()),
        version,
        abi,
        depends_on: Vec::new(),
        capabilities,
        determinism,
        side_effects,
        scope: Scope::Instance,
        hash: None,
        signature: None,
        provides: ProvidedSurfaces::default(),
        docs: component.description.clone().unwrap_or_default(),
        metadata: std::collections::BTreeMap::new(),
    })
}

/// A loaded WASM component presented as a [`uni_plugin::Plugin`].
///
/// Produced by [`WasmLoader::load_as_plugin`]. Holds the warm scalar pool plus
/// the component bytes and negotiated capabilities so its `register` impl can
/// rebuild aggregate/procedure pools and replay registration on each call.
pub struct ComponentPlugin {
    manifest: uni_plugin::PluginManifest,
    bytes: Vec<u8>,
    prepared: PreparedComponent,
    scalar_pool: Arc<WasmInstancePool<ScalarPluginInstance>>,
    registration: RegistrationManifest,
}

impl std::fmt::Debug for ComponentPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentPlugin")
            .field("id", &self.manifest.id.as_str())
            .field("scalars", &self.registration.entries.len())
            .finish()
    }
}

impl uni_plugin::Plugin for ComponentPlugin {
    fn manifest(&self) -> &uni_plugin::PluginManifest {
        &self.manifest
    }

    fn register(
        &self,
        r: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<(), uni_plugin::PluginError> {
        apply_registration(
            &self.bytes,
            &self.prepared,
            &self.scalar_pool,
            self.registration.clone(),
            r,
        )
        .map_err(|e| {
            uni_plugin::PluginError::WasmInstantiate(format!("component register: {e}"))
        })?;
        Ok(())
    }
}

/// Lease one warm scalar instance and read its `register` export.
///
/// Both [`WasmLoader::load`] and [`WasmLoader::load_as_plugin`] need the
/// parsed registration before they can build adapters; they share this so the
/// acquire-read-release dance stays in one place.
fn read_registration(
    pool: &Arc<WasmInstancePool<ScalarPluginInstance>>,
) -> Result<RegistrationManifest, WasmError> {
    let mut leased = crate::pool::PooledInstance::acquire(Arc::clone(pool))
        .map_err(|e| WasmError::Instantiate(format!("acquire warm instance: {e}")))?;
    let registration = leased.get_mut().read_register()?;
    drop(leased);
    Ok(registration)
}

fn build_scalar_pool(
    bytes: &[u8],
    prepared: &PreparedComponent,
) -> Result<Arc<WasmInstancePool<ScalarPluginInstance>>, WasmError> {
    build_pool(
        bytes,
        prepared,
        |component, linker| {
            let pre = linker
                .instantiate_pre(component)
                .map_err(|e| WasmError::Instantiate(format!("scalar-plugin pre: {e}")))?;
            ScalarPluginPre::new(pre)
                .map_err(|e| WasmError::Instantiate(format!("scalar-plugin pre-new: {e}")))
        },
        |pre, mut store, limits| {
            let bindings = pre
                .instantiate(&mut store)
                .map_err(|e| WasmError::Instantiate(format!("scalar-plugin instantiate: {e}")))?;
            Ok(ScalarPluginInstance {
                store,
                bindings,
                limits,
            })
        },
    )
}

fn build_aggregate_pool(
    bytes: &[u8],
    prepared: &PreparedComponent,
) -> Result<Arc<WasmInstancePool<AggregatePluginInstance>>, WasmError> {
    build_pool(
        bytes,
        prepared,
        |component, linker| {
            let pre = linker
                .instantiate_pre(component)
                .map_err(|e| WasmError::Instantiate(format!("aggregate-plugin pre: {e}")))?;
            AggregatePluginPre::new(pre)
                .map_err(|e| WasmError::Instantiate(format!("aggregate-plugin pre-new: {e}")))
        },
        |pre, mut store, limits| {
            let bindings = pre.instantiate(&mut store).map_err(|e| {
                WasmError::Instantiate(format!("aggregate-plugin instantiate: {e}"))
            })?;
            Ok(AggregatePluginInstance {
                store,
                bindings,
                limits,
            })
        },
    )
}

fn build_procedure_pool(
    bytes: &[u8],
    prepared: &PreparedComponent,
) -> Result<Arc<WasmInstancePool<ProcedurePluginInstance>>, WasmError> {
    build_pool(
        bytes,
        prepared,
        |component, linker| {
            let pre = linker
                .instantiate_pre(component)
                .map_err(|e| WasmError::Instantiate(format!("procedure-plugin pre: {e}")))?;
            ProcedurePluginPre::new(pre)
                .map_err(|e| WasmError::Instantiate(format!("procedure-plugin pre-new: {e}")))
        },
        |pre, mut store, limits| {
            let bindings = pre.instantiate(&mut store).map_err(|e| {
                WasmError::Instantiate(format!("procedure-plugin instantiate: {e}"))
            })?;
            Ok(ProcedurePluginInstance {
                store,
                bindings,
                limits,
            })
        },
    )
}

/// Build an instance pool for the `algorithm-plugin` world.
///
/// Unlike the other pools this uses [`crate::linker::build_algorithm_linker_v1`]
/// so the guest's `host-graph` import resolves, and each fresh store carries the
/// GraphCompute registry via [`fresh_store`] → [`HostState::with_graph`].
fn build_algorithm_pool(
    bytes: &[u8],
    prepared: &PreparedComponent,
) -> Result<Arc<WasmInstancePool<AlgorithmPluginInstance>>, WasmError> {
    let limits = EffectiveLimits::resolve(&prepared.manifest);
    let engine = build_engine(&limits)?;
    let component = Component::from_binary(&engine, bytes)
        .map_err(|e| WasmError::InvalidWasm(format!("component compile: {e}")))?;
    let linker = crate::linker::build_algorithm_linker_v1(&engine, &prepared.effective)?;
    let pre = AlgorithmPluginPre::new(
        linker
            .instantiate_pre(&component)
            .map_err(|e| WasmError::Instantiate(format!("algorithm-plugin pre: {e}")))?,
    )
    .map_err(|e| WasmError::Instantiate(format!("algorithm-plugin pre-new: {e}")))?;

    let prepared_owned = Arc::new(prepared.clone());
    let engine_owned = Arc::new(engine);
    let factory = move || -> Result<AlgorithmPluginInstance, WasmError> {
        let mut store = fresh_store(&engine_owned, &prepared_owned, &limits);
        let bindings = pre
            .instantiate(&mut store)
            .map_err(|e| WasmError::Instantiate(format!("algorithm-plugin instantiate: {e}")))?;
        Ok(AlgorithmPluginInstance {
            store,
            bindings,
            limits,
        })
    };
    let pool = WasmInstancePool::new(crate::pool::PoolConfig::default(), factory)?;
    Ok(Arc::new(pool))
}

/// Build an `AlgorithmSignature` from declared `"name:type"` yield strings.
fn build_algorithm_signature(
    args: &[WireArgType],
    yields: &[String],
) -> Result<uni_plugin::traits::algorithm::AlgorithmSignature, WasmError> {
    use arrow_schema::{DataType, Field};
    use uni_plugin::traits::procedure::NamedArgType;
    // Declared algorithm args are now typed and validated (G4): they were parsed
    // then dropped, leaving `signature.args` empty so `coerce_config_json` was a
    // no-op. `WireArgType::CypherValue` lets a guest declare a variable-length
    // seed set without generating the plugin per-arity.
    let mut named_args: Vec<NamedArgType> = args
        .iter()
        .enumerate()
        .map(|(i, w)| {
            Ok(NamedArgType {
                name: format!("arg{i}").into(),
                ty: wire_arg(w)?,
                default: None,
                doc: String::new(),
            })
        })
        .collect::<Result<_, WasmError>>()?;
    // Accept the optional trailing projection-config object the CALL convention
    // appends after the guest args (stripped by the adapter before the guest runs).
    named_args.push(NamedArgType::projection_config());
    let output_fields: Vec<Field> = yields
        .iter()
        .enumerate()
        .map(|(i, spec)| {
            let (name, type_name) = match spec.split_once(':') {
                Some((n, t)) => (n.trim().to_string(), t.trim()),
                None => (format!("col{i}"), spec.as_str()),
            };
            let dt = match type_name.to_ascii_lowercase().as_str() {
                "int" | "integer" | "i64" => DataType::Int64,
                "float" | "double" | "f64" => DataType::Float64,
                other => {
                    return Err(WasmError::InvalidWasm(format!(
                        "algorithm yield type `{other}` unsupported (int/float)"
                    )));
                }
            };
            Ok(Field::new(name, dt, false))
        })
        .collect::<Result<_, WasmError>>()?;
    Ok(uni_plugin::traits::algorithm::AlgorithmSignature {
        output_fields,
        args: named_args,
        docs: String::new(),
        ..Default::default()
    })
}

/// Translate one wire arg type into the internal [`ArgType`].
fn wire_arg(w: &WireArgType) -> Result<uni_plugin::traits::scalar::ArgType, WasmError> {
    use uni_plugin::traits::scalar::ArgType;
    Ok(match w {
        WireArgType::Primitive { arrow } => ArgType::Primitive(arrow_name_to_dt(arrow)?),
        WireArgType::CypherValue => ArgType::CypherValue,
        WireArgType::Vector { len, element } => ArgType::Vector {
            len: *len,
            element: arrow_name_to_dt(element)?,
        },
        WireArgType::Variadic { inner } => ArgType::Variadic(Box::new(wire_arg(inner)?)),
    })
}

/// Parse a wire `volatility` string into a DataFusion [`Volatility`].
fn parse_volatility(s: &str) -> Result<datafusion::logical_expr::Volatility, WasmError> {
    use datafusion::logical_expr::Volatility;
    Ok(match s {
        "immutable" => Volatility::Immutable,
        "stable" => Volatility::Stable,
        "volatile" => Volatility::Volatile,
        other => {
            return Err(WasmError::InvalidWasm(format!(
                "unsupported volatility: `{other}`"
            )));
        }
    })
}

/// Parse a wire `null_handling` string into a [`NullHandling`].
fn parse_null_handling(s: &str) -> Result<uni_plugin::traits::scalar::NullHandling, WasmError> {
    use uni_plugin::traits::scalar::NullHandling;
    Ok(match s {
        "propagate" => NullHandling::PropagateNulls,
        "user_handled" => NullHandling::UserHandled,
        other => {
            return Err(WasmError::InvalidWasm(format!(
                "unsupported null_handling: `{other}`"
            )));
        }
    })
}

/// Parse a wire procedure `mode` string into a [`ProcedureMode`].
fn parse_proc_mode(s: &str) -> Result<uni_plugin::traits::procedure::ProcedureMode, WasmError> {
    use uni_plugin::traits::procedure::ProcedureMode;
    Ok(match s {
        "read" => ProcedureMode::Read,
        "write" => ProcedureMode::Write,
        "schema" => ProcedureMode::Schema,
        "dbms" => ProcedureMode::Dbms,
        other => {
            return Err(WasmError::InvalidWasm(format!(
                "unsupported procedure mode: `{other}`"
            )));
        }
    })
}

fn wire_agg_sig_to_internal(
    wire_sig: &WireFnSignature,
    wire_state: &WireArgType,
) -> Result<uni_plugin::traits::aggregate::AggSignature, WasmError> {
    use arrow_schema::Field;
    use uni_plugin::traits::aggregate::AggSignature;

    let internal = wire_fn_sig_to_internal(wire_sig)?;
    let state_field = match wire_state {
        WireArgType::Primitive { arrow } => {
            let dt = arrow_name_to_dt(arrow)?;
            Field::new("state", dt, true)
        }
        _ => {
            return Err(WasmError::InvalidWasm(
                "aggregate state must be a Primitive Arrow type".to_owned(),
            ));
        }
    };
    Ok(AggSignature {
        volatility: internal.volatility,
        args: internal.args,
        returns: internal.returns,
        state_fields: vec![state_field],
        supports_partial: true,
    })
}

fn wire_proc_sig_to_internal(
    args: &[WireArgType],
    yields: &[WireArgType],
    mode: &str,
) -> Result<uni_plugin::traits::procedure::ProcedureSignature, WasmError> {
    use arrow_schema::Field;
    use uni_plugin::capability::SideEffects;
    use uni_plugin::traits::procedure::{NamedArgType, ProcedureSignature};
    use uni_plugin::traits::scalar::ArgType;

    let named_args: Vec<NamedArgType> = args
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let ty = wire_arg(w)?;
            Ok::<NamedArgType, WasmError>(NamedArgType {
                name: format!("arg{i}").into(),
                ty,
                default: None,
                doc: String::new(),
            })
        })
        .collect::<Result<_, _>>()?;
    let yield_fields: Vec<Field> = yields
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let ty = wire_arg(w)?;
            let dt = match ty {
                ArgType::Primitive(d) => d,
                ArgType::CypherValue | ArgType::Variadic(_) => arrow_schema::DataType::LargeBinary,
                ArgType::Vector { element, .. } => element,
            };
            Ok::<Field, WasmError>(Field::new(format!("yield{i}"), dt, true))
        })
        .collect::<Result<_, _>>()?;
    Ok(ProcedureSignature {
        args: named_args,
        yields: yield_fields,
        mode: parse_proc_mode(mode)?,
        side_effects: SideEffects::default(),
        retry_contract: None,
        batch_input: None,
        docs: String::new(),
    })
}

fn arrow_name_to_dt(name: &str) -> Result<arrow_schema::DataType, WasmError> {
    uni_plugin::adapter_common::arrow_types::arrow_name_to_datatype(name)
        .ok_or_else(|| WasmError::InvalidWasm(format!("unsupported arrow primitive: `{name}`")))
}

fn wire_fn_sig_to_internal(
    wire: &WireFnSignature,
) -> Result<uni_plugin::traits::scalar::FnSignature, WasmError> {
    use uni_plugin::traits::scalar::{ArgType, FnSignature};

    let args: Vec<ArgType> = wire.args.iter().map(wire_arg).collect::<Result<_, _>>()?;
    Ok(FnSignature {
        args,
        returns: wire_arg(&wire.returns)?,
        volatility: parse_volatility(&wire.volatility)?,
        null_handling: parse_null_handling(&wire.null_handling)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use uni_plugin::{Capability, CapabilitySet};

    /// A `kind:"vector"` arg used to load over Extism and be *rejected* here:
    /// this loader carried its own `WireArgType` with only `Primitive` and
    /// `CypherValue`, and `deny_unknown_fields` turned the extra variants into
    /// a parse error. The schema is now shared, so it must both parse and
    /// convert to a `FixedSizeList`-backed `ArgType::Vector`.
    #[test]
    fn vector_wire_arg_parses_and_converts() {
        use uni_plugin::traits::scalar::ArgType;

        let json = r#"{"kind":"vector","len":128,"element":"float32"}"#;
        let wire: WireArgType = serde_json::from_str(json).expect("vector arg must parse");

        let converted = wire_arg(&wire).expect("vector arg must convert");
        match converted {
            ArgType::Vector { len, element } => {
                assert_eq!(len, 128);
                assert_eq!(element, arrow_schema::DataType::Float32);
            }
            other => panic!("expected ArgType::Vector, got {other:?}"),
        }
    }

    /// Same story for `kind:"variadic"`, including the recursive inner type.
    #[test]
    fn variadic_wire_arg_parses_and_converts() {
        use uni_plugin::traits::scalar::ArgType;

        let json = r#"{"kind":"variadic","inner":{"kind":"primitive","arrow":"int64"}}"#;
        let wire: WireArgType = serde_json::from_str(json).expect("variadic arg must parse");

        match wire_arg(&wire).expect("variadic arg must convert") {
            ArgType::Variadic(inner) => match *inner {
                ArgType::Primitive(dt) => assert_eq!(dt, arrow_schema::DataType::Int64),
                other => panic!("expected inner Primitive, got {other:?}"),
            },
            other => panic!("expected ArgType::Variadic, got {other:?}"),
        }
    }

    /// Build a manifest JSON declaring the given (kebab-case) capability names.
    fn manifest_json(caps: &[&str]) -> String {
        let caps_json: Vec<String> = caps.iter().map(|c| format!("\"{c}\"")).collect();
        format!(
            r#"{{ "id": "ai.example.test", "version": "1.0.0", "capabilities": [{}] }}"#,
            caps_json.join(", ")
        )
    }

    #[test]
    fn loader_constructs() {
        let _ = WasmLoader::new();
    }

    #[test]
    fn prepare_parses_minimal_manifest() {
        let l = WasmLoader::new();
        let json = manifest_json(&[]);
        let prep = l.prepare(json.as_bytes(), &CapabilitySet::new()).unwrap();
        assert_eq!(prep.manifest.id, "ai.example.test");
        assert!(prep.effective.is_empty());
    }

    #[test]
    fn prepare_intersects_capabilities() {
        let l = WasmLoader::new();
        // Declared: filesystem + network + kms (bare names → zero-attenuation).
        let json = manifest_json(&["filesystem", "network", "kms"]);
        // Host grants only filesystem + network.
        let grants = CapabilitySet::from_iter_of([
            Capability::Filesystem {
                read: vec![],
                write: vec![],
            },
            Capability::Network { allow: vec![] },
        ]);
        let prep = l.prepare(json.as_bytes(), &grants).unwrap();
        assert_eq!(prep.effective.len(), 2);
        assert!(
            prep.effective
                .contains_variant(&Capability::Network { allow: vec![] })
        );
        assert!(
            !prep
                .effective
                .contains_variant(&Capability::Kms { key_ids: vec![] })
        );
    }

    #[test]
    fn prepare_carries_structured_network_allowlist() {
        let l = WasmLoader::new();
        // Structured declaration with an allow-list; grant the same.
        let json = r#"{ "id": "a.b", "version": "1.0.0",
            "capabilities": [{"kind":"network","allow":["https://api.example/**"]}] }"#;
        let grants = CapabilitySet::from_iter_of([Capability::Network {
            allow: vec!["https://api.example/**".into()],
        }]);
        let prep = l.prepare(json.as_bytes(), &grants).unwrap();
        // The intersected grant retains the host's allow-list patterns.
        assert!(
            prep.effective
                .iter()
                .any(|c| c.network_allows("https://api.example/v1/x"))
        );
        assert!(
            !prep
                .effective
                .iter()
                .any(|c| c.network_allows("https://evil.example/x"))
        );
    }

    #[test]
    fn prepare_rejects_malformed_manifest() {
        let l = WasmLoader::new();
        let err = l.prepare(b"not json", &CapabilitySet::new()).unwrap_err();
        assert!(matches!(err, WasmError::InvalidWasm(_)));
    }

    #[test]
    fn instantiate_rejects_garbage_bytes() {
        let l = WasmLoader::new();
        let prep = l
            .prepare(
                b"{\"id\":\"a.b\",\"version\":\"0.0.0\"}",
                &CapabilitySet::new(),
            )
            .unwrap();
        let err = l.instantiate(b"not real wasm", &prep).unwrap_err();
        assert!(matches!(err, WasmError::InvalidWasm(_)));
    }

    /// Regression tests for architecture review finding §2.3: wall-clock
    /// timeouts were a no-op (epoch deadline set, but nothing ticked the
    /// engine epoch), `memory_max_pages` was parsed but never applied, fuel
    /// was set once per store instead of per call, and resource-limit traps
    /// were misclassified as `WasmError::Invoke`. The engine/limits helpers
    /// are exercised with core wasm modules (component fixtures need
    /// cargo-component; the enforcement mechanisms are identical).
    mod resource_limits {
        use super::*;

        fn empty_manifest() -> ComponentManifest {
            serde_json::from_str(r#"{"id":"a.b","version":"0.0.0"}"#).unwrap()
        }

        fn manifest_with(json: &str) -> ComponentManifest {
            serde_json::from_str(json).unwrap()
        }

        fn test_store(engine: &Engine) -> Store<HostState> {
            Store::new(engine, HostState::new(CapabilitySet::new(), None))
        }

        #[test]
        fn effective_limits_defaults_and_overrides() {
            // Undeclaring plugins get the host floors.
            let defaults = EffectiveLimits::resolve(&empty_manifest());
            assert_eq!(defaults.timeout_ms, DEFAULT_TIMEOUT_MS);
            assert_eq!(defaults.memory_max_pages, DEFAULT_MEMORY_MAX_PAGES);
            assert_eq!(defaults.fuel_per_call, None);

            // Declared values win over the floors.
            let declared = EffectiveLimits::resolve(&manifest_with(
                r#"{"id":"a.b","version":"0.0.0",
                    "timeout_ms":120000,"memory_max_pages":64,"fuel_per_call":5000}"#,
            ));
            assert_eq!(declared.timeout_ms, 120_000);
            assert_eq!(declared.memory_max_pages, 64);
            assert_eq!(declared.fuel_per_call, Some(5_000));
        }

        /// THE timeout repro: an infinite pure-compute loop must trap with
        /// `Trap::Interrupt` within (roughly) the configured wall-clock
        /// budget. Before the per-engine epoch ticker existed this call hung
        /// forever despite `timeout_ms` being configured.
        #[test]
        fn infinite_loop_traps_within_timeout() {
            let limits = EffectiveLimits::resolve(&manifest_with(
                r#"{"id":"a.b","version":"0.0.0","timeout_ms":200}"#,
            ));
            let engine = build_engine(&limits).unwrap();
            let module = wasmtime::Module::new(
                &engine,
                wat::parse_str(r#"(module (func (export "spin") (loop (br 0))))"#).unwrap(),
            )
            .unwrap();
            let mut store = test_store(&engine);
            apply_resource_limits(&mut store, &limits);
            let instance = wasmtime::Instance::new(&mut store, &module, &[]).unwrap();
            let spin = instance
                .get_typed_func::<(), ()>(&mut store, "spin")
                .unwrap();

            let start = std::time::Instant::now();
            let err = spin.call(&mut store, ()).expect_err("must trap, not hang");
            let elapsed = start.elapsed();

            assert_eq!(
                err.downcast_ref::<wasmtime::Trap>(),
                Some(&wasmtime::Trap::Interrupt),
                "expected an epoch interrupt trap, got: {err}"
            );
            // 200ms budget at 50ms ticks; generous ceiling so slow CI can't flake.
            assert!(
                elapsed < std::time::Duration::from_secs(10),
                "timeout took {elapsed:?}, expected ~200ms"
            );
            assert!(matches!(
                classify_trap("spin", &err),
                WasmError::ResourceLimit(_)
            ));
        }

        /// `memory_max_pages` must be enforced by the store limiter:
        /// `memory.grow` past the cap fails (returns -1) instead of growing.
        #[test]
        fn memory_grow_beyond_cap_fails() {
            let limits = EffectiveLimits::resolve(&manifest_with(
                r#"{"id":"a.b","version":"0.0.0","memory_max_pages":4}"#,
            ));
            let engine = build_engine(&limits).unwrap();
            let module = wasmtime::Module::new(
                &engine,
                wat::parse_str(
                    r#"(module
                        (memory (export "mem") 1)
                        (func (export "grow") (param i32) (result i32)
                            (memory.grow (local.get 0))))"#,
                )
                .unwrap(),
            )
            .unwrap();
            let mut store = test_store(&engine);
            apply_resource_limits(&mut store, &limits);
            let instance = wasmtime::Instance::new(&mut store, &module, &[]).unwrap();
            let grow = instance
                .get_typed_func::<i32, i32>(&mut store, "grow")
                .unwrap();

            // 1 page initial + 3 = 4 pages: at the cap, allowed.
            assert_eq!(grow.call(&mut store, 3).unwrap(), 1, "grow to cap allowed");
            // Any further growth must be denied (memory.grow returns -1).
            assert_eq!(
                grow.call(&mut store, 1).unwrap(),
                -1,
                "grow past cap denied"
            );
        }

        /// Fuel exhaustion traps `OutOfFuel` and classifies as
        /// `ResourceLimit`; `reset_call_limits` re-arms the budget so pooled
        /// instances get the full `fuel_per_call` on every call.
        #[test]
        fn fuel_exhausts_and_resets_per_call() {
            let limits = EffectiveLimits::resolve(&manifest_with(
                r#"{"id":"a.b","version":"0.0.0","fuel_per_call":10000,"timeout_ms":30000}"#,
            ));
            let engine = build_engine(&limits).unwrap();
            let module = wasmtime::Module::new(
                &engine,
                wat::parse_str(
                    r#"(module
                        (func (export "burn") (param i32)
                            (local $i i32)
                            (loop $l
                                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                                (br_if $l (i32.lt_s (local.get $i) (local.get 0))))))"#,
                )
                .unwrap(),
            )
            .unwrap();
            let mut store = test_store(&engine);
            apply_resource_limits(&mut store, &limits);
            let instance = wasmtime::Instance::new(&mut store, &module, &[]).unwrap();
            let burn = instance
                .get_typed_func::<i32, ()>(&mut store, "burn")
                .unwrap();

            // A small burn fits the budget and consumes measurable fuel.
            reset_call_limits(&mut store, &limits);
            burn.call(&mut store, 100).unwrap();
            let after_first = store.get_fuel().unwrap();
            assert!(after_first < 10_000, "fuel must be consumed");

            // Per-call reset restores the full budget.
            reset_call_limits(&mut store, &limits);
            assert_eq!(store.get_fuel().unwrap(), 10_000);

            // Burning far past the budget traps OutOfFuel → ResourceLimit.
            let err = burn
                .call(&mut store, i32::MAX)
                .expect_err("must run out of fuel");
            assert_eq!(
                err.downcast_ref::<wasmtime::Trap>(),
                Some(&wasmtime::Trap::OutOfFuel),
                "expected out-of-fuel trap, got: {err}"
            );
            assert!(matches!(
                classify_trap("burn", &err),
                WasmError::ResourceLimit(_)
            ));
        }
    }
}
