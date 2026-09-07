//! `ExtismLoader` — top-level entry point for loading Extism plugins.
//!
//! Manifest parsing, capability filtering, and real `extism-sdk`
//! instantiation (with cap-filtered host fns + resource limits) ship
//! here, alongside the end-to-end [`ExtismLoader::load`] path: read the
//! manifest export → re-instantiate with effective grants → read the
//! register export → push adapters into the `PluginRegistrar`.

// Rust guideline compliant

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::error::ExtismError;
use crate::host_fns::HostFnRegistry;

/// Host-imposed default wall-clock budget per call when the manifest does not
/// declare `timeout_ms`. Mirrors `uni_plugin_wasm::loader::DEFAULT_TIMEOUT_MS`
/// so the Extism and Component-Model loaders sandbox identically.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Host-imposed default linear-memory cap (in 64 KiB pages, = 1 GiB) when the
/// manifest does not declare `memory_max_pages`. Mirrors
/// `uni_plugin_wasm::loader::DEFAULT_MEMORY_MAX_PAGES`.
const DEFAULT_MEMORY_MAX_PAGES: u32 = 16_384;

/// Extism host-fn surface majors this host implements.
///
/// The Component Model path keeps its own list in
/// `uni_plugin_wasm::multi_version::SUPPORTED_MAJORS` (currently `[1, 2]`,
/// one per linker version). Extism has a single host-fn surface, so this is
/// `[1]` — but it is a separate list on purpose: the two ABIs version
/// independently.
pub const SUPPORTED_ABI_MAJORS: &[u64] = &[1];

/// Resolve the host major satisfying a plugin's declared `abi-extism` range.
///
/// Mirrors `uni_plugin_wasm::multi_version::major_for_abi`. An **absent**
/// `abi-extism` is treated as `^1`, matching what the Component Model loader
/// assumes when a manifest omits `abi` — so an existing plugin that never
/// declared the field keeps loading.
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] if the range is not a valid semver
///   requirement.
/// - [`ExtismError::AbiUnsupported`] if it names no host-supported major.
fn major_for_abi(plugin: &str, declared: Option<&str>) -> Result<u64, ExtismError> {
    let requested = declared.unwrap_or("^1");
    let abi = uni_plugin::AbiRange::parse(requested).map_err(|e| {
        ExtismError::ManifestInvalid(format!("plugin {plugin}: invalid abi-extism range: {e}"))
    })?;
    SUPPORTED_ABI_MAJORS
        .iter()
        .copied()
        .find(|m| abi.matches(*m))
        .ok_or_else(|| ExtismError::AbiUnsupported {
            plugin: plugin.to_owned(),
            required: requested.to_owned(),
            supported: SUPPORTED_ABI_MAJORS.to_vec(),
        })
}

/// Plugin manifest in the Extism plugin's canonical JSON form.
///
/// Returned from the plugin's `manifest` export. Mirrors the shape of
/// the §14 manifest, but on the Extism wire.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtismPluginManifest {
    /// Reverse-DNS plugin id.
    pub id: String,
    /// Semver string.
    pub version: String,
    /// Extism ABI range the plugin was built against.
    #[serde(default, rename = "abi-extism")]
    pub abi_extism: Option<String>,
    /// Capabilities the plugin declares it needs — each a bare name
    /// (`"network"`) or a structured object with attenuation patterns
    /// (`{"kind":"network","allow":[...]}`); see [`uni_plugin::ManifestCapability`].
    #[serde(default)]
    pub capabilities: Vec<uni_plugin::ManifestCapability>,
    /// Determinism class (`"pure"`, `"session-scoped"`, `"nondeterministic"`).
    #[serde(default)]
    pub determinism: Option<String>,
    /// Free-form human description.
    #[serde(default)]
    pub description: Option<String>,

    // Resource limits. All optional — if absent, the host's defaults
    // apply. Plugin authors can request tighter limits than the host
    // default; the host's grant model decides whether to honor a looser
    // request (M6a leaves the negotiation to the caller of `build_plugin`).
    /// Per-call wasmtime fuel limit. Per proposal §10 / §5.5.4.
    #[serde(default)]
    pub fuel_per_call: Option<u64>,
    /// Maximum linear-memory pages (one page = 64 KiB).
    #[serde(default)]
    pub memory_max_pages: Option<u32>,
    /// Wall-clock per-call timeout in milliseconds.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl ExtismPluginManifest {
    /// The declared capabilities as a rich [`uni_plugin::CapabilitySet`].
    #[must_use]
    pub fn declared_capability_set(&self) -> uni_plugin::CapabilitySet {
        uni_plugin::CapabilitySet::from_manifest(self.capabilities.iter().cloned())
    }
}

/// Result of [`ExtismLoader::prepare`] — everything the host needs to
/// instantiate the plugin once the SDK integration is wired.
#[derive(Debug, Clone)]
pub struct PreparedExtismPlugin {
    /// Parsed manifest.
    pub manifest: ExtismPluginManifest,
    /// Capabilities granted to the plugin (rich, with attenuation patterns):
    /// intersection of declared (manifest) and granted (host).
    pub effective: uni_plugin::CapabilitySet,
    /// Host fns the plugin is allowed to import (post-capability filter).
    pub allowed_host_fns: Vec<String>,
    /// Capabilities the plugin requested but the host did not grant —
    /// the loader uses these for diagnostics and decides whether to
    /// reject the load or proceed with reduced functionality.
    pub denied_capabilities: Vec<String>,
}

/// Top-level Extism plugin loader.
///
/// Construct one per uni-db instance; the loader owns the
/// [`HostFnRegistry`] (capability metadata) and a parallel map of the
/// runtime-callable [`extism::Function`]s keyed by host-fn name. The
/// metadata map exists unconditionally so embedders without
/// `extism-runtime` can still introspect the host-fn surface; the
/// runtime functions only materialize when the SDK feature is on.
#[derive(Default)]
pub struct ExtismLoader {
    host_fns: HostFnRegistry,
    /// Concrete host-fn implementations. Inserts via
    /// [`Self::register_host_function`] keep this in lock-step with the
    /// [`HostFnSpec`] metadata; `build_plugin` filters this map by
    /// the plugin's effective capability set before handing functions to
    /// `extism::PluginBuilder`.
    // `extism::Function` doesn't implement Debug, so we hand-roll Debug
    // for the enclosing type below.
    runtime_fns: BTreeMap<String, extism::Function>,
    /// Optional KMS provider backing `uni_kms_*`. Absent → those fns error
    /// loudly at call time ("no KMS provider configured").
    kms: Option<std::sync::Arc<dyn uni_plugin::KmsProvider>>,
    /// Optional secret store backing `uni_secret_acquire`.
    secrets: Option<std::sync::Arc<uni_plugin::secrets::SecretStore>>,
    /// Optional HTTP egress backing `uni_http_*`.
    http: Option<std::sync::Arc<dyn uni_plugin::HttpEgress>>,
    /// Optional GraphCompute session registry backing `uni_graph_call`.
    graph: Option<uni_plugin_builtin::algorithms::graph_compute::SharedRegistry>,
}

impl std::fmt::Debug for ExtismLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtismLoader")
            .field("host_fns", &self.host_fns)
            .field("runtime_fn_count", &self.runtime_fns.len())
            .finish()
    }
}

impl ExtismLoader {
    /// Construct a fresh loader with an empty host-fn registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mutable access to the host-fn registry (metadata).
    pub fn host_fns_mut(&mut self) -> &mut HostFnRegistry {
        &mut self.host_fns
    }

    /// Shared access to the host-fn registry (metadata).
    #[must_use]
    pub fn host_fns(&self) -> &HostFnRegistry {
        &self.host_fns
    }

    /// Register a host function with both its metadata and its concrete
    /// `extism::Function` implementation.
    ///
    /// The function is invocable from any plugin whose effective
    /// capability set contains `spec.required_capability` (or any plugin,
    /// if `required_capability` is `None`). The capability filter runs at
    /// [`Self::build_plugin`] time — plugins that don't pass the filter
    /// never see this function in their import table.
    pub fn register_host_function(
        &mut self,
        spec: crate::host_fns::HostFnSpec,
        function: extism::Function,
    ) {
        let name = spec.name.clone();
        self.host_fns.register(spec);
        self.runtime_fns.insert(name, function);
    }

    /// Number of registered runtime functions. Diagnostic / test helper.
    #[must_use]
    pub fn runtime_fn_count(&self) -> usize {
        self.runtime_fns.len()
    }

    /// Names of the host fns a plugin holding `caps` is allowed to import.
    ///
    /// A host fn is allowed when its `required_capability` *variant* is in
    /// `caps`, or when it declares no required capability (always
    /// available). Pattern attenuation (key-id / secret-id / URL globs) is
    /// enforced later, in the host-fn body — this is the structural,
    /// link-time half of capability enforcement.
    ///
    /// Used both for the per-load allow-list ([`Self::prepare_parsed`],
    /// against the effective `declared ∩ granted` set) and for the pass-1
    /// bootstrap ([`Self::load`], against the host's *offered* grants).
    /// Both call sites must produce byte-identical sets for the same
    /// capability input, so the filter lives here once.
    fn allowed_host_fn_names(&self, caps: &uni_plugin::CapabilitySet) -> Vec<String> {
        self.host_fns
            .iter()
            .filter(|spec| match &spec.required_capability {
                None => true,
                Some(req) => caps.contains_variant(req),
            })
            .map(|s| s.name.clone())
            .collect()
    }

    /// Attach a KMS provider backing `uni_kms_*` (builder style).
    ///
    /// Pair with [`crate::host_svc::register_default_host_svc`] to register the
    /// metadata specs; the concrete functions are built per load with the
    /// effective grant set so call-time attenuation is enforced.
    #[must_use]
    pub fn with_kms(mut self, kms: std::sync::Arc<dyn uni_plugin::KmsProvider>) -> Self {
        self.kms = Some(kms);
        self
    }

    /// Attach a secret store backing `uni_secret_acquire` (builder style).
    #[must_use]
    pub fn with_secret_store(
        mut self,
        store: std::sync::Arc<uni_plugin::secrets::SecretStore>,
    ) -> Self {
        self.secrets = Some(store);
        self
    }

    /// Attach an HTTP egress backing `uni_http_*` (builder style).
    #[must_use]
    pub fn with_http(mut self, http: std::sync::Arc<dyn uni_plugin::HttpEgress>) -> Self {
        self.http = Some(http);
        self
    }

    /// Attach a GraphCompute session registry backing `uni_graph_call`.
    #[must_use]
    pub fn with_graph(
        mut self,
        registry: uni_plugin_builtin::algorithms::graph_compute::SharedRegistry,
    ) -> Self {
        self.graph = Some(registry);
        self
    }

    /// Shared access to the GraphCompute registry, if configured.
    #[must_use]
    pub fn graph_registry(
        &self,
    ) -> Option<&uni_plugin_builtin::algorithms::graph_compute::SharedRegistry> {
        self.graph.as_ref()
    }

    /// The host-fn map for a single load: the static `runtime_fns` plus the
    /// per-load capability-gated service functions (`uni_kms_*`,
    /// `uni_secret_acquire`, `uni_http_*`).
    ///
    /// Each service function is built with `prepared.effective` and the loader's
    /// service handles baked into its [`extism::UserData`], so it enforces *this*
    /// load's attenuation patterns. Only the names this plugin is actually
    /// allowed (`prepared.allowed_host_fns`) are materialized, so a plugin
    /// without the matching capability variant never pays the build cost.
    fn runtime_fns_for_load(
        &self,
        prepared: &PreparedExtismPlugin,
    ) -> BTreeMap<String, extism::Function> {
        let mut fns = self.runtime_fns.clone();
        // Build the per-load context once; cloned (cheaply, Arc handles) into
        // each materialized service function.
        let ctx = crate::host_svc::HostSvcCtx {
            effective: prepared.effective.clone(),
            kms: self.kms.clone(),
            secrets: self.secrets.clone(),
            http: self.http.clone(),
            graph: self.graph.clone(),
        };
        for name in &prepared.allowed_host_fns {
            if fns.contains_key(name) {
                continue;
            }
            if let Some(function) = crate::host_svc::build_service_fn(name, &ctx) {
                fns.insert(name.clone(), function);
            }
        }
        fns
    }

    /// Parse a manifest JSON blob (as the plugin's `manifest` export
    /// returns) and filter the host-fn registry through the granted
    /// capability set.
    ///
    /// This is the **deterministic, sandbox-free** portion of the M6a
    /// loader path: it doesn't instantiate any wasm. The host can use
    /// the returned [`PreparedExtismPlugin`] to decide whether to
    /// proceed with full SDK instantiation, prompt the user for
    /// additional capability grants, or reject the load outright.
    ///
    /// # Errors
    ///
    /// - [`ExtismError::ManifestInvalid`] if the JSON doesn't parse or
    ///   doesn't match [`ExtismPluginManifest`].
    pub fn prepare(
        &self,
        manifest_json: &[u8],
        grants: &uni_plugin::CapabilitySet,
    ) -> Result<PreparedExtismPlugin, ExtismError> {
        let manifest = crate::exports::parse_manifest_json(manifest_json)?;
        self.prepare_parsed(manifest, grants)
    }

    /// Intersect declared/granted capabilities for an already-parsed
    /// manifest, skipping the JSON round-trip.
    ///
    /// [`Self::load`] reads the manifest export off a bootstrap plugin
    /// (parsed `ExtismPluginManifest`), then needs the combined
    /// cap-intersection and host-fn-allow-list result. The previous
    /// implementation re-serialized the parsed struct to JSON and called
    /// [`Self::prepare`] which deserialized it straight back — a
    /// wasteful round-trip whose only purpose was reusing the
    /// cap-intersection loop. This entry point preserves the loop and
    /// skips the (de)serialization.
    ///
    /// # Errors
    ///
    /// - [`ExtismError::AbiUnsupported`] if the manifest's `abi-extism` range
    ///   names no host-supported major.
    /// - [`ExtismError::ManifestInvalid`] if that range is not valid semver.
    pub fn prepare_parsed(
        &self,
        manifest: ExtismPluginManifest,
        grants: &uni_plugin::CapabilitySet,
    ) -> Result<PreparedExtismPlugin, ExtismError> {
        // ABI gate first: reject before any capability is intersected, so a
        // guest built against a host-fn surface this host does not implement
        // never reaches a prepared plugin carrying real grants.
        major_for_abi(&manifest.id, manifest.abi_extism.as_deref())?;

        // Effective = declared ∩ granted (retains per-variant attenuation).
        let declared = manifest.declared_capability_set();
        let effective = declared.intersect(grants);
        // Shared variant-aware derivation via `CapabilitySet::denied_against`.
        let denied: Vec<String> = declared
            .denied_against(&effective)
            .iter()
            .map(|c| format!("{c:?}"))
            .collect();

        // Host-fn filter: only fns whose required_capability *variant* is in
        // the effective set (or which have no required_capability — always
        // available). Pattern attenuation is enforced in the host-fn body.
        let allowed = self.allowed_host_fn_names(&effective);

        Ok(PreparedExtismPlugin {
            manifest,
            effective,
            allowed_host_fns: allowed,
            denied_capabilities: denied,
        })
    }

    /// Build an `extism::Plugin` from raw wasm bytes against a prepared
    /// capability set.
    ///
    /// Capability-gated host functions are filtered through
    /// `prepared.allowed_host_fns` — fns whose `required_capability` is
    /// not in the plugin's effective set are *omitted from the plugin's
    /// import table*. This is the Extism analogue of Component Model's
    /// linker absence: the plugin literally cannot resolve an unauthorized
    /// host fn at link time. Per proposal §5.6.2 this is the structural
    /// half of capability enforcement; the call-time pattern attenuation in
    /// each `host_svc` body (`kms_allows` / `secret_allows` /
    /// `network_allows`) is the defense-in-depth half.
    ///
    /// Resource limits declared in the parsed manifest are applied to
    /// the underlying wasmtime config: `memory_max_pages` (linear
    /// memory cap), `timeout_ms` (per-call wall-clock), `fuel_per_call`
    /// (instruction budget). If a field is `None`, the host's default
    /// (no cap) applies.
    ///
    /// # Errors
    ///
    /// - [`ExtismError::Instantiate`] if the wasm bytes fail to compile,
    ///   link, or instantiate (invalid wasm, missing required imports,
    ///   wasmtime errors).
    /// - [`ExtismError::Internal`] if a runtime function recorded in the
    ///   registry's allow-list is somehow absent from `runtime_fns`
    ///   (should be unreachable; indicates a registry-state bug).
    pub fn build_plugin(
        &self,
        bytes: &[u8],
        prepared: &PreparedExtismPlugin,
    ) -> Result<extism::Plugin, ExtismError> {
        build_plugin_from_parts(bytes, prepared, &self.runtime_fns_for_load(prepared))
    }

    /// End-to-end load: read manifest, intersect with host grants,
    /// re-instantiate with effective caps, read register export, push
    /// adapters into the supplied [`uni_plugin::PluginRegistrar`].
    ///
    /// The two-pass dance is the proposal's §5.6 contract: the host
    /// cannot know what capabilities the plugin needs until it reads
    /// the `manifest` export, and reading that export requires a built
    /// plugin. The first pass uses an **empty grant set** — the
    /// `manifest` export must be implementable without any
    /// capability-gated host fn, which is trivially true (it just
    /// returns JSON). The second pass rebuilds with the intersected
    /// grants and the register export is read against that.
    ///
    /// The currently-supported registration kinds are
    /// [`crate::exports::RegistrationEntry::Scalar`]; aggregate and
    /// procedure adapters land in M6a.2. Entries of unsupported kinds
    /// cause [`ExtismError::OutputDecode`] — better to fail loudly than
    /// silently ignore part of a plugin's surface.
    ///
    /// # Errors
    ///
    /// - [`ExtismError::Instantiate`] for wasmtime / extism build
    ///   failures.
    /// - [`ExtismError::ManifestInvalid`] for malformed manifests or
    ///   unsupported argument types.
    /// - [`ExtismError::InvalidPlugin`] if required exports
    ///   (`manifest`, `register`) are missing.
    /// - [`ExtismError::OutputDecode`] for malformed register payloads
    ///   or unsupported entry kinds.
    /// - [`ExtismError::Internal`] for `PluginRegistrar` registration
    ///   failures (capability / qname conflicts).
    pub fn load(
        &self,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
        registrar: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<LoadOutcome, ExtismError> {
        // Pass 1: read the manifest export. A wasm module resolves *all* of
        // its imports at instantiate time, so a guest that imports a host fn
        // (e.g. `uni_http_get`) cannot even be instantiated to read its
        // manifest unless that import is present in the linker. We therefore
        // materialize the service fns whose capability variant the host offers
        // (by NAME, for the linker) — but with an EMPTY effective grant set, so
        // if the guest's `manifest` export actually *calls* one of them (nothing
        // enforces manifest-export purity), the call is denied at the fn body's
        // allow-list check instead of running with the host's full grants before
        // `declared ∩ grants` intersection. Otherwise a zero-declaring plugin
        // could exfiltrate/sign/read secrets during bootstrap. The live
        // execution pool below is rebuilt with the real attenuation. A guest
        // importing a host fn the host did *not* offer still fails to instantiate
        // here, which is the intended link-time gate.
        let bootstrap_allowed = self.allowed_host_fn_names(host_grants);
        let bootstrap_prepared = PreparedExtismPlugin {
            manifest: ExtismPluginManifest {
                id: String::new(),
                version: String::new(),
                abi_extism: None,
                capabilities: Vec::new(),
                determinism: None,
                description: None,
                fuel_per_call: None,
                memory_max_pages: None,
                timeout_ms: None,
            },
            // Empty, NOT host_grants: bootstrap host-fn bodies must grant nothing.
            effective: uni_plugin::CapabilitySet::new(),
            allowed_host_fns: bootstrap_allowed,
            denied_capabilities: Vec::new(),
        };
        let mut bootstrap_plugin = self.build_plugin(bytes, &bootstrap_prepared)?;
        let parsed_manifest = crate::exports::read_manifest_export(&mut bootstrap_plugin)?;
        drop(bootstrap_plugin);

        // Rewrite the registrar's plugin id to match the manifest. The
        // caller supplies a placeholder id (e.g., `"extism.loading"`)
        // because the canonical id is unknown until pass 1 reads the
        // manifest export. Setting it here lets `validate_qname`
        // accept entries in the plugin's declared namespace.
        registrar.set_plugin_id(uni_plugin::PluginId::new(parsed_manifest.id.clone()));

        // Pass 2: intersect declared/granted, re-build with full host
        // fn set, read register export. The parsed manifest from pass 1
        // is reused directly via `prepare_parsed`, avoiding a JSON
        // re-serialize / re-parse round-trip.
        let prepared = self.prepare_parsed(parsed_manifest, host_grants)?;

        // Build the instance pool: factory closes over owned bytes,
        // prepared (cap-filtered), and the per-load host-fn map (static
        // `runtime_fns` plus the capability-gated `uni_kms_*` / `uni_secret_*`
        // / `uni_http_*` service fns built with this load's effective grant
        // set). Pre-warm count is from `PoolConfig::default` (proposal §5.3.1 —
        // `min_warm = 1`); future commits surface this through the manifest.
        let pool = build_pool(bytes, &prepared, &self.runtime_fns_for_load(&prepared))?;

        // Lease one warm instance, read the register export once, and
        // drop the lease. A previous two-pass shape re-read the same
        // export from a fresh instance; both reads were pure JSON
        // parses of the same wasm export, so the second pass added no
        // signal.
        let mut leased = crate::pool::PooledInstance::acquire(std::sync::Arc::clone(&pool))?;
        let registration = crate::exports::read_register_export(leased.get_mut())?;
        drop(leased);

        let mut scalars_registered: Vec<String> = Vec::new();
        let mut aggregates_registered: Vec<String> = Vec::new();
        let mut procedures_registered: Vec<String> = Vec::new();
        let mut algorithms_registered: Vec<String> = Vec::new();

        for entry in registration.entries {
            match entry {
                crate::exports::RegistrationEntry::Scalar { qname, signature } => {
                    let parsed_qname = parse_entry_qname(&qname)?;
                    let sig = crate::wire_translate::wire_fn_sig_to_internal(&signature)?;
                    let adapter = std::sync::Arc::new(crate::adapter::ExtismScalarFn::new(
                        std::sync::Arc::clone(&pool),
                        parsed_qname.clone(),
                        sig.clone(),
                    ));
                    registrar
                        .scalar_fn(parsed_qname, sig, adapter)
                        .map_err(|e| {
                            ExtismError::Internal(format!("registrar.scalar_fn `{qname}`: {e}"))
                        })?;
                    scalars_registered.push(qname);
                }
                crate::exports::RegistrationEntry::Aggregate {
                    qname,
                    signature,
                    state,
                } => {
                    let parsed_qname = parse_entry_qname(&qname)?;
                    let sig = crate::wire_translate::wire_agg_sig_to_internal(&signature, &state)?;
                    let adapter =
                        std::sync::Arc::new(crate::adapter_aggregate::ExtismAggregateFn::new(
                            std::sync::Arc::clone(&pool),
                            parsed_qname.clone(),
                            sig.clone(),
                        ));
                    registrar
                        .aggregate_fn(parsed_qname, sig, adapter)
                        .map_err(|e| {
                            ExtismError::Internal(format!("registrar.aggregate_fn `{qname}`: {e}"))
                        })?;
                    aggregates_registered.push(qname);
                }
                crate::exports::RegistrationEntry::Procedure {
                    qname,
                    args,
                    yields,
                    mode,
                } => {
                    let parsed_qname = parse_entry_qname(&qname)?;
                    let sig =
                        crate::wire_translate::wire_proc_sig_to_internal(&args, &yields, &mode)?;
                    let adapter =
                        std::sync::Arc::new(crate::adapter_procedure::ExtismProcedure::new(
                            std::sync::Arc::clone(&pool),
                            parsed_qname.clone(),
                            sig.clone(),
                        ));
                    registrar
                        .procedure(parsed_qname, sig, adapter)
                        .map_err(|e| {
                            ExtismError::Internal(format!("registrar.procedure `{qname}`: {e}"))
                        })?;
                    procedures_registered.push(qname);
                }
                crate::exports::RegistrationEntry::Algorithm {
                    qname,
                    args,
                    yields,
                } => {
                    let parsed_qname = parse_entry_qname(&qname)?;
                    let registry = self.graph.clone().ok_or_else(|| {
                        ExtismError::Internal(format!(
                            "algorithm `{qname}` needs a GraphCompute registry \
                             (call ExtismLoader::with_graph)"
                        ))
                    })?;
                    let sig = build_algorithm_signature(&args, &yields)?;
                    let adapter =
                        std::sync::Arc::new(crate::adapter_algorithm::ExtismAlgorithm::new(
                            std::sync::Arc::clone(&pool),
                            registry,
                            parsed_qname.clone(),
                            sig,
                        ));
                    registrar.algorithm(parsed_qname, adapter).map_err(|e| {
                        ExtismError::Internal(format!("registrar.algorithm `{qname}`: {e}"))
                    })?;
                    algorithms_registered.push(qname);
                }
            }
        }

        Ok(LoadOutcome {
            plugin_id: prepared.manifest.id.clone(),
            version: prepared.manifest.version.clone(),
            effective_capabilities: prepared
                .effective
                .iter()
                .map(|c| format!("{c:?}"))
                .collect(),
            denied_capabilities: prepared.denied_capabilities,
            scalars_registered,
            aggregates_registered,
            procedures_registered,
            algorithms_registered,
            pool,
        })
    }
}

/// Parse a registration entry's qname, mapping a parse failure to
/// [`ExtismError::OutputDecode`].
///
/// Shared by the three `RegistrationEntry` arms in [`ExtismLoader::load`]
/// so every entry kind reports an invalid qname identically.
fn parse_entry_qname(qname: &str) -> Result<uni_plugin::QName, ExtismError> {
    uni_plugin::QName::parse(qname)
        .map_err(|e| ExtismError::OutputDecode(format!("invalid qname `{qname}`: {e}")))
}

/// Build an `AlgorithmSignature` from declared wire arg types + `"name:type"`
/// yield strings. Declared args are now typed and validated (G4): they were
/// parsed then dropped, leaving `signature.args` empty so `coerce_config_json`
/// was a no-op. `WireArgType::CypherValue` lets a guest declare a
/// variable-length seed set without generating the plugin per-arity.
fn build_algorithm_signature(
    args: &[crate::exports::WireArgType],
    yields: &[String],
) -> Result<uni_plugin::traits::algorithm::AlgorithmSignature, ExtismError> {
    use arrow_schema::{DataType, Field};
    use uni_plugin::traits::procedure::NamedArgType;
    let mut named_args: Vec<NamedArgType> = args
        .iter()
        .enumerate()
        .map(|(i, w)| {
            Ok(NamedArgType {
                name: format!("arg{i}").into(),
                ty: crate::wire_translate::wire_arg_to_internal(w)?,
                default: None,
                doc: String::new(),
            })
        })
        .collect::<Result<_, ExtismError>>()?;
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
                    return Err(ExtismError::OutputDecode(format!(
                        "algorithm yield type `{other}` unsupported (int/float)"
                    )));
                }
            };
            Ok(Field::new(name, dt, false))
        })
        .collect::<Result<_, ExtismError>>()?;
    Ok(uni_plugin::traits::algorithm::AlgorithmSignature {
        output_fields,
        args: named_args,
        docs: String::new(),
        ..Default::default()
    })
}

/// Build an `extism::Plugin` from owned-data inputs.
///
/// Module-private free function so the pool factory closure can call
/// it without holding a reference to the loader. The closure captures
/// `Arc`-owned bytes / prepared / runtime_fns and re-invokes this each
/// time the pool needs to cold-construct a new instance.
fn build_plugin_from_parts(
    bytes: &[u8],
    prepared: &PreparedExtismPlugin,
    runtime_fns: &BTreeMap<String, extism::Function>,
) -> Result<extism::Plugin, ExtismError> {
    let manifest = build_extism_manifest(bytes, &prepared.manifest);
    let mut builder = extism::PluginBuilder::new(manifest).with_wasi(true);
    if let Some(fuel) = prepared.manifest.fuel_per_call {
        builder = builder.with_fuel_limit(fuel);
    }
    let mut selected: Vec<extism::Function> = Vec::with_capacity(prepared.allowed_host_fns.len());
    for fn_name in &prepared.allowed_host_fns {
        let function = runtime_fns.get(fn_name).ok_or_else(|| {
            ExtismError::Internal(format!(
                "allowed host fn `{fn_name}` missing from runtime_fns; \
                 registry-state bug — every spec.name should have a Function"
            ))
        })?;
        selected.push(function.clone());
    }
    builder = builder.with_functions(selected);
    builder
        .build()
        .map_err(|e| ExtismError::Instantiate(e.to_string()))
}

fn build_extism_manifest(bytes: &[u8], plugin_manifest: &ExtismPluginManifest) -> extism::Manifest {
    // Apply the host memory cap and wall-clock timeout UNCONDITIONALLY: an
    // undeclared limit resolves to the host default rather than "unbounded", so
    // an untrusted manifest cannot opt out of its own sandbox (a manifest with
    // all limits `None` previously ran with no memory cap and no timeout).
    // Mirrors the Component-Model loader's `EffectiveLimits::resolve`. A plugin
    // may still declare a *larger* value if it genuinely needs one. (review H15)
    let pages = plugin_manifest
        .memory_max_pages
        .unwrap_or(DEFAULT_MEMORY_MAX_PAGES);
    let ms = plugin_manifest.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS);
    extism::Manifest::new([extism::Wasm::data(bytes.to_vec())])
        .with_memory_max(pages)
        .with_timeout(std::time::Duration::from_millis(ms))
}

fn build_pool(
    bytes: &[u8],
    prepared: &PreparedExtismPlugin,
    runtime_fns: &BTreeMap<String, extism::Function>,
) -> Result<std::sync::Arc<crate::pool::ExtismInstancePool<extism::Plugin>>, ExtismError> {
    let bytes_owned: std::sync::Arc<Vec<u8>> = std::sync::Arc::new(bytes.to_vec());
    let prepared_owned: std::sync::Arc<PreparedExtismPlugin> =
        std::sync::Arc::new(prepared.clone());
    let runtime_fns_owned: std::sync::Arc<BTreeMap<String, extism::Function>> =
        std::sync::Arc::new(runtime_fns.clone());

    let factory = {
        let bytes = std::sync::Arc::clone(&bytes_owned);
        let prepared = std::sync::Arc::clone(&prepared_owned);
        let runtime_fns = std::sync::Arc::clone(&runtime_fns_owned);
        move || build_plugin_from_parts(&bytes, &prepared, &runtime_fns)
    };

    let pool = crate::pool::ExtismInstancePool::new(crate::pool::PoolConfig::default(), factory)?;
    Ok(std::sync::Arc::new(pool))
}

/// Outcome of a successful [`ExtismLoader::load`].
///
/// Carries the diagnostic state the caller (typically `Uni::load_wasm_extism`)
/// needs to construct a `PluginHandle`, surface denied capabilities to the
/// user, and keep the live plugin alive for the duration of the
/// registration.
pub struct LoadOutcome {
    /// Reverse-DNS plugin id from the manifest.
    pub plugin_id: String,
    /// Plugin version from the manifest.
    pub version: String,
    /// Capabilities granted to the plugin (intersection of declared ∩ host).
    pub effective_capabilities: Vec<String>,
    /// Capabilities the plugin requested but the host did not grant.
    pub denied_capabilities: Vec<String>,
    /// Qnames registered as scalar fns.
    pub scalars_registered: Vec<String>,
    /// Qnames registered as aggregate fns.
    pub aggregates_registered: Vec<String>,
    /// Qnames registered as procedures.
    pub procedures_registered: Vec<String>,
    /// Qnames registered as graph-compute algorithms.
    pub algorithms_registered: Vec<String>,
    /// The instance pool, shared across every adapter bound to this
    /// plugin. Adapters hold an `Arc` clone; the pool is kept alive as
    /// long as any adapter remains in the registry.
    pub pool: std::sync::Arc<crate::pool::ExtismInstancePool<extism::Plugin>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_fns::HostFnSpec;
    use uni_plugin::{Capability, CapabilitySet};

    fn manifest_json(caps: &[&str]) -> String {
        let caps_json: Vec<String> = caps.iter().map(|c| format!("\"{c}\"")).collect();
        format!(
            r#"{{ "id": "ai.example.test", "version": "1.0.0", "capabilities": [{}] }}"#,
            caps_json.join(", ")
        )
    }

    /// `abi_extism` was declared on the manifest struct and read NOWHERE — a
    /// workspace-wide grep found only the declaration and a `None` in the
    /// bootstrap literal. So a guest declaring `"abi-extism": "^9"` loaded
    /// against a v1 host and called a mismatched host-fn surface. The
    /// Component Model loader has always gated this via `major_for_abi`.
    #[test]
    fn an_unsupported_abi_range_is_refused() {
        let l = ExtismLoader::new();
        let grants = CapabilitySet::new();
        let with_abi = |abi: &str| {
            format!(r#"{{ "id": "ai.example.test", "version": "1.0.0", "abi-extism": "{abi}" }}"#)
        };

        // A major this host does not implement is refused, and the error names
        // both what was asked for and what is on offer.
        let err = l
            .prepare(with_abi("^9").as_bytes(), &grants)
            .expect_err("an unsupported ABI major must be refused");
        match err {
            ExtismError::AbiUnsupported {
                ref plugin,
                ref required,
                ref supported,
            } => {
                assert_eq!(plugin, "ai.example.test");
                assert_eq!(required, "^9");
                assert_eq!(supported, SUPPORTED_ABI_MAJORS);
            }
            other => panic!("expected AbiUnsupported, got {other:?}"),
        }

        // A range that is not valid semver is a manifest error, not a silent pass.
        assert!(
            matches!(
                l.prepare(with_abi("not-a-range").as_bytes(), &grants),
                Err(ExtismError::ManifestInvalid(_))
            ),
            "an unparseable abi-extism range must be refused"
        );

        // Control: a supported major still loads. Without this the assertions
        // above could pass because `prepare` rejects everything.
        assert!(
            l.prepare(with_abi("^1").as_bytes(), &grants).is_ok(),
            "a supported ABI major must still load"
        );

        // An ABSENT abi-extism assumes ^1, matching what the Component Model
        // loader does for a manifest that omits `abi` — so a plugin that never
        // declared the field keeps loading.
        assert!(
            l.prepare(manifest_json(&[]).as_bytes(), &grants).is_ok(),
            "an absent abi-extism must default to ^1, not become an error"
        );
    }

    #[test]
    fn loader_constructs_with_empty_host_fns() {
        let l = ExtismLoader::new();
        assert!(l.host_fns().is_empty());
    }

    // M6a.1.5: load() is now real. Smoke-test against garbage bytes —
    // pass-1 build_plugin fails with Instantiate. Full e2e against a
    // real plugin lives in tests/instantiate_with_minimal_wasm.rs and
    // (T#7) tests/example_extism_geo_e2e.rs.

    fn fs_cap() -> Capability {
        Capability::Filesystem {
            read: vec![],
            write: vec![],
        }
    }

    #[test]
    fn loader_accepts_host_fn_registrations() {
        let mut l = ExtismLoader::new();
        l.host_fns_mut().register(HostFnSpec {
            name: "host_fs_read".to_owned(),
            required_capability: Some(fs_cap()),
            docs: "Read file.".to_owned(),
        });
        assert_eq!(l.host_fns().len(), 1);
    }

    #[test]
    fn prepare_parses_minimal_manifest() {
        let l = ExtismLoader::new();
        let json = manifest_json(&[]);
        let prep = l.prepare(json.as_bytes(), &CapabilitySet::new()).unwrap();
        assert_eq!(prep.manifest.id, "ai.example.test");
        assert_eq!(prep.manifest.version, "1.0.0");
        assert!(prep.effective.is_empty());
        assert!(prep.denied_capabilities.is_empty());
        assert!(prep.allowed_host_fns.is_empty());
    }

    #[test]
    fn prepare_intersects_declared_and_granted_capabilities() {
        let l = ExtismLoader::new();
        // Declared (kebab bare names → zero-attenuation variants).
        let json = manifest_json(&["filesystem", "network", "kms"]);
        let grants = CapabilitySet::from_iter_of([fs_cap(), Capability::Network { allow: vec![] }]);
        let prep = l.prepare(json.as_bytes(), &grants).unwrap();
        // Granted: Filesystem + Network. Denied: Kms.
        assert_eq!(prep.effective.len(), 2);
        assert!(prep.effective.contains_variant(&fs_cap()));
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
    fn prepare_filters_host_fns_through_effective_capabilities() {
        let mut l = ExtismLoader::new();
        l.host_fns_mut().register(HostFnSpec {
            name: "host_fs_read".to_owned(),
            required_capability: Some(fs_cap()),
            docs: "Read file.".to_owned(),
        });
        l.host_fns_mut().register(HostFnSpec {
            name: "host_net_http_get".to_owned(),
            required_capability: Some(Capability::Network { allow: vec![] }),
            docs: "HTTP GET.".to_owned(),
        });
        l.host_fns_mut().register(HostFnSpec {
            name: "host_log".to_owned(),
            required_capability: None, // always-available
            docs: "Log a message.".to_owned(),
        });

        // Plugin requests filesystem only; host grants filesystem only.
        let json = manifest_json(&["filesystem"]);
        let prep = l
            .prepare(json.as_bytes(), &CapabilitySet::from_iter_of([fs_cap()]))
            .unwrap();

        // host_log is always-available; host_fs_read enabled by grant;
        // host_net_http_get filtered out (Network not granted).
        assert_eq!(prep.allowed_host_fns.len(), 2);
        assert!(prep.allowed_host_fns.iter().any(|n| n == "host_log"));
        assert!(prep.allowed_host_fns.iter().any(|n| n == "host_fs_read"));
        assert!(
            !prep
                .allowed_host_fns
                .iter()
                .any(|n| n == "host_net_http_get")
        );
    }

    #[test]
    fn prepare_rejects_malformed_manifest() {
        let l = ExtismLoader::new();
        let err = l.prepare(b"not json", &CapabilitySet::new()).unwrap_err();
        assert!(matches!(err, ExtismError::ManifestInvalid(_)));
    }

    #[test]
    fn build_plugin_rejects_garbage_bytes_as_instantiate_error() {
        // M6a.1.1: `build_plugin` is real now. With garbage bytes,
        // wasmtime fails to compile/instantiate — surface as
        // `ExtismError::Instantiate`.
        let l = ExtismLoader::new();
        let prep = l
            .prepare(
                b"{\"id\":\"a.b\",\"version\":\"0.0.0\"}",
                &CapabilitySet::new(),
            )
            .unwrap();
        let err = l.build_plugin(b"not real wasm", &prep).unwrap_err();
        assert!(
            matches!(err, ExtismError::Instantiate(_)),
            "expected Instantiate(_), got: {err:?}"
        );
    }

    /// H15: a manifest that declares NO resource limits must still be sandboxed
    /// — the host memory cap and timeout are applied unconditionally so an
    /// untrusted plugin cannot opt out of its own limits.
    #[test]
    fn undeclared_limits_get_host_defaults() {
        let l = ExtismLoader::new();
        let json = manifest_json(&[]);
        let prep = l.prepare(json.as_bytes(), &CapabilitySet::new()).unwrap();
        // The manifest itself declares nothing.
        assert_eq!(prep.manifest.memory_max_pages, None);
        assert_eq!(prep.manifest.timeout_ms, None);

        let m = build_extism_manifest(b"\0asm", &prep.manifest);
        assert_eq!(
            m.memory.max_pages,
            Some(DEFAULT_MEMORY_MAX_PAGES),
            "undeclared memory cap must fall back to the host default"
        );
        assert_eq!(
            m.timeout_ms,
            Some(DEFAULT_TIMEOUT_MS),
            "undeclared timeout must fall back to the host default"
        );
    }

    /// A manifest may still request its own (e.g. larger) limits — those are
    /// honored rather than overwritten by the default.
    #[test]
    fn declared_limits_are_honored() {
        let l = ExtismLoader::new();
        let json = r#"{ "id": "ai.example.test", "version": "1.0.0", "capabilities": [], "memory_max_pages": 4, "timeout_ms": 500 }"#;
        let prep = l.prepare(json.as_bytes(), &CapabilitySet::new()).unwrap();
        let m = build_extism_manifest(b"\0asm", &prep.manifest);
        assert_eq!(m.memory.max_pages, Some(4));
        assert_eq!(m.timeout_ms, Some(500));
    }
}
