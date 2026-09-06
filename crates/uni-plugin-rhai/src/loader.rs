//! Rhai loader — three-phase load mirroring `ExtismLoader::load`.
//!
//! Phase 1: build a sandboxed engine, compile the script, read
//!           `uni_manifest()` to discover declared capabilities and
//!           function entries.
//! Phase 2: intersect declared capabilities with host grants → effective
//!           set; rebuild the engine with capability-gated host fns
//!           registered for the effective set.
//! Phase 3: register each manifest entry on the supplied
//!           `PluginRegistrar` as a `ScalarPluginFn` / `AggregatePluginFn` /
//!           `ProcedurePlugin` adapter. The caller commits the registrar
//!           atomically to the registry.

#![cfg(feature = "rhai-runtime")]

use std::sync::Arc;

use uni_plugin::{
    Capability, CapabilitySet, HttpEgress, KmsProvider, PluginError, PluginId, PluginRegistrar,
    QName,
};

use arrow_schema::Field;

use uni_plugin::adapter_common::arrow_types::arg_type_from_token;
use uni_plugin::capability::SideEffects;
use uni_plugin::secrets::SecretStore;
use uni_plugin::traits::procedure::{NamedArgType, ProcedureMode, ProcedureSignature};

use uni_plugin::traits::algorithm::AlgorithmSignature;

use crate::adapter::RhaiScalarFn;
use crate::adapter_aggregate::{RhaiAggregateFn, build_agg_signature};
use crate::adapter_algorithm::RhaiAlgorithm;
use crate::adapter_procedure::RhaiProcedure;
use crate::engine::build_engine;
use crate::error::RhaiError;
use crate::host_fns::RhaiHostFnRegistry;
use crate::manifest::{AlgorithmEntry, ProcedureEntry, RhaiManifest, compile, parse_manifest};
use crate::runtime::RhaiPluginRuntime;
use crate::wire_translate::{build_fn_signature, type_name_to_argtype, type_name_to_datatype};

/// Outcome of a successful Rhai plugin load.
#[derive(Debug)]
pub struct LoadOutcome {
    /// Plugin id as declared in `uni_manifest()`.
    pub plugin_id: PluginId,
    /// Plugin version string (semver).
    pub version: String,
    /// Capabilities that were both declared by the plugin and granted by
    /// the host (the intersection).
    pub effective_capabilities: CapabilitySet,
    /// Capabilities the plugin declared but the host did not grant.
    pub denied_capabilities: Vec<Capability>,
    /// Fully-qualified names of scalar fns the loader registered.
    pub scalars_registered: Vec<String>,
    /// Aggregate qnames registered.
    pub aggregates_registered: Vec<String>,
    /// Procedure qnames registered.
    pub procedures_registered: Vec<String>,
    /// Algorithm qnames registered (gated on `Capability::Algorithm`).
    pub algorithms_registered: Vec<String>,
    /// Strong reference to the per-plugin runtime. Adapters hold inner
    /// `Arc` clones; the host can drop this on unload to release the
    /// engine.
    pub runtime: Arc<RhaiPluginRuntime>,
}

/// Rhai loader.
///
/// Holds the host-fn registry; one loader can serve many plugins. Cheap
/// to clone (host fns are `Arc`'d closures).
#[derive(Default, Clone)]
pub struct RhaiLoader {
    host_fns: RhaiHostFnRegistry,
    /// Optional KMS provider backing `uni.kms.*`. Absent → those fns error
    /// loudly at call time ("no KMS provider configured").
    kms: Option<Arc<dyn KmsProvider>>,
    /// Optional secret store backing `uni.secret.acquire`.
    secrets: Option<Arc<SecretStore>>,
    /// Optional HTTP egress backing `uni.http.*`.
    http: Option<Arc<dyn HttpEgress>>,
}

impl std::fmt::Debug for RhaiLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RhaiLoader")
            .field("host_fn_count", &self.host_fns.len())
            .finish()
    }
}

impl RhaiLoader {
    /// Construct an empty loader (no host fns yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mutable access to the host-fn registry for the host to register
    /// its capability-gated functions before any plugin is loaded.
    pub fn host_fns_mut(&mut self) -> &mut RhaiHostFnRegistry {
        &mut self.host_fns
    }

    /// Read access to the host-fn registry.
    #[must_use]
    pub fn host_fns(&self) -> &RhaiHostFnRegistry {
        &self.host_fns
    }

    /// Number of host fns currently registered.
    #[must_use]
    pub fn host_fn_count(&self) -> usize {
        self.host_fns.len()
    }

    /// Attach a KMS provider backing `uni.kms.*` (builder style).
    #[must_use]
    pub fn with_kms(mut self, kms: Arc<dyn KmsProvider>) -> Self {
        self.kms = Some(kms);
        self
    }

    /// Attach a secret store backing `uni.secret.acquire` (builder style).
    #[must_use]
    pub fn with_secret_store(mut self, store: Arc<SecretStore>) -> Self {
        self.secrets = Some(store);
        self
    }

    /// Attach an HTTP egress backing `uni.http.*` (builder style).
    #[must_use]
    pub fn with_http(mut self, http: Arc<dyn HttpEgress>) -> Self {
        self.http = Some(http);
        self
    }

    /// Clone of the configured KMS provider handle, if any.
    #[must_use]
    pub fn kms(&self) -> Option<Arc<dyn KmsProvider>> {
        self.kms.clone()
    }

    /// Clone of the configured secret store handle, if any.
    #[must_use]
    pub fn secret_store(&self) -> Option<Arc<SecretStore>> {
        self.secrets.clone()
    }

    /// Clone of the configured HTTP egress handle, if any.
    #[must_use]
    pub fn http(&self) -> Option<Arc<dyn HttpEgress>> {
        self.http.clone()
    }

    /// Load a Rhai script into a `PluginRegistrar`.
    ///
    /// The caller is responsible for calling
    /// `registrar.commit_to_registry()` on success.
    ///
    /// `registrar_caps` is the **host grant set** — what capabilities
    /// the host is willing to give this plugin. The effective set is
    /// the intersection of `registrar_caps` and the manifest's
    /// declared capability set. Granted-but-not-declared capabilities
    /// are silently ignored (least-authority); declared-but-not-granted
    /// are surfaced as `denied_capabilities`.
    pub fn load(
        &self,
        script: &str,
        registrar: &mut PluginRegistrar<'_>,
        registrar_caps: &CapabilitySet,
    ) -> Result<LoadOutcome, RhaiError> {
        // Phase 1: build an engine with the host's grant set so that
        // scripts referring to capability-gated host fns parse-resolve
        // during manifest extraction. The manifest call doesn't invoke
        // host fns, but the script may reference them in other fns and
        // Rhai resolves all function calls at parse time.
        //
        // Host-fn registration is gated by the host's GRANT set
        // (`registrar_caps`), not by the manifest's declared capabilities —
        // host fns like `uni.fs.read` aren't enumerated in the manifest, but
        // the plugin can still call them if and only if the host granted the
        // underlying capability. Extension-surface caps (ScalarFn etc.) are
        // gated separately at registration time via the `effective` set.
        //
        // The same engine + AST become the runtime artifacts: phase 2's real
        // engine would be built from the identical `(registrar_caps,
        // host_fns)` inputs and the identical script, and `parse_manifest`
        // only *calls* `uni_manifest()` against the AST (it never mutates it),
        // so a second build+compile would be byte-for-byte redundant.
        let engine = build_engine(registrar_caps, &self.host_fns);
        let ast = compile(&engine, script)?;
        let manifest = parse_manifest(&engine, &ast)?;

        let plugin_id = PluginId::new(manifest.id.clone());

        // Phase 2: declared capabilities for this plugin. v1 derives the
        // declared set from the function-kind entries — every script
        // implicitly declares `ScalarFn` / `AggregateFn` / `Procedure`
        // for each entry it provides. Future: an explicit
        // `capabilities:` field in the manifest can request specific
        // host-fn caps (Filesystem, Network, etc).
        let declared = derive_declared_capabilities(&manifest);
        let (effective, denied) = intersect_caps(&declared, registrar_caps);

        let runtime = RhaiPluginRuntime::new(plugin_id.clone(), engine, ast);

        // Phase 3: register entries.
        registrar.set_plugin_id(plugin_id.clone());

        // Per proposal §10.2 / §M7: only register entries whose
        // declared capability is in the effective set. Entries whose
        // capability was denied surface via `denied_capabilities` so
        // operators can see what was dropped.
        let mut scalars_registered = Vec::with_capacity(manifest.scalar_fns.len());
        if effective.contains(&Capability::ScalarFn) {
            for entry in &manifest.scalar_fns {
                let sig = build_fn_signature(&entry.args, &entry.returns, &manifest.determinism)?;
                let qname = QName::new(plugin_id.as_str(), entry.name.clone());
                let adapter = if entry.vectorized {
                    RhaiScalarFn::new_vectorized(
                        Arc::clone(&runtime),
                        entry.name.clone(),
                        sig.clone(),
                    )
                } else {
                    RhaiScalarFn::new(Arc::clone(&runtime), entry.name.clone(), sig.clone())
                };
                registrar
                    .scalar_fn(qname.clone(), sig, Arc::new(adapter))
                    .map_err(plugin_to_rhai_err)?;
                scalars_registered.push(qname.to_string());
            }
        }

        let mut aggregates_registered = Vec::with_capacity(manifest.aggregate_fns.len());
        if effective.contains(&Capability::AggregateFn) {
            for entry in &manifest.aggregate_fns {
                let sig = build_agg_signature(&entry.args, &entry.returns, &manifest.determinism)?;
                let qname = QName::new(plugin_id.as_str(), entry.name.clone());
                let adapter =
                    RhaiAggregateFn::new(Arc::clone(&runtime), entry.name.clone(), sig.clone());
                registrar
                    .aggregate_fn(qname.clone(), sig, Arc::new(adapter))
                    .map_err(plugin_to_rhai_err)?;
                aggregates_registered.push(qname.to_string());
            }
        }

        let mut procedures_registered = Vec::with_capacity(manifest.procedures.len());
        if effective.contains(&Capability::Procedure) {
            for entry in &manifest.procedures {
                let sig = build_procedure_signature(entry)?;
                let qname = QName::new(plugin_id.as_str(), entry.name.clone());
                let adapter =
                    RhaiProcedure::new(Arc::clone(&runtime), entry.name.clone(), sig.clone());
                registrar
                    .procedure(qname.clone(), sig, Arc::new(adapter))
                    .map_err(plugin_to_rhai_err)?;
                procedures_registered.push(qname.to_string());
            }
        }

        let mut algorithms_registered = Vec::new();
        if effective.contains(&Capability::Algorithm) {
            for entry in &manifest.algorithms {
                let sig = build_algorithm_signature(entry)?;
                let qname = QName::new(plugin_id.as_str(), entry.name.clone());
                let adapter =
                    RhaiAlgorithm::new(Arc::clone(&runtime), entry.name.clone(), sig.clone());
                algorithms_registered.push(qname.to_string());
                registrar
                    .algorithm(qname, Arc::new(adapter))
                    .map_err(plugin_to_rhai_err)?;
            }
        }

        Ok(LoadOutcome {
            plugin_id,
            version: manifest.version,
            effective_capabilities: effective,
            denied_capabilities: denied,
            scalars_registered,
            aggregates_registered,
            procedures_registered,
            algorithms_registered,
            runtime,
        })
    }
}

fn build_procedure_signature(entry: &ProcedureEntry) -> Result<ProcedureSignature, RhaiError> {
    let args: Vec<NamedArgType> = entry
        .args
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let ty = type_name_to_argtype(t)?;
            Ok(NamedArgType {
                name: format!("arg{i}").into(),
                ty,
                default: None,
                doc: String::new(),
            })
        })
        .collect::<Result<_, RhaiError>>()?;

    let yields: Vec<Field> = entry
        .yields
        .iter()
        .enumerate()
        .map(|(i, y)| {
            let dt = type_name_to_datatype(&y.type_name)?;
            // Prefer the declared column name so it aligns with the keys the
            // procedure uses in its returned row maps. Only fall back to a
            // positional `col{i}` name when the manifest declared none — a
            // fabricated name would never match a natural-key row map and the
            // column would silently read all-NULL.
            // #233 Tier 1: the comment above states the hazard and the code
            // did it anyway. A procedure's rows are MAPS keyed by name, so a
            // fabricated `col{i}` never matches and the column reads all-NULL.
            // A yield with no declared name is a manifest error here. (The
            // algorithm path below keeps the positional fallback: its emit is
            // positional, so a generated name is correct there.)
            let Some(name) = y.name.clone() else {
                return Err(RhaiError::ManifestInvalid(format!(
                    "procedure `{}` yield {i} declares no name; a positional `col{i}` name \
                     would never match the row maps the procedure returns, leaving the column \
                     silently all-NULL",
                    entry.name
                )));
            };
            Ok(Field::new(name, dt, true))
        })
        .collect::<Result<_, RhaiError>>()?;

    let mode = match entry.mode.trim().to_ascii_lowercase().as_str() {
        "write" => ProcedureMode::Write,
        "schema" => ProcedureMode::Schema,
        "dbms" => ProcedureMode::Dbms,
        _ => ProcedureMode::Read,
    };
    let side_effects = match mode {
        ProcedureMode::Read => SideEffects::ReadOnly,
        _ => SideEffects::Writes,
    };

    Ok(ProcedureSignature {
        args,
        yields,
        mode,
        side_effects,
        retry_contract: None,
        batch_input: None,
        docs: String::new(),
    })
}

fn build_algorithm_signature(entry: &AlgorithmEntry) -> Result<AlgorithmSignature, RhaiError> {
    // Declared algorithm args are now typed and validated (G4). Previously
    // `entry.args` was parsed then silently ignored — `signature.args` was left
    // empty, so `coerce_config_json` was a no-op and a guest got no arity/type
    // checking. The shared token vocabulary also lets a guest declare a
    // `value`/`list` argument (not just fixed scalars), so a variable-length seed
    // set can be passed without generating the plugin per-arity.
    let mut args: Vec<NamedArgType> = entry
        .args
        .iter()
        .enumerate()
        .map(|(i, tok)| {
            let ty = arg_type_from_token(tok).ok_or_else(|| {
                RhaiError::ManifestInvalid(format!(
                    "unknown arg type `{tok}` for algorithm `{}`; supported: \
                     float/int/string/bool/null/value/list",
                    entry.name
                ))
            })?;
            Ok(NamedArgType {
                name: format!("arg{i}").into(),
                ty,
                default: None,
                doc: String::new(),
            })
        })
        .collect::<Result<_, RhaiError>>()?;
    // Accept the optional trailing projection-config object the CALL convention
    // appends after the guest args (the adapter strips it before the guest runs).
    args.push(NamedArgType::projection_config());
    let output_fields: Vec<Field> = entry
        .yields
        .iter()
        .enumerate()
        .map(|(i, y)| {
            // See the pyo3 loader: algorithm yields are narrower than the
            // general type map, because the emit channel is `Vec<f64>`.
            let dt = uni_plugin_builtin::algorithms::graph_compute::algorithm_yield_datatype(
                &y.type_name,
            )
            .ok_or_else(|| {
                RhaiError::ManifestInvalid(format!(
                    "algorithm `{}` declares yield type `{}`, which the emit path cannot \
                     build; algorithm yields must be int or float",
                    entry.name, y.type_name
                ))
            })?;
            let name = y.name.clone().unwrap_or_else(|| format!("col{i}"));
            Ok(Field::new(name, dt, false))
        })
        .collect::<Result<_, RhaiError>>()?;
    Ok(AlgorithmSignature {
        output_fields,
        args,
        docs: String::new(),
        ..Default::default()
    })
}

fn derive_declared_capabilities(m: &RhaiManifest) -> CapabilitySet {
    let mut set = CapabilitySet::new();
    if !m.scalar_fns.is_empty() {
        set.insert(Capability::ScalarFn);
    }
    if !m.aggregate_fns.is_empty() {
        set.insert(Capability::AggregateFn);
    }
    if !m.procedures.is_empty() {
        set.insert(Capability::Procedure);
    }
    if !m.algorithms.is_empty() {
        // A GraphCompute algorithm needs three orthogonal grants (proposal
        // §4.6): `Algorithm` to register the entry, `GraphCompute` for the
        // kernel surface, and `HostQuery` to project the graph. The Rhai
        // manifest derives capabilities from entry-kind presence, so declaring
        // `algorithms:` derives all three; each is still intersected with the
        // host's grants, so the host retains final control.
        set.insert(Capability::Algorithm);
        set.insert(Capability::GraphCompute);
        set.insert(Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        });
    }
    set
}

fn intersect_caps(
    declared: &CapabilitySet,
    granted: &CapabilitySet,
) -> (CapabilitySet, Vec<Capability>) {
    let effective = declared.intersect(granted);
    // Denied = declared minus effective, tested by variant so an
    // attenuated-but-granted payload cap (e.g. a narrowed HostQuery) is not
    // misreported as denied. See `CapabilitySet::denied_against`.
    let denied = declared.denied_against(&effective);
    (effective, denied)
}

fn plugin_to_rhai_err(e: PluginError) -> RhaiError {
    match e {
        PluginError::DuplicateRegistration(q) => {
            RhaiError::ManifestInvalid(format!("duplicate registration: {q}"))
        }
        PluginError::CapabilityRequired(c) => {
            RhaiError::ManifestInvalid(format!("registrar caps missing: {c:?}"))
        }
        other => RhaiError::Internal(format!("registrar: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uni_plugin::PluginRegistry;

    fn loader_with_caps() -> (RhaiLoader, CapabilitySet) {
        let loader = RhaiLoader::new();
        let caps = CapabilitySet::from_iter_of([
            Capability::ScalarFn,
            Capability::AggregateFn,
            Capability::Procedure,
        ]);
        (loader, caps)
    }

    #[test]
    fn loads_minimal_scalar_plugin() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.scalar",
                    version: "0.1.0",
                    scalar_fns: [
                        #{ name: "double", args: ["float"], returns: "float" },
                    ],
                }
            }
            fn double(x) { x * 2.0 }
        "#;
        let (loader, caps) = loader_with_caps();
        let registry = PluginRegistry::new();
        let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
        let outcome = loader.load(script, &mut r, &caps).expect("loads");
        assert_eq!(outcome.plugin_id.as_str(), "ai.test.scalar");
        assert_eq!(outcome.scalars_registered.len(), 1);
        assert!(outcome.denied_capabilities.is_empty());
        r.commit_to_registry().expect("commits");
        // Registry now has the qname.
        let q = QName::new("ai.test.scalar", "double");
        assert!(registry.scalar_fn(&q).is_some());
    }

    #[test]
    fn declared_but_not_granted_caps_show_as_denied() {
        let script = r#"
            fn uni_manifest() {
                #{
                    id: "ai.test.denied",
                    version: "0.1.0",
                    scalar_fns: [
                        #{ name: "noop", args: [], returns: "int" },
                    ],
                    aggregate_fns: [
                        #{ name: "agg", args: ["float"], returns: "float", state: "map" },
                    ],
                }
            }
            fn noop() { 0 }
        "#;
        let loader = RhaiLoader::new();
        let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
        let registry = PluginRegistry::new();
        let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
        let outcome = loader.load(script, &mut r, &caps).expect("loads");
        assert!(
            outcome
                .denied_capabilities
                .contains(&Capability::AggregateFn)
        );
        assert_eq!(outcome.scalars_registered.len(), 1);
    }

    #[test]
    fn parse_failure_returns_parse_error() {
        let script = r#"this is not valid rhai @@@"#;
        let (loader, caps) = loader_with_caps();
        let registry = PluginRegistry::new();
        let mut r = PluginRegistrar::new(PluginId::new("rhai.loading"), &caps, &registry);
        let err = loader.load(script, &mut r, &caps).unwrap_err();
        assert!(matches!(err, RhaiError::ParseFailed(_)));
    }
}
