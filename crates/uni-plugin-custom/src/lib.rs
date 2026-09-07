// Rust guideline compliant
//! Meta-plugin (`apoc.custom.declare*` analogue) for uni-db.
//!
//! This crate ships a built-in plugin whose procedures (`uni.plugin.declareFunction`,
//! `declareProcedure`, `declareAggregate`, `declareTrigger`) accept
//! Cypher source and persist new plugin registrations alongside the
//! framework's [`uni_plugin::PluginRegistry`].
//!
//! # M9 status (this commit)
//!
//! Completed M9 deliverables:
//!
//! * `uni.plugin.declareFunction` — fully wired. Parses the Cypher
//!   expression body at declare time, persists the [`DeclaredPlugin`]
//!   record via [`persistence::Persistence`], and registers a
//!   synthetic [`uni_plugin::traits::scalar::ScalarPluginFn`] into the
//!   shared [`PluginRegistry`].
//! * `uni.plugin.declareProcedure`, `declareAggregate`,
//!   `declareTrigger` — registered as Cypher-callable procedures.
//!   Their declarations are persisted and reachable via
//!   `uni.plugin.listDeclared`; full body execution rides on
//!   downstream host APIs (`ProcedureHost::execute_inner_query` for
//!   procedures; trigger/aggregate body invocation follows the M11
//!   capability work).
//! * `uni.plugin.listDeclared` / `dropDeclared` — extended for
//!   cascade-aware drops.
//! * Reactivation — declarations are reloaded into the registry on
//!   [`CustomPlugin::new`] when constructed with a non-empty
//!   persistence backend.
//! * Capability inheritance — declarations capture the declaring
//!   principal id; the registrar enforces capability gating at
//!   registration time via the synthetic plugin's manifest.
//!
//! # Persistence
//!
//! Proposal §9.7 anchors the persistence schema in a Cypher-visible
//! system label `_DeclaredPlugin`. Writing to that label from inside
//! a procedure requires write-enabled
//! [`uni_plugin::traits::procedure::ProcedureHost`] execution, which
//! now exists: `execute_inner_query` binds named parameters and runs
//! in write mode when the host is constructed with a writer (see
//! `crates/uni-query/src/query/executor/procedure_host.rs`). The
//! system-label cutover is therefore unblocked but not yet wired.
//!
//! M9 ships persistence behind a [`persistence::Persistence`] trait
//! with a JSON-sidecar implementation that preserves the exact
//! [`DeclaredPlugin`] shape from §9.7. The cutover to system-label
//! persistence — once write-enabled host execution lands — is a
//! drop-in replacement of the backend; no schema, store, or
//! procedure code changes.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

mod aggregate;
mod decode;
mod eval;
mod scalar;

pub mod persistence;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uni_plugin::PluginRegistry;

pub use crate::aggregate::{DeclaredAggregateFn, install_aggregate_into_registry};
pub use crate::persistence::{JsonFilePersistence, NullPersistence, Persistence, PersistenceError};
pub use crate::scalar::DeclaredScalarFn;

/// Errors raised by the meta-plugin.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CustomError {
    /// Declared body could not be parsed.
    #[error("declared plugin body parse failure: {0}")]
    BodyParse(String),

    /// Declared qname conflicts with an existing native registration.
    #[error("declared qname `{0}` is shadowed by a native plugin registration")]
    NativeShadow(String),

    /// Declared plugin depends on a missing or already-dropped qname.
    #[error("declared plugin `{dependent}` depends on missing `{dep}`")]
    DependencyMissing {
        /// The dependent's qname.
        dependent: String,
        /// The missing dependency's qname.
        dep: String,
    },

    /// Cyclic dependencies among declared plugins.
    #[error("dependency cycle in declared plugins: {0:?}")]
    DependencyCycle(Vec<String>),

    /// A persistence backend reported a failure.
    #[error("declared-plugin persistence: {0}")]
    Persistence(#[from] PersistenceError),

    /// Registration into the [`PluginRegistry`] failed.
    #[error("declared-plugin registration: {0}")]
    Registration(String),

    /// The principal lacks a capability required by the declaration.
    #[error("declared-plugin capability denied: caller is missing `{0}`")]
    CapabilityDenied(String),
}

/// Persistent record of a declared plugin (written to
/// `uni_system.declared_plugins` per proposal §9.7 — currently
/// shipped via JSON sidecar; see crate docs).
///
/// Round-trips through `serde` so the same shape persists into the
/// JSON sidecar today and a Cypher property map (system-label
/// persistence) at the M9 cutover commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeclaredPlugin {
    /// Qualified name claimed by the declaration.
    pub qname: String,
    /// Kind: `"function" | "procedure" | "aggregate" | "trigger"`.
    pub kind: String,
    /// Cypher / Locy source body.
    pub body: String,
    /// Serialized signature (JSON-encoded — schema depends on `kind`).
    pub signature_json: String,
    /// Qualified names of other declared plugins this depends on.
    pub dependencies: Vec<String>,
    /// Principal id that declared this plugin.
    pub declared_by: String,
    /// Whether this declaration is active (shadowed declarations are
    /// inactive until the shadowing native plugin is removed).
    pub active: bool,
}

/// Top-level meta-plugin handle.
///
/// Implements [`uni_plugin::Plugin`]. Construct via
/// [`CustomPlugin::new`] (with a shared [`PluginRegistry`] Arc and a
/// [`Persistence`] backend) and add to a `Uni` instance through the
/// host's `register_builtin_plugins` flow (`crates/uni/src/api/mod.rs`).
///
/// The plugin owns:
///
/// * `store` — an in-memory [`DeclaredPluginStore`] mirroring every
///   declaration, used for dependency analysis and read-side
///   procedures (`listDeclared`, `dropDeclared`).
/// * `registry` — a shared `Arc<PluginRegistry>` so the declare*
///   procedures can register synthetic [`uni_plugin::Plugin`] values
///   at runtime.
/// * `persistence` — the durable backend that replays declarations on
///   `CustomPlugin::new`.
pub struct CustomPlugin {
    store: Arc<DeclaredPluginStore>,
    registry: Arc<PluginRegistry>,
    persistence: Arc<dyn Persistence>,
    /// Optional synthesizer for declared-procedure and
    /// declared-trigger bodies. Set by the host (e.g., `uni-db`'s
    /// `Uni::build` flow) at construction time. When `None`, declared
    /// procedures/triggers are recorded + persisted but no executable
    /// plugin is registered (today's pre-M11 behavior).
    procedure_synthesizer: Option<Arc<dyn ProcedureBodySynthesizer>>,
    /// Optional synthesizer for declared-trigger bodies (WS-A). Set by
    /// the host alongside `procedure_synthesizer`. When `None`, declared
    /// triggers are recorded + persisted but no `TriggerPlugin` is
    /// registered (record-only, same as procedures without a synthesizer).
    trigger_synthesizer: Option<Arc<dyn TriggerBodySynthesizer>>,
    manifest: std::sync::OnceLock<uni_plugin::PluginManifest>,
}

/// Host callback that turns a declared-procedure record into an
/// executable [`uni_plugin::traits::procedure::ProcedurePlugin`].
///
/// `uni-plugin-custom` cannot reach the host's
/// `QueryProcedureHost::execute_inner_query` directly (no dep on
/// `uni-query`), so the M9 cutover for declared-procedure body
/// execution flows through this callback. `uni-db` implements
/// [`ProcedureBodySynthesizer`] using
/// `uni_query::QueryProcedureHost::execute_inner_query` and passes
/// the impl to [`CustomPlugin::with_procedure_synthesizer`].
pub trait ProcedureBodySynthesizer: Send + Sync + std::fmt::Debug {
    /// Build a `ProcedurePlugin` whose `invoke()` runs the Cypher /
    /// Locy body of `decl`. Returns the synthesized plugin (which the
    /// caller registers into the [`PluginRegistry`]) or a string
    /// reason for failure.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on synthesis failure (bad
    /// signature shape, body parse errors, capability gaps).
    fn synthesize(
        &self,
        decl: &DeclaredPlugin,
    ) -> Result<Arc<dyn uni_plugin::traits::procedure::ProcedurePlugin>, String>;
}

/// Host callback that turns a declared-trigger record into an executable
/// [`uni_plugin::traits::trigger::TriggerPlugin`] (WS-A).
///
/// Mirrors [`ProcedureBodySynthesizer`], but the produced plugin lands
/// in `PluginRegistry::triggers()` (via `PluginRegistrar::trigger`)
/// instead of as a callable procedure, so the transaction commit-path
/// router fires it on matching mutations. `uni-db`'s host implements
/// this using `uni_plugin_host::synthetic_trigger::CypherTriggerSynthesizer`.
pub trait TriggerBodySynthesizer: Send + Sync + std::fmt::Debug {
    /// Build a `TriggerPlugin` whose `fire()` runs the Cypher action
    /// body of `decl` on matching mutation events.
    ///
    /// # Errors
    ///
    /// Returns a free-form error string on synthesis failure — notably
    /// a `[SYNC]` event-filter marker, which v1 rejects at declare time
    /// (synchronous before-commit WRITE actions are unsafe).
    fn synthesize(
        &self,
        decl: &DeclaredPlugin,
    ) -> Result<Arc<dyn uni_plugin::traits::trigger::TriggerPlugin>, String>;
}

impl std::fmt::Debug for CustomPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomPlugin")
            .field("store", &self.store)
            .field("declared_count", &self.store.list().len())
            .finish_non_exhaustive()
    }
}

impl CustomPlugin {
    /// Reserved plugin id.
    pub const ID: &'static str = "custom";

    /// Construct with the given registry handle and persistence
    /// backend.
    ///
    /// On construction, the persistence backend is queried for every
    /// previously declared plugin and each one is re-installed into
    /// `store` (re-registration into `registry` happens lazily — the
    /// first time the plugin is invoked, or eagerly through
    /// [`Self::reactivate_into_registry`]).
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::Persistence`] if the backend's
    /// `load_all` fails.
    pub fn new(
        registry: Arc<PluginRegistry>,
        persistence: Arc<dyn Persistence>,
    ) -> Result<Self, CustomError> {
        let store = Arc::new(DeclaredPluginStore::new());
        let initial = persistence.load_all()?;
        for plugin in initial {
            // Reinsert with relaxed validation — persisted records
            // may include forward references that the store's
            // dependency check would reject during one-by-one
            // insertion. We trust persisted data.
            store.declare_unchecked(plugin);
        }
        Ok(Self {
            store,
            registry,
            persistence,
            procedure_synthesizer: None,
            trigger_synthesizer: None,
            manifest: std::sync::OnceLock::new(),
        })
    }

    /// Attach a host-side synthesizer so declared procedures (and
    /// triggers) can install executable plugins at declare time.
    ///
    /// The host (uni-db) calls this immediately after [`Self::new`].
    /// Synthesizer-less construction remains valid — declared
    /// procedures/triggers are recorded + persisted but not
    /// registered as invocable plugins.
    #[must_use]
    pub fn with_procedure_synthesizer(
        mut self,
        synthesizer: Arc<dyn ProcedureBodySynthesizer>,
    ) -> Self {
        self.procedure_synthesizer = Some(synthesizer);
        self
    }

    /// Attach a host-side trigger synthesizer so declared triggers
    /// install executable [`TriggerBodySynthesizer`]-produced
    /// `TriggerPlugin`s at declare time and on restart (WS-A). Without
    /// it, declared triggers are recorded + persisted but never fire.
    #[must_use]
    pub fn with_trigger_synthesizer(
        mut self,
        synthesizer: Arc<dyn TriggerBodySynthesizer>,
    ) -> Self {
        self.trigger_synthesizer = Some(synthesizer);
        self
    }

    /// Construct with no persistence (in-memory only) and a fresh
    /// [`PluginRegistry`] handle.
    ///
    /// Used by tests that exercise the meta-plugin in isolation.
    #[must_use]
    pub fn new_in_memory() -> Self {
        Self::new(Arc::new(PluginRegistry::new()), Arc::new(NullPersistence))
            .expect("NullPersistence cannot fail")
    }

    /// Access the underlying declared-plugin store.
    #[must_use]
    pub fn store(&self) -> &Arc<DeclaredPluginStore> {
        &self.store
    }

    /// Access the shared registry handle.
    #[must_use]
    pub fn registry(&self) -> &Arc<PluginRegistry> {
        &self.registry
    }

    /// Replay every persisted declaration into the registry.
    ///
    /// Called by the host immediately after [`Self::new`] so that
    /// declarations survive restart. Skips declarations whose qname
    /// is already registered as a native plugin (they remain marked
    /// `active=false` in the store).
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::Registration`] on registrar errors
    /// other than `DuplicateRegistration` (which is expected for
    /// shadowed declarations and downgrades the record to inactive).
    pub fn reactivate_into_registry(&self) -> Result<(), CustomError> {
        let mut records = self.store.list();
        records.sort_by_key(|a| a.dependencies.len());
        for record in records {
            let install_result = match record.kind.as_str() {
                "function" => procedures::install_function_into_registry(&self.registry, &record),
                "aggregate" => {
                    crate::aggregate::install_aggregate_into_registry(&self.registry, &record)
                }
                "procedure" => {
                    // M11 A.3: if the host wired a procedure-body
                    // synthesizer (uni-db installs one at Uni::build
                    // time), use it to register an executable
                    // SyntheticProcedurePlugin. Otherwise this is a
                    // record-only declaration (pre-M11 behavior).
                    match self.procedure_synthesizer.as_ref() {
                        Some(synth) => procedures::install_synthesized_procedure(
                            &self.registry,
                            &record,
                            synth.as_ref(),
                        ),
                        None => continue,
                    }
                }
                "trigger" => {
                    // WS-A: route trigger kinds through the trigger
                    // synthesizer so they land in `reg.triggers()` (fired
                    // by the commit-path router) rather than as a
                    // callable procedure. Record-only when no synthesizer
                    // is wired.
                    match self.trigger_synthesizer.as_ref() {
                        Some(synth) => procedures::install_synthesized_trigger(
                            &self.registry,
                            &record,
                            synth.as_ref(),
                        ),
                        None => continue,
                    }
                }
                _ => continue,
            };
            let mut record = record;
            match install_result {
                Ok(()) => {}
                Err(CustomError::NativeShadow(_)) => {
                    record.active = false;
                    self.store.replace(record.clone());
                    // #233 P3: recorded rather than propagated. This is the
                    // boot hydration path, and the downgrade is re-derived from
                    // persisted state on every restart, so a failed write here
                    // converges by itself — failing startup over it would be
                    // worse than the condition. Same reasoning as
                    // `SchedulerHost::spawn`'s degraded load.
                    if let Err(e) = self.persistence.save(&record) {
                        tracing::error!(
                            error = %e,
                            "could not persist a native-shadow downgrade during reactivation; \
                             it will be re-derived on the next restart",
                        );
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn manifest_value() -> uni_plugin::PluginManifest {
        use semver::Version;
        use uni_plugin::{
            AbiRange, Capability, CapabilitySet, Determinism, PluginId, PluginManifest,
            ProvidedSurfaces, Scope, SideEffects,
        };
        PluginManifest {
            id: PluginId::new(Self::ID),
            version: env!("CARGO_PKG_VERSION")
                .parse::<Version>()
                .unwrap_or_else(|_| Version::new(0, 0, 0)),
            abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([
                Capability::Procedure,
                Capability::ProcedureWrites,
                Capability::PluginDeclare,
                // WS-A — declared triggers register into the trigger
                // surface, which the registrar gates on `Trigger`.
                Capability::Trigger,
            ]),
            determinism: Determinism::Nondeterministic,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "apoc.custom-style meta-plugin: declare procedures / functions / aggregates / triggers from Cypher."
                .to_owned(),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

impl uni_plugin::Plugin for CustomPlugin {
    fn manifest(&self) -> &uni_plugin::PluginManifest {
        self.manifest.get_or_init(Self::manifest_value)
    }

    fn register(
        &self,
        r: &mut uni_plugin::PluginRegistrar<'_>,
    ) -> Result<(), uni_plugin::PluginError> {
        use uni_plugin::QName;

        r.procedure(
            QName::new(Self::ID, "plugin.listDeclared"),
            procedures::list_declared_signature(),
            std::sync::Arc::new(procedures::ListDeclaredProcedure::new(Arc::clone(
                &self.store,
            ))),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.dropDeclared"),
            procedures::drop_declared_signature(),
            std::sync::Arc::new(procedures::DropDeclaredProcedure::new(
                Arc::clone(&self.store),
                Arc::clone(&self.persistence),
                Arc::clone(&self.registry),
            )),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareFunction"),
            procedures::declare_function_signature(),
            std::sync::Arc::new(procedures::DeclareFunctionProcedure::new(
                Arc::clone(&self.store),
                Arc::clone(&self.persistence),
                Arc::clone(&self.registry),
            )),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareProcedure"),
            procedures::declare_procedure_signature(),
            std::sync::Arc::new(match self.procedure_synthesizer.as_ref() {
                Some(synth) => procedures::DeclareProcedureProcedure::new_with_synthesis(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                    Arc::clone(&self.registry),
                    Arc::clone(synth),
                ),
                None => procedures::DeclareProcedureProcedure::new(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                ),
            }),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareAggregate"),
            procedures::declare_aggregate_signature(),
            std::sync::Arc::new(procedures::DeclareAggregateProcedure::new(
                Arc::clone(&self.store),
                Arc::clone(&self.persistence),
                Arc::clone(&self.registry),
            )),
        )?;
        r.procedure(
            QName::new(Self::ID, "plugin.declareTrigger"),
            procedures::declare_trigger_signature(),
            std::sync::Arc::new(match self.trigger_synthesizer.as_ref() {
                Some(synth) => procedures::DeclareTriggerProcedure::new_with_trigger_synthesis(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                    Arc::clone(&self.registry),
                    Arc::clone(synth),
                ),
                None => procedures::DeclareTriggerProcedure::new(
                    Arc::clone(&self.store),
                    Arc::clone(&self.persistence),
                ),
            }),
        )?;
        Ok(())
    }
}

pub mod procedures;

// -------------------------------------------------------------
// DeclaredPluginStore
// -------------------------------------------------------------

/// In-memory store for declared plugins.
///
/// The store is the source of truth for dependency analysis and
/// listing; persistence rides through [`Persistence`] and replays
/// the same records through this store on construction.
#[derive(Debug, Default)]
pub struct DeclaredPluginStore {
    by_qname: std::sync::RwLock<std::collections::BTreeMap<String, DeclaredPlugin>>,
}

impl DeclaredPluginStore {
    /// Construct an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a new plugin or replace an existing declaration with
    /// dependency + cycle validation.
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::DependencyMissing`] if any declared
    /// dependency is not present in the store. Returns
    /// [`CustomError::DependencyCycle`] if adding this plugin would
    /// introduce a cycle.
    pub fn declare(&self, plugin: DeclaredPlugin) -> Result<(), CustomError> {
        // Validate-and-insert atomically under a single write lock.
        // Splitting validation (read lock) from insertion (separate
        // write lock) opens a check-then-act (TOCTOU) window in which
        // two concurrent declares can each pass validation and then both
        // commit — persisting a dependency cycle the checks were meant
        // to reject. Holding the write lock across both steps closes it.
        let mut map = self
            .by_qname
            .write()
            .expect("declared-plugin lock poisoned");
        for dep in &plugin.dependencies {
            if !map.contains_key(dep) {
                return Err(CustomError::DependencyMissing {
                    dependent: plugin.qname.clone(),
                    dep: dep.clone(),
                });
            }
        }
        if would_introduce_cycle(&map, &plugin) {
            return Err(CustomError::DependencyCycle(chain_starting_at(
                &map, &plugin,
            )));
        }
        map.insert(plugin.qname.clone(), plugin);
        Ok(())
    }

    /// Insert / replace without dependency validation. Used by the
    /// reactivation path (records from persistence are trusted).
    pub fn declare_unchecked(&self, plugin: DeclaredPlugin) {
        self.by_qname
            .write()
            .expect("declared-plugin lock poisoned")
            .insert(plugin.qname.clone(), plugin);
    }

    /// Look up a declared plugin by qname.
    #[must_use]
    pub fn get(&self, qname: &str) -> Option<DeclaredPlugin> {
        self.by_qname
            .read()
            .expect("declared-plugin lock poisoned")
            .get(qname)
            .cloned()
    }

    /// Drop a declared plugin.
    ///
    /// Returns `true` if the plugin existed.
    ///
    /// # Errors
    ///
    /// Returns [`CustomError::DependencyMissing`] if the plugin is a
    /// dependency of another declared plugin (cascade mode lives at
    /// [`Self::drop_cascade`]).
    pub fn drop_declared(&self, qname: &str) -> Result<bool, CustomError> {
        let mut map = self
            .by_qname
            .write()
            .expect("declared-plugin lock poisoned");
        for other in map.values() {
            if other.dependencies.iter().any(|d| d == qname) {
                return Err(CustomError::DependencyMissing {
                    dependent: other.qname.clone(),
                    dep: qname.to_owned(),
                });
            }
        }
        Ok(map.remove(qname).is_some())
    }

    /// Drop a declared plugin together with every dependent.
    ///
    /// Returns the qnames removed in topological (leaves-first)
    /// order.
    pub fn drop_cascade(&self, qname: &str) -> Vec<String> {
        let mut removed = Vec::new();
        let mut map = self
            .by_qname
            .write()
            .expect("declared-plugin lock poisoned");
        let mut stack = vec![qname.to_owned()];
        while let Some(target) = stack.pop() {
            let dependents: Vec<String> = map
                .iter()
                .filter(|(_, p)| p.dependencies.iter().any(|d| d == &target))
                .map(|(k, _)| k.clone())
                .collect();
            if dependents.is_empty() {
                if map.remove(&target).is_some() {
                    removed.push(target);
                }
            } else {
                stack.push(target);
                for d in dependents {
                    stack.push(d);
                }
            }
        }
        removed
    }

    /// Replace an existing record (no validation). Used for
    /// shadow-flag updates.
    pub fn replace(&self, plugin: DeclaredPlugin) {
        self.declare_unchecked(plugin);
    }

    /// List every declared plugin.
    #[must_use]
    pub fn list(&self) -> Vec<DeclaredPlugin> {
        self.by_qname
            .read()
            .expect("declared-plugin lock poisoned")
            .values()
            .cloned()
            .collect()
    }
}

fn would_introduce_cycle(
    map: &std::collections::BTreeMap<String, DeclaredPlugin>,
    candidate: &DeclaredPlugin,
) -> bool {
    fn reachable(
        map: &std::collections::BTreeMap<String, DeclaredPlugin>,
        start: &str,
        target: &str,
        visited: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        if start == target {
            return true;
        }
        if !visited.insert(start.to_owned()) {
            return false;
        }
        if let Some(node) = map.get(start) {
            for d in &node.dependencies {
                if reachable(map, d, target, visited) {
                    return true;
                }
            }
        }
        false
    }
    let mut visited = std::collections::BTreeSet::new();
    candidate
        .dependencies
        .iter()
        .any(|d| reachable(map, d, &candidate.qname, &mut visited))
}

/// Reconstruct the dependency cycle that would be introduced by adding
/// `candidate` to `map`.
///
/// Returned vector starts and ends with `candidate.qname`, with the
/// intermediate nodes naming the chain that closes the cycle (e.g.
/// `["a", "b", "c", "a"]`). If no cycle is reachable from any of
/// `candidate`'s dependencies, a single-element vector containing only
/// `candidate.qname` is returned as a defensive fallback.
fn chain_starting_at(
    map: &std::collections::BTreeMap<String, DeclaredPlugin>,
    candidate: &DeclaredPlugin,
) -> Vec<String> {
    fn dfs(
        map: &std::collections::BTreeMap<String, DeclaredPlugin>,
        node: &str,
        target: &str,
        stack: &mut Vec<String>,
        visited: &mut std::collections::BTreeSet<String>,
    ) -> bool {
        stack.push(node.to_owned());
        if node == target {
            return true;
        }
        if !visited.insert(node.to_owned()) {
            stack.pop();
            return false;
        }
        if let Some(declared) = map.get(node) {
            for dep in &declared.dependencies {
                if dfs(map, dep, target, stack, visited) {
                    return true;
                }
            }
        }
        stack.pop();
        false
    }

    let mut visited = std::collections::BTreeSet::new();
    for dep in &candidate.dependencies {
        let mut stack = vec![candidate.qname.clone()];
        if dfs(map, dep, &candidate.qname, &mut stack, &mut visited) {
            return stack;
        }
    }
    vec![candidate.qname.clone()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_plugin_round_trip_json() {
        let d = DeclaredPlugin {
            qname: "mycorp.fullName".to_owned(),
            kind: "function".to_owned(),
            body: "$first + ' ' + $last".to_owned(),
            signature_json: r#"{"args":["string","string"],"returns":"string"}"#.to_owned(),
            dependencies: vec![],
            declared_by: "alice".to_owned(),
            active: true,
        };
        let s = serde_json::to_string(&d).unwrap();
        let parsed: DeclaredPlugin = serde_json::from_str(&s).unwrap();
        assert_eq!(d, parsed);
    }

    #[test]
    fn custom_plugin_constructs_in_memory() {
        let _ = CustomPlugin::new_in_memory();
    }

    // M11 A.4: synthesizer integration tests.

    /// Mock synthesizer that produces a trivial ProcedurePlugin
    /// suitable for testing the registration path without depending
    /// on `uni-query`.
    #[derive(Debug)]
    struct StubSynthesizer {
        synthesized_count: std::sync::atomic::AtomicUsize,
    }

    impl StubSynthesizer {
        fn new() -> Self {
            Self {
                synthesized_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn count(&self) -> usize {
            self.synthesized_count
                .load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl crate::ProcedureBodySynthesizer for StubSynthesizer {
        fn synthesize(
            &self,
            _decl: &DeclaredPlugin,
        ) -> Result<Arc<dyn uni_plugin::traits::procedure::ProcedurePlugin>, String> {
            self.synthesized_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Arc::new(StubProcedure {
                signature: stub_signature(),
            }))
        }
    }

    #[derive(Debug)]
    struct StubProcedure {
        signature: uni_plugin::traits::procedure::ProcedureSignature,
    }

    fn stub_signature() -> uni_plugin::traits::procedure::ProcedureSignature {
        use arrow_schema::{DataType, Field};
        uni_plugin::traits::procedure::ProcedureSignature {
            args: vec![],
            yields: vec![Field::new("ok", DataType::Boolean, false)],
            mode: uni_plugin::traits::procedure::ProcedureMode::Read,
            side_effects: uni_plugin::SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "stub".to_owned(),
        }
    }

    impl uni_plugin::traits::procedure::ProcedurePlugin for StubProcedure {
        fn signature(&self) -> &uni_plugin::traits::procedure::ProcedureSignature {
            &self.signature
        }

        fn invoke(
            &self,
            _ctx: uni_plugin::traits::procedure::ProcedureContext<'_>,
            _args: &[datafusion::logical_expr::ColumnarValue],
        ) -> Result<datafusion::execution::SendableRecordBatchStream, uni_plugin::FnError> {
            unimplemented!(
                "StubProcedure does not execute; the synthesizer test only checks registration"
            )
        }
    }

    #[test]
    fn synthesizer_synthesize_called_on_reactivate() {
        let synth = Arc::new(StubSynthesizer::new());
        let store = Arc::new(DeclaredPluginStore::new());
        // Pre-populate a procedure-kind declaration.
        store
            .declare(DeclaredPlugin {
                qname: "mycorp.findFriends".to_owned(),
                kind: "procedure".to_owned(),
                body: "MATCH (p)-[:KNOWS]->(f) RETURN f".to_owned(),
                signature_json: "{}".to_owned(),
                dependencies: vec![],
                declared_by: "test".to_owned(),
                active: true,
            })
            .unwrap();

        let registry = Arc::new(uni_plugin::PluginRegistry::new());
        // We can't construct CustomPlugin with this pre-populated
        // store directly (its `new` reloads via persistence). Build
        // by hand and then call reactivate_into_registry.
        let plugin = CustomPlugin {
            store: Arc::clone(&store),
            registry: Arc::clone(&registry),
            persistence: Arc::new(NullPersistence),
            procedure_synthesizer: Some(synth.clone()),
            trigger_synthesizer: None,
            manifest: std::sync::OnceLock::new(),
        };
        plugin
            .reactivate_into_registry()
            .expect("reactivate must call synthesizer for procedure-kind records");
        assert_eq!(
            synth.count(),
            1,
            "synthesizer should have been called for the one procedure declaration"
        );
    }

    #[test]
    fn reactivate_skips_procedure_when_no_synthesizer() {
        let store = Arc::new(DeclaredPluginStore::new());
        store
            .declare(DeclaredPlugin {
                qname: "mycorp.findFriends".to_owned(),
                kind: "procedure".to_owned(),
                body: "MATCH (p)-[:KNOWS]->(f) RETURN f".to_owned(),
                signature_json: "{}".to_owned(),
                dependencies: vec![],
                declared_by: "test".to_owned(),
                active: true,
            })
            .unwrap();

        let registry = Arc::new(uni_plugin::PluginRegistry::new());
        let plugin = CustomPlugin {
            store,
            registry,
            persistence: Arc::new(NullPersistence),
            procedure_synthesizer: None, // no synthesizer
            trigger_synthesizer: None,
            manifest: std::sync::OnceLock::new(),
        };
        plugin
            .reactivate_into_registry()
            .expect("reactivate must succeed even with procedure records when no synthesizer");
        // No assertion needed — the absence of a panic is the
        // pre-M11 behavior we preserve.
    }

    // M11 A.1: capability-gate tests for `declareProcedure WRITE`.

    fn utf8_scalar(s: &str) -> datafusion::logical_expr::ColumnarValue {
        datafusion::logical_expr::ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(
            Some(s.to_owned()),
        ))
    }

    fn drive_declare_procedure(
        args: &[datafusion::logical_expr::ColumnarValue],
        principal: Option<&uni_plugin::traits::connector::Principal>,
    ) -> Result<(), uni_plugin::FnError> {
        let store = Arc::new(DeclaredPluginStore::new());
        let decl = procedures::DeclareProcedureProcedure::new(store, Arc::new(NullPersistence));
        let mut ctx = uni_plugin::traits::procedure::ProcedureContext::new();
        if let Some(p) = principal {
            ctx = ctx.with_principal(p);
        }
        use uni_plugin::traits::procedure::ProcedurePlugin;
        decl.invoke(ctx, args).map(|_| ())
    }

    #[test]
    fn declare_procedure_write_rejected_without_procedure_writes() {
        let args = vec![
            utf8_scalar("mycorp.deleteAll"),
            utf8_scalar("MATCH (n) DETACH DELETE n"),
            utf8_scalar("WRITE"),
            utf8_scalar("[]"),
            utf8_scalar("[]"),
        ];
        let p = uni_plugin::traits::connector::Principal {
            id: "alice".to_owned(),
            groups: vec![],
            capabilities: uni_plugin::CapabilitySet::new(),
        };
        let err = drive_declare_procedure(&args, Some(&p))
            .expect_err("WRITE without ProcedureWrites must fail");
        assert_eq!(err.code, 0xB09, "expected capability-denied code 0xB09");
    }

    #[test]
    fn declare_procedure_write_allowed_with_procedure_writes() {
        let args = vec![
            utf8_scalar("mycorp.deleteAll"),
            utf8_scalar("MATCH (n) DETACH DELETE n"),
            utf8_scalar("WRITE"),
            utf8_scalar("[]"),
            utf8_scalar("[]"),
        ];
        let mut caps = uni_plugin::CapabilitySet::new();
        caps.insert(uni_plugin::Capability::ProcedureWrites);
        let p = uni_plugin::traits::connector::Principal {
            id: "admin".to_owned(),
            groups: vec!["admin".to_owned()],
            capabilities: caps,
        };
        drive_declare_procedure(&args, Some(&p)).expect("WRITE with ProcedureWrites must succeed");
    }

    #[test]
    fn declare_procedure_read_does_not_require_procedure_writes() {
        let args = vec![
            utf8_scalar("mycorp.findFriends"),
            utf8_scalar("MATCH (p)-[:KNOWS]->(f) RETURN f"),
            utf8_scalar("READ"),
            utf8_scalar("[]"),
            utf8_scalar("[]"),
        ];
        let p = uni_plugin::traits::connector::Principal::anonymous();
        drive_declare_procedure(&args, Some(&p))
            .expect("READ mode declaration must not require ProcedureWrites");
    }

    fn make(qname: &str, deps: &[&str]) -> DeclaredPlugin {
        DeclaredPlugin {
            qname: qname.to_owned(),
            kind: "function".to_owned(),
            body: String::new(),
            signature_json: "{}".to_owned(),
            dependencies: deps.iter().map(|s| s.to_string()).collect(),
            declared_by: "test".to_owned(),
            active: true,
        }
    }

    #[test]
    fn store_declare_and_get() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a.foo", &[])).unwrap();
        assert_eq!(s.get("a.foo").unwrap().qname, "a.foo");
    }

    #[test]
    fn store_rejects_missing_dependency() {
        let s = DeclaredPluginStore::new();
        match s.declare(make("a.foo", &["a.bar"])) {
            Err(CustomError::DependencyMissing { dependent, dep }) => {
                assert_eq!(dependent, "a.foo");
                assert_eq!(dep, "a.bar");
            }
            other => panic!("expected DependencyMissing, got {other:?}"),
        }
    }

    #[test]
    fn store_detects_cycle() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a", &[])).unwrap();
        s.declare(make("b", &["a"])).unwrap();
        match s.declare(make("a", &["b"])) {
            Err(CustomError::DependencyCycle(_)) => {}
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn store_protects_against_drop_with_dependents() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a", &[])).unwrap();
        s.declare(make("b", &["a"])).unwrap();
        assert!(s.drop_declared("a").is_err());
        assert!(s.drop_declared("b").unwrap());
        assert!(s.drop_declared("a").unwrap());
    }

    #[test]
    fn store_cascade_removes_dependents() {
        let s = DeclaredPluginStore::new();
        s.declare(make("a", &[])).unwrap();
        s.declare(make("b", &["a"])).unwrap();
        s.declare(make("c", &["b"])).unwrap();
        let removed = s.drop_cascade("a");
        assert_eq!(removed.len(), 3);
        assert!(removed.iter().any(|q| q == "a"));
        assert!(removed.iter().any(|q| q == "b"));
        assert!(removed.iter().any(|q| q == "c"));
        assert!(s.list().is_empty());
    }

    #[test]
    fn store_list_returns_all_declared() {
        let s = DeclaredPluginStore::new();
        s.declare(make("x", &[])).unwrap();
        s.declare(make("y", &[])).unwrap();
        assert_eq!(s.list().len(), 2);
    }
}
