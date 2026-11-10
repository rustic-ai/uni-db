// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Database-level plugin install, reload, and dynamic-language loading.
//!
//! Holds the registrar plumbing shared by every loader, the built-in
//! plugin bootstrap run at `Uni::build()` time, the [`UniPluginEntry`]
//! record `UniInner.plugins` tracks, and the `Uni` methods that install,
//! enumerate, reload, remove, and dynamically load plugins.

use std::sync::Arc;

use uni_common::{Result, UniError};

use crate::api::Uni;

/// Map a [`uni_plugin::PluginError`] to a [`UniError`] for user-facing
/// surfaces (`Uni::add_plugin`). The catch-all variant is
/// [`UniError::InvalidArgument`] so we preserve the plugin id /
/// capability detail via the `Display` impl.
pub(crate) fn plugin_err_to_uni(e: uni_plugin::PluginError) -> UniError {
    UniError::InvalidArgument {
        arg: "plugin".to_string(),
        message: e.to_string(),
    }
}

/// Map a [`uni_plugin_pyo3::PyPluginError`] to a [`UniError`] for the
/// Python loader surfaces (`Uni::load_python_plugin` /
/// `Session::add_python_plugin`). `arg` names the offending input so the
/// [`UniError::InvalidArgument`] payload points at the caller's parameter.
#[cfg(feature = "pyo3-plugins")]
pub(crate) fn py_plugin_err_to_uni(
    arg: &'static str,
    e: uni_plugin_pyo3::PyPluginError,
) -> UniError {
    match e {
        uni_plugin_pyo3::PyPluginError::PythonException {
            qname,
            message,
            traceback,
        } => UniError::InvalidArgument {
            arg: arg.to_owned(),
            message: format!("python exception in {qname}: {message}\n{traceback}"),
        },
        uni_plugin_pyo3::PyPluginError::ManifestInvalid(m) => UniError::InvalidArgument {
            arg: arg.to_owned(),
            message: format!("python plugin manifest: {m}"),
        },
        uni_plugin_pyo3::PyPluginError::ArrowConversion(m) => UniError::InvalidArgument {
            arg: arg.to_owned(),
            message: format!("python plugin arrow conversion: {m}"),
        },
        other => UniError::Internal(anyhow::anyhow!(other.to_string())),
    }
}

/// Run a plugin-loader pass under a freshly-built placeholder registrar.
///
/// Every loader (`load_wasm_extism` / `load_wasm_component` /
/// `load_rhai_plugin` / `load_python_plugin`, plus the session-scoped
/// Python add/finalize paths) follows the same three-step dance:
/// construct a [`uni_plugin::PluginRegistrar`] under a placeholder plugin
/// id (the loader rewrites the real id from the manifest into the returned
/// `LoadOutcome`), run the loader's `load*` call, then atomically commit
/// the staged registrations into `registry`.
///
/// `f` performs the loader-specific `load*` call and maps the loader's
/// error enum to a [`UniError`] — that mapping stays at the call site so
/// each loader preserves its exact error-variant handling. This helper
/// only owns the placeholder/registrar construction and the final commit.
pub(crate) fn with_loading_registrar<T>(
    registry: &Arc<uni_plugin::PluginRegistry>,
    placeholder: &str,
    caps: &uni_plugin::CapabilitySet,
    f: impl FnOnce(&mut uni_plugin::PluginRegistrar) -> Result<T>,
) -> Result<T> {
    use uni_plugin::{PluginId, PluginRegistrar};
    let mut r = PluginRegistrar::new(PluginId::new(placeholder), caps, registry);
    let outcome = f(&mut r)?;
    // Snapshot the aggregate qnames staged by the loader *before* the commit
    // consumes the registrar, so we can publish their Cypher routing hints
    // once the commit succeeds.
    let staged_aggregates: Vec<String> = r
        .staged_aggregate_qnames()
        .iter()
        .map(|q| format!("{}.{}", q.namespace(), q.local()))
        .collect();
    r.commit_to_registry().map_err(plugin_err_to_uni)?;
    // Publish each committed aggregate to the Cypher planner's plugin-aggregate
    // hint set so `RETURN ns.myAgg(x)` (and `GROUP BY`) routes through aggregate
    // translation instead of scalar-UDF resolution. This is the single point
    // every dynamic loader (rhai / pyo3 / wasm / extism) passes through, so all
    // of them are covered without each depending on `uni-cypher`. Mirrors the
    // declared-aggregate path in `uni-plugin-custom` (`declareAggregate`).
    // Idempotent: the hint set is a deduplicating set.
    for dotted in staged_aggregates {
        uni_cypher::register_plugin_aggregate(dotted);
    }
    Ok(outcome)
}

/// The mode the procedure `name` is registered with in `registry`, or `None`
/// when it is not registered there.
///
/// Resolved as dispatch resolves it (`ProcedureRegistry::resolve_user_procedure`):
/// every namespace split ([`uni_plugin::QName::candidate_splits`]), then the
/// name without its `uni.` prefix under each built-in plugin's namespace.
/// The built-in plugins register first-dot names (`uni` + `nlp.extract`) or
/// names reached only through that prefix (`custom` + `plugin.declareTrigger`,
/// called as `uni.plugin.declareTrigger`), and a last-dot `QName::parse`
/// finds none of them. The read-only gate and the authorization verbs used
/// `parse`, so they classified every such procedure as unregistered, and a
/// read-only query ran the write ones.
pub(crate) fn registered_procedure_mode(
    registry: &uni_plugin::PluginRegistry,
    name: &str,
) -> Option<uni_plugin::traits::procedure::ProcedureMode> {
    let stripped = name.strip_prefix("uni.").unwrap_or(name);
    uni_plugin::QName::candidate_splits(name)
        .find_map(|q| registry.procedure(&q))
        .or_else(|| {
            ["uni", "builtin", "apoc-core", "custom"]
                .iter()
                .find_map(|ns| registry.procedure(&uni_plugin::QName::new(*ns, stripped)))
        })
        .map(|e| e.signature.mode)
}

/// Register the framework-wide built-in plugins into a fresh
/// `PluginRegistry`. Called once at `Uni::build()` time.
///
/// - `BuiltinPlugin` is always registered (closed-enum replacement
///   infrastructure: Locy aggregates, storage backends, CRDTs, collations,
///   hooks, logical types, plus a handful of system procedures).
/// - `ApocCorePlugin` is registered when the `apoc-core` cargo feature is
///   on (default). Library embedders who don't want APOC content disable
///   the feature.
pub(crate) fn register_builtin_plugins(
    registry: &Arc<uni_plugin::PluginRegistry>,
    data_path: Option<&std::path::Path>,
) -> std::result::Result<Option<Arc<crate::persistence::LazyCypherSink>>, uni_plugin::PluginError> {
    use uni_plugin::{Plugin, PluginRegistrar};

    // BuiltinPlugin — always.
    {
        let plugin = uni_plugin_builtin::BuiltinPlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, registry);
        plugin.register(&mut r)?;
        r.commit_to_registry()?;
    }

    // ApocCorePlugin — feature-gated, default-on.
    #[cfg(feature = "apoc-core")]
    {
        let plugin = uni_plugin_apoc_core::ApocCorePlugin::new();
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, registry);
        plugin.register(&mut r)?;
        r.commit_to_registry()?;
    }

    // Host-coupled built-in procedures (uni.schema.*, uni.vector.query,
    // uni.fts.query, uni.search, uni.algo.*). These live in `uni-query`
    // rather than `uni-plugin-builtin` because they depend on
    // `uni-store` / `uni-algo` types that the latter cannot reach
    // without inverting the crate layering.
    {
        use uni_plugin::{Capability, CapabilitySet, PluginId};

        let plugin_id = PluginId::new("uni");
        let caps = CapabilitySet::from_iter_of([
            Capability::Procedure,
            Capability::ProcedureSchema,
            // M11: background-job registration gate. The three built-in
            // maintenance jobs (`uni.system.ttl_sweep` /
            // `statistics_refresh` / `compaction`) register through
            // this plugin id; the registrar's variant-match treats any
            // `BackgroundJob` cap as sufficient regardless of
            // `max_concurrent`.
            Capability::BackgroundJob { max_concurrent: 0 },
        ]);
        let mut caps = caps;
        // M5c.1: this block also registers `AlgorithmProvider`s so the
        // `Capability::Algorithm` must be in scope. The host's "uni"
        // plugin owns both procedure registrations (`uni.algo.*`
        // adapters) and the AlgorithmProvider chain.
        caps.insert(Capability::Algorithm);
        // GraphView (P0): the first-party `uni.algo.reachability`
        // provider reaches topology through `AlgorithmHost::project`,
        // which is gated on `HostQuery`. Grant it read-only, unscoped so
        // the built-in provider passes its own gate; third-party plugins
        // must declare `HostQuery` in their manifest to earn the same.
        caps.insert(Capability::HostQuery {
            read_only: true,
            scopes: Vec::new(),
        });
        // GraphCompute (Phase 1): the first-party `uni.algo.gcpagerank`
        // provider drives the coarse kernel catalog, gated on
        // `Capability::GraphCompute` (orthogonal to the `HostQuery` gate on
        // `project`). Third-party plugins must declare `graph-compute` in
        // their manifest to earn the same.
        caps.insert(Capability::GraphCompute);
        let mut r = PluginRegistrar::new(plugin_id, &caps, registry);
        let algo_registry: Arc<uni_algo::algo::AlgorithmRegistry> =
            Arc::new(uni_algo::algo::AlgorithmRegistry::new());
        uni_query::procedures_plugin::register_into(&mut r, Some(&algo_registry))?;
        // M5c.1: register each algorithm as a phased `AlgorithmProvider`
        // so consumers can `registry.iter_algorithms()` /
        // `registry.algorithm(qname)`. The static `AlgorithmRegistry`
        // path above is the M4 adapter and stays in place during M5c.1
        // — both surfaces resolve to the same underlying `AlgoProcedure`
        // impls.
        uni_plugin_builtin::algorithms::register_into(&mut r)?;
        // M11: the three built-in maintenance jobs (`ttl_sweep`,
        // `statistics_refresh`, `compaction`). The host scheduler
        // driver in `crates/uni/src/scheduler.rs` looks each up by
        // qname and dispatches per its `Schedule::Periodic` interval.
        uni_plugin_builtin::background_jobs::register_into(&mut r)?;
        r.commit_to_registry()?;
    }

    // CustomPlugin — apoc.custom-style meta-plugin always-on. Exposes
    // `uni.plugin.declareFunction/Procedure/Aggregate/Trigger` plus
    // `listDeclared` / `dropDeclared`. The plugin holds a shared
    // `Arc<PluginRegistry>` so its declare* procedures can register
    // new scalar functions at runtime; persistence rides through
    // `SystemLabelPersistence` (M11 A.2) when the instance has a
    // local data directory, else `NullPersistence` (in-memory /
    // object-store-backed instances).
    let (persistence, cypher_sink) = crate::persistence::persistence_for_data_path(data_path);
    {
        let synthesizer: Arc<dyn uni_plugin_custom::ProcedureBodySynthesizer> =
            Arc::new(crate::synthetic_procedure::CypherProcedureSynthesizer::new());
        // WS-A — trigger synthesizer so `declareTrigger` installs a real
        // `TriggerPlugin` that fires on the commit path.
        let trigger_synthesizer: Arc<dyn uni_plugin_custom::TriggerBodySynthesizer> =
            Arc::new(crate::synthetic_trigger::CypherTriggerSynthesizer::new());
        let plugin = uni_plugin_custom::CustomPlugin::new(Arc::clone(registry), persistence)
            .map_err(|e| uni_plugin::PluginError::internal(format!("uni-plugin-custom: {e}")))?
            .with_procedure_synthesizer(synthesizer)
            .with_trigger_synthesizer(trigger_synthesizer);
        plugin.reactivate_into_registry().map_err(|e| {
            uni_plugin::PluginError::internal(format!("uni-plugin-custom reactivate: {e}"))
        })?;
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, registry);
        plugin.register(&mut r)?;
        r.commit_to_registry()?;
    }

    Ok(cypher_sink)
}

/// A live plugin entry tracked by `UniInner.plugins`.
///
/// Holds the installed plugin object, the lifecycle handle the reload
/// driver advances through `Active → Draining → Removed`, and the
/// monotonic `generation` exposed via [`uni_plugin::PluginHandle`]. The
/// `generation` is bumped on every successful reload so handles handed
/// to callers identify the *epoch* of the plugin, not just its id.
#[derive(Clone)]
pub struct UniPluginEntry {
    /// The installed plugin object (Arc-shared so `shutdown()` can run
    /// after the registry has dropped its references).
    pub plugin: Arc<dyn uni_plugin::Plugin>,
    /// Shared lifecycle handle the `EpochFencedReload` driver advances.
    pub lifecycle: Arc<uni_plugin::lifecycle::PluginLifecycle>,
    /// Monotonic generation counter; bumped per successful reload.
    pub generation: u64,
}

impl std::fmt::Debug for UniPluginEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UniPluginEntry")
            .field("plugin_id", &self.lifecycle.plugin())
            .field("state", &self.lifecycle.state())
            .field("generation", &self.generation)
            .finish()
    }
}

impl Uni {
    /// Install a user plugin into this database's [`uni_plugin::PluginRegistry`].
    ///
    /// Runs the standard registrar dance: clone the plugin's
    /// [`uni_plugin::PluginManifest`], build a [`uni_plugin::PluginRegistrar`] scoped
    /// to the manifest's capability set, invoke
    /// [`uni_plugin::Plugin::register`], and commit the pending
    /// registrations atomically.
    ///
    /// This is the recommended replacement for the deprecated
    /// `Session::add_hook` / `Uni::add_hook` legacy API: callers wrap
    /// their legacy [`crate::api::hooks::SessionHook`] in a
    /// [`crate::api::hooks::BuiltinHookPlugin`] and pass it here.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::InvalidArgument`] if the plugin's
    /// `register()` fails or any pending registration collides with an
    /// existing qname.
    pub fn add_plugin<P: uni_plugin::Plugin>(&self, plugin: P) -> Result<()> {
        use uni_plugin::PluginRegistrar;
        use uni_plugin::lifecycle::{LifecycleState, PluginLifecycle};

        let plugin: Arc<dyn uni_plugin::Plugin> = Arc::new(plugin);
        let manifest = plugin.manifest();
        // Enforce the host signature policy. Default (Disabled) is a no-op;
        // RequireSigned rejects an unsigned manifest or an untrusted key.
        self.inner
            .plugin_trust
            .enforce(manifest)
            .map_err(plugin_err_to_uni)?;
        let plugin_id = manifest.id.clone();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(plugin_id.clone(), &caps, &self.inner.plugin_registry);
        plugin.register(&mut r).map_err(plugin_err_to_uni)?;
        r.commit_to_registry().map_err(plugin_err_to_uni)?;

        // Lifecycle: Loaded → Linked → Initialized → Active.
        let lifecycle = Arc::new(PluginLifecycle::new(plugin_id.clone()));
        lifecycle.set(LifecycleState::Active);
        self.inner.plugins.write().insert(
            plugin_id,
            UniPluginEntry {
                plugin,
                lifecycle,
                generation: 0,
            },
        );
        Ok(())
    }

    /// Snapshot every installed plugin's id, handle, and current state.
    ///
    /// Returns a vector ordered by the order plugins were inserted into
    /// the host's internal map (note: iteration order across the map is
    /// not stable across reloads — callers needing stable ordering
    /// should sort on `PluginId`).
    pub fn plugins(
        &self,
    ) -> Vec<(
        uni_plugin::PluginId,
        uni_plugin::PluginHandle,
        uni_plugin::lifecycle::LifecycleState,
    )> {
        self.inner
            .plugins
            .read()
            .iter()
            .map(|(id, entry)| {
                let handle = uni_plugin::PluginHandle::new(id.clone(), entry.generation);
                (id.clone(), handle, entry.lifecycle.state())
            })
            .collect()
    }

    /// Look up a plugin handle by id.
    ///
    /// Returns `None` when the id is not installed (or has been removed
    /// via [`Self::remove_plugin`]).
    #[must_use]
    pub fn plugin(&self, id: &uni_plugin::PluginId) -> Option<uni_plugin::PluginHandle> {
        self.inner
            .plugins
            .read()
            .get(id)
            .map(|entry| uni_plugin::PluginHandle::new(id.clone(), entry.generation))
    }

    /// Remove an installed plugin, draining in-flight references first.
    ///
    /// Implements the §11.2 cutover for the removal direction: snapshot
    /// the old plugin's per-kind state, evict its registry footprint,
    /// drive `EpochFencedReload::begin_drain → wait_for_drain →
    /// finalize`, then run the plugin's `shutdown()` callback and drop
    /// the entry.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] if the handle's id is not
    ///   installed or the generation does not match (stale handle).
    /// - [`UniError::Internal`] if the drain times out (default 30 s).
    pub fn remove_plugin(&self, handle: &uni_plugin::PluginHandle) -> Result<()> {
        let _outcome = self.reload_internal(handle, None)?;
        Ok(())
    }

    /// Reload a plugin, swapping in a new instance under the same id.
    ///
    /// Implements the §11.2 epoch-fenced cutover. Drains in-flight
    /// references to the old instance, runs the per-kind reload
    /// discipline (CRDT schema-compat check, logical-type contract
    /// check), evicts the old registry footprint, runs the new
    /// plugin's `register()` + `init()`, and bumps the handle's
    /// generation counter on success.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] if the handle is stale or the
    ///   id is not installed.
    /// - [`UniError::InvalidArgument`] if a per-kind compat check or
    ///   the new plugin's `register()` rejects the swap (the old
    ///   plugin remains installed and active on rejection).
    pub fn reload<P: uni_plugin::Plugin>(
        &self,
        handle: &uni_plugin::PluginHandle,
        new_plugin: P,
    ) -> Result<uni_plugin::PluginHandle> {
        let new_arc: Arc<dyn uni_plugin::Plugin> = Arc::new(new_plugin);
        self.reload_internal(handle, Some(new_arc))
    }

    fn reload_internal(
        &self,
        handle: &uni_plugin::PluginHandle,
        new_plugin: Option<Arc<dyn uni_plugin::Plugin>>,
    ) -> Result<uni_plugin::PluginHandle> {
        use uni_plugin::PluginRegistrar;
        use uni_plugin::lifecycle::{EpochFencedReload, LifecycleState, PluginLifecycle};
        use uni_plugin::reload::{OldProviders, ReloadDispatcher};

        let plugin_id = handle.id.clone();

        // Step 1: validate handle + extract the live plugin / lifecycle /
        // generation. We do **not** keep a clone of the entry around —
        // the only `Arc<PluginLifecycle>` clones we want at drain time
        // are (a) the driver's and (b) whichever in-flight captures
        // still hold one. Holding extra clones here would inflate the
        // strong-count and force the drain wait to time out.
        let (old_plugin, old_lifecycle, old_generation) = {
            let map = self.inner.plugins.read();
            let entry = map
                .get(&plugin_id)
                .ok_or_else(|| UniError::InvalidArgument {
                    arg: "handle".to_owned(),
                    message: format!("plugin {plugin_id} not installed"),
                })?;
            if entry.generation != handle.generation {
                return Err(UniError::InvalidArgument {
                    arg: "handle".to_owned(),
                    message: format!(
                        "stale handle for plugin {plugin_id}: expected generation {}, got {}",
                        entry.generation, handle.generation
                    ),
                });
            }
            (
                Arc::clone(&entry.plugin),
                Arc::clone(&entry.lifecycle),
                entry.generation,
            )
        };

        // Step 2: snapshot the per-kind providers the old plugin owned
        // for the dispatcher's schema-compat check.
        let snapshot = self
            .inner
            .plugin_registry
            .iter_for_plugin(&plugin_id)
            .unwrap_or_default();
        let mut old_providers = OldProviders::default();
        for kind in &snapshot.crdt_kinds {
            if let Some(p) = self.inner.plugin_registry.crdt_kind(kind) {
                old_providers.crdt_kinds.insert(kind.clone(), p);
            }
        }

        // Step 3: begin drain on the old lifecycle.
        let driver = EpochFencedReload::new(Arc::clone(&old_lifecycle));
        driver
            .begin_drain()
            .map_err(|e| UniError::Internal(anyhow::anyhow!("reload drain begin: {e}")))?;

        // Step 4: evict the old plugin's registry footprint.
        self.inner.plugin_registry.remove_plugin(&plugin_id);

        // Step 5: if reloading, run the new plugin's registrar dance.
        if let Some(new) = new_plugin.as_ref() {
            let manifest = new.manifest();
            if manifest.id != plugin_id {
                let _ = self.replay_register_for(&old_plugin);
                old_lifecycle.set(LifecycleState::Active);
                return Err(UniError::InvalidArgument {
                    arg: "new_plugin".to_owned(),
                    message: format!(
                        "reload plugin id mismatch: handle is {plugin_id}, new plugin id is {}",
                        manifest.id
                    ),
                });
            }
            let caps = manifest.capabilities.clone();
            let mut r = PluginRegistrar::new(plugin_id.clone(), &caps, &self.inner.plugin_registry);
            new.register(&mut r).map_err(plugin_err_to_uni)?;
            r.commit_to_registry().map_err(plugin_err_to_uni)?;

            // Step 6: per-kind compat checks on the now-committed new
            // registry. Compat failures abort by re-replaying the old
            // plugin's registrations.
            let dispatcher = ReloadDispatcher::new(&snapshot, &self.inner.plugin_registry);
            if let Err(e) = dispatcher.check_compat(&old_providers) {
                self.inner.plugin_registry.remove_plugin(&plugin_id);
                let _ = self.replay_register_for(&old_plugin);
                old_lifecycle.set(LifecycleState::Active);
                return Err(UniError::InvalidArgument {
                    arg: "new_plugin".to_owned(),
                    message: format!("reload compat-check rejected: {e}"),
                });
            }
        }

        // Step 7: replace (or remove) the host's `plugins` map entry
        // **before** the drain wait so the map's `Arc<PluginLifecycle>`
        // is no longer counted. After this, the only lifecycle Arcs
        // outstanding from the host should be: (a) the driver's `old`
        // ref, (b) our local `old_lifecycle`, plus any in-flight
        // captures. Threshold=2 lets the wait succeed as soon as no
        // in-flight capture survives.
        let new_handle = {
            let mut map = self.inner.plugins.write();
            if let Some(new) = new_plugin.clone() {
                let new_lifecycle = Arc::new(PluginLifecycle::new(plugin_id.clone()));
                new_lifecycle.set(LifecycleState::Active);
                let new_generation = old_generation.wrapping_add(1);
                map.insert(
                    plugin_id.clone(),
                    UniPluginEntry {
                        plugin: new,
                        lifecycle: new_lifecycle,
                        generation: new_generation,
                    },
                );
                uni_plugin::PluginHandle::new(plugin_id.clone(), new_generation)
            } else {
                map.remove(&plugin_id);
                uni_plugin::PluginHandle::new(plugin_id.clone(), old_generation)
            }
        };

        // Step 8: wait for in-flight references to drain. Threshold 2
        // accounts for the driver's own `old` Arc plus our local
        // `old_lifecycle`. If captures outlast the wait, surface a
        // warning but proceed — the new plugin is already live in the
        // registry.
        if let Err(e) = driver.wait_for_drain(
            2,
            std::time::Duration::from_millis(10),
            std::time::Duration::from_secs(30),
        ) {
            tracing::warn!(
                plugin_id = %plugin_id,
                error = %e,
                "reload drain wait timed out; proceeding with cutover"
            );
        }
        driver.finalize();

        // Step 9: run shutdown on the old plugin object after the
        // drain. Safe to call even if other Arcs outlive us because
        // shutdown is on `&self` and `Plugin: Send + Sync`.
        old_plugin.shutdown();

        Ok(new_handle)
    }

    /// Re-run the registrar dance for the given plugin object.
    ///
    /// Used as a best-effort rollback when [`Self::reload_internal`]
    /// rejects a reload after evicting the old plugin's registry
    /// footprint.
    fn replay_register_for(
        &self,
        plugin: &Arc<dyn uni_plugin::Plugin>,
    ) -> std::result::Result<(), UniError> {
        use uni_plugin::PluginRegistrar;
        let manifest = plugin.manifest();
        let caps = manifest.capabilities.clone();
        let mut r = PluginRegistrar::new(manifest.id.clone(), &caps, &self.inner.plugin_registry);
        plugin.register(&mut r).map_err(plugin_err_to_uni)?;
        r.commit_to_registry().map_err(plugin_err_to_uni)?;
        Ok(())
    }

    /// Load an Extism-shaped WASM plugin from raw bytes.
    ///
    /// The two-pass dance defined by
    /// [`uni_plugin_extism::ExtismLoader::load`] is executed against the
    /// database's plugin registry: the plugin's `manifest` export is
    /// read, declared capabilities are intersected with `host_grants`,
    /// and the plugin's `register` export is consulted to surface every
    /// qname through an Extism-backed adapter.
    ///
    /// `registrar_caps` is the [`uni_plugin::CapabilitySet`] the
    /// inner [`uni_plugin::PluginRegistrar`] runs under — it gates
    /// **which surfaces** the plugin may register (e.g.,
    /// [`uni_plugin::Capability::ScalarFn`]). It must include every
    /// surface kind a plugin entry will use, or registration will
    /// fail with [`uni_plugin::PluginError::CapabilityRequired`].
    ///
    /// `host_grants` is the [`uni_plugin::CapabilitySet`] the host grants the
    /// plugin for **host-fn access** (e.g. `Capability::Network { allow }` with
    /// an attenuation allow-list). It is intersected with the plugin manifest's
    /// declared capabilities to compute the effective grant set; only host fns
    /// whose `required_capability` variant is in that set become part of the
    /// plugin's
    /// import table.
    ///
    /// # Errors
    ///
    /// Wraps [`uni_plugin_extism::ExtismError`] in
    /// [`UniError::InvalidArgument`] for plugin-side faults and
    /// [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `extism-plugins` feature.
    #[cfg(feature = "extism-plugins")]
    pub fn load_wasm_extism(
        &self,
        loader: &uni_plugin_extism::ExtismLoader,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_extism::loader::LoadOutcome> {
        // Enforce the host artifact hash-pin allowlist before the payload is
        // instantiated. Default (empty allowlist) is a no-op.
        self.inner
            .plugin_trust
            .enforce_artifact_pin(bytes)
            .map_err(plugin_err_to_uni)?;

        // The placeholder plugin id is rewritten by the loader with the
        // real id from the manifest into the returned LoadOutcome. We
        // need a non-empty placeholder because QName::namespace()
        // comparisons in `validate_qname` require a non-builtin
        // namespace; we let the registrar accept any qname by leaning on
        // `validate_qname`'s `is_builtin` short-circuit (M6a.2 expands
        // this with a per-plugin namespace gate).
        with_loading_registrar(
            &self.inner.plugin_registry,
            "extism.loading",
            registrar_caps,
            |r| {
                loader.load(bytes, host_grants, r).map_err(|e| match e {
                    uni_plugin_extism::ExtismError::Instantiate(m)
                    | uni_plugin_extism::ExtismError::InvalidPlugin(m)
                    | uni_plugin_extism::ExtismError::ManifestInvalid(m)
                    | uni_plugin_extism::ExtismError::OutputDecode(m) => {
                        UniError::InvalidArgument {
                            arg: "bytes".to_owned(),
                            message: format!("extism plugin: {m}"),
                        }
                    }
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })
            },
        )
    }

    /// Load a Component Model WASM plugin from raw bytes.
    ///
    /// The two-pass dance defined by
    /// [`uni_plugin_wasm::WasmLoader::load`] is executed against the
    /// database's plugin registry: the plugin's `manifest` export is
    /// called, declared capabilities are intersected with `host_grants`,
    /// and the plugin's `register` export is consulted to surface every
    /// qname through a Component Model-backed adapter.
    ///
    /// `registrar_caps` gates which **surfaces** the plugin may
    /// register; `host_grants` gates which **host fns** become part of
    /// the plugin's import table (per-major Linker absence for
    /// capabilities outside the grant set — structural enforcement,
    /// proposal §5.6.2).
    ///
    /// # Errors
    ///
    /// Wraps [`uni_plugin_wasm::WasmError`] in
    /// [`UniError::InvalidArgument`] for plugin-side faults
    /// (invalid wasm, missing required exports, manifest parse) and
    /// [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `wasm-plugins` feature.
    #[cfg(feature = "wasm-plugins")]
    pub fn load_wasm_component(
        &self,
        loader: &uni_plugin_wasm::WasmLoader,
        bytes: &[u8],
        host_grants: &uni_plugin::CapabilitySet,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_wasm::loader::LoadOutcome> {
        // Enforce the host artifact hash-pin allowlist before the payload is
        // instantiated. Default (empty allowlist) is a no-op.
        self.inner
            .plugin_trust
            .enforce_artifact_pin(bytes)
            .map_err(plugin_err_to_uni)?;
        with_loading_registrar(
            &self.inner.plugin_registry,
            "wasm.loading",
            registrar_caps,
            |r| {
                loader.load(bytes, host_grants, r).map_err(|e| match e {
                    uni_plugin_wasm::WasmError::Instantiate(m)
                    | uni_plugin_wasm::WasmError::Invoke(m)
                    | uni_plugin_wasm::WasmError::InvalidWasm(m)
                    | uni_plugin_wasm::WasmError::ResourceLimit(m) => UniError::InvalidArgument {
                        arg: "bytes".to_owned(),
                        message: format!("wasm component: {m}"),
                    },
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })
            },
        )
    }

    /// Load a Rhai-script plugin from source text.
    ///
    /// Rhai is a pure-Rust embedded scripting language; no WASM wrapper,
    /// no C toolchain. The Rhai engine is sandboxed by language design —
    /// scripts have no built-in I/O, every effectful operation comes
    /// from a host-registered function. The loader's three-phase shape
    /// mirrors `Self::load_wasm_extism`: read the script's
    /// `uni_manifest()` to discover declared entries, intersect declared
    /// capabilities with `registrar_caps`, then register each entry on
    /// the inner [`uni_plugin::PluginRegistrar`] as a Rhai-backed
    /// adapter.
    ///
    /// `registrar_caps` is **both** the registration gate (it must
    /// include `Capability::ScalarFn`/`AggregateFn`/`Procedure` matching
    /// the script's entries) **and** the host-fn grant set (host fns
    /// like `uni_fs_read` are only registered on the engine if the
    /// matching `Capability::Filesystem` etc. is present). Rhai's
    /// capability-enforcement layer 2 is *Engine-import absence* —
    /// ungranted host fns are not registered, so any call to them
    /// fails at parse-resolution with `ErrorFunctionNotFound`.
    ///
    /// # Errors
    ///
    /// Wraps [`uni_plugin_rhai::RhaiError`] in
    /// [`UniError::InvalidArgument`] for plugin-side faults and
    /// [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `rhai-plugins` feature (on by default).
    #[cfg(feature = "rhai-plugins")]
    pub fn load_rhai_plugin(
        &self,
        loader: &uni_plugin_rhai::RhaiLoader,
        script: &str,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_rhai::LoadOutcome> {
        // Enforce the host artifact hash-pin allowlist before the payload is
        // instantiated. Default (empty allowlist) is a no-op.
        self.inner
            .plugin_trust
            .enforce_artifact_pin(script.as_bytes())
            .map_err(plugin_err_to_uni)?;
        with_loading_registrar(
            &self.inner.plugin_registry,
            "rhai.loading",
            registrar_caps,
            |r| {
                loader.load(script, r, registrar_caps).map_err(|e| match e {
                    uni_plugin_rhai::RhaiError::ParseFailed(m) => UniError::InvalidArgument {
                        arg: "script".to_owned(),
                        message: format!("rhai parse: {m}"),
                    },
                    uni_plugin_rhai::RhaiError::InvalidPlugin(m)
                    | uni_plugin_rhai::RhaiError::ManifestInvalid(m)
                    | uni_plugin_rhai::RhaiError::Conversion(m)
                    | uni_plugin_rhai::RhaiError::RuntimeError(m) => UniError::InvalidArgument {
                        arg: "script".to_owned(),
                        message: format!("rhai plugin: {m}"),
                    },
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })
            },
        )
    }

    /// Load a PyO3 (Python source) plugin into this Uni instance.
    ///
    /// The supplied [`PyPluginLoader`](uni_plugin_pyo3::PythonPluginLoader)
    /// holds the loader's default plugin id (used when the module
    /// doesn't call `db.set_plugin_id(...)`). `module_src` is Python
    /// source code; `module_name` is the simulated `__name__`. The
    /// loader executes the source against a fresh module namespace
    /// that includes a `_uni_decorator_sink` / `db` global; each
    /// `@db.scalar_fn(...)` / `@db.aggregate_fn(...)` / `@db.procedure(...)`
    /// decorator records into a builder and the loader drains it on
    /// completion. Scalar / aggregate / procedure adapters are pushed
    /// onto a fresh [`PluginRegistrar`](uni_plugin::PluginRegistrar)
    /// and committed atomically.
    ///
    /// **M8 scope:** the plugin is added to the *instance* registry.
    /// Session-scoped registration (proposal §5.4.2 default) is the
    /// `M8-followup.session-scope` work item; until then, callers that
    /// want session-scoped behavior should drop the plugin on session
    /// drop themselves via `Uni::remove_plugin`.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] for plugin-side faults (parse,
    ///   manifest, unknown type name).
    /// - [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `pyo3-plugins` feature.
    #[cfg(feature = "pyo3-plugins")]
    pub fn load_python_plugin(
        &self,
        py: pyo3::Python<'_>,
        loader: &uni_plugin_pyo3::PythonPluginLoader,
        module_src: &str,
        module_name: &str,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_pyo3::LoadOutcome> {
        // Enforce the host artifact hash-pin allowlist before the payload is
        // instantiated. Default (empty allowlist) is a no-op.
        self.inner
            .plugin_trust
            .enforce_artifact_pin(module_src.as_bytes())
            .map_err(plugin_err_to_uni)?;
        with_loading_registrar(
            &self.inner.plugin_registry,
            "pyo3.loading",
            registrar_caps,
            |r| {
                loader
                    .load(py, module_src, module_name, r, registrar_caps)
                    .map_err(|e| py_plugin_err_to_uni("module_src", e))
            },
        )
    }
}
