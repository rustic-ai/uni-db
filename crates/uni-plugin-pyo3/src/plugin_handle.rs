//! [`uni_plugin::Plugin`] adapter for a loaded PyO3 plugin.
//!
//! After [`PyPluginLoader::load`](crate::loader::PyPluginLoader::load)
//! drains the decorator builder, the loader has already pushed
//! adapters onto a `PluginRegistrar`. [`PyPluginHandle`] gives the
//! host the typed `Arc<dyn Plugin>` it needs to feed the conformance
//! suite, the host's plugin lifecycle machinery, and `Uni::add_plugin`.
//!
//! The handle is **idempotent**: its `register()` is a no-op because
//! adapters were already pushed by the loader. The handle exists to
//! satisfy the [`Plugin`] trait contract — it carries the manifest
//! and a reference to the runtime so the host can drop the plugin
//! (releasing captured callables) on shutdown.

#![cfg(feature = "pyo3")]

use std::collections::BTreeMap;
use std::sync::Arc;

use semver::Version;

use uni_plugin::manifest::{AbiRange, PluginManifest, ProvidedSurfaces};
use uni_plugin::{
    Capability, Determinism, Plugin, PluginError, PluginRegistrar, Scope, SideEffects,
};

use crate::loader::LoadOutcome;
use crate::runtime::PyPluginRuntime;

/// `Plugin`-trait wrapper over a successful [`LoadOutcome`].
#[derive(Debug)]
pub struct PyPluginHandle {
    manifest: PluginManifest,
    /// Strong reference to the runtime so dropping the handle drops the
    /// captured Python callables.
    #[allow(
        dead_code,
        reason = "carried to keep the runtime alive across the host's plugin lifetime"
    )]
    runtime: Arc<PyPluginRuntime>,
}

impl PyPluginHandle {
    /// Build a handle from a [`LoadOutcome`].
    #[must_use]
    pub fn new(outcome: LoadOutcome) -> Self {
        let manifest = build_manifest(&outcome);
        Self {
            manifest,
            runtime: outcome.runtime,
        }
    }

    /// Build a handle with an explicit determinism override (the
    /// loader infers a default from manifest entries; host code may
    /// want to pin it to `Pure` or `SessionScoped` based on its own
    /// signals).
    #[must_use]
    pub fn with_determinism(outcome: LoadOutcome, determinism: Determinism) -> Self {
        let mut h = Self::new(outcome);
        h.manifest.determinism = determinism;
        h
    }

    /// Borrow the assembled manifest.
    #[must_use]
    pub fn manifest_ref(&self) -> &PluginManifest {
        &self.manifest
    }

    /// Borrow the runtime — useful for tests that need to inspect the
    /// registered callable names.
    #[must_use]
    pub fn runtime(&self) -> &Arc<PyPluginRuntime> {
        &self.runtime
    }
}

fn build_manifest(outcome: &LoadOutcome) -> PluginManifest {
    // #233 P3: a malformed guest version becomes `0.0.0`, which then fails any
    // `depends_on` semver constraint — loudly, at registration, so this is not
    // a wrong answer. It is reported so the cause is the version string rather
    // than a mysterious unsatisfied dependency.
    let version = outcome.version.parse::<Version>().unwrap_or_else(|e| {
        tracing::warn!(
            version = %outcome.version,
            error = %e,
            "plugin declared an unparseable version; reporting it as 0.0.0, which will fail \
             any dependency constraint on this plugin",
        );
        Version::new(0, 0, 0)
    });

    // Declared capabilities are the effective set the loader resolved.
    // For PyO3 we also fold in `Scope::Session` as the default.
    let mut declared = outcome.effective_capabilities.clone();
    // Ensure surface caps for any registered entries (defensive — the
    // loader already intersected, so these are no-ops if missing).
    if !outcome.scalars_registered.is_empty() {
        declared.insert(Capability::ScalarFn);
    }
    if !outcome.aggregates_registered.is_empty() {
        declared.insert(Capability::AggregateFn);
    }
    if !outcome.procedures_registered.is_empty() {
        declared.insert(Capability::Procedure);
    }

    PluginManifest {
        id: outcome.plugin_id.clone(),
        version,
        abi: AbiRange::parse("^1").expect("static"),
        depends_on: vec![],
        capabilities: declared,
        determinism: Determinism::SessionScoped,
        side_effects: SideEffects::ReadOnly,
        scope: Scope::Session,
        hash: None,
        signature: None,
        provides: ProvidedSurfaces::default(),
        docs: format!(
            "PyO3 plugin `{id}` v{ver} — session-scoped Python callables registered via decorators.",
            id = outcome.plugin_id,
            ver = outcome.version
        ),
        metadata: BTreeMap::new(),
    }
}

impl Plugin for PyPluginHandle {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn register(&self, _r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        // The loader has already pushed adapters onto a registrar.
        // This impl exists to satisfy the trait so the handle can be
        // fed to lifecycle / conformance code that expects an
        // `Arc<dyn Plugin>`.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loader::PyPluginLoader;
    use pyo3::prelude::*;
    use uni_plugin::{CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};

    #[test]
    fn handle_carries_loader_outcome() {
        Python::initialize();
        Python::attach(|py| {
            let loader = PyPluginLoader::with_default_plugin_id("ai.test.handle");
            let caps = CapabilitySet::from_iter_of([Capability::ScalarFn]);
            let registry = PluginRegistry::new();
            let mut r =
                PluginRegistrar::new(PluginId::new("ai.test.placeholder"), &caps, &registry);
            let src = r#"
db.set_version("0.2.0")

@db.scalar_fn("triple", args=["float"], returns="float", determinism="pure")
def triple(x):
    return x * 3.0
"#;
            let outcome = loader
                .load(py, src, "ai.test.handle", &mut r, &caps)
                .expect("load");
            r.commit_to_registry().expect("commit");
            let handle = PyPluginHandle::new(outcome);
            assert_eq!(handle.manifest().id.as_str(), "ai.test.handle");
            assert_eq!(handle.manifest().version.to_string(), "0.2.0");
            assert!(
                handle
                    .manifest()
                    .capabilities
                    .contains(&Capability::ScalarFn)
            );
            assert_eq!(handle.runtime().len(), 1);
        });
    }
}
