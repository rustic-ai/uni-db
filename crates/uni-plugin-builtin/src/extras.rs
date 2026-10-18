//! Built-in reference impls for the remaining M5 surface traits:
//! `CdcOutputProvider`, `CatalogProvider`, `ReplacementScanProvider`.
//! Each is a minimal-but-real implementation suitable for tests, the
//! conformance suite, and as the authoring template user plugins follow.
//!
//! The in-memory CDC provider is **not** part of the default
//! registration. The CDC runtime starts a stream for every registered
//! `CdcOutputProvider`, and this provider keeps every delivered batch, so
//! registering it by default made every database retain every commit for
//! its whole lifetime. Opt in with [`MemoryCdcPlugin`] or
//! [`register_memory_cdc`].

use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use semver::Version;
use uni_plugin::traits::catalog::{
    CatalogEdgeType, CatalogLabel, CatalogProvider, CatalogTable, Replacement, ReplacementRequest,
    ReplacementScanProvider,
};
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, Plugin, PluginError, PluginId,
    PluginManifest, PluginRegistrar, ProvidedSurfaces, Scope, SideEffects,
};

/// Register the default reference impls for the `catalog` and
/// `replacement-scan` surfaces.
///
/// The in-memory CDC provider is deliberately not registered here: every
/// registered CDC provider gets a live stream fed with every commit, and
/// [`MemoryCdcOutputProvider`] retains each batch it receives. Use
/// [`register_memory_cdc`] or [`MemoryCdcPlugin`] to opt in.
///
/// # Errors
///
/// Returns [`PluginError`] on registration failure.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.catalog(Arc::new(EmptyCatalog::new("builtin")))?;
    r.replacement_scan(Arc::new(NeverReplacementScan))?;
    Ok(())
}

/// Register `provider` as the `memory` CDC output.
///
/// Opt-in counterpart of the default registration: once registered, the
/// host's CDC runtime starts a stream for it and delivers every commit,
/// which `provider` then retains until the stream shuts down. Keep a
/// clone of `provider` to read the captured batches.
///
/// # Errors
///
/// Returns [`PluginError::CapabilityRequired`] when the registrar's plugin
/// does not declare [`Capability::Cdc`], or a collision error when a CDC
/// output named `memory` is already registered.
pub fn register_memory_cdc(
    r: &mut PluginRegistrar<'_>,
    provider: MemoryCdcOutputProvider,
) -> Result<(), PluginError> {
    r.cdc_output(Arc::new(provider))?;
    Ok(())
}

// =========================================================================
// CdcOutputProvider — in-memory reference impl
// =========================================================================

/// `CdcOutputProvider` that captures batches in memory.
///
/// Useful for tests and authoring-time validation of mutation event
/// shapes. The buffer is unbounded: it grows by one [`CdcBatch`] per
/// commit until the stream shuts down, so it is opt-in and never part of
/// the default registration (see [`MemoryCdcPlugin`]).
///
/// Clones share one buffer, so a clone kept by the caller observes what
/// the stream started from the registered clone receives.
///
/// # Examples
///
/// ```
/// use uni_plugin::traits::cdc::{CdcOutputProvider, CdcStartContext};
/// use uni_plugin_builtin::extras::MemoryCdcOutputProvider;
///
/// let provider = MemoryCdcOutputProvider::new();
/// let _stream = provider.start(CdcStartContext::new(None)).unwrap();
/// assert!(provider.delivered().is_empty());
/// ```
#[derive(Debug, Default, Clone)]
pub struct MemoryCdcOutputProvider {
    delivered: Arc<Mutex<Vec<CdcBatch>>>,
}

impl MemoryCdcOutputProvider {
    /// Construct a provider with an empty capture buffer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of every batch captured since the latest `start`.
    #[must_use]
    pub fn delivered(&self) -> Vec<CdcBatch> {
        self.delivered.lock().clone()
    }

    /// Number of batches captured since the latest `start`.
    #[must_use]
    pub fn delivered_len(&self) -> usize {
        self.delivered.lock().len()
    }
}

impl CdcOutputProvider for MemoryCdcOutputProvider {
    fn name(&self) -> &str {
        "memory"
    }
    fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        // Every stream begins empty at LSN 0.
        self.delivered.lock().clear();
        Ok(Box::new(MemoryCdcStream {
            delivered: Arc::clone(&self.delivered),
            checkpoint: CdcLsn(0),
        }))
    }
    /// `false`: `start` ignores `from_lsn` and every stream begins empty at
    /// LSN 0, so a persisted checkpoint is never read back.
    fn resumes_from_checkpoint(&self) -> bool {
        false
    }
}

/// In-memory CDC stream.
///
/// Captures every `deliver()` call into the buffer it shares with its
/// [`MemoryCdcOutputProvider`]. `checkpoint()` advances the high-water mark
/// to the max LSN seen; `shutdown()` clears the buffer.
#[derive(Debug)]
pub struct MemoryCdcStream {
    /// Every batch this stream has received, in delivery order. Shared
    /// with the provider that started the stream.
    pub delivered: Arc<Mutex<Vec<CdcBatch>>>,
    /// Most-recent acknowledged LSN.
    pub checkpoint: CdcLsn,
}

impl CdcStream for MemoryCdcStream {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
        self.delivered.lock().push(batch.clone());
        Ok(())
    }
    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        // Advance to the highest lsn_end we've delivered.
        if let Some(b) = self.delivered.lock().last() {
            self.checkpoint = b.lsn_end;
        }
        Ok(self.checkpoint)
    }
    fn shutdown(&mut self) -> Result<(), FnError> {
        self.delivered.lock().clear();
        Ok(())
    }
}

/// Opt-in plugin that registers the in-memory `memory` CDC output.
///
/// Add it with `Uni::add_plugin(MemoryCdcPlugin::new())` (or
/// [`MemoryCdcPlugin::with_provider`] to keep a handle on the captured
/// batches). It is a separate plugin rather than part of
/// [`crate::BuiltinPlugin`] because a registered CDC provider receives
/// every commit and this one retains them all.
///
/// # Examples
///
/// ```
/// use uni_plugin::Plugin;
/// use uni_plugin_builtin::extras::{MemoryCdcOutputProvider, MemoryCdcPlugin};
///
/// let provider = MemoryCdcOutputProvider::new();
/// let plugin = MemoryCdcPlugin::with_provider(provider.clone());
/// assert_eq!(plugin.manifest().id.as_str(), MemoryCdcPlugin::ID);
/// ```
#[derive(Debug)]
pub struct MemoryCdcPlugin {
    provider: MemoryCdcOutputProvider,
    manifest: OnceLock<PluginManifest>,
}

impl MemoryCdcPlugin {
    /// Reserved plugin id.
    pub const ID: &'static str = "memory-cdc";

    /// Construct the plugin with a fresh provider.
    #[must_use]
    pub fn new() -> Self {
        Self::with_provider(MemoryCdcOutputProvider::new())
    }

    /// Construct the plugin around `provider`; keep a clone of it to read
    /// the captured batches.
    #[must_use]
    pub fn with_provider(provider: MemoryCdcOutputProvider) -> Self {
        Self {
            provider,
            manifest: OnceLock::new(),
        }
    }

    fn manifest_value() -> PluginManifest {
        PluginManifest {
            id: PluginId::new(Self::ID),
            version: env!("CARGO_PKG_VERSION")
                .parse::<Version>()
                .unwrap_or_else(|_| Version::new(0, 0, 0)),
            abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
            depends_on: vec![],
            capabilities: CapabilitySet::from_iter_of([Capability::Cdc]),
            determinism: Determinism::Pure,
            side_effects: SideEffects::ReadOnly,
            scope: Scope::Instance,
            hash: None,
            signature: None,
            provides: ProvidedSurfaces::default(),
            docs: "In-memory CDC output that captures every delivered batch (unbounded; opt-in)."
                .to_owned(),
            metadata: std::collections::BTreeMap::new(),
        }
    }
}

impl Default for MemoryCdcPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for MemoryCdcPlugin {
    fn manifest(&self) -> &PluginManifest {
        self.manifest.get_or_init(Self::manifest_value)
    }

    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
        register_memory_cdc(r, self.provider.clone())
    }
}

// =========================================================================
// CatalogProvider — empty reference impl
// =========================================================================

/// `CatalogProvider` reference impl that exposes an empty catalog. Real
/// catalogs (e.g., federated Postgres tables, HuggingFace datasets)
/// follow the same shape but populate `list_labels` / `resolve_label`
/// from external sources.
#[derive(Debug)]
pub struct EmptyCatalog {
    name: String,
}

impl EmptyCatalog {
    /// Construct an empty catalog with the given name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl CatalogProvider for EmptyCatalog {
    fn name(&self) -> &str {
        &self.name
    }
    fn list_labels(&self) -> Result<Vec<CatalogLabel>, FnError> {
        Ok(vec![])
    }
    fn list_edge_types(&self) -> Result<Vec<CatalogEdgeType>, FnError> {
        Ok(vec![])
    }
    fn resolve_label(&self, _label: &str) -> Option<Arc<dyn CatalogTable>> {
        None
    }
    fn resolve_edge_type(&self, _edge: &str) -> Option<Arc<dyn CatalogTable>> {
        None
    }
}

// =========================================================================
// ReplacementScanProvider — never-replace reference impl
// =========================================================================

/// `ReplacementScanProvider` that never replaces — every unknown
/// identifier passes through to the standard "unknown" error. Useful
/// as the conservative default; real replacement scans (DuckDB-style)
/// route unknown labels to a catalog or external table function.
#[derive(Debug)]
pub struct NeverReplacementScan;

impl ReplacementScanProvider for NeverReplacementScan {
    fn replace(&self, _request: &ReplacementRequest<'_>) -> Option<Replacement> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::SystemTime;

    fn make_cdc_batch(start: u64, end: u64) -> CdcBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "event_kind",
            DataType::Int64,
            false,
        )]));
        let arr: Arc<dyn arrow_array::Array> =
            Arc::new(Int64Array::from(vec![1_i64; (end - start) as usize]));
        let batch = arrow_array::RecordBatch::try_new(schema, vec![arr]).unwrap();
        CdcBatch {
            lsn_start: CdcLsn(start),
            lsn_end: CdcLsn(end),
            mutations: Arc::new(batch),
            commit_timestamp: SystemTime::now(),
        }
    }

    #[test]
    fn memory_cdc_captures_delivered_batches() {
        let provider = MemoryCdcOutputProvider::new();
        let ctx = CdcStartContext::new(None);
        let mut stream = provider.start(ctx).unwrap();
        stream.deliver(&make_cdc_batch(0, 3)).unwrap();
        stream.deliver(&make_cdc_batch(3, 7)).unwrap();
        let ack = stream.checkpoint().unwrap();
        assert_eq!(ack, CdcLsn(7));
        // The provider handle observes what its stream captured.
        let seen = provider.delivered();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[1].lsn_end, CdcLsn(7));
        // shutdown clears.
        stream.shutdown().unwrap();
        assert_eq!(provider.delivered_len(), 0);
        // Re-checkpoint after shutdown returns the last ack value (no
        // new batches to advance).
        let ack2 = stream.checkpoint().unwrap();
        assert_eq!(ack2, CdcLsn(7));
    }

    /// Registers `plugin` into a fresh registry, as `Uni::add_plugin` does.
    fn registry_with(plugin: &dyn Plugin) -> uni_plugin::PluginRegistry {
        let registry = uni_plugin::PluginRegistry::new();
        let m = plugin.manifest();
        let mut r = PluginRegistrar::new(m.id.clone(), &m.capabilities, &registry);
        plugin.register(&mut r).unwrap();
        r.commit_to_registry().unwrap();
        registry
    }

    /// W2: the CDC runtime starts a stream for every registered provider,
    /// so the default built-in registration must register none — the
    /// in-memory provider retains every commit it is handed.
    #[test]
    fn builtin_plugin_registers_no_cdc_output_by_default() {
        let registry = registry_with(&crate::BuiltinPlugin::new());
        let names: Vec<String> = registry
            .cdc_outputs_snapshot()
            .into_iter()
            .map(|(k, _)| k.to_string())
            .collect();
        assert!(
            names.is_empty(),
            "default registration must not start any CDC stream, got {names:?}"
        );
        assert!(registry.cdc_outputs_is_empty());
    }

    #[test]
    fn memory_cdc_plugin_registers_the_memory_output() {
        let registry = registry_with(&MemoryCdcPlugin::new());
        let names: Vec<String> = registry
            .cdc_outputs_snapshot()
            .into_iter()
            .map(|(k, _)| k.to_string())
            .collect();
        assert_eq!(names, vec!["memory".to_owned()]);
    }

    #[test]
    fn memory_cdc_start_resets_the_shared_buffer() {
        let provider = MemoryCdcOutputProvider::new();
        let mut first = provider.start(CdcStartContext::new(None)).unwrap();
        first.deliver(&make_cdc_batch(0, 1)).unwrap();
        assert_eq!(provider.delivered_len(), 1);
        let _second = provider.start(CdcStartContext::new(None)).unwrap();
        assert_eq!(provider.delivered_len(), 0);
    }

    #[test]
    fn empty_catalog_returns_empty_lists() {
        let c = EmptyCatalog::new("test-catalog");
        assert_eq!(c.name(), "test-catalog");
        assert!(c.list_labels().unwrap().is_empty());
        assert!(c.list_edge_types().unwrap().is_empty());
        assert!(c.resolve_label("anything").is_none());
        assert!(c.resolve_edge_type("anything").is_none());
    }

    #[test]
    fn never_replacement_scan_returns_none() {
        let r = NeverReplacementScan;
        let qname = uni_plugin::QName::builtin("anything");
        assert!(r.replace(&ReplacementRequest::Label("Foo")).is_none());
        assert!(r.replace(&ReplacementRequest::Procedure(&qname)).is_none());
        assert!(r.replace(&ReplacementRequest::Function(&qname)).is_none());
    }
}
