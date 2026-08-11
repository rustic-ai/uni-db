// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Async Python API — `AsyncDatabase`, `AsyncTransaction`, `AsyncSession`,
//! `AsyncBulkWriter`.

use crate::convert;
use crate::core::{self, OpenMode};
use crate::types::*;
use ::uni_db::Uni;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Borrow the active transaction out of a locked guard, raising the standard
/// "transaction already completed" error when it has been consumed.
fn active_tx<'a>(
    guard: &'a tokio::sync::MutexGuard<'_, Option<::uni_db::Transaction>>,
) -> PyResult<&'a ::uni_db::Transaction> {
    guard.as_ref().ok_or_else(|| {
        crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
            "Transaction already completed",
        )
    })
}

// ============================================================================
// AsyncRuleRegistry
// ============================================================================

/// Async, durable facade for the database-level Locy rule registry.
///
/// Mutating methods return awaitables because they persist registered rule
/// sources to `catalog/locy_rules.json`. Read-only methods are synchronous.
#[pyclass(name = "AsyncRuleRegistry")]
pub struct AsyncRuleRegistry {
    pub(crate) registry: Arc<std::sync::RwLock<::uni_db::LocyRuleRegistry>>,
    pub(crate) persister: Option<Arc<::uni_db::LocyRulePersister>>,
}

#[pymethods]
impl AsyncRuleRegistry {
    /// Register Locy rules from a program string (returns awaitable).
    fn register<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let registry = self.registry.clone();
        let persister = self.persister.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let facade = match &persister {
                Some(persister) => ::uni_db::RuleRegistry::with_persister(&registry, persister),
                None => ::uni_db::RuleRegistry::new(&registry),
            };
            facade
                .register(&program)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Remove a rule by name (returns awaitable resolving to a bool).
    fn remove<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let registry = self.registry.clone();
        let persister = self.persister.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let facade = match &persister {
                Some(persister) => ::uni_db::RuleRegistry::with_persister(&registry, persister),
                None => ::uni_db::RuleRegistry::new(&registry),
            };
            facade
                .remove(&name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Clear all registered rules (returns awaitable).
    fn clear<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let registry = self.registry.clone();
        let persister = self.persister.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let facade = match &persister {
                Some(persister) => ::uni_db::RuleRegistry::with_persister(&registry, persister),
                None => ::uni_db::RuleRegistry::new(&registry),
            };
            facade
                .clear()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List names of all registered rules.
    fn list(&self) -> Vec<String> {
        ::uni_db::RuleRegistry::new(&self.registry).list()
    }

    /// Get metadata about a registered rule.
    fn get(&self, name: &str) -> Option<crate::types::PyRuleInfo> {
        ::uni_db::RuleRegistry::new(&self.registry)
            .get(name)
            .map(|info| crate::types::PyRuleInfo {
                name: info.name,
                clause_count: info.clause_count,
                is_recursive: info.is_recursive,
            })
    }

    /// Get the number of registered rules.
    fn count(&self) -> usize {
        ::uni_db::RuleRegistry::new(&self.registry).count()
    }
}

// ============================================================================
// AsyncDatabase
// ============================================================================

/// Async entry point for the Uni embedded graph database.
#[pyclass(name = "AsyncUni")]
pub struct AsyncDatabase {
    inner: Arc<Uni>,
}

#[pymethods]
impl AsyncDatabase {
    // ========================================================================
    // Static Factory Methods
    // ========================================================================

    /// Open or create a database at the given path.
    #[staticmethod]
    fn open<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::open(&path)
                .build()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Create a temporary in-memory database.
    #[staticmethod]
    fn temporary<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::temporary()
                .build()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Self::temporary(py)
    }

    /// Create a new database. Fails if it already exists.
    #[staticmethod]
    fn create<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::create(&path)
                .build()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Open an existing database. Fails if it does not exist.
    #[staticmethod]
    fn open_existing<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::open_existing(&path)
                .build()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }

    /// Return an AsyncDatabaseBuilder for advanced configuration.
    #[staticmethod]
    fn builder() -> AsyncDatabaseBuilder {
        AsyncDatabaseBuilder::default()
    }

    // ========================================================================
    // Plugin Loading (async, instance-scoped — mirrors sync `Uni`)
    // ========================================================================

    /// Async `await db.load_wasm_component(wasm_bytes, grants=None)`.
    ///
    /// Same contract as the sync [`Uni.load_wasm_component`]; returned as a
    /// Python awaitable so an asyncio app never blocks the event loop on
    /// wasmtime instantiation.
    #[cfg(feature = "wasm-plugins")]
    #[pyo3(signature = (wasm_bytes, grants=None))]
    fn load_wasm_component<'py>(
        &self,
        py: Python<'py>,
        wasm_bytes: Vec<u8>,
        grants: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let cap_set = crate::builders::build_capability_set(grants);
            // Wire a fresh GraphCompute registry so guest `algorithm` entries can
            // register; harmless for scalar-only plugins.
            let loader = uni_plugin_wasm::WasmLoader::new().with_graph(std::sync::Arc::new(
                uni_plugin_builtin::algorithms::graph_compute::GraphComputeRegistry::new(),
            ));
            let outcome = inner
                .load_wasm_component(&loader, &wasm_bytes, &cap_set, &cap_set)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                crate::sync_api::wasm_outcome_to_pydict(
                    py,
                    outcome.plugin_id,
                    outcome.version,
                    outcome.scalars_registered,
                    outcome.aggregates_registered,
                    outcome.procedures_registered,
                    outcome.algorithms_registered,
                    outcome.effective_capabilities,
                    outcome.denied_capabilities,
                )
            })
        })
    }

    /// Async `await db.load_wasm_extism(wasm_bytes, grants=None)`.
    ///
    /// Async counterpart of the sync [`Uni.load_wasm_extism`]; see
    /// [`Self::load_wasm_component`] for the contract.
    #[cfg(feature = "extism-plugins")]
    #[pyo3(signature = (wasm_bytes, grants=None))]
    fn load_wasm_extism<'py>(
        &self,
        py: Python<'py>,
        wasm_bytes: Vec<u8>,
        grants: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let cap_set = crate::builders::build_capability_set(grants);
            let mut loader = uni_plugin_extism::ExtismLoader::new();
            uni_plugin_extism::register_default_host_svc(&mut loader);
            // Wire a fresh GraphCompute registry so guest `algorithm` entries can
            // register; harmless for scalar-only plugins.
            let loader = loader.with_graph(std::sync::Arc::new(
                uni_plugin_builtin::algorithms::graph_compute::GraphComputeRegistry::new(),
            ));
            let outcome = inner
                .load_wasm_extism(&loader, &wasm_bytes, &cap_set, &cap_set)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                crate::sync_api::wasm_outcome_to_pydict(
                    py,
                    outcome.plugin_id,
                    outcome.version,
                    outcome.scalars_registered,
                    outcome.aggregates_registered,
                    outcome.procedures_registered,
                    outcome.algorithms_registered,
                    outcome.effective_capabilities,
                    outcome.denied_capabilities,
                )
            })
        })
    }

    /// Async `await db.load_rhai_plugin(script, grants=None)`.
    ///
    /// Async counterpart of the sync [`Uni.load_rhai_plugin`]. Loads a Rhai
    /// script plugin and registers its scalar / aggregate / procedure surfaces
    /// on this instance. `grants` uses the same capability names as the other
    /// loaders, defaulting to `ScalarFn` / `AggregateFn` / `Procedure`. The
    /// resolved future is a dict with `plugin_id`, `version`,
    /// `scalars_registered`, `aggregates_registered`, `procedures_registered`,
    /// and `denied_capabilities`.
    #[pyo3(signature = (script, grants=None))]
    fn load_rhai_plugin<'py>(
        &self,
        py: Python<'py>,
        script: String,
        grants: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Validate grants up front so an unknown grant raises immediately,
        // matching the sync `Uni.load_rhai_plugin`.
        let cap_set = crate::builders::build_capability_set_strict(grants)?;
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut loader = uni_plugin_rhai::RhaiLoader::new();
            uni_plugin_rhai::host_fn_impls::register_default_host_fns(&mut loader);
            // `load_rhai_plugin` is synchronous (script parsing only, no I/O),
            // so it runs inline in the future without `spawn_blocking`.
            let outcome = inner
                .load_rhai_plugin(&loader, &script, &cap_set)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("plugin_id", outcome.plugin_id.as_str())?;
                dict.set_item("version", outcome.version)?;
                dict.set_item("scalars_registered", outcome.scalars_registered)?;
                dict.set_item("aggregates_registered", outcome.aggregates_registered)?;
                dict.set_item("procedures_registered", outcome.procedures_registered)?;
                dict.set_item("algorithms_registered", outcome.algorithms_registered)?;
                let denied: Vec<String> = outcome
                    .denied_capabilities
                    .iter()
                    .map(|c| format!("{c:?}"))
                    .collect();
                dict.set_item("denied_capabilities", denied)?;
                Ok(dict.into_any().unbind())
            })
        })
    }

    // ========================================================================
    // Context Manager
    // ========================================================================

    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Shutdown on exit — really, not just a flush. The sync facade's
        // `__exit__` delegates to `shutdown`; this one open-coded a flush, so
        // `async with AsyncUni.in_memory() as db:` left the background tasks
        // running and stranded the scratch directory on exit. That is the
        // idiomatic path, so it leaked more often than the explicit call did.
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = inner.shutdown_in_place().await;
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        "AsyncUni(open)".to_string()
    }

    /// The filesystem path or URI backing this database.
    ///
    /// For a `temporary()` / `in_memory()` database this is the `uni_mem_*`
    /// scratch directory. Exposed so a caller who never reaches the
    /// context-manager teardown can still account for the directory — without
    /// it there is no way to discover the path short of globbing `$TMPDIR`,
    /// which is unsafe when another process owns one.
    ///
    /// Synchronous, like the Rust getter: reading a path needs no runtime, and
    /// making it awaitable would only obscure that.
    #[getter]
    fn uri(&self) -> String {
        self.inner.uri().to_string()
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::flush_core(&inner)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    // ========================================================================
    // Schema Methods
    // ========================================================================

    /// Check if a label exists.
    fn label_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::label_exists_core(&inner, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Check if an edge type exists.
    fn edge_type_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::edge_type_exists_core(&inner, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List all label names.
    fn list_labels<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_labels_core(&inner)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List all edge type names.
    fn list_edge_types<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_edge_types_core(&inner)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Get detailed information about a label.
    fn get_label_info<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = core::get_label_info_core(&inner, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;

            Ok(info.map(LabelInfo::from))
        })
    }

    /// Get detailed information about an edge type.
    fn get_edge_type_info<'py>(
        &self,
        py: Python<'py>,
        name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = core::get_edge_type_info_core(&inner, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;

            Ok(info.map(crate::types::EdgeTypeInfo::from))
        })
    }

    /// Load schema from a JSON file.
    fn load_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::load_schema_core(&inner, &path)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Save schema to a JSON file.
    fn save_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::save_schema_core(&inner, &path)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create a new async session wrapping a real Rust Session.
    fn session(&self) -> AsyncSession {
        let session = self.inner.session();
        let rule_reg = session.rules().clone_registry_arc();
        let params_arc = session.params().clone_store_arc();
        AsyncSession {
            inner: Arc::new(tokio::sync::Mutex::new(session)),
            rule_registry_arc: rule_reg,
            params_arc,
            pending_plugin_builder: uni_plugin_pyo3::ManifestBuilder::new(),
        }
    }

    /// Create a session template builder for async sessions.
    fn session_template(&self) -> AsyncSessionTemplateBuilder {
        AsyncSessionTemplateBuilder {
            inner: Some(self.inner.session_template()),
        }
    }

    /// Create a schema builder.
    fn schema(&self) -> AsyncSchemaBuilder {
        AsyncSchemaBuilder {
            inner: self.inner.clone(),
            pending_labels: Vec::new(),
            pending_edge_types: Vec::new(),
            pending_properties: Vec::new(),
            pending_indexes: Vec::new(),
        }
    }

    /// Get an AsyncXervo facade for embedding and generation operations.
    fn xervo(&self) -> PyResult<AsyncXervo> {
        Ok(AsyncXervo {
            inner: self.inner.clone(),
        })
    }

    /// Access the durable database-level rule registry.
    ///
    /// Mutating methods return awaitables because they persist to
    /// `catalog/locy_rules.json`; rules survive restarts.
    fn rules(&self) -> AsyncRuleRegistry {
        AsyncRuleRegistry {
            registry: self.inner.rules().clone_registry_arc(),
            persister: self.inner.rules().clone_persister_arc(),
        }
    }

    /// Flush data and prepare for shutdown.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Same correction as the sync facade: flushing is not shutting down.
            db.shutdown_in_place()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Get database-wide metrics.
    fn metrics(&self, py: Python) -> PyResult<crate::types::PyDatabaseMetrics> {
        let m = self.inner.metrics();
        convert::database_metrics_to_py(py, m)
    }

    /// Get the current database configuration as a dict.
    fn config(&self, py: Python) -> PyResult<Py<pyo3::PyAny>> {
        convert::uni_config_to_py(py, self.inner.config())
    }

    /// Get the configured write lease, if any.
    fn write_lease(&self) -> Option<crate::types::PyWriteLease> {
        self.inner.write_lease().map(convert::write_lease_to_py)
    }

    // -----------------------------------------------------------------------
    // Snapshot management
    // -----------------------------------------------------------------------

    /// Create a point-in-time snapshot. Returns the snapshot ID.
    fn create_snapshot<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_snapshot_core(&db, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List all available snapshots.
    fn list_snapshots<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let manifests = core::list_snapshots_core(&db)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                manifests
                    .into_iter()
                    .map(|m| convert::snapshot_manifest_to_py(py, m))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Restore the database to a specific snapshot.
    fn restore_snapshot<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::restore_snapshot_core(&db, &snapshot_id)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    // -----------------------------------------------------------------------
    // Fork management (Phase 4b)
    // -----------------------------------------------------------------------

    /// List all currently-Active forks across the database.
    fn list_forks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let forks = db.list_forks().await;
            Ok(forks
                .into_iter()
                .map(crate::types::PyForkInfo::from_rust)
                .collect::<Vec<_>>())
        })
    }

    /// Look up a single fork by name. Returns `None` if absent.
    fn fork_info<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match db.fork_info(&name).await {
                Ok(info) => Ok(Some(crate::types::PyForkInfo::from_rust(info))),
                Err(uni_common::UniError::ForkNotFound { .. }) => Ok(None),
                Err(e) => Err(crate::exceptions::uni_error_to_pyerr(e)),
            }
        })
    }

    /// Drop a fork.
    fn drop_fork<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.drop_fork(&name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Drop a fork and every descendant in its subtree.
    fn drop_fork_cascade<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.drop_fork_cascade(&name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Tag a fork with a Lance tag (GC-exempt; survives drop).
    fn tag_fork<'py>(
        &self,
        py: Python<'py>,
        fork_name: String,
        tag: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.tag_fork(&fork_name, &tag)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Remove a previously-applied tag from a fork. Idempotent per dataset.
    fn untag_fork<'py>(
        &self,
        py: Python<'py>,
        fork_name: String,
        tag: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.untag_fork(&fork_name, &tag)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// List the unique user-visible tag names applied to this fork.
    fn list_fork_tags<'py>(
        &self,
        py: Python<'py>,
        fork_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.list_fork_tags(&fork_name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Structural diff between primary and a named fork.
    fn diff_fork_primary<'py>(
        &self,
        py: Python<'py>,
        fork_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.diff_fork_primary(&fork_name)
                .await
                .map(crate::types::PyForkDiff::from_rust)
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Structural diff between two named forks.
    fn diff_forks<'py>(
        &self,
        py: Python<'py>,
        a: String,
        b: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.diff_forks(&a, &b)
                .await
                .map(crate::types::PyForkDiff::from_rust)
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Promote matched fork rows onto primary in one transaction.
    fn promote_from_fork<'py>(
        &self,
        py: Python<'py>,
        fork_name: String,
        patterns: Vec<crate::types::PyPromotePattern>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let rust_patterns: Vec<uni_db::PromotePattern> =
            patterns.into_iter().map(|p| p.inner).collect();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.promote_from_fork(&fork_name, &rust_patterns)
                .await
                .map(crate::types::PyPromoteReport::from_rust)
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Promote matched fork rows onto primary with explicit merge options.
    ///
    /// See the sync `promote_from_fork_with_options`. `options` is a
    /// `PromoteOptions`; default options reproduce `promote_from_fork`.
    fn promote_from_fork_with_options<'py>(
        &self,
        py: Python<'py>,
        fork_name: String,
        patterns: Vec<crate::types::PyPromotePattern>,
        options: crate::types::PyPromoteOptions,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let rust_patterns: Vec<uni_db::PromotePattern> =
            patterns.into_iter().map(|p| p.inner).collect();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            db.promote_from_fork_with_options(&fork_name, &rust_patterns, &options.inner)
                .await
                .map(crate::types::PyPromoteReport::from_rust)
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Access compaction operations.
    fn compaction(&self) -> AsyncCompaction {
        AsyncCompaction {
            inner: self.inner.clone(),
        }
    }

    /// Access index management operations.
    fn indexes(&self) -> AsyncIndexes {
        AsyncIndexes {
            inner: self.inner.clone(),
        }
    }
}

// ============================================================================
// AsyncXervo
// ============================================================================

/// Async facade for Uni-Xervo embedding and generation.
#[pyclass]
pub struct AsyncXervo {
    inner: Arc<Uni>,
}

#[pymethods]
impl AsyncXervo {
    /// Check if Xervo (embedding/generation) is available for this database.
    fn is_available(&self) -> bool {
        self.inner.xervo().is_available()
    }

    /// Return this database's Xervo runtime handle for sharing, if configured.
    ///
    /// Pass the result to another builder's ``xervo_runtime()`` to open a second
    /// database over the same loaded models instead of a second copy of the
    /// weights. Returns ``None`` exactly when :meth:`is_available` is ``False``.
    fn raw_runtime(&self) -> Option<crate::types::PyModelRuntime> {
        // `xervo()` returns a temporary and `raw_runtime()` borrows from it, so
        // the facade must outlive the borrow (E0716).
        let xervo = self.inner.xervo();
        xervo
            .raw_runtime()
            .cloned()
            .map(|inner| crate::types::PyModelRuntime { inner })
    }

    /// Embed texts using a configured model alias (async).
    fn embed<'py>(
        &self,
        py: Python<'py>,
        alias: String,
        texts: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::xervo_embed_core(&db, &alias, texts)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Generate text using structured messages (async).
    ///
    /// Each message may be a `Message` instance or a dict with `"role"` and `"content"` keys.
    #[pyo3(signature = (alias, messages, max_tokens=None, temperature=None, top_p=None))]
    fn generate<'py>(
        &self,
        py: Python<'py>,
        alias: String,
        messages: Vec<Py<PyAny>>,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        // Extract messages while the GIL is held, before entering the async block.
        let msg_pairs = crate::convert::extract_messages(py, messages)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result =
                core::xervo_generate_core(&db, &alias, msg_pairs, max_tokens, temperature, top_p)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| crate::convert::generation_result_to_py(py, result))
        })
    }

    /// Generate text from a single user prompt (async). Convenience wrapper around `generate()`.
    #[pyo3(signature = (alias, prompt, max_tokens=None, temperature=None, top_p=None))]
    fn generate_text<'py>(
        &self,
        py: Python<'py>,
        alias: String,
        prompt: String,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let msg_pairs = vec![("user".to_string(), prompt)];
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result =
                core::xervo_generate_core(&db, &alias, msg_pairs, max_tokens, temperature, top_p)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| crate::convert::generation_result_to_py(py, result))
        })
    }

    /// Pre-load and cache specific model aliases so first inference is instant.
    fn prefetch<'py>(&self, py: Python<'py>, aliases: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::xervo_prefetch_core(&db, aliases)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Pre-load and cache every model in the Xervo catalog.
    fn prefetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::xervo_prefetch_all_core(&db)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }
}

// ============================================================================
// AsyncCompaction
// ============================================================================

/// Async facade for compaction operations.
#[pyclass(name = "AsyncCompaction")]
pub struct AsyncCompaction {
    inner: Arc<Uni>,
}

#[pymethods]
impl AsyncCompaction {
    /// Compact data for a label or edge type.
    fn compact<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::compact_core(&db, &name)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Wait for all background compaction to complete.
    fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::wait_for_compaction_core(&db)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }
}

// ============================================================================
// AsyncIndexes
// ============================================================================

/// Async facade for index management operations.
#[pyclass(name = "AsyncIndexes")]
pub struct AsyncIndexes {
    inner: Arc<Uni>,
}

#[pymethods]
impl AsyncIndexes {
    /// List index definitions, optionally filtered by label.
    #[pyo3(signature = (label=None))]
    fn list(
        &self,
        py: Python,
        label: Option<String>,
    ) -> PyResult<Vec<crate::types::IndexDefinitionInfo>> {
        let indexes = match label.as_deref() {
            Some(l) => core::list_indexes_core(&self.inner, l),
            None => core::list_all_indexes_core(&self.inner),
        };
        indexes
            .into_iter()
            .map(|i| convert::index_definition_to_py(py, i))
            .collect()
    }

    /// Rebuild indexes for a label. If background=True, returns a task ID.
    #[pyo3(signature = (label, background=false))]
    fn rebuild<'py>(
        &self,
        py: Python<'py>,
        label: String,
        background: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::rebuild_indexes_core(&db, &label, background)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Get status of all rebuild tasks.
    fn rebuild_status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let tasks = core::index_rebuild_status_core(&db)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                tasks
                    .into_iter()
                    .map(|t| convert::index_rebuild_task_to_py(py, t))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Retry failed rebuild tasks. Returns task IDs scheduled for retry.
    fn retry_failed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::retry_index_rebuilds_core(&db)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }
}

// ============================================================================
// AsyncDatabaseBuilder
// ============================================================================

/// Async builder for creating and configuring an AsyncUni instance.
#[pyclass(name = "AsyncUniBuilder", from_py_object)]
#[derive(Debug, Clone)]
pub struct AsyncDatabaseBuilder {
    uri: String,
    mode: OpenMode,
    hybrid_local: Option<String>,
    hybrid_remote: Option<String>,
    cache_size: Option<usize>,
    parallelism: Option<usize>,
    schema_file: Option<String>,
    xervo_catalog_json: Option<String>,
    xervo_catalog_file: Option<String>,
    xervo_runtime: Option<crate::types::PyModelRuntime>,
    cloud_config: Option<uni_common::CloudStorageConfig>,
    uni_config: Option<uni_common::UniConfig>,
    read_only: bool,
    write_lease: Option<crate::types::PyWriteLease>,
    skip_invalid_locy_rules: bool,
}

impl Default for AsyncDatabaseBuilder {
    fn default() -> Self {
        Self {
            uri: String::new(),
            mode: OpenMode::Temporary,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
            schema_file: None,
            xervo_catalog_json: None,
            xervo_catalog_file: None,
            xervo_runtime: None,
            cloud_config: None,
            uni_config: None,
            read_only: false,
            write_lease: None,
            skip_invalid_locy_rules: false,
        }
    }
}

#[pymethods]
impl AsyncDatabaseBuilder {
    /// Open or create a database at the given path.
    #[staticmethod]
    fn open(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Open,
            ..Default::default()
        }
    }

    /// Open an existing database.
    #[staticmethod]
    fn open_existing(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::OpenExisting,
            ..Default::default()
        }
    }

    /// Create a new database.
    #[staticmethod]
    fn create(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Create,
            ..Default::default()
        }
    }

    /// Create a temporary database.
    #[staticmethod]
    fn temporary() -> Self {
        Self::default()
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory() -> Self {
        Self::temporary()
    }

    /// Configure hybrid storage.
    fn hybrid(
        mut slf: PyRefMut<'_, Self>,
        local_path: String,
        remote_url: String,
    ) -> PyRefMut<'_, Self> {
        slf.hybrid_local = Some(local_path);
        slf.hybrid_remote = Some(remote_url);
        slf
    }

    /// Set maximum cache size in bytes.
    fn cache_size(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.cache_size = Some(bytes);
        slf
    }

    /// Set query parallelism.
    fn parallelism(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.parallelism = Some(n);
        slf
    }

    /// Load schema from a JSON file on initialization.
    fn schema_file(mut slf: PyRefMut<'_, Self>, path: String) -> PyRefMut<'_, Self> {
        slf.schema_file = Some(path);
        slf
    }

    /// Configure the Xervo model catalog from a JSON string.
    fn xervo_catalog_from_str(mut slf: PyRefMut<'_, Self>, json: String) -> PyRefMut<'_, Self> {
        slf.xervo_catalog_json = Some(json);
        slf.xervo_catalog_file = None;
        slf.xervo_runtime = None;
        slf
    }

    /// Configure the Xervo model catalog from a JSON file path.
    fn xervo_catalog_from_file(mut slf: PyRefMut<'_, Self>, path: String) -> PyRefMut<'_, Self> {
        slf.xervo_catalog_file = Some(path);
        slf.xervo_catalog_json = None;
        slf.xervo_runtime = None;
        slf
    }

    /// Reuse an already-built Xervo runtime instead of building one from a catalog.
    ///
    /// Databases sharing one runtime share its loaded models, so the second and
    /// later ones cost no additional model memory. Supersedes any catalog set
    /// on this builder.
    fn xervo_runtime(
        mut slf: PyRefMut<'_, Self>,
        runtime: crate::types::PyModelRuntime,
    ) -> PyRefMut<'_, Self> {
        slf.xervo_runtime = Some(runtime);
        slf.xervo_catalog_json = None;
        slf.xervo_catalog_file = None;
        slf
    }

    /// Configure cloud storage credentials (dict with 'provider' key: 's3', 'gcs', or 'azure').
    fn cloud_config(
        mut slf: PyRefMut<'_, Self>,
        config: std::collections::HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.cloud_config = Some(crate::convert::extract_cloud_config(py, &config)?);
        Ok(slf)
    }

    /// Configure database options (query_timeout, max_query_memory, etc.).
    fn config(
        mut slf: PyRefMut<'_, Self>,
        config: std::collections::HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.uni_config = Some(crate::convert::extract_uni_config(py, &config)?);
        Ok(slf)
    }

    /// Set I/O batch size (default 1024).
    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .batch_size = size;
        slf
    }

    /// Enable or disable write-ahead log (default true).
    fn wal_enabled(mut slf: PyRefMut<'_, Self>, enabled: bool) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .wal_enabled = enabled;
        slf
    }

    /// Open the database in read-only mode (no writes allowed).
    fn read_only(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.read_only = true;
        slf
    }

    /// Skip persisted Locy rules that no longer compile, instead of failing.
    fn skip_invalid_locy_rules(mut slf: PyRefMut<'_, Self>, skip: bool) -> PyRefMut<'_, Self> {
        slf.skip_invalid_locy_rules = skip;
        slf
    }

    /// Configure write lease for multi-agent coordination.
    fn write_lease(
        mut slf: PyRefMut<'_, Self>,
        lease: crate::types::PyWriteLease,
    ) -> PyRefMut<'_, Self> {
        slf.write_lease = Some(lease);
        slf
    }

    /// Phase 4b — cap on total fork count (Active + Pending + Tombstoned).
    fn max_forks(mut slf: PyRefMut<'_, Self>, cap: Option<usize>) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .max_forks = cap;
        slf
    }

    /// Phase 4b — default TTL applied to forks created without one.
    fn fork_default_ttl<'py>(
        mut slf: PyRefMut<'py, Self>,
        ttl: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let bound = ttl.bind(py);
        let dur = if bound.is_none() {
            None
        } else {
            Some(crate::convert::py_timedelta_to_duration(bound)?)
        };
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .fork_default_ttl = dur;
        Ok(slf)
    }

    /// Phase 4b — TTL sweeper polling interval.
    fn fork_sweeper_interval<'py>(
        mut slf: PyRefMut<'py, Self>,
        interval: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let dur = crate::convert::py_timedelta_to_duration(interval.bind(py))?;
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .fork_sweeper_interval = dur;
        Ok(slf)
    }

    /// Phase 4b — disable the TTL sweeper. Tests should set `True`
    /// when they want deterministic control over fork lifetimes.
    fn disable_fork_sweeper(mut slf: PyRefMut<'_, Self>, disabled: bool) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .disable_fork_sweeper = disabled;
        slf
    }

    /// Phase 4b — strict-schema mode (also exposed via `config(dict)`).
    fn strict_schema(mut slf: PyRefMut<'_, Self>, enabled: bool) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .strict_schema = enabled;
        slf
    }

    /// Build and return the AsyncDatabase instance (returns awaitable).
    fn build<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let uri = self.uri.clone();
        let mode = self.mode;
        let hybrid_local = self.hybrid_local.clone();
        let hybrid_remote = self.hybrid_remote.clone();
        let cache_size = self.cache_size;
        let parallelism = self.parallelism;
        let schema_file = self.schema_file.clone();
        let xervo_catalog_json = self.xervo_catalog_json.clone();
        let xervo_catalog_file = self.xervo_catalog_file.clone();
        let cloud_config = self.cloud_config.clone();
        let uni_config = self.uni_config.clone();
        let read_only = self.read_only;
        let skip_invalid_locy_rules = self.skip_invalid_locy_rules;
        let xervo_runtime = self.xervo_runtime.as_ref().map(|rt| rt.inner.clone());
        let rust_write_lease = self.write_lease.as_ref().map(|wl| match &wl.variant {
            crate::types::WriteLeaseVariant::Local => ::uni_db::api::multi_agent::WriteLease::Local,
            crate::types::WriteLeaseVariant::DynamoDB { table } => {
                ::uni_db::api::multi_agent::WriteLease::DynamoDB {
                    table: table.clone(),
                }
            }
        });

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = core::build_database_core(
                &uri,
                mode,
                hybrid_local.as_deref(),
                hybrid_remote.as_deref(),
                cache_size,
                parallelism,
                schema_file.as_deref(),
                xervo_catalog_json.as_deref(),
                xervo_catalog_file.as_deref(),
                cloud_config,
                uni_config,
                read_only,
                rust_write_lease,
                skip_invalid_locy_rules,
                xervo_runtime,
            )
            .await
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

            Ok(AsyncDatabase {
                inner: Arc::new(uni),
            })
        })
    }
}

// ============================================================================
// AsyncTransaction
// ============================================================================

/// An async database transaction.
///
/// Wraps a Rust `Transaction` which provides ACID guarantees.
/// Use as an async context manager for automatic rollback on error.
#[pyclass]
pub struct AsyncTransaction {
    pub(crate) inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    pub(crate) rule_registry_arc: std::sync::Arc<std::sync::RwLock<::uni_db::LocyRuleRegistry>>,
    /// Cloned cancellation token, captured before the `Transaction` is moved
    /// into the `Arc<Mutex<..>>`, so `cancel()` can fire without taking the
    /// async lock (which may be held by the in-flight operation being cancelled).
    pub(crate) cancellation_token: tokio_util::sync::CancellationToken,
}

#[pymethods]
impl AsyncTransaction {
    /// Execute a query within this transaction.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params(py, params)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let result = if let Some(params) = rust_params {
                let mut builder = tx.query_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(&k, v);
                }
                builder
                    .fetch_all()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            } else {
                tx.query(&cypher)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            };
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Execute a mutation query within this transaction, returning an ExecuteResult.
    #[pyo3(signature = (cypher, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params(py, params)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let result = if let Some(params) = rust_params {
                let mut builder = tx.execute_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(&k, v);
                }
                builder
                    .run()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            } else {
                tx.execute(&cypher)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            };
            Python::attach(|py| {
                let py_result = convert::execute_result_to_py(py, result)?;
                Ok(py_result.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Evaluate a Locy program within this transaction.
    #[pyo3(signature = (program, params=None))]
    fn locy<'py>(
        &self,
        py: Python<'py>,
        program: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params(py, params)?;
        let inner = self.inner.clone();
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let tx = active_tx(&guard)?;
                    if let Some(params) = rust_params {
                        let mut builder = tx.locy_with(&program);
                        for (k, v) in params {
                            builder = builder.param(&k, v);
                        }
                        builder
                            .run()
                            .await
                            .map_err(crate::exceptions::uni_error_to_pyerr)
                    } else {
                        tx.locy(&program)
                            .await
                            .map_err(crate::exceptions::uni_error_to_pyerr)
                    }
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }

    /// Apply a DerivedFactSet to this transaction.
    ///
    /// Freshness is required by default (`require_fresh=True`): a commit
    /// between DERIVE evaluation and apply raises a stale-derived-facts
    /// error. Pass `require_fresh=False` to apply regardless of staleness,
    /// or `max_version_gap=n` to bound the allowed gap.
    #[pyo3(signature = (derived, require_fresh=true, max_version_gap=None))]
    fn apply<'py>(
        &self,
        py: Python<'py>,
        derived: &mut crate::types::PyDerivedFactSet,
        require_fresh: bool,
        max_version_gap: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dfs = derived.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let result = {
                let mut builder = tx.apply_with(dfs);
                // `require_fresh=False` without a gap bound is the explicit
                // stale opt-out (the pre-2.0.7 default behavior).
                if !require_fresh && max_version_gap.is_none() {
                    builder = builder.allow_stale();
                }
                if let Some(gap) = max_version_gap {
                    builder = builder.max_version_gap(gap);
                }
                builder.run().await
            }
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyApplyResult {
                facts_applied: result.facts_applied,
                version_gap: result.version_gap,
            })
        })
    }

    /// Prepare a Cypher query for repeated execution within this transaction.
    fn prepare<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let prepared = tx
                .prepare(&cypher)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedQuery {
                inner: std::sync::Arc::new(prepared),
            })
        })
    }

    /// Prepare a Locy program for repeated execution within this transaction.
    fn prepare_locy<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let prepared = tx
                .prepare_locy(&program)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedLocy {
                inner: std::sync::Arc::new(prepared),
            })
        })
    }

    /// Access the transaction-scoped rule registry.
    fn rules(&self) -> crate::sync_api::PyRuleRegistry {
        crate::sync_api::PyRuleRegistry {
            registry: self.rule_registry_arc.clone(),
            // Session/transaction-scoped rules are ephemeral.
            persister: None,
        }
    }

    /// Cancel in-progress operations on this transaction.
    ///
    /// Fires the cancellation token directly without taking the async lock,
    /// which the in-flight operation being cancelled may still be holding.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let token = self.cancellation_token.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            token.cancel();
            Ok(())
        })
    }

    /// Commit the transaction, returning a CommitResult.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let tx = guard.take().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            let result = tx
                .commit()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                Ok(crate::types::PyCommitResult::from(result)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind())
            })
        })
    }

    /// Rollback the transaction.
    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let tx = guard.take().ok_or_else(|| {
                crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                    "Transaction already completed",
                )
            })?;
            tx.rollback();
            Ok(())
        })
    }

    /// Get the transaction ID.
    fn id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            Ok(tx.id().to_string())
        })
    }

    /// Database version when this transaction was started.
    fn started_at_version<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            Ok(tx.started_at_version())
        })
    }

    /// Check if the transaction has uncommitted changes.
    fn is_dirty<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            Ok(tx.is_dirty())
        })
    }

    /// Check if the transaction has been completed (committed or rolled back).
    fn is_completed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.is_none())
        })
    }

    // ── Builder factories (per-operation timeout, cancellation, etc.) ──

    /// Create a query builder for this transaction.
    fn query_with(&self, cypher: &str) -> AsyncTxQueryBuilder {
        AsyncTxQueryBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            cancellation_token: None,
        }
    }

    /// Create a mutation builder for this transaction.
    fn execute_with(&self, cypher: &str) -> AsyncTxExecuteBuilder {
        AsyncTxExecuteBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
        }
    }

    /// Create a Locy evaluation builder for this transaction.
    fn locy_with(&self, program: &str) -> AsyncTxLocyBuilder {
        AsyncTxLocyBuilder {
            inner: self.inner.clone(),
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            locy_config: None,
            cancellation_token: None,
        }
    }

    /// Create an apply builder with staleness controls.
    fn apply_with(
        &self,
        py: Python<'_>,
        derived: Py<crate::types::PyDerivedFactSet>,
    ) -> PyResult<AsyncApplyBuilder> {
        let mut dfs_ref = derived.borrow_mut(py);
        let dfs = dfs_ref.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        Ok(AsyncApplyBuilder {
            inner: self.inner.clone(),
            derived: Some(dfs),
            allow_stale: false,
            max_version_gap: None,
        })
    }

    /// Get the transaction's cancellation token.
    ///
    /// Returns the cloned token directly without taking the async lock.
    fn cancellation_token<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let token = self.cancellation_token.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            Ok(crate::types::PyCancellationToken { inner: token })
        })
    }

    /// Create a bulk writer builder for high-throughput data ingestion.
    fn bulk_writer(&self) -> AsyncTxBulkWriterBuilder {
        AsyncTxBulkWriterBuilder {
            tx: self.inner.clone(),
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: None,
            async_indexes: false,
            validate_constraints: None,
            max_buffer_size_bytes: None,
            on_progress: None,
        }
    }

    /// Create a streaming appender for the given label (returns awaitable).
    fn appender<'py>(&self, py: Python<'py>, label: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let builder = tx.appender(&label);
            let appender = builder
                .build()
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncStreamingAppender {
                inner: Arc::new(std::sync::Mutex::new(Some(appender))),
            })
        })
    }

    /// Create a configurable streaming-appender builder for the given label.
    fn appender_builder(&self, label: String) -> AsyncTxAppenderBuilder {
        AsyncTxAppenderBuilder {
            tx: self.inner.clone(),
            label,
            batch_size: None,
            defer_vector_indexes: None,
            max_buffer_size_bytes: None,
        }
    }

    /// Async context manager support.
    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Async context manager exit — rolls back if still active.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            if let Some(tx) = guard.take() {
                // Transaction still active — roll it back.
                // The Rust Drop impl will also auto-rollback, but explicit is clearer.
                tx.rollback();
            }
            Ok(false) // Don't suppress exceptions
        })
    }
}

// ============================================================================
// AsyncTransaction Bulk Writer Builder
// ============================================================================

/// Builder for async bulk writer within a transaction.
#[pyclass(name = "AsyncTxBulkWriterBuilder")]
pub struct AsyncTxBulkWriterBuilder {
    tx: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    defer_vector_indexes: bool,
    defer_scalar_indexes: bool,
    batch_size: Option<usize>,
    async_indexes: bool,
    validate_constraints: Option<bool>,
    max_buffer_size_bytes: Option<usize>,
    on_progress: Option<Py<PyAny>>,
}

#[pymethods]
impl AsyncTxBulkWriterBuilder {
    fn defer_vector_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_vector_indexes = defer;
        slf
    }

    fn defer_scalar_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_scalar_indexes = defer;
        slf
    }

    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = Some(size);
        slf
    }

    fn async_indexes(mut slf: PyRefMut<'_, Self>, async_: bool) -> PyRefMut<'_, Self> {
        slf.async_indexes = async_;
        slf
    }

    fn validate_constraints(mut slf: PyRefMut<'_, Self>, validate: bool) -> PyRefMut<'_, Self> {
        slf.validate_constraints = Some(validate);
        slf
    }

    fn max_buffer_size_bytes(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.max_buffer_size_bytes = Some(size);
        slf
    }

    fn on_progress(mut slf: PyRefMut<'_, Self>, callback: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.on_progress = Some(callback);
        slf
    }

    fn build<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let defer_vector = self.defer_vector_indexes;
        let defer_scalar = self.defer_scalar_indexes;
        let batch_size = self.batch_size;
        let async_indexes = self.async_indexes;
        let validate_constraints = self.validate_constraints;
        let max_buffer_size_bytes = self.max_buffer_size_bytes;
        let on_progress = self.on_progress.as_ref().map(|cb| cb.clone_ref(py));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = tx.lock().await;
            let tx_ref = active_tx(&guard)?;
            let mut builder = tx_ref
                .bulk_writer()
                .defer_vector_indexes(defer_vector)
                .defer_scalar_indexes(defer_scalar)
                .async_indexes(async_indexes);
            crate::apply_opt!(builder, batch_size, batch_size);
            crate::apply_opt!(builder, validate_constraints, validate_constraints);
            crate::apply_opt!(builder, max_buffer_size_bytes, max_buffer_size_bytes);
            if let Some(callback) = on_progress {
                builder = builder.on_progress(convert::make_progress_callback(callback));
            }
            let real_writer = builder
                .build()
                .map_err(crate::exceptions::anyhow_to_pyerr)?;
            Ok(AsyncBulkWriter {
                inner: Arc::new(std::sync::Mutex::new(Some(real_writer))),
            })
        })
    }
}

// ============================================================================
// AsyncStreamingAppender
// ============================================================================

/// Async streaming appender for single-label data loading.
///
/// Async counterpart of the sync
/// [`StreamingAppender`](crate::builders::StreamingAppender): `append`,
/// `write_batch`, `finish`, and `abort` return awaitables. The core appender
/// requires owned/`&mut` access and is held in `Arc<std::sync::Mutex<Option<..>>>`,
/// so its async work runs via `spawn_blocking` + `block_on` to avoid blocking the
/// event loop. Use as an async context manager for auto-abort on error.
#[pyclass]
pub struct AsyncStreamingAppender {
    inner: Arc<std::sync::Mutex<Option<::uni_db::StreamingAppender>>>,
}

#[pymethods]
impl AsyncStreamingAppender {
    /// Append a single row of properties (returns awaitable).
    fn append<'py>(
        &self,
        py: Python<'py>,
        properties: HashMap<String, Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Convert Python values under the GIL before entering the future.
        let mut rust_props = HashMap::new();
        for (k, v) in properties {
            rust_props.insert(k, convert::py_object_to_value(py, &v)?);
        }
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let appender = guard.as_mut().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(appender.append(rust_props))
                    .map_err(crate::exceptions::uni_error_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(())
        })
    }

    /// Write an Arrow RecordBatch of rows (returns awaitable).
    ///
    /// Accepts a PyArrow RecordBatch via the Arrow PyCapsule C Data Interface
    /// (`__arrow_c_array__`) for zero-copy transfer.
    fn write_batch<'py>(
        &self,
        py: Python<'py>,
        batch: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Marshal the PyArrow batch into an Arrow `RecordBatch` under the GIL,
        // before moving the (Send) batch into the blocking task.
        let record_batch = crate::builders::record_batch_from_pyarrow(batch)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let appender = guard.as_mut().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(appender.write_batch(&record_batch))
                    .map_err(crate::exceptions::uni_error_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(())
        })
    }

    /// Flush remaining rows and commit (returns awaitable).
    fn finish<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let appender = guard.take().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(appender.finish())
                    .map_err(crate::exceptions::uni_error_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(BulkStats {
                vertices_inserted: stats.vertices_inserted,
                edges_inserted: stats.edges_inserted,
                indexes_rebuilt: stats.indexes_rebuilt,
                duration_secs: stats.duration.as_secs_f64(),
                index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
                index_task_ids: stats.index_task_ids.clone(),
                indexes_pending: stats.indexes_pending,
            })
        })
    }

    /// Abort without committing (returns awaitable).
    fn abort<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Take the appender out under the lock, then drop the guard before
            // awaiting the async rollback so the non-Send guard never crosses an
            // await point.
            let appender = {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                guard.take()
            };
            if let Some(appender) = appender {
                appender
                    .abort()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
            }
            Ok(())
        })
    }

    /// Number of rows currently buffered.
    fn buffered_count(&self) -> PyResult<usize> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        guard.as_ref().map(|a| a.buffered_count()).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })
    }

    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Auto-abort on exception if not finished.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Take the appender out under the lock, then drop the guard before
            // awaiting the async rollback so the non-Send guard never crosses an
            // await point.
            let appender = {
                let mut guard = inner.lock().unwrap();
                guard.take()
            };
            if let Some(appender) = appender {
                appender
                    .abort()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
            }
            Ok(false)
        })
    }
}

// ============================================================================
// AsyncTxAppenderBuilder
// ============================================================================

/// Builder for an async streaming appender within a transaction.
///
/// Async counterpart of the sync
/// [`TxAppenderBuilder`](crate::sync_api::TxAppenderBuilder).
#[pyclass(name = "AsyncTxAppenderBuilder")]
pub struct AsyncTxAppenderBuilder {
    tx: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    label: String,
    batch_size: Option<usize>,
    defer_vector_indexes: Option<bool>,
    max_buffer_size_bytes: Option<usize>,
}

#[pymethods]
impl AsyncTxAppenderBuilder {
    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = Some(size);
        slf
    }

    fn defer_vector_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_vector_indexes = Some(defer);
        slf
    }

    fn max_buffer_size_bytes(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.max_buffer_size_bytes = Some(size);
        slf
    }

    /// Build the streaming appender (returns awaitable).
    fn build<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tx = self.tx.clone();
        let label = self.label.clone();
        let batch_size = self.batch_size;
        let defer_vector_indexes = self.defer_vector_indexes;
        let max_buffer_size_bytes = self.max_buffer_size_bytes;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = tx.lock().await;
            let tx_ref = active_tx(&guard)?;
            let mut builder = tx_ref.appender(&label);
            crate::apply_opt!(builder, batch_size, batch_size);
            crate::apply_opt!(builder, defer_vector_indexes, defer_vector_indexes);
            crate::apply_opt!(builder, max_buffer_size_bytes, max_buffer_size_bytes);
            let appender = builder
                .build()
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(AsyncStreamingAppender {
                inner: Arc::new(std::sync::Mutex::new(Some(appender))),
            })
        })
    }
}

// ============================================================================
// AsyncSessionTemplateBuilder / AsyncSessionTemplate
// ============================================================================

/// Builder for pre-configured async session templates.
///
/// Async counterpart of the sync
/// [`SessionTemplateBuilder`](crate::builders::SessionTemplateBuilder); the
/// built template's `create()` yields an [`AsyncSession`].
#[pyclass]
pub struct AsyncSessionTemplateBuilder {
    inner: Option<::uni_db::SessionTemplateBuilder>,
}

#[pymethods]
impl AsyncSessionTemplateBuilder {
    /// Bind a parameter that all sessions created from this template inherit.
    fn param<'py>(
        mut slf: PyRefMut<'py, Self>,
        key: String,
        value: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let val = convert::py_object_to_value(py, &value)?;
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.param(key, val));
        Ok(slf)
    }

    /// Pre-compile Locy rules.
    fn rules<'py>(mut slf: PyRefMut<'py, Self>, program: &str) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(
            builder
                .rules(program)
                .map_err(crate::exceptions::uni_error_to_pyerr)?,
        );
        Ok(slf)
    }

    /// Attach a named hook.
    fn hook<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        hook: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.hook(name, crate::builders::PySessionHook { py_obj: hook }));
        Ok(slf)
    }

    /// Set default query timeout in seconds.
    fn query_timeout<'py>(
        mut slf: PyRefMut<'py, Self>,
        seconds: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.query_timeout(std::time::Duration::from_secs_f64(seconds)));
        Ok(slf)
    }

    /// Set default transaction timeout in seconds.
    fn transaction_timeout<'py>(
        mut slf: PyRefMut<'py, Self>,
        seconds: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        slf.inner = Some(builder.transaction_timeout(std::time::Duration::from_secs_f64(seconds)));
        Ok(slf)
    }

    /// Build the session template.
    fn build(&mut self) -> PyResult<AsyncSessionTemplate> {
        let builder = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        let template = builder
            .build()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(AsyncSessionTemplate {
            inner: std::sync::Arc::new(template),
        })
    }
}

/// A pre-configured async session factory.
///
/// Async counterpart of the sync
/// [`SessionTemplate`](crate::builders::SessionTemplate); `create()` builds an
/// [`AsyncSession`] cheaply with no I/O.
#[pyclass]
pub struct AsyncSessionTemplate {
    inner: std::sync::Arc<::uni_db::SessionTemplate>,
}

#[pymethods]
impl AsyncSessionTemplate {
    /// Create a new async session from this template (cheap, no I/O).
    fn create(&self) -> AsyncSession {
        let session = self.inner.create();
        let rule_reg = session.rules().clone_registry_arc();
        let params_arc = session.params().clone_store_arc();
        AsyncSession {
            inner: Arc::new(tokio::sync::Mutex::new(session)),
            rule_registry_arc: rule_reg,
            params_arc,
            pending_plugin_builder: uni_plugin_pyo3::ManifestBuilder::new(),
        }
    }
}

// ============================================================================
// AsyncSession
// ============================================================================

/// An async query session with scoped variables.
///
/// Sessions are the primary scope for reads and the factory for transactions.
/// Create via `db.session()`.
#[pyclass]
pub struct AsyncSession {
    pub(crate) inner: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    pub(crate) rule_registry_arc: std::sync::Arc<std::sync::RwLock<::uni_db::LocyRuleRegistry>>,
    pub(crate) params_arc:
        std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, ::uni_db::Value>>>,
    /// M8 async-bindings follow-up: per-session decorator-accumulator.
    /// Mirrors `crate::builders::Session::pending_plugin_builder`. Each
    /// `@session.scalar_fn` / `@session.aggregate_fn` /
    /// `@session.procedure` decoration pushes into this builder;
    /// `await session.finalize_plugin(plugin_id)` drains it into the
    /// session-local plugin registry (proposal §5.4.2 default scope).
    pub(crate) pending_plugin_builder: std::sync::Arc<uni_plugin_pyo3::ManifestBuilder>,
}

#[pymethods]
impl AsyncSession {
    /// Access the session-scoped parameter store.
    fn params(&self) -> crate::sync_api::PyParams {
        crate::sync_api::PyParams {
            inner: self.params_arc.clone(),
        }
    }

    // ── PyO3 plugin decorator surface (M8 async follow-up) ───────────
    //
    // Mirrors the sync `crate::builders::Session` decorator methods
    // verbatim. The decorator methods themselves are sync because they
    // only push into the `Arc<ManifestBuilder>` — no async work. The
    // `finalize_plugin` and `load_python_plugin` methods are async,
    // returning Python awaitables that lock the inner tokio mutex and
    // dispatch to the (sync) Rust `Session::finalize_python_plugin` /
    // `add_python_plugin` methods.

    /// `@session.scalar_fn(name, args=[...], returns=..., vectorized=False, determinism="pure")`
    #[pyo3(signature = (name, args, returns, vectorized=false, determinism="pure"))]
    fn scalar_fn(
        &self,
        py: Python<'_>,
        name: String,
        args: Bound<'_, PyAny>,
        returns: String,
        vectorized: bool,
        determinism: &str,
    ) -> PyResult<Py<PyAny>> {
        uni_plugin_pyo3::make_scalar_trampoline(
            py,
            Arc::clone(&self.pending_plugin_builder),
            name,
            args,
            returns,
            vectorized,
            determinism,
        )
    }

    /// `@session.aggregate_fn(name, args=[...], returns=..., determinism="pure")`
    #[pyo3(signature = (name, args, returns, determinism="pure"))]
    fn aggregate_fn(
        &self,
        py: Python<'_>,
        name: String,
        args: Bound<'_, PyAny>,
        returns: String,
        determinism: &str,
    ) -> PyResult<Py<PyAny>> {
        uni_plugin_pyo3::make_aggregate_trampoline(
            py,
            Arc::clone(&self.pending_plugin_builder),
            name,
            args,
            returns,
            determinism,
        )
    }

    /// `@session.procedure(name, args=[...], yields=[...], mode="read")`
    #[pyo3(signature = (name, args, yields, mode="read"))]
    fn procedure(
        &self,
        py: Python<'_>,
        name: String,
        args: Bound<'_, PyAny>,
        yields: Bound<'_, PyAny>,
        mode: &str,
    ) -> PyResult<Py<PyAny>> {
        uni_plugin_pyo3::make_procedure_trampoline(
            py,
            Arc::clone(&self.pending_plugin_builder),
            name,
            args,
            yields,
            mode,
        )
    }

    /// `session.set_plugin_id(id)` — sets the plugin id used by the
    /// next `finalize_plugin()`.
    fn set_plugin_id(&self, plugin_id: String) {
        self.pending_plugin_builder.set_id(plugin_id);
    }

    /// `session.set_plugin_version(version)` — sets the version used
    /// by the next `finalize_plugin()`.
    fn set_plugin_version(&self, version: String) {
        self.pending_plugin_builder.set_version(version);
    }

    /// `await session.finalize_plugin(plugin_id, version=None, grants=None)`
    /// — drain accumulated decorator entries and register them into
    /// this session's local plugin registry. Returns a metadata dict
    /// (`plugin_id`, `version`, `scalars_registered`, etc.) — identical
    /// shape to the sync `Session.finalize_plugin` return value.
    #[pyo3(signature = (plugin_id, version=None, grants=None))]
    fn finalize_plugin<'py>(
        &self,
        py: Python<'py>,
        plugin_id: String,
        version: Option<String>,
        grants: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let builder = Arc::clone(&self.pending_plugin_builder);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if builder.entry_count() == 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "session.finalize_plugin: no pending decorators — call \
                     @session.scalar_fn / aggregate_fn / procedure first",
                ));
            }
            builder.set_id(&plugin_id);
            if let Some(v) = version {
                builder.set_version(v);
            }
            let caps = crate::builders::build_capability_set(grants);
            let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id(&plugin_id);
            let session = inner.lock().await;
            let outcome = session
                .finalize_python_plugin(&loader, &builder, &caps)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| crate::builders::load_outcome_to_pydict(py, &outcome))
        })
    }

    /// `await session.load_python_plugin(module_src, module_name, grants=None)`
    /// — load a Python plugin from a source string. The module body
    /// uses `@db.scalar_fn(...)` etc. on the host-injected `db`
    /// global. Registers session-scoped per proposal §5.4.2.
    #[pyo3(signature = (module_src, module_name, grants=None))]
    fn load_python_plugin<'py>(
        &self,
        py: Python<'py>,
        module_src: String,
        module_name: String,
        grants: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let caps = crate::builders::build_capability_set(grants);
            let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id(&module_name);
            let session = inner.lock().await;
            Python::attach(|py| {
                let outcome = session
                    .add_python_plugin(py, &loader, &module_src, &module_name, &caps)
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
                crate::builders::load_outcome_to_pydict(py, &outcome)
            })
        })
    }

    /// Execute a query with session variables.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params(py, params)?;
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let result = if let Some(params) = rust_params {
                let mut builder = guard.query_with(&cypher);
                for (k, v) in params {
                    builder = builder.param(k, v);
                }
                builder
                    .fetch_all()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            } else {
                guard
                    .query(&cypher)
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?
            };
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Create a new async transaction for multi-statement writes.
    #[pyo3(signature = (timeout=None))]
    fn tx<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = if let Some(secs) = timeout {
                guard
                    .tx_with()
                    .timeout(std::time::Duration::from_secs_f64(secs))
                    .start()
                    .await
            } else {
                guard.tx().await
            }
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            let rule_reg = tx.rules().clone_registry_arc();
            let token = tx.cancellation_token();
            Ok(AsyncTransaction {
                inner: Arc::new(tokio::sync::Mutex::new(Some(tx))),
                rule_registry_arc: rule_reg,
                cancellation_token: token,
            })
        })
    }

    /// Create a transaction builder for configuring transaction options.
    fn tx_with(&self) -> AsyncTransactionBuilder {
        AsyncTransactionBuilder {
            session: self.inner.clone(),
            timeout_secs: None,
            isolation_level: None,
        }
    }

    // ── Forks (Phase 4b) ────────────────────────────────────────────────

    /// Open or create a fork. Returns a builder synchronously; chain
    /// `.new_()` / `.ttl(timedelta)`, then `await .build()`.
    ///
    /// Returning the builder synchronously (rather than as an
    /// awaitable wrapping it) keeps the chain ergonomic: users write
    /// `await session.fork(name).ttl(td).build()` instead of
    /// `await (await session.fork(name)).build()`.
    fn fork(&self, name: String) -> PyResult<AsyncForkBuilder> {
        // Take a synchronous read of the wrapped session. `try_lock`
        // is the right call here: `AsyncSession.fork()` is invoked
        // outside any await, so the mutex is always immediately
        // available unless another task is currently holding the
        // session for a long-running op (e.g. an in-flight tx) — in
        // which case the user has a wider concurrency bug to fix.
        let guard = self.inner.try_lock().map_err(|_| {
            crate::exceptions::UniInvalidArgumentError::new_err(
                "session is busy; await any in-flight operation before forking",
            )
        })?;
        Ok(AsyncForkBuilder {
            parent: guard.clone(),
            name,
            must_create: false,
            ttl: None,
        })
    }

    /// Fork-local schema mutation builder. Returns synchronously;
    /// chain `.label(...)` / `.edge_type(...)`, then `await .apply()`.
    fn fork_schema(&self) -> PyResult<AsyncForkSchemaBuilder> {
        let guard = self.inner.try_lock().map_err(|_| {
            crate::exceptions::UniInvalidArgumentError::new_err(
                "session is busy; await any in-flight operation before fork_schema",
            )
        })?;
        Ok(AsyncForkSchemaBuilder {
            parent: guard.clone(),
            pending: Vec::new(),
        })
    }

    /// Flush this session's writer to L1.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard
                .flush()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// `True` when this session was returned by a `fork()` call.
    fn is_forked<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.is_forked())
        })
    }

    /// Get the session ID.
    fn id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.id().to_string())
        })
    }

    /// Get session capabilities.
    fn capabilities<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let caps = guard.capabilities();
            let write_lease = caps.write_lease.map(|wl| format!("{:?}", wl));
            Ok(crate::types::PySessionCapabilities {
                can_write: caps.can_write,
                can_pin: caps.can_pin,
                isolation: caps.isolation.to_string(),
                has_notifications: caps.has_notifications,
                write_lease,
            })
        })
    }

    /// Evaluate a Locy program within this session (returns awaitable).
    #[pyo3(signature = (program, params=None))]
    fn locy<'py>(
        &self,
        py: Python<'py>,
        program: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params(py, params)?;
        let inner = self.inner.clone();
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    if let Some(params) = rust_params {
                        let mut builder = guard.locy_with(&program);
                        for (k, v) in params {
                            builder = builder.param(&k, v);
                        }
                        builder.run().await
                    } else {
                        guard.locy(&program).await
                    }
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }

    /// Access the session-scoped rule registry.
    fn rules(&self) -> crate::sync_api::PyRuleRegistry {
        crate::sync_api::PyRuleRegistry {
            registry: self.rule_registry_arc.clone(),
            // Session/transaction-scoped rules are ephemeral.
            persister: None,
        }
    }

    /// Prepare a Cypher query for repeated execution (returns awaitable).
    fn prepare<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let prepared = guard
                .prepare(&cypher)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedQuery {
                inner: std::sync::Arc::new(prepared),
            })
        })
    }

    /// Compile a Locy program without executing it.
    fn compile_locy<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let compiled = guard
                .compile_locy(&program)
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyCompiledProgram { inner: compiled })
        })
    }

    /// Get session metrics.
    fn metrics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let m = guard.metrics();
            Ok(crate::types::PySessionMetrics {
                session_id: m.session_id,
                active_since_secs: m.active_since.elapsed().as_secs_f64(),
                queries_executed: m.queries_executed,
                locy_evaluations: m.locy_evaluations,
                total_query_time_secs: m.total_query_time.as_secs_f64(),
                transactions_committed: m.transactions_committed,
                transactions_rolled_back: m.transactions_rolled_back,
                total_rows_returned: m.total_rows_returned,
                total_rows_scanned: m.total_rows_scanned,
                plan_cache_hits: m.plan_cache_hits,
                plan_cache_misses: m.plan_cache_misses,
                plan_cache_size: m.plan_cache_size,
            })
        })
    }

    /// Cancel in-progress operations on this session.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard.cancel();
            Ok(())
        })
    }

    /// Pin this session to a specific snapshot version.
    fn pin_to_version<'py>(
        &self,
        py: Python<'py>,
        snapshot_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard
                .pin_to_version(&snapshot_id)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Pin this session to a specific timestamp (seconds since epoch).
    fn pin_to_timestamp<'py>(
        &self,
        py: Python<'py>,
        epoch_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let ts = chrono::DateTime::from_timestamp(
                epoch_secs as i64,
                ((epoch_secs.fract()) * 1_000_000_000.0) as u32,
            )
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid timestamp"))?;
            let mut guard = inner.lock().await;
            guard
                .pin_to_timestamp(ts)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Refresh session to latest database version (unpins if pinned).
    fn refresh<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard
                .refresh()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }

    /// Check if this session is pinned to a specific version.
    fn is_pinned<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.is_pinned())
        })
    }

    /// Prepare a Locy program for repeated execution.
    fn prepare_locy<'py>(&self, py: Python<'py>, program: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let prepared = guard
                .prepare_locy(&program)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyPreparedLocy {
                inner: std::sync::Arc::new(prepared),
            })
        })
    }

    /// Add a named session hook.
    #[allow(deprecated)] // Python binding still uses per-session hooks; migration to BuiltinHookPlugin tracked alongside the v2.0 ABI break.
    fn add_hook<'py>(
        &self,
        py: Python<'py>,
        name: String,
        hook: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.add_hook(name, crate::builders::PySessionHook { py_obj: hook });
            Ok(())
        })
    }

    /// Remove a hook by name.
    #[allow(deprecated)]
    fn remove_hook<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            Ok(guard.remove_hook(&name))
        })
    }

    /// List names of all registered hooks.
    #[allow(deprecated)]
    fn list_hooks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(guard.list_hooks())
        })
    }

    /// Remove all hooks.
    #[allow(deprecated)]
    fn clear_hooks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.clear_hooks();
            Ok(())
        })
    }

    /// Watch for commit notifications (returns an async CommitStream).
    fn watch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let stream = guard.watch();
            Ok(AsyncCommitStream {
                inner: Arc::new(tokio::sync::Mutex::new(Some(stream))),
            })
        })
    }

    /// Create a WatchBuilder for configuring commit notification filters.
    fn watch_with<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let builder = guard.watch_with();
            Ok(crate::types::PyWatchBuilder {
                inner: Some(builder),
            })
        })
    }

    // ── Builder factories (per-operation timeout, cancellation, etc.) ──

    /// Create a query builder with parameter binding, timeout, max_memory, cancellation.
    fn query_with(&self, cypher: &str) -> AsyncSessionQueryBuilder {
        AsyncSessionQueryBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
            cancellation_token: None,
        }
    }

    /// Create a Locy evaluation builder with parameters, timeout, max_iterations.
    fn locy_with(&self, program: &str) -> AsyncSessionLocyBuilder {
        AsyncSessionLocyBuilder {
            inner: self.inner.clone(),
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            locy_config: None,
            cancellation_token: None,
        }
    }

    /// Get the session's cancellation token.
    fn cancellation_token<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            Ok(crate::types::PyCancellationToken {
                inner: guard.cancellation_token(),
            })
        })
    }

    /// Async context manager support.
    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Async context manager exit — cancels in-progress operations.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            guard.cancel();
            Ok(false) // Don't suppress exceptions
        })
    }
}

// ============================================================================
// AsyncCommitStream (async iterator)
// ============================================================================

/// An async iterator over commit notifications.
#[pyclass(name = "AsyncCommitStream")]
pub struct AsyncCommitStream {
    pub(crate) inner: Arc<tokio::sync::Mutex<Option<::uni_db::CommitStream>>>,
}

#[pymethods]
impl AsyncCommitStream {
    fn __aiter__(slf: pyo3::PyRef<'_, Self>) -> pyo3::PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let stream = match guard.as_mut() {
                Some(s) => s,
                None => {
                    return Err(PyErr::new::<pyo3::exceptions::PyStopAsyncIteration, _>(""));
                }
            };
            match stream.next().await {
                Some(n) => Ok(crate::types::PyCommitNotification::from(n)),
                None => Err(PyErr::new::<pyo3::exceptions::PyStopAsyncIteration, _>("")),
            }
        })
    }

    /// Close the stream, releasing resources.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut guard = inner.lock().await;
            guard.take();
            Ok(())
        })
    }
}

// ============================================================================
// AsyncTransactionBuilder
// ============================================================================

/// Builder for configuring async transaction options.
#[pyclass(name = "AsyncTransactionBuilder")]
pub struct AsyncTransactionBuilder {
    session: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    timeout_secs: Option<f64>,
    isolation_level: Option<String>,
}

#[pymethods]
impl AsyncTransactionBuilder {
    /// Set transaction timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set isolation level (currently only "serialized" is supported).
    fn isolation(mut slf: PyRefMut<'_, Self>, level: String) -> PyRefMut<'_, Self> {
        slf.isolation_level = Some(level);
        slf
    }

    /// Start the transaction.
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.session.clone();
        let timeout_secs = self.timeout_secs;
        let isolation_level = self.isolation_level.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = session.lock().await;
            let mut builder = guard.tx_with();
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(ref level) = isolation_level {
                match level.to_lowercase().as_str() {
                    "serialized" => {
                        builder = builder.isolation(::uni_db::IsolationLevel::Serialized);
                    }
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Unknown isolation level: {}",
                            level
                        )));
                    }
                }
            }
            let tx = builder
                .start()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            let rule_reg = tx.rules().clone_registry_arc();
            let token = tx.cancellation_token();
            Ok(AsyncTransaction {
                inner: Arc::new(tokio::sync::Mutex::new(Some(tx))),
                rule_registry_arc: rule_reg,
                cancellation_token: token,
            })
        })
    }
}

// ============================================================================
// AsyncBulkWriter (wrapping real Rust BulkWriter)
// ============================================================================

/// Async bulk writer for high-throughput data ingestion.
///
/// Uses `std::sync::Mutex` + `spawn_blocking` because `BulkWriter`'s progress
/// callback (`Box<dyn Fn + Send>`) is not `Sync`, so it can't be held across
/// `.await` points in a `tokio::sync::Mutex`.
#[pyclass]
pub struct AsyncBulkWriter {
    inner: Arc<std::sync::Mutex<Option<::uni_db::api::bulk::BulkWriter>>>,
}

#[pymethods]
impl AsyncBulkWriter {
    /// Insert vertices in bulk.
    fn insert_vertices<'py>(
        &self,
        py: Python<'py>,
        label: String,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_props: Vec<HashMap<String, ::uni_db::Value>> =
            Vec::with_capacity(vertices.len());
        for v in vertices {
            let mut map = HashMap::new();
            for (k, val) in v {
                map.insert(k, convert::py_object_to_value(py, &val)?);
            }
            rust_props.push(map);
        }
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // BulkWriter is !Sync (progress callback), use spawn_blocking
            let vids = tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let writer = guard.as_mut().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "BulkWriter already completed",
                    )
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(writer.insert_vertices(&label, rust_props))
                    .map_err(crate::exceptions::anyhow_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(vids.into_iter().map(|v| v.as_u64()).collect::<Vec<_>>())
        })
    }

    /// Insert edges in bulk.
    fn insert_edges<'py>(
        &self,
        py: Python<'py>,
        edge_type: String,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_edges = Vec::with_capacity(edges.len());
        for (src, dst, props) in edges {
            let mut map = HashMap::new();
            for (k, v) in props {
                map.insert(k, convert::py_object_to_value(py, &v)?);
            }
            rust_edges.push(::uni_db::api::bulk::EdgeData::new(
                ::uni_db::Vid::from(src),
                ::uni_db::Vid::from(dst),
                map,
            ));
        }
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let writer = guard.as_mut().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "BulkWriter already completed",
                    )
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(writer.insert_edges(&edge_type, rust_edges))
                    .map_err(crate::exceptions::anyhow_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(())
        })
    }

    /// Get current bulk load statistics.
    fn stats(&self) -> PyResult<BulkStats> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
        let s = writer.stats();
        Ok(BulkStats {
            vertices_inserted: s.vertices_inserted,
            edges_inserted: s.edges_inserted,
            indexes_rebuilt: s.indexes_rebuilt,
            duration_secs: s.duration.as_secs_f64(),
            index_build_duration_secs: s.index_build_duration.as_secs_f64(),
            index_task_ids: s.index_task_ids.clone(),
            indexes_pending: s.indexes_pending,
        })
    }

    /// Get labels that have been written to.
    fn touched_labels(&self) -> PyResult<Vec<String>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
        Ok(writer.touched_labels())
    }

    /// Get edge types that have been written to.
    fn touched_edge_types(&self) -> PyResult<Vec<String>> {
        let guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
        Ok(writer.touched_edge_types())
    }

    /// Commit all pending data and rebuild indexes.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                let writer = guard.take().ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "BulkWriter already completed",
                    )
                })?;
                let rt = tokio::runtime::Handle::current();
                rt.block_on(writer.commit())
                    .map_err(crate::exceptions::anyhow_to_pyerr)
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(BulkStats {
                vertices_inserted: stats.vertices_inserted,
                edges_inserted: stats.edges_inserted,
                indexes_rebuilt: stats.indexes_rebuilt,
                duration_secs: stats.duration.as_secs_f64(),
                index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
                index_task_ids: stats.index_task_ids.clone(),
                indexes_pending: stats.indexes_pending,
            })
        })
    }

    /// Abort bulk loading and discard uncommitted changes.
    fn abort<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;
                if let Some(writer) = guard.take() {
                    let rt = tokio::runtime::Handle::current();
                    rt.block_on(writer.abort())
                        .map_err(crate::exceptions::anyhow_to_pyerr)?;
                }
                Ok::<_, PyErr>(())
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Auto-abort on exception if not committed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::task::spawn_blocking(move || {
                let mut guard = inner.lock().unwrap();
                if let Some(writer) = guard.take() {
                    let rt = tokio::runtime::Handle::current();
                    let _ = rt.block_on(writer.abort());
                }
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(false)
        })
    }
}

// ============================================================================
// AsyncQueryCursor
// ============================================================================

/// Async cursor-based result streaming for large query result sets.
///
/// Implements Python's async iterator (`__aiter__`/`__anext__`) and async context
/// manager (`__aenter__`/`__aexit__`).
#[pyclass]
pub struct AsyncQueryCursor {
    cursor: Arc<tokio::sync::Mutex<Option<core::QueryCursor>>>,
    buffer: Arc<tokio::sync::Mutex<VecDeque<core::Row>>>,
    #[pyo3(get)]
    columns: Vec<String>,
}

/// Pull the next single row, refilling from the batch stream as needed.
///
/// Free function over the cursor/buffer `Arc`s so the awaitable cursor methods
/// can drive it without rebuilding a throwaway `AsyncQueryCursor`.
///
/// The error type is deliberately `UniError`, not `String`: stringifying here
/// erases the variant, and every caller is then forced to invent an exception
/// class. They all guessed `PyRuntimeError`, so a retriable conflict surfaced
/// through `fetch_one`/`fetch_many`/`async for` could not be recognised by
/// `_retry.RETRIABLE_EXCEPTIONS`, which matches by class. The sync twin
/// (`sync_api.rs`) and `fetch_all` below both keep the typed error; this path
/// was the odd one out.
async fn next_row_async(
    cursor: &Arc<tokio::sync::Mutex<Option<core::QueryCursor>>>,
    buffer: &Arc<tokio::sync::Mutex<VecDeque<core::Row>>>,
) -> Result<Option<core::Row>, uni_common::UniError> {
    {
        let mut buf = buffer.lock().await;
        if let Some(row) = buf.pop_front() {
            return Ok(Some(row));
        }
    }
    // Buffer empty – fetch next batch from cursor.
    let mut guard = cursor.lock().await;
    let cursor = match guard.as_mut() {
        Some(c) => c,
        None => return Ok(None),
    };
    match cursor.next_batch().await {
        Some(Ok(rows)) => {
            let mut buf = buffer.lock().await;
            let mut iter = rows.into_iter();
            let first = iter.next();
            buf.extend(iter);
            Ok(first)
        }
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

#[pymethods]
impl AsyncQueryCursor {
    /// Fetch a single row, or `None` if exhausted.
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor.clone();
        let buffer = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match next_row_async(&cursor, &buffer).await {
                Ok(Some(row)) => {
                    Python::attach(|py| Ok(Some(convert::row_to_dict(py, &row)?.into_py_any(py)?)))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(crate::exceptions::uni_error_to_pyerr(e)),
            }
        })
    }

    /// Fetch up to `n` rows.
    #[pyo3(signature = (n))]
    fn fetch_many<'py>(&self, py: Python<'py>, n: usize) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor.clone();
        let buffer = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut rows = Vec::with_capacity(n);
            for _ in 0..n {
                match next_row_async(&cursor, &buffer).await {
                    Ok(Some(row)) => rows.push(row),
                    Ok(None) => break,
                    Err(e) => return Err(crate::exceptions::uni_error_to_pyerr(e)),
                }
            }
            Python::attach(|py| convert::rows_to_py(py, rows))
        })
    }

    /// Fetch all remaining rows.
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor_arc = self.cursor.clone();
        let buffer_arc = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            // Drain buffer first
            let mut rows: Vec<core::Row> = {
                let mut buf = buffer_arc.lock().await;
                buf.drain(..).collect()
            };
            // Take and consume the cursor
            let cursor_opt = {
                let mut guard = cursor_arc.lock().await;
                guard.take()
            };
            if let Some(cursor) = cursor_opt {
                let remaining = cursor
                    .collect_remaining()
                    .await
                    .map_err(crate::exceptions::uni_error_to_pyerr)?;
                rows.extend(remaining);
            }
            Python::attach(|py| convert::rows_to_py(py, rows))
        })
    }

    /// Close the cursor, releasing resources.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor_arc = self.cursor.clone();
        let buffer_arc = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = cursor_arc.lock().await.take();
            buffer_arc.lock().await.clear();
            Ok(())
        })
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor.clone();
        let buffer = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            match next_row_async(&cursor, &buffer).await {
                Ok(Some(row)) => {
                    Python::attach(|py| convert::row_to_dict(py, &row)?.into_py_any(py))
                }
                Ok(None) => Err(pyo3::exceptions::PyStopAsyncIteration::new_err(
                    "end of cursor",
                )),
                Err(e) => Err(crate::exceptions::uni_error_to_pyerr(e)),
            }
        })
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let obj: Py<PyAny> = slf.into_py_any(py)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cursor_arc = self.cursor.clone();
        let buffer_arc = self.buffer.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = cursor_arc.lock().await.take();
            buffer_arc.lock().await.clear();
            Ok(false) // don't suppress exceptions
        })
    }
}

// ============================================================================
// Async Session-level Builders (query_with, execute_with, locy_with, profile_with)
// ============================================================================

/// Async builder for parameterized queries on a Session.
#[pyclass(name = "AsyncSessionQueryBuilder")]
pub struct AsyncSessionQueryBuilder {
    inner: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
    cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl AsyncSessionQueryBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters.
    fn params(
        mut slf: PyRefMut<'_, Self>,
        params: HashMap<String, Py<PyAny>>,
    ) -> PyRefMut<'_, Self> {
        slf.params.extend(params);
        slf
    }

    /// Set query timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum memory in bytes.
    fn max_memory(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.max_memory = Some(bytes);
        slf
    }

    /// Attach a cancellation token.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Fetch all results (returns awaitable QueryResult).
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(m) = max_memory {
                builder = builder.max_memory(m);
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let result = builder
                .fetch_all()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Fetch the first row or None (returns awaitable).
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(m) = max_memory {
                builder = builder.max_memory(m);
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            // `fetch_one` returns at most one `Row`, so only that row is
            // converted to Python instead of materializing the whole result set.
            let row = builder
                .fetch_one()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| match row {
                Some(row) => Ok(convert::rows_to_py(py, vec![row])?.into_iter().next()),
                None => Ok(None),
            })
        })
    }

    /// Open a streaming cursor (returns awaitable AsyncQueryCursor).
    fn cursor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(m) = max_memory {
                builder = builder.max_memory(m);
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let cursor = builder
                .cursor()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }

    /// Explain the query plan without executing it.
    fn explain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Validate params (surfacing any conversion error) even though the
        // explain path does not bind them onto the builder.
        let _rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let builder = guard.query_with(&cypher);
            let output = builder
                .explain()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::explain_output_to_py_class(py, output))
        })
    }

    /// Profile the query execution, returning results with profiling output.
    fn profile<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let mut builder = guard.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            let (results, profile) = builder
                .profile()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let query_result = convert::query_result_to_py_class(py, results)?;
                let profile_output = convert::profile_output_to_py_class(py, profile)?;
                let tuple = (query_result, profile_output);
                tuple.into_py_any(py)
            })
        })
    }
}

/// Async builder for Locy evaluation on a Session.
#[pyclass(name = "AsyncSessionLocyBuilder")]
pub struct AsyncSessionLocyBuilder {
    inner: Arc<tokio::sync::Mutex<::uni_db::Session>>,
    program: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_iterations: Option<usize>,
    locy_config: Option<::uni_locy::LocyConfig>,
    cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl AsyncSessionLocyBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters.
    fn params(
        mut slf: PyRefMut<'_, Self>,
        params: HashMap<String, Py<PyAny>>,
    ) -> PyRefMut<'_, Self> {
        slf.params.extend(params);
        slf
    }

    /// Set evaluation timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum fixpoint iterations.
    fn max_iterations(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.max_iterations = Some(n);
        slf
    }

    /// Apply a full Locy configuration (LocyConfig object or dict).
    fn with_config<'py>(
        mut slf: PyRefMut<'py, Self>,
        config: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let locy_config = if let Ok(cfg) = config.extract::<crate::types::PyLocyConfig>() {
            cfg.inner
        } else {
            let dict: HashMap<String, Py<PyAny>> = config.extract()?;
            crate::convert::extract_locy_config(config.py(), dict)?
        };
        slf.locy_config = Some(locy_config);
        Ok(slf)
    }

    /// Attach a cancellation token.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation (returns awaitable LocyResult).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let program = self.program.clone();
        let timeout_secs = self.timeout_secs;
        let max_iterations = self.max_iterations;
        let locy_config = self.locy_config.clone();
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let mut builder = guard.locy_with(&program);
                    for (k, v) in rust_params {
                        builder = builder.param(k, v);
                    }
                    if let Some(t) = timeout_secs {
                        builder = builder.timeout(std::time::Duration::from_secs_f64(t));
                    }
                    if let Some(n) = max_iterations {
                        builder = builder.max_iterations(n);
                    }
                    if let Some(c) = locy_config {
                        builder = builder.with_config(c);
                    }
                    if let Some(ct) = cancel_token {
                        builder = builder.cancellation_token(ct);
                    }
                    builder.run().await
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }

    /// Explain the Locy program without executing it.
    fn explain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let program = self.program.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    guard.locy_with(&program).explain()
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(convert::locy_explain_to_py_class(result))
        })
    }

    /// Profile the Locy program: returns an awaitable `(LocyResult, profile)`.
    ///
    /// Async analog of the sync `SessionLocyBuilder.profile()` and the read-path
    /// `AsyncSessionQueryBuilder.profile()`.
    fn profile<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let program = self.program.clone();
        let timeout_secs = self.timeout_secs;
        let max_iterations = self.max_iterations;
        let locy_config = self.locy_config.clone();
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (result, profile) = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let mut builder = guard.locy_with(&program);
                    for (k, v) in rust_params {
                        builder = builder.param(k, v);
                    }
                    if let Some(t) = timeout_secs {
                        builder = builder.timeout(std::time::Duration::from_secs_f64(t));
                    }
                    if let Some(n) = max_iterations {
                        builder = builder.max_iterations(n);
                    }
                    if let Some(c) = locy_config {
                        builder = builder.with_config(c);
                    }
                    if let Some(ct) = cancel_token {
                        builder = builder.cancellation_token(ct);
                    }
                    builder.profile().await
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let locy_result = convert::locy_result_to_py_class(py, result)?;
                let profile_output = convert::locy_profile_to_py_class(profile);
                let tuple = (locy_result, profile_output);
                tuple.into_py_any(py)
            })
        })
    }
}

// ============================================================================
// Async Transaction-level Builders (query_with, execute_with, locy_with, apply_with)
// ============================================================================

/// Async builder for parameterized queries on a Transaction.
#[pyclass(name = "AsyncTxQueryBuilder")]
pub struct AsyncTxQueryBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    /// Mirrors the sync `TxQueryBuilder`; the cross-language surfaces must not
    /// drift apart.
    cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl AsyncTxQueryBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set query timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Attach a cancellation token for cooperative query cancellation.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Fetch all results (returns awaitable QueryResult).
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let cancel_token = self.cancellation_token.as_ref().map(|t| t.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let mut builder = tx.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(&k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let result = builder
                .fetch_all()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| convert::query_result_to_py_class(py, result))
        })
    }

    /// Fetch the first row or None (returns awaitable).
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let cancel_token = self.cancellation_token.as_ref().map(|t| t.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let mut builder = tx.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(&k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            // `fetch_one` returns at most one `Row`, so only that row is
            // converted to Python instead of materializing the whole result set.
            let row = builder
                .fetch_one()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| match row {
                Some(row) => Ok(convert::rows_to_py(py, vec![row])?.into_iter().next()),
                None => Ok(None),
            })
        })
    }

    /// Execute as a mutation (returns awaitable ExecuteResult).
    fn execute<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let cancel_token = self.cancellation_token.as_ref().map(|t| t.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let mut builder = tx.execute_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let result = builder
                .run()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let py_result = convert::execute_result_to_py(py, result)?;
                Ok(py_result.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Open a streaming cursor (returns awaitable AsyncQueryCursor).
    fn cursor<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let cancel_token = self.cancellation_token.as_ref().map(|t| t.inner.clone());
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let mut builder = tx.query_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(&k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            if let Some(ct) = cancel_token {
                builder = builder.cancellation_token(ct);
            }
            let cursor = builder
                .cursor()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            let columns = cursor.columns().to_vec();
            Ok(AsyncQueryCursor {
                cursor: Arc::new(tokio::sync::Mutex::new(Some(cursor))),
                buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
                columns,
            })
        })
    }
}

/// Async builder for parameterized mutations on a Transaction.
#[pyclass(name = "AsyncTxExecuteBuilder")]
pub struct AsyncTxExecuteBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
}

#[pymethods]
impl AsyncTxExecuteBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set execution timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Execute the mutation (returns awaitable ExecuteResult).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let mut builder = tx.execute_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            let result = builder
                .run()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let py_result = convert::execute_result_to_py(py, result)?;
                Ok(py_result.into_pyobject(py)?.into_any().unbind())
            })
        })
    }

    /// Profile the mutation. Returns an awaitable resolving to
    /// `(ExecuteResult, ProfileOutput)`. Mirrors the sync
    /// `TxExecuteBuilder.profile()` and the read-path
    /// `AsyncSessionQueryBuilder.profile()`.
    fn profile<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let mut builder = tx.execute_with(&cypher);
            for (k, v) in rust_params {
                builder = builder.param(k, v);
            }
            if let Some(t) = timeout_secs {
                builder = builder.timeout(std::time::Duration::from_secs_f64(t));
            }
            let (result, profile) = builder
                .profile()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Python::attach(|py| {
                let exec_result = convert::execute_result_to_py(py, result)?;
                let profile_output = convert::profile_output_to_py_class(py, profile)?;
                let tuple = (exec_result, profile_output);
                Ok(tuple.into_pyobject(py)?.into_any().unbind())
            })
        })
    }
}

/// Async builder for Locy evaluation on a Transaction.
#[pyclass(name = "AsyncTxLocyBuilder")]
pub struct AsyncTxLocyBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    program: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_iterations: Option<usize>,
    locy_config: Option<::uni_locy::LocyConfig>,
    cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl AsyncTxLocyBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set evaluation timeout in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum fixpoint iterations.
    fn max_iterations(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.max_iterations = Some(n);
        slf
    }

    /// Apply a full Locy configuration (LocyConfig object or dict).
    fn with_config<'py>(
        mut slf: PyRefMut<'py, Self>,
        config: &Bound<'py, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let locy_config = if let Ok(cfg) = config.extract::<crate::types::PyLocyConfig>() {
            cfg.inner
        } else {
            let dict: HashMap<String, Py<PyAny>> = config.extract()?;
            crate::convert::extract_locy_config(config.py(), dict)?
        };
        slf.locy_config = Some(locy_config);
        Ok(slf)
    }

    /// Attach a cancellation token.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation (returns awaitable LocyResult).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let program = self.program.clone();
        let timeout_secs = self.timeout_secs;
        let max_iterations = self.max_iterations;
        let locy_config = self.locy_config.clone();
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let tx = active_tx(&guard)?;
                    let mut builder = tx.locy_with(&program);
                    for (k, v) in rust_params {
                        builder = builder.param(k, v);
                    }
                    if let Some(t) = timeout_secs {
                        builder = builder.timeout(std::time::Duration::from_secs_f64(t));
                    }
                    if let Some(n) = max_iterations {
                        builder = builder.max_iterations(n);
                    }
                    if let Some(c) = locy_config {
                        builder = builder.with_config(c);
                    }
                    if let Some(ct) = cancel_token {
                        builder = builder.cancellation_token(ct);
                    }
                    builder
                        .run()
                        .await
                        .map_err(crate::exceptions::uni_error_to_pyerr)
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Python::attach(|py| convert::locy_result_to_py_class(py, result))
        })
    }

    /// Profile the Locy program: returns an awaitable `(LocyResult, profile)`.
    ///
    /// Async analog of the sync `TxLocyBuilder.profile()`.
    fn profile<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::convert_params_ref(py, &self.params)?;
        let inner = self.inner.clone();
        let program = self.program.clone();
        let timeout_secs = self.timeout_secs;
        let max_iterations = self.max_iterations;
        let locy_config = self.locy_config.clone();
        let cancel_token = self.cancellation_token.as_ref().map(|ct| ct.inner.clone());
        // Locy future is !Send — use spawn_blocking
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (result, profile) = tokio::task::spawn_blocking(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let guard = inner.lock().await;
                    let tx = active_tx(&guard)?;
                    let mut builder = tx.locy_with(&program);
                    for (k, v) in rust_params {
                        builder = builder.param(k, v);
                    }
                    if let Some(t) = timeout_secs {
                        builder = builder.timeout(std::time::Duration::from_secs_f64(t));
                    }
                    if let Some(n) = max_iterations {
                        builder = builder.max_iterations(n);
                    }
                    if let Some(c) = locy_config {
                        builder = builder.with_config(c);
                    }
                    if let Some(ct) = cancel_token {
                        builder = builder.cancellation_token(ct);
                    }
                    builder
                        .profile()
                        .await
                        .map_err(crate::exceptions::uni_error_to_pyerr)
                })
            })
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))??;
            Python::attach(|py| {
                let locy_result = convert::locy_result_to_py_class(py, result)?;
                let profile_output = convert::locy_profile_to_py_class(profile);
                let tuple = (locy_result, profile_output);
                tuple.into_py_any(py)
            })
        })
    }
}

/// Async builder for applying a DerivedFactSet with staleness controls.
///
/// Defaults to fresh-required; chain `allow_stale()` or
/// `max_version_gap(n)` to opt out.
#[pyclass(name = "AsyncApplyBuilder")]
pub struct AsyncApplyBuilder {
    inner: Arc<tokio::sync::Mutex<Option<::uni_db::Transaction>>>,
    derived: Option<uni_locy::DerivedFactSet>,
    allow_stale: bool,
    max_version_gap: Option<u64>,
}

#[pymethods]
impl AsyncApplyBuilder {
    /// Require fresh version (fail if version gap is non-zero). This is the
    /// default; `require_fresh(False)` is the explicit stale opt-out (the
    /// pre-2.0.7 default behavior).
    fn require_fresh(mut slf: PyRefMut<'_, Self>, require: bool) -> PyRefMut<'_, Self> {
        slf.allow_stale = !require;
        slf
    }

    /// Apply regardless of how many commits happened since the DERIVE was
    /// evaluated.
    fn allow_stale(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.allow_stale = true;
        slf
    }

    /// Set maximum allowed version gap.
    fn max_version_gap(mut slf: PyRefMut<'_, Self>, gap: u64) -> PyRefMut<'_, Self> {
        slf.max_version_gap = Some(gap);
        slf
    }

    /// Execute the apply (returns awaitable ApplyResult).
    fn run<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let dfs = self.derived.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let allow_stale = self.allow_stale;
        let max_version_gap = self.max_version_gap;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let guard = inner.lock().await;
            let tx = active_tx(&guard)?;
            let mut builder = tx.apply_with(dfs);
            if allow_stale && max_version_gap.is_none() {
                builder = builder.allow_stale();
            }
            if let Some(gap) = max_version_gap {
                builder = builder.max_version_gap(gap);
            }
            let result = builder
                .run()
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(crate::types::PyApplyResult {
                facts_applied: result.facts_applied,
                version_gap: result.version_gap,
            })
        })
    }
}

// ============================================================================
// AsyncSchemaBuilder, AsyncLabelBuilder, AsyncEdgeTypeBuilder
// ============================================================================

/// Async builder for defining and modifying the graph schema.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct AsyncSchemaBuilder {
    inner: Arc<Uni>,
    pending_labels: Vec<crate::core::PendingLabel>,
    pending_edge_types: Vec<crate::core::PendingEdgeType>,
    pending_properties: Vec<crate::core::PendingProperty>,
    pending_indexes: Vec<uni_common::core::schema::IndexDefinition>,
}

#[pymethods]
impl AsyncSchemaBuilder {
    /// Get the current schema as a dictionary.
    fn current(&self, py: Python) -> PyResult<pyo3::Py<pyo3::types::PyAny>> {
        let schema = self.inner.schema().current();
        let dict = pyo3::types::PyDict::new(py);

        let labels = pyo3::types::PyDict::new(py);
        for (name, meta) in &schema.labels {
            let label_dict = pyo3::types::PyDict::new(py);
            label_dict.set_item("id", meta.id)?;
            labels.set_item(name, label_dict)?;
        }
        dict.set_item("labels", labels)?;

        let edge_types = pyo3::types::PyDict::new(py);
        for (name, meta) in &schema.edge_types {
            let et_dict = pyo3::types::PyDict::new(py);
            et_dict.set_item("id", meta.id)?;
            edge_types.set_item(name, et_dict)?;
        }
        dict.set_item("edge_types", edge_types)?;

        Ok(dict.into())
    }

    /// Get the current schema as a typed `Schema` object.
    fn current_typed(&self) -> crate::types::PySchema {
        crate::types::PySchema {
            inner: self.inner.schema().current(),
        }
    }

    /// Start defining a new label.
    #[pyo3(signature = (name, *, description=None))]
    fn label(&self, name: &str, description: Option<String>) -> PyResult<AsyncLabelBuilder> {
        Ok(AsyncLabelBuilder {
            parent_inner: self.inner.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
            description,
            properties: Vec::new(),
            indexes: Vec::new(),
        })
    }

    /// Start defining a new edge type.
    #[pyo3(signature = (name, from_labels, to_labels, *, description=None))]
    fn edge_type(
        &self,
        name: &str,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
        description: Option<String>,
    ) -> PyResult<AsyncEdgeTypeBuilder> {
        Ok(AsyncEdgeTypeBuilder {
            parent_inner: self.inner.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
            description,
            from_labels,
            to_labels,
            properties: Vec::new(),
        })
    }

    /// Apply all pending schema changes (returns awaitable).
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let labels = self.pending_labels.clone();
        let edge_types = self.pending_edge_types.clone();
        let properties = self.pending_properties.clone();
        let indexes = self.pending_indexes.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::apply_schema_core(&inner, &labels, &edge_types, &properties, &indexes)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }
}

/// Async builder for defining a label with its properties and indexes.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct AsyncLabelBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<crate::core::PendingLabel>,
    parent_edge_types: Vec<crate::core::PendingEdgeType>,
    parent_properties: Vec<crate::core::PendingProperty>,
    parent_indexes: Vec<uni_common::core::schema::IndexDefinition>,
    name: String,
    description: Option<String>,
    properties: Vec<(
        String,
        uni_common::core::schema::DataType,
        bool,
        Option<String>,
    )>,
    indexes: Vec<uni_common::core::schema::IndexDefinition>,
}

#[pymethods]
impl AsyncLabelBuilder {
    /// Add a required property to this label.
    #[pyo3(signature = (name, data_type, *, description=None))]
    fn property<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
        description: Option<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, false, description));
        Ok(slf)
    }

    /// Add a nullable property to this label.
    #[pyo3(signature = (name, data_type, *, description=None))]
    fn property_nullable<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
        description: Option<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, true, description));
        Ok(slf)
    }

    /// Add a vector property.
    fn vector(mut slf: PyRefMut<'_, Self>, name: String, dimensions: usize) -> PyRefMut<'_, Self> {
        slf.properties.push((
            name,
            uni_common::core::schema::DataType::Vector { dimensions },
            false,
            None,
        ));
        slf
    }

    /// Add an index on a property.
    ///
    /// `index_type` can be a string or a dict with rich config (see `LabelBuilder.index()`).
    fn index<'py>(
        mut slf: PyRefMut<'py, Self>,
        property: String,
        index_type: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let label = slf.name.clone();
        let idx = crate::builders::parse_index_config(py, &label, &property, &index_type)?;
        slf.indexes.push(idx);
        Ok(slf)
    }

    /// Finish this label and return to AsyncSchemaBuilder.
    fn done(&self) -> PyResult<AsyncSchemaBuilder> {
        let mut labels = self.parent_labels.clone();
        labels.push(crate::core::PendingLabel {
            name: self.name.clone(),
            description: self.description.clone(),
        });

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable, desc) in &self.properties {
            properties.push(crate::core::PendingProperty {
                label_or_type: self.name.clone(),
                name: prop_name.clone(),
                data_type: dt.clone(),
                nullable: *nullable,
                description: desc.clone(),
            });
        }

        let mut indexes = self.parent_indexes.clone();
        indexes.extend(self.indexes.clone());

        Ok(AsyncSchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: labels,
            pending_edge_types: self.parent_edge_types.clone(),
            pending_properties: properties,
            pending_indexes: indexes,
        })
    }

    /// Apply schema changes immediately (returns awaitable).
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.done()?.apply(py)
    }
}

/// Async builder for defining an edge type with its properties.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct AsyncEdgeTypeBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<crate::core::PendingLabel>,
    parent_edge_types: Vec<crate::core::PendingEdgeType>,
    parent_properties: Vec<crate::core::PendingProperty>,
    parent_indexes: Vec<uni_common::core::schema::IndexDefinition>,
    name: String,
    description: Option<String>,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    properties: Vec<(
        String,
        uni_common::core::schema::DataType,
        bool,
        Option<String>,
    )>,
}

#[pymethods]
impl AsyncEdgeTypeBuilder {
    /// Add a required property to this edge type.
    #[pyo3(signature = (name, data_type, *, description=None))]
    fn property<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
        description: Option<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, false, description));
        Ok(slf)
    }

    /// Add a nullable property to this edge type.
    #[pyo3(signature = (name, data_type, *, description=None))]
    fn property_nullable<'py>(
        mut slf: PyRefMut<'py, Self>,
        name: String,
        data_type: &Bound<'py, PyAny>,
        description: Option<String>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dt = if let Ok(py_dt) = data_type.extract::<crate::types::PyDataType>() {
            py_dt.inner
        } else {
            let s: String = data_type.extract()?;
            crate::core::parse_data_type(&s)
                .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?
        };
        slf.properties.push((name, dt, true, description));
        Ok(slf)
    }

    /// Finish this edge type and return to AsyncSchemaBuilder.
    fn done(&self) -> PyResult<AsyncSchemaBuilder> {
        let mut edge_types = self.parent_edge_types.clone();
        edge_types.push(crate::core::PendingEdgeType {
            name: self.name.clone(),
            from: self.from_labels.clone(),
            to: self.to_labels.clone(),
            description: self.description.clone(),
        });

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable, desc) in &self.properties {
            properties.push(crate::core::PendingProperty {
                label_or_type: self.name.clone(),
                name: prop_name.clone(),
                data_type: dt.clone(),
                nullable: *nullable,
                description: desc.clone(),
            });
        }

        Ok(AsyncSchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: self.parent_labels.clone(),
            pending_edge_types: edge_types,
            pending_properties: properties,
            pending_indexes: self.parent_indexes.clone(),
        })
    }

    /// Apply schema changes immediately (returns awaitable).
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.done()?.apply(py)
    }
}

// ============================================================================
// Phase 4b — Async ForkBuilder + Async ForkSchemaBuilder
// ============================================================================

/// Async builder returned by `AsyncSession.fork(name)`. Drive it via
/// `await .build()` after chaining configuration methods.
#[pyclass(name = "AsyncForkBuilder")]
pub struct AsyncForkBuilder {
    pub(crate) parent: ::uni_db::Session,
    pub(crate) name: String,
    pub(crate) must_create: bool,
    pub(crate) ttl: Option<std::time::Duration>,
}

#[pymethods]
impl AsyncForkBuilder {
    /// Require fresh creation; errors with `UniForkAlreadyExistsError`
    /// if the fork name is already taken.
    fn new_(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.must_create = true;
        slf
    }

    /// Stamp a wall-clock TTL on the fork.
    fn ttl<'py>(mut slf: PyRefMut<'py, Self>, ttl: Py<PyAny>) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.ttl = Some(crate::convert::py_timedelta_to_duration(ttl.bind(py))?);
        Ok(slf)
    }

    /// Drive the open-or-create flow and return an `AsyncSession`
    /// wrapping the forked session.
    fn build<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let parent = self.parent.clone();
        let name = self.name.clone();
        let must_create = self.must_create;
        let ttl = self.ttl;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut b = parent.fork(name);
            if must_create {
                b = b.new_();
            }
            if let Some(d) = ttl {
                b = b.ttl(d);
            }
            let forked = b.await.map_err(crate::exceptions::uni_error_to_pyerr)?;
            let rule_reg = forked.rules().clone_registry_arc();
            let params_arc = forked.params().clone_store_arc();
            Ok(AsyncSession {
                inner: Arc::new(tokio::sync::Mutex::new(forked)),
                rule_registry_arc: rule_reg,
                params_arc,
                pending_plugin_builder: uni_plugin_pyo3::ManifestBuilder::new(),
            })
        })
    }
}

/// Async builder returned by `AsyncSession.fork_schema()`. Chain
/// `.label(...)` and `.edge_type(...)` calls, then `await .apply()`.
#[pyclass(name = "AsyncForkSchemaBuilder")]
pub struct AsyncForkSchemaBuilder {
    pub(crate) parent: ::uni_db::Session,
    pub(crate) pending: Vec<crate::builders::ForkSchemaPending>,
}

#[pymethods]
impl AsyncForkSchemaBuilder {
    /// Add a fork-local label.
    #[pyo3(signature = (name, description=None))]
    fn label(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        description: Option<String>,
    ) -> PyRefMut<'_, Self> {
        slf.pending
            .push(crate::builders::ForkSchemaPending::Label { name, description });
        slf
    }

    /// Add a fork-local edge type.
    #[pyo3(signature = (name, from_labels, to_labels, description=None))]
    fn edge_type(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
        description: Option<String>,
    ) -> PyRefMut<'_, Self> {
        slf.pending
            .push(crate::builders::ForkSchemaPending::EdgeType {
                name,
                from_labels,
                to_labels,
                description,
            });
        slf
    }

    /// Persist the pending entries to the fork's overlay file and the
    /// fork's in-memory `SchemaManager`.
    fn apply<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let parent = self.parent.clone();
        let pending = self.pending.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if pending.is_empty() {
                return Ok(());
            }
            crate::builders::apply_fork_schema_pending(parent, pending)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)
        })
    }
}
