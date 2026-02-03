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
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// AsyncDatabase
// ============================================================================

/// Async entry point for the Uni embedded graph database.
#[pyclass]
pub struct AsyncDatabase {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl AsyncDatabase {
    /// Open or create a database at the given path.
    #[staticmethod]
    fn open<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create runtime: {}",
                        e
                    ))
                })?,
        );
        let rt2 = rt.clone();
        let _guard = rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::open(&path)
                .build()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
                rt: rt2,
            })
        })
    }

    /// Create a temporary in-memory database.
    #[staticmethod]
    fn temporary<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create runtime: {}",
                        e
                    ))
                })?,
        );
        let rt2 = rt.clone();
        let _guard = rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = Uni::temporary()
                .build()
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;
            Ok(AsyncDatabase {
                inner: Arc::new(uni),
                rt: rt2,
            })
        })
    }

    /// Return an AsyncDatabaseBuilder for advanced configuration.
    #[staticmethod]
    fn builder() -> AsyncDatabaseBuilder {
        AsyncDatabaseBuilder {
            uri: String::new(),
            mode: OpenMode::Temporary,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
        }
    }

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a Cypher query and return results.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::prepare_params(py, params)?;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = core::query_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }

    /// Execute a mutation query, returning affected row count.
    #[pyo3(signature = (cypher, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = convert::prepare_params(py, params)?;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        if rust_params.is_empty() {
            pyo3_async_runtimes::tokio::future_into_py(py, async move {
                core::execute_core(&inner, &cypher)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
            })
        } else {
            pyo3_async_runtimes::tokio::future_into_py(py, async move {
                core::execute_with_params_core(&inner, &cypher, rust_params)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
            })
        }
    }

    /// Explain the query plan without executing.
    fn explain<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let output = core::explain_core(&inner, &cypher)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

            Python::attach(|py| {
                let dict = PyDict::new(py);
                dict.set_item("plan_text", &output.plan_text)?;
                dict.set_item("warnings", &output.warnings)?;

                let cost_dict = PyDict::new(py);
                cost_dict.set_item("estimated_rows", output.cost_estimates.estimated_rows)?;
                cost_dict.set_item("estimated_cost", output.cost_estimates.estimated_cost)?;
                dict.set_item("cost_estimates", cost_dict)?;

                let index_usage = PyList::empty(py);
                for usage in &output.index_usage {
                    let usage_dict = PyDict::new(py);
                    usage_dict.set_item("label_or_type", &usage.label_or_type)?;
                    usage_dict.set_item("property", &usage.property)?;
                    usage_dict.set_item("index_type", &usage.index_type)?;
                    usage_dict.set_item("used", usage.used)?;
                    if let Some(reason) = &usage.reason {
                        usage_dict.set_item("reason", reason)?;
                    }
                    index_usage.append(usage_dict)?;
                }
                dict.set_item("index_usage", index_usage)?;

                let suggestions = PyList::empty(py);
                for suggestion in &output.suggestions {
                    let sug_dict = PyDict::new(py);
                    sug_dict.set_item("label_or_type", &suggestion.label_or_type)?;
                    sug_dict.set_item("property", &suggestion.property)?;
                    sug_dict.set_item("index_type", &suggestion.index_type)?;
                    sug_dict.set_item("reason", &suggestion.reason)?;
                    sug_dict.set_item("create_statement", &suggestion.create_statement)?;
                    suggestions.append(sug_dict)?;
                }
                dict.set_item("suggestions", suggestions)?;

                dict.into_py_any(py)
            })
        })
    }

    /// Profile query execution with operator-level statistics.
    fn profile<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (results, profile) = core::profile_core(&inner, &cypher)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

            Python::attach(|py| {
                let rows = convert::rows_to_py(py, results.rows)?;

                let profile_dict = PyDict::new(py);
                profile_dict.set_item("total_time_ms", profile.total_time_ms)?;
                profile_dict.set_item("peak_memory_bytes", profile.peak_memory_bytes)?;
                profile_dict.set_item("plan_text", &profile.explain.plan_text)?;

                let ops = PyList::empty(py);
                for op in &profile.runtime_stats {
                    let op_dict = PyDict::new(py);
                    op_dict.set_item("operator", &op.operator)?;
                    op_dict.set_item("actual_rows", op.actual_rows)?;
                    op_dict.set_item("time_ms", op.time_ms)?;
                    op_dict.set_item("memory_bytes", op.memory_bytes)?;
                    if let Some(hits) = op.index_hits {
                        op_dict.set_item("index_hits", hits)?;
                    }
                    if let Some(misses) = op.index_misses {
                        op_dict.set_item("index_misses", misses)?;
                    }
                    ops.append(op_dict)?;
                }
                profile_dict.set_item("operators", ops)?;

                let tuple = (rows, profile_dict.into_py_any(py)?);
                tuple.into_py_any(py)
            })
        })
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::flush_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    // ========================================================================
    // Transaction Methods
    // ========================================================================

    /// Begin a new async transaction.
    fn begin<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let rt = self.rt.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::begin_transaction_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Ok(AsyncTransaction {
                inner,
                rt,
                completed: false,
            })
        })
    }

    // ========================================================================
    // Schema Methods
    // ========================================================================

    /// Create a label.
    fn create_label<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_label_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Create an edge type.
    #[pyo3(signature = (name, from_labels=None, to_labels=None))]
    fn create_edge_type<'py>(
        &self,
        py: Python<'py>,
        name: String,
        from_labels: Option<Vec<String>>,
        to_labels: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let from = from_labels.unwrap_or_default();
        let to = to_labels.unwrap_or_default();
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_edge_type_core(&inner, &name, from, to)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Add a property to a label or edge type.
    fn add_property<'py>(
        &self,
        py: Python<'py>,
        label_or_type: String,
        name: String,
        data_type: String,
        nullable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::add_property_core(&inner, &label_or_type, &name, dt, nullable)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Check if a label exists.
    fn label_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::label_exists_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Check if an edge type exists.
    fn edge_type_exists<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::edge_type_exists_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// List all label names.
    fn list_labels<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_labels_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// List all edge type names.
    fn list_edge_types<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::list_edge_types_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Get detailed information about a label.
    fn get_label_info<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let info = core::get_label_info_core(&inner, &name)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

            Ok(info.map(|i| LabelInfo {
                name: i.name,
                count: i.count,
                properties: i
                    .properties
                    .into_iter()
                    .map(|p| PropertyInfo {
                        name: p.name,
                        data_type: p.data_type,
                        nullable: p.nullable,
                        is_indexed: p.is_indexed,
                    })
                    .collect(),
                indexes: i
                    .indexes
                    .into_iter()
                    .map(|idx| IndexInfo {
                        name: idx.name,
                        index_type: idx.index_type,
                        properties: idx.properties,
                        status: idx.status,
                    })
                    .collect(),
                constraints: i
                    .constraints
                    .into_iter()
                    .map(|c| ConstraintInfo {
                        name: c.name,
                        constraint_type: c.constraint_type,
                        properties: c.properties,
                        enabled: c.enabled,
                    })
                    .collect(),
            }))
        })
    }

    /// Get the full schema as a dictionary.
    fn get_schema<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        // Schema is synchronous, no need for async wrapper
        let schema = self.inner.get_schema();
        let dict = PyDict::new(py);

        let labels = PyDict::new(py);
        for (name, meta) in &schema.labels {
            let label_dict = PyDict::new(py);
            label_dict.set_item("id", meta.id)?;
            labels.set_item(name, label_dict)?;
        }
        dict.set_item("labels", labels)?;

        let edge_types = PyDict::new(py);
        for (name, meta) in &schema.edge_types {
            let et_dict = PyDict::new(py);
            et_dict.set_item("id", meta.id)?;
            edge_types.set_item(name, et_dict)?;
        }
        dict.set_item("edge_types", edge_types)?;

        Ok(dict.into())
    }

    /// Load schema from a JSON file.
    fn load_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::load_schema_core(&inner, &path)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Save schema to a JSON file.
    fn save_schema<'py>(&self, py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::save_schema_core(&inner, &path)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    // ========================================================================
    // Index Methods
    // ========================================================================

    /// Create a scalar index.
    fn create_scalar_index<'py>(
        &self,
        py: Python<'py>,
        label: String,
        property: String,
        index_type: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_scalar_index_core(&inner, &label, &property, &index_type)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Create a vector index.
    fn create_vector_index<'py>(
        &self,
        py: Python<'py>,
        label: String,
        property: String,
        metric: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::create_vector_index_core(&inner, &label, &property, &metric)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create an async session builder.
    fn session(&self) -> AsyncSessionBuilder {
        AsyncSessionBuilder {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            variables: HashMap::new(),
        }
    }

    // ========================================================================
    // Bulk Loading Methods
    // ========================================================================

    /// Create an async bulk writer builder.
    fn bulk_writer(&self) -> AsyncBulkWriterBuilder {
        AsyncBulkWriterBuilder {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: 10_000,
            async_indexes: false,
        }
    }

    /// Bulk insert vertices (convenience method).
    fn bulk_insert_vertices<'py>(
        &self,
        py: Python<'py>,
        label: String,
        properties_list: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_props = Vec::new();
        for p in properties_list {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_props.push(map);
        }

        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let vids = core::bulk_insert_vertices_core(&inner, &label, rust_props)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Ok(vids.into_iter().map(|v| v.as_u64()).collect::<Vec<_>>())
        })
    }

    /// Bulk insert edges (convenience method).
    fn bulk_insert_edges<'py>(
        &self,
        py: Python<'py>,
        edge_type: String,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_edges = Vec::new();
        for (src, dst, props) in edges {
            let mut map = HashMap::new();
            for (k, v) in props {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_edges.push((::uni_db::Vid::from(src), ::uni_db::Vid::from(dst), map));
        }

        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::bulk_insert_edges_core(&inner, &edge_type, rust_edges)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Create a query builder for parameterized queries.
    fn query_with(&self, cypher: String) -> AsyncQueryBuilder {
        AsyncQueryBuilder {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            cypher,
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
        }
    }

    /// Create a schema builder.
    fn schema(&self) -> AsyncSchemaBuilder {
        AsyncSchemaBuilder {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            pending_labels: Vec::new(),
            pending_edge_types: Vec::new(),
            pending_properties: Vec::new(),
            pending_indexes: Vec::new(),
        }
    }
}

// ============================================================================
// AsyncDatabaseBuilder
// ============================================================================

/// Async builder for creating and configuring an AsyncDatabase instance.
#[pyclass]
#[derive(Debug, Clone)]
pub struct AsyncDatabaseBuilder {
    uri: String,
    mode: OpenMode,
    hybrid_local: Option<String>,
    hybrid_remote: Option<String>,
    cache_size: Option<usize>,
    parallelism: Option<usize>,
}

#[pymethods]
impl AsyncDatabaseBuilder {
    /// Open or create a database at the given path.
    #[staticmethod]
    fn open(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Open,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
        }
    }

    /// Open an existing database.
    #[staticmethod]
    fn open_existing(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::OpenExisting,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
        }
    }

    /// Create a new database.
    #[staticmethod]
    fn create(path: &str) -> Self {
        Self {
            uri: path.to_string(),
            mode: OpenMode::Create,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
        }
    }

    /// Create a temporary database.
    #[staticmethod]
    fn temporary() -> Self {
        Self {
            uri: String::new(),
            mode: OpenMode::Temporary,
            hybrid_local: None,
            hybrid_remote: None,
            cache_size: None,
            parallelism: None,
        }
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

    /// Build and return the AsyncDatabase instance (returns awaitable).
    fn build<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let uri = self.uri.clone();
        let mode = self.mode;
        let hybrid_local = self.hybrid_local.clone();
        let hybrid_remote = self.hybrid_remote.clone();
        let cache_size = self.cache_size;
        let parallelism = self.parallelism;

        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                        "Failed to create runtime: {}",
                        e
                    ))
                })?,
        );
        let rt2 = rt.clone();
        let _guard = rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let uni = core::build_database_core(
                &uri,
                mode,
                hybrid_local.as_deref(),
                hybrid_remote.as_deref(),
                cache_size,
                parallelism,
            )
            .await
            .map_err(PyErr::new::<pyo3::exceptions::PyIOError, _>)?;

            Ok(AsyncDatabase {
                inner: Arc::new(uni),
                rt: rt2,
            })
        })
    }
}

// ============================================================================
// AsyncTransaction
// ============================================================================

/// An async database transaction.
#[pyclass]
pub struct AsyncTransaction {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
    completed: bool,
}

#[pymethods]
impl AsyncTransaction {
    /// Execute a query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        let rust_params = convert::prepare_params(py, params)?;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = core::query_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }

    /// Commit the transaction.
    fn commit<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        self.completed = true;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::commit_transaction_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Rollback the transaction.
    fn rollback<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        self.completed = true;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::rollback_transaction_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Async context manager support.
    fn __aenter__<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let obj: Py<PyAny> = slf.into_any().unbind();
        let rt_clone = {
            let borrowed = obj.bind(py).cast::<Self>().unwrap();
            borrowed.borrow().rt.clone()
        };
        let _guard = rt_clone.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(obj) })
    }

    /// Async context manager exit — commits on success, rolls back on error.
    fn __aexit__<'py>(
        &mut self,
        py: Python<'py>,
        exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.completed {
            // Already committed or rolled back
            let _guard = self.rt.enter();
            return pyo3_async_runtimes::tokio::future_into_py(py, async move {
                Ok(false) // Don't suppress exceptions
            });
        }
        self.completed = true;
        let inner = self.inner.clone();
        let has_exception = !exc_type.is_none(py);
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if has_exception {
                core::rollback_transaction_core(&inner)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            } else {
                core::commit_transaction_core(&inner)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            }
            Ok(false) // Don't suppress exceptions
        })
    }
}

// ============================================================================
// AsyncSession
// ============================================================================

/// Builder for async sessions.
#[pyclass]
#[derive(Clone)]
pub struct AsyncSessionBuilder {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
    variables: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl AsyncSessionBuilder {
    /// Set a session variable.
    fn set(&mut self, py: Python, key: String, value: Py<PyAny>) -> PyResult<()> {
        let json_val = convert::py_object_to_json(py, &value)?;
        self.variables.insert(key, json_val);
        Ok(())
    }

    /// Build the session.
    fn build(&self) -> PyResult<AsyncSession> {
        Ok(AsyncSession {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            variables: self.variables.clone(),
        })
    }
}

/// An async query session with scoped variables.
#[pyclass]
#[derive(Clone)]
pub struct AsyncSession {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
    variables: HashMap<String, serde_json::Value>,
}

impl AsyncSession {
    /// Build session params: creates a nested Value::Map under key "session".
    fn build_session_params(
        &self,
        py: Python,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<HashMap<String, ::uni_db::Value>> {
        let mut rust_params = HashMap::new();

        // Build session variable map
        let mut session_map = HashMap::new();
        for (k, v) in &self.variables {
            let val = ::uni_db::Value::from(v.clone());
            session_map.insert(k.clone(), val);
        }
        rust_params.insert("session".to_string(), ::uni_db::Value::Map(session_map));

        // Add query params (may override if same key)
        if let Some(p) = params {
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                rust_params.insert(k, val);
            }
        }

        Ok(rust_params)
    }
}

#[pymethods]
impl AsyncSession {
    /// Execute a query with session variables.
    #[pyo3(signature = (cypher, params=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        cypher: String,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = self.build_session_params(py, params)?;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = core::query_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }

    /// Execute a mutation query.
    fn execute<'py>(&self, py: Python<'py>, cypher: String) -> PyResult<Bound<'py, PyAny>> {
        let rust_params = self.build_session_params(py, None)?;
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::execute_with_params_core(&inner, &cypher, rust_params)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }

    /// Get a session variable value.
    fn get(&self, py: Python, key: &str) -> PyResult<Option<Py<PyAny>>> {
        match self.variables.get(key) {
            Some(v) => Ok(Some(convert::json_value_to_py(py, v)?)),
            None => Ok(None),
        }
    }
}

// ============================================================================
// AsyncBulkWriter
// ============================================================================

/// Builder for async bulk writer.
#[pyclass]
#[derive(Clone)]
pub struct AsyncBulkWriterBuilder {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
    defer_vector_indexes: bool,
    defer_scalar_indexes: bool,
    batch_size: usize,
    async_indexes: bool,
}

#[pymethods]
impl AsyncBulkWriterBuilder {
    /// Defer vector index building until commit.
    fn defer_vector_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_vector_indexes = defer;
        slf
    }

    /// Defer scalar index building until commit.
    fn defer_scalar_indexes(mut slf: PyRefMut<'_, Self>, defer: bool) -> PyRefMut<'_, Self> {
        slf.defer_scalar_indexes = defer;
        slf
    }

    /// Set batch size for flushing to storage.
    fn batch_size(mut slf: PyRefMut<'_, Self>, size: usize) -> PyRefMut<'_, Self> {
        slf.batch_size = size;
        slf
    }

    /// Build indexes asynchronously after commit.
    fn async_indexes(mut slf: PyRefMut<'_, Self>, async_: bool) -> PyRefMut<'_, Self> {
        slf.async_indexes = async_;
        slf
    }

    /// Build the AsyncBulkWriter.
    fn build(&self) -> PyResult<AsyncBulkWriter> {
        Ok(AsyncBulkWriter {
            inner: self.inner.clone(),
            rt: self.rt.clone(),
            stats: Arc::new(std::sync::Mutex::new(BulkStats::default())),
            aborted: false,
            committed: false,
        })
    }
}

/// Async bulk writer for high-throughput data ingestion.
#[pyclass]
pub struct AsyncBulkWriter {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
    stats: Arc<std::sync::Mutex<BulkStats>>,
    aborted: bool,
    committed: bool,
}

#[pymethods]
impl AsyncBulkWriter {
    /// Insert vertices in bulk.
    fn insert_vertices<'py>(
        &mut self,
        py: Python<'py>,
        label: String,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        let mut rust_props = Vec::new();
        for v in vertices {
            let mut map = HashMap::new();
            for (k, val) in v {
                let value = convert::py_object_to_value(py, &val)?;
                map.insert(k, serde_json::Value::from(value));
            }
            rust_props.push(map);
        }

        let inner = self.inner.clone();
        let stats = self.stats.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let vids = core::bulk_insert_vertices_core(&inner, &label, rust_props)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            if let Ok(mut s) = stats.lock() {
                s.vertices_inserted += vids.len();
            }
            Ok(vids.into_iter().map(|v| v.as_u64()).collect::<Vec<_>>())
        })
    }

    /// Insert edges in bulk.
    fn insert_edges<'py>(
        &mut self,
        py: Python<'py>,
        edge_type: String,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        let edge_count = edges.len();
        let mut rust_edges = Vec::new();
        for (src, dst, props) in edges {
            let mut map = HashMap::new();
            for (k, v) in props {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_edges.push((::uni_db::Vid::from(src), ::uni_db::Vid::from(dst), map));
        }

        let inner = self.inner.clone();
        let stats = self.stats.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::bulk_insert_edges_core(&inner, &edge_type, rust_edges)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            if let Ok(mut s) = stats.lock() {
                s.edges_inserted += edge_count;
            }
            Ok(())
        })
    }

    /// Commit all pending data.
    fn commit<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }
        self.committed = true;
        let stats = self.stats.clone();
        let inner = self.inner.clone();
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::flush_core(&inner)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            let final_stats = stats.lock().unwrap().clone();
            Ok(final_stats)
        })
    }

    /// Abort bulk loading.
    fn abort(&mut self) -> PyResult<()> {
        if self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Cannot abort: already committed",
            ));
        }
        self.aborted = true;
        Ok(())
    }
}

// ============================================================================
// AsyncQueryBuilder
// ============================================================================

/// Async builder for constructing and executing parameterized queries.
#[pyclass]
pub struct AsyncQueryBuilder {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
    cypher: String,
    params: HashMap<String, Py<PyAny>>,
    timeout_secs: Option<f64>,
    max_memory: Option<usize>,
}

#[pymethods]
impl AsyncQueryBuilder {
    /// Bind a parameter to the query.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Bind multiple parameters from a dictionary.
    fn params(&mut self, params: Bound<'_, PyDict>) {
        for (k, v) in params {
            if let Ok(key) = k.extract::<String>() {
                self.params.insert(key, v.into());
            }
        }
    }

    /// Set maximum execution time in seconds.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Set maximum memory for this query in bytes.
    fn max_memory(mut slf: PyRefMut<'_, Self>, bytes: usize) -> PyRefMut<'_, Self> {
        slf.max_memory = Some(bytes);
        slf
    }

    /// Execute the query and fetch all results (returns awaitable).
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let inner = self.inner.clone();
        let cypher = self.cypher.clone();
        let timeout_secs = self.timeout_secs;
        let max_memory = self.max_memory;
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows =
                core::query_builder_core(&inner, &cypher, rust_params, timeout_secs, max_memory)
                    .await
                    .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
            Python::attach(|py| convert::rows_to_py(py, rows.rows))
        })
    }
}

// ============================================================================
// AsyncSchemaBuilder, AsyncLabelBuilder, AsyncEdgeTypeBuilder
// ============================================================================

/// Async builder for defining and modifying the graph schema.
#[pyclass]
#[derive(Clone)]
pub struct AsyncSchemaBuilder {
    inner: Arc<Uni>,
    rt: Arc<tokio::runtime::Runtime>,
    pending_labels: Vec<String>,
    pending_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    pending_properties: Vec<(String, String, uni_common::core::schema::DataType, bool)>,
    pending_indexes: Vec<uni_common::core::schema::IndexDefinition>,
}

#[pymethods]
impl AsyncSchemaBuilder {
    /// Start defining a new label.
    fn label(&self, name: &str) -> PyResult<AsyncLabelBuilder> {
        Ok(AsyncLabelBuilder {
            parent_inner: self.inner.clone(),
            parent_rt: self.rt.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
            properties: Vec::new(),
            indexes: Vec::new(),
        })
    }

    /// Start defining a new edge type.
    fn edge_type(
        &self,
        name: &str,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
    ) -> PyResult<AsyncEdgeTypeBuilder> {
        Ok(AsyncEdgeTypeBuilder {
            parent_inner: self.inner.clone(),
            parent_rt: self.rt.clone(),
            parent_labels: self.pending_labels.clone(),
            parent_edge_types: self.pending_edge_types.clone(),
            parent_properties: self.pending_properties.clone(),
            parent_indexes: self.pending_indexes.clone(),
            name: name.to_string(),
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
        let _guard = self.rt.enter();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            core::apply_schema_core(&inner, &labels, &edge_types, &properties, &indexes)
                .await
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        })
    }
}

/// Async builder for defining a label with its properties and indexes.
#[pyclass]
#[derive(Clone)]
pub struct AsyncLabelBuilder {
    parent_inner: Arc<Uni>,
    parent_rt: Arc<tokio::runtime::Runtime>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, uni_common::core::schema::DataType, bool)>,
    parent_indexes: Vec<uni_common::core::schema::IndexDefinition>,
    name: String,
    properties: Vec<(String, uni_common::core::schema::DataType, bool)>,
    indexes: Vec<uni_common::core::schema::IndexDefinition>,
}

#[pymethods]
impl AsyncLabelBuilder {
    /// Add a required property to this label.
    fn property(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this label.
    fn property_nullable(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        slf.properties.push((name, dt, true));
        Ok(slf)
    }

    /// Add a vector property.
    fn vector(mut slf: PyRefMut<'_, Self>, name: String, dimensions: usize) -> PyRefMut<'_, Self> {
        slf.properties.push((
            name,
            uni_common::core::schema::DataType::Vector { dimensions },
            false,
        ));
        slf
    }

    /// Add an index on a property.
    fn index(
        mut slf: PyRefMut<'_, Self>,
        property: String,
        index_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let label = slf.name.clone();
        let idx = core::create_index_definition(&label, &property, &index_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        slf.indexes.push(idx);
        Ok(slf)
    }

    /// Finish this label and return to AsyncSchemaBuilder.
    fn done(&self) -> PyResult<AsyncSchemaBuilder> {
        let mut labels = self.parent_labels.clone();
        labels.push(self.name.clone());

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        let mut indexes = self.parent_indexes.clone();
        indexes.extend(self.indexes.clone());

        Ok(AsyncSchemaBuilder {
            inner: self.parent_inner.clone(),
            rt: self.parent_rt.clone(),
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
#[pyclass]
#[derive(Clone)]
pub struct AsyncEdgeTypeBuilder {
    parent_inner: Arc<Uni>,
    parent_rt: Arc<tokio::runtime::Runtime>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, uni_common::core::schema::DataType, bool)>,
    parent_indexes: Vec<uni_common::core::schema::IndexDefinition>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    properties: Vec<(String, uni_common::core::schema::DataType, bool)>,
}

#[pymethods]
impl AsyncEdgeTypeBuilder {
    /// Add a required property to this edge type.
    fn property(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        slf.properties.push((name, dt, false));
        Ok(slf)
    }

    /// Add a nullable property to this edge type.
    fn property_nullable(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        data_type: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let dt = core::parse_data_type(&data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        slf.properties.push((name, dt, true));
        Ok(slf)
    }

    /// Finish this edge type and return to AsyncSchemaBuilder.
    fn done(&self) -> PyResult<AsyncSchemaBuilder> {
        let mut edge_types = self.parent_edge_types.clone();
        edge_types.push((
            self.name.clone(),
            self.from_labels.clone(),
            self.to_labels.clone(),
        ));

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        Ok(AsyncSchemaBuilder {
            inner: self.parent_inner.clone(),
            rt: self.parent_rt.clone(),
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
