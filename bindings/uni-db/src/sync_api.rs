// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synchronous Python API — `Database` and `Transaction`.

use crate::builders::{BulkWriterBuilder, QueryBuilder, SchemaBuilder, SessionBuilder};
use crate::convert;
use crate::core;
use crate::types::*;
use ::uni_db::Uni;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;

// ============================================================================
// Transaction
// ============================================================================

/// A database transaction for atomic operations.
#[pyclass]
pub struct Transaction {
    pub(crate) inner: Arc<Uni>,
    pub(crate) completed: bool,
}

#[pymethods]
impl Transaction {
    /// Execute a query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        let rust_params = convert::prepare_params(py, params)?;

        let rows = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_core(&self.inner, cypher, rust_params))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        convert::rows_to_py(py, rows.rows)
    }

    /// Commit the transaction.
    fn commit(&mut self) -> PyResult<()> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::commit_transaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        self.completed = true;
        Ok(())
    }

    /// Rollback the transaction.
    fn rollback(&mut self) -> PyResult<()> {
        if self.completed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction already completed",
            ));
        }
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::rollback_transaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        self.completed = true;
        Ok(())
    }
}

// ============================================================================
// Database (main entry point)
// ============================================================================

/// Main entry point for the Uni embedded graph database.
#[pyclass]
pub struct Database {
    pub(crate) inner: Arc<Uni>,
}

#[pymethods]
impl Database {
    /// Create or open a database at the given path.
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { Uni::open(path).build().await })
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(e.to_string()))?;

        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    // ========================================================================
    // Query Methods
    // ========================================================================

    /// Execute a Cypher query and return results.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let rust_params = convert::prepare_params(py, params)?;

        let rows = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_core(&self.inner, cypher, rust_params))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        convert::rows_to_py(py, rows.rows)
    }

    /// Create a query builder for parameterized queries.
    fn query_with(&self, cypher: &str) -> QueryBuilder {
        QueryBuilder {
            inner: self.inner.clone(),
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
        }
    }

    /// Execute a mutation query, returning affected row count.
    #[pyo3(signature = (cypher, params=None))]
    fn execute(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<usize> {
        let rust_params = convert::prepare_params(py, params)?;
        if rust_params.is_empty() {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::execute_core(&self.inner, cypher))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        } else {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::execute_with_params_core(
                    &self.inner,
                    cypher,
                    rust_params,
                ))
                .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
        }
    }

    /// Explain the query plan without executing.
    fn explain(&self, py: Python, cypher: &str) -> PyResult<Py<PyAny>> {
        let output = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::explain_core(&self.inner, cypher))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

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

        Ok(dict.into())
    }

    /// Profile query execution with operator-level statistics.
    fn profile(&self, py: Python, cypher: &str) -> PyResult<(Vec<Py<PyAny>>, Py<PyAny>)> {
        let (results, profile) = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::profile_core(&self.inner, cypher))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

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

        Ok((rows, profile_dict.into()))
    }

    // ========================================================================
    // Transaction Methods
    // ========================================================================

    /// Begin a new transaction.
    fn begin(&self) -> PyResult<Transaction> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::begin_transaction_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(Transaction {
            inner: self.inner.clone(),
            completed: false,
        })
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::flush_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    // ========================================================================
    // Schema Methods
    // ========================================================================

    /// Create a schema builder.
    fn schema(&self) -> SchemaBuilder {
        SchemaBuilder {
            inner: self.inner.clone(),
            pending_labels: Vec::new(),
            pending_edge_types: Vec::new(),
            pending_properties: Vec::new(),
            pending_indexes: Vec::new(),
        }
    }

    /// Create a label.
    fn create_label(&self, name: &str) -> PyResult<u16> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_label_core(&self.inner, name))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Create an edge type.
    #[pyo3(signature = (name, from_labels=None, to_labels=None))]
    fn create_edge_type(
        &self,
        name: &str,
        from_labels: Option<Vec<String>>,
        to_labels: Option<Vec<String>>,
    ) -> PyResult<u32> {
        let from = from_labels.unwrap_or_default();
        let to = to_labels.unwrap_or_default();
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_edge_type_core(&self.inner, name, from, to))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Add a property to a label or edge type.
    fn add_property(
        &self,
        label_or_type: &str,
        name: &str,
        data_type: &str,
        nullable: bool,
    ) -> PyResult<()> {
        let dt = core::parse_data_type(data_type)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::add_property_core(
                &self.inner,
                label_or_type,
                name,
                dt,
                nullable,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Check if a label exists.
    fn label_exists(&self, name: &str) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::label_exists_core(&self.inner, name))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Check if an edge type exists.
    fn edge_type_exists(&self, name: &str) -> PyResult<bool> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::edge_type_exists_core(&self.inner, name))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Get all label names.
    fn list_labels(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_labels_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Get all edge type names.
    fn list_edge_types(&self) -> PyResult<Vec<String>> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::list_edge_types_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Get detailed information about a label.
    fn get_label_info(&self, name: &str) -> PyResult<Option<LabelInfo>> {
        let info = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::get_label_info_core(&self.inner, name))
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
    }

    /// Get the full schema as a dictionary.
    fn get_schema(&self, py: Python) -> PyResult<Py<PyAny>> {
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
    fn load_schema(&self, path: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::load_schema_core(&self.inner, path))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    /// Save schema to a JSON file.
    fn save_schema(&self, path: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::save_schema_core(&self.inner, path))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
    }

    // ========================================================================
    // Index Methods
    // ========================================================================

    /// Create a scalar index on a property.
    fn create_scalar_index(&self, label: &str, property: &str, index_type: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_scalar_index_core(
                &self.inner,
                label,
                property,
                index_type,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    /// Create a vector index on a property.
    fn create_vector_index(&self, label: &str, property: &str, metric: &str) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::create_vector_index_core(
                &self.inner,
                label,
                property,
                metric,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create a session builder.
    fn session(&self) -> SessionBuilder {
        SessionBuilder {
            inner: self.inner.clone(),
            variables: HashMap::new(),
        }
    }

    // ========================================================================
    // Bulk Loading Methods
    // ========================================================================

    /// Create a bulk writer builder.
    fn bulk_writer(&self) -> BulkWriterBuilder {
        BulkWriterBuilder {
            inner: self.inner.clone(),
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: 10_000,
            async_indexes: false,
        }
    }

    /// Bulk insert vertices (legacy API, prefer bulk_writer()).
    fn bulk_insert_vertices(
        &self,
        py: Python,
        label: &str,
        properties_list: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<u64>> {
        let mut rust_props = Vec::new();
        for p in properties_list {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_props.push(map);
        }

        let vids = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::bulk_insert_vertices_core(
                &self.inner,
                label,
                rust_props,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(vids.into_iter().map(|v| v.as_u64()).collect())
    }

    /// Bulk insert edges (legacy API, prefer bulk_writer()).
    fn bulk_insert_edges(
        &self,
        py: Python,
        edge_type: &str,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<()> {
        let mut rust_edges = Vec::new();
        for (src, dst, p) in edges {
            let mut map = HashMap::new();
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                map.insert(k, serde_json::Value::from(val));
            }
            rust_edges.push((::uni_db::Vid::from(src), ::uni_db::Vid::from(dst), map));
        }

        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::bulk_insert_edges_core(
                &self.inner,
                edge_type,
                rust_edges,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        Ok(())
    }
}
