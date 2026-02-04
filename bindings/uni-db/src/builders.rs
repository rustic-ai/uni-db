// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Builder types for schema, labels, edge types, sessions, and bulk writers.

use crate::convert;
use crate::core::{self, OpenMode};
use crate::types::BulkStats;
use ::uni_db::Uni;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::schema::{DataType, IndexDefinition};

// ============================================================================
// DatabaseBuilder
// ============================================================================

/// Builder for creating and configuring a Database instance.
#[pyclass]
#[derive(Debug, Clone)]
pub struct DatabaseBuilder {
    pub(crate) uri: String,
    pub(crate) mode: OpenMode,
    pub(crate) hybrid_local: Option<String>,
    pub(crate) hybrid_remote: Option<String>,
    pub(crate) cache_size: Option<usize>,
    pub(crate) parallelism: Option<usize>,
}

#[pymethods]
impl DatabaseBuilder {
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

    /// Open an existing database. Fails if it does not exist.
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

    /// Create a new database. Fails if it already exists.
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

    /// Create a temporary database that is deleted when dropped.
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

    /// Configure hybrid storage with local metadata and remote data.
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

    /// Set query parallelism (number of worker threads).
    fn parallelism(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.parallelism = Some(n);
        slf
    }

    /// Build and return the Database instance.
    fn build(&self) -> PyResult<crate::sync_api::Database> {
        let uni = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::build_database_core(
                &self.uri,
                self.mode,
                self.hybrid_local.as_deref(),
                self.hybrid_remote.as_deref(),
                self.cache_size,
                self.parallelism,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyIOError, _>)?;

        Ok(crate::sync_api::Database {
            inner: Arc::new(uni),
        })
    }
}

// ============================================================================
// QueryBuilder
// ============================================================================

/// Builder for constructing and executing parameterized queries.
#[pyclass]
pub struct QueryBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_memory: Option<usize>,
}

#[pymethods]
impl QueryBuilder {
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

    /// Execute the query and fetch all results.
    fn fetch_all(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let mut rust_params = HashMap::new();
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            rust_params.insert(k.clone(), val);
        }

        let rows = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_builder_core(
                &self.inner,
                &self.cypher,
                rust_params,
                self.timeout_secs,
                self.max_memory,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        convert::rows_to_py(py, rows.rows)
    }
}

// ============================================================================
// SchemaBuilder, LabelBuilder, EdgeTypeBuilder
// ============================================================================

/// Builder for defining and modifying the graph schema.
#[pyclass]
#[derive(Clone)]
pub struct SchemaBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) pending_labels: Vec<String>,
    pub(crate) pending_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    pub(crate) pending_properties: Vec<(String, String, DataType, bool)>,
    pub(crate) pending_indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl SchemaBuilder {
    /// Start defining a new label.
    fn label(&self, name: &str) -> PyResult<LabelBuilder> {
        Ok(LabelBuilder {
            parent_inner: self.inner.clone(),
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
    ) -> PyResult<EdgeTypeBuilder> {
        Ok(EdgeTypeBuilder {
            parent_inner: self.inner.clone(),
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

    /// Apply all pending schema changes.
    fn apply(&self) -> PyResult<()> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::apply_schema_core(
                &self.inner,
                &self.pending_labels,
                &self.pending_edge_types,
                &self.pending_properties,
                &self.pending_indexes,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;
        Ok(())
    }
}

/// Builder for defining a label with its properties and indexes.
#[pyclass]
#[derive(Clone)]
pub struct LabelBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, DataType, bool)>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    properties: Vec<(String, DataType, bool)>,
    indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl LabelBuilder {
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

    /// Add a vector property (shorthand for vector type + index).
    fn vector(mut slf: PyRefMut<'_, Self>, name: String, dimensions: usize) -> PyRefMut<'_, Self> {
        slf.properties
            .push((name, DataType::Vector { dimensions }, false));
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

    /// Finish this label and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
        let mut labels = self.parent_labels.clone();
        labels.push(self.name.clone());

        let mut properties = self.parent_properties.clone();
        for (prop_name, dt, nullable) in &self.properties {
            properties.push((self.name.clone(), prop_name.clone(), dt.clone(), *nullable));
        }

        let mut indexes = self.parent_indexes.clone();
        indexes.extend(self.indexes.clone());

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: labels,
            pending_edge_types: self.parent_edge_types.clone(),
            pending_properties: properties,
            pending_indexes: indexes,
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self) -> PyResult<()> {
        self.done()?.apply()
    }
}

/// Builder for defining an edge type with its properties.
#[pyclass]
#[derive(Clone)]
pub struct EdgeTypeBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<String>,
    parent_edge_types: Vec<(String, Vec<String>, Vec<String>)>,
    parent_properties: Vec<(String, String, DataType, bool)>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    properties: Vec<(String, DataType, bool)>,
}

#[pymethods]
impl EdgeTypeBuilder {
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

    /// Finish this edge type and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
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

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: self.parent_labels.clone(),
            pending_edge_types: edge_types,
            pending_properties: properties,
            pending_indexes: self.parent_indexes.clone(),
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self) -> PyResult<()> {
        self.done()?.apply()
    }
}

// ============================================================================
// SessionBuilder and Session
// ============================================================================

/// Builder for creating query sessions with scoped variables.
#[pyclass]
#[derive(Clone)]
pub struct SessionBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) variables: HashMap<String, serde_json::Value>,
}

#[pymethods]
impl SessionBuilder {
    /// Set a session variable.
    fn set(&mut self, py: Python, key: String, value: Py<PyAny>) -> PyResult<()> {
        let json_val = convert::py_object_to_json(py, &value)?;
        self.variables.insert(key, json_val);
        Ok(())
    }

    /// Build the session.
    fn build(&self) -> PyResult<Session> {
        Ok(Session {
            inner: self.inner.clone(),
            variables: self.variables.clone(),
        })
    }
}

/// A query session with scoped variables.
#[pyclass]
#[derive(Clone)]
pub struct Session {
    pub(crate) inner: Arc<Uni>,
    pub(crate) variables: HashMap<String, serde_json::Value>,
}

impl Session {
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
impl Session {
    /// Execute a query with session variables available.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let rust_params = self.build_session_params(py, params)?;

        let rows = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::query_core(&self.inner, cypher, rust_params))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        convert::rows_to_py(py, rows.rows)
    }

    /// Execute a mutation query, returning affected row count.
    fn execute(&self, py: Python, cypher: &str) -> PyResult<usize> {
        let rust_params = self.build_session_params(py, None)?;

        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::execute_with_params_core(
                &self.inner,
                cypher,
                rust_params,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)
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
// BulkWriterBuilder and BulkWriter
// ============================================================================

/// Builder for configuring bulk data loading.
#[pyclass]
#[derive(Clone)]
pub struct BulkWriterBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) defer_vector_indexes: bool,
    pub(crate) defer_scalar_indexes: bool,
    pub(crate) batch_size: usize,
    pub(crate) async_indexes: bool,
}

#[pymethods]
impl BulkWriterBuilder {
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

    /// Build the BulkWriter.
    fn build(&self) -> PyResult<BulkWriter> {
        Ok(BulkWriter {
            inner: self.inner.clone(),
            stats: BulkStats::default(),
            aborted: false,
            committed: false,
        })
    }
}

/// Bulk writer for high-throughput data ingestion.
#[pyclass]
pub struct BulkWriter {
    inner: Arc<Uni>,
    stats: BulkStats,
    aborted: bool,
    committed: bool,
}

#[pymethods]
impl BulkWriter {
    /// Insert vertices in bulk, returning allocated VIDs.
    fn insert_vertices(
        &mut self,
        py: Python,
        label: &str,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<u64>> {
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

        let vids = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::bulk_insert_vertices_core(
                &self.inner,
                label,
                rust_props,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.stats.vertices_inserted += vids.len();
        Ok(vids.into_iter().map(|v| v.as_u64()).collect())
    }

    /// Insert edges in bulk.
    fn insert_edges(
        &mut self,
        py: Python,
        edge_type: &str,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<()> {
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

        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::bulk_insert_edges_core(
                &self.inner,
                edge_type,
                rust_edges,
            ))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.stats.edges_inserted += edge_count;
        Ok(())
    }

    /// Commit all pending data and rebuild indexes.
    fn commit(&mut self) -> PyResult<BulkStats> {
        if self.aborted || self.committed {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "BulkWriter already completed",
            ));
        }

        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(core::flush_core(&self.inner))
            .map_err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>)?;

        self.committed = true;
        Ok(self.stats.clone())
    }

    /// Abort bulk loading and discard uncommitted changes.
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
