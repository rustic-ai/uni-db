// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Builder types for schema, labels, edge types, sessions, and bulk writers.

use crate::convert;
use crate::core::{self, OpenMode};
use crate::types::BulkStats;
use ::uni_db::Uni;
use pyo3::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use uni_common::core::schema::{DataType, IndexDefinition};

/// M8 F2 helper: convert a list of grant strings into a
/// [`uni_plugin::CapabilitySet`]. Empty / `None` → default decorator set.
///
/// Grant strings are parsed by [`uni_plugin::Capability::parse_grant`] — the
/// single source of truth shared with the strict loader path and the CLI. On this
/// best-effort decorator surface, unrecognized / non-grantable strings are
/// silently dropped (stricter validation lives in the Python source-load path).
pub(crate) fn build_capability_set(grants: Option<Vec<String>>) -> uni_plugin::CapabilitySet {
    use uni_plugin::Capability;
    let mut cap_set = uni_plugin::CapabilitySet::new();
    if let Some(list) = grants {
        for g in list {
            // Best-effort: ignore anything not grantable from a bare string.
            if let Ok(cap) = Capability::parse_grant(&g) {
                cap_set.insert(cap);
            }
        }
    } else {
        // Default for decorator surface: enable scalar / aggregate /
        // procedure registration. Users opt into network / fs /
        // host-query grants explicitly.
        cap_set.insert(Capability::ScalarFn);
        cap_set.insert(Capability::AggregateFn);
        cap_set.insert(Capability::Procedure);
    }
    cap_set
}

/// Like [`build_capability_set`] but rejects unknown grant names.
///
/// Used by the Rhai loader (sync and async), which validates grants strictly.
/// Grants default to `ScalarFn` / `AggregateFn` / `Procedure` when `None`.
/// Grant strings are parsed by [`uni_plugin::Capability::parse_grant`] — the
/// single source of truth shared with the lenient path and the CLI — so any
/// grantable capability (buckets A + B), including `Algorithm` / `GraphCompute`,
/// is accepted with its broadest attenuation; tightening is host-side work.
///
/// # Errors
/// Returns a `ValueError` if `grants` contains a name that is unknown, a resource
/// quota (needs a value in the manifest), or an internal / first-party capability.
pub(crate) fn build_capability_set_strict(
    grants: Option<Vec<String>>,
) -> PyResult<uni_plugin::CapabilitySet> {
    use uni_plugin::Capability;
    let mut cap_set = uni_plugin::CapabilitySet::new();
    let grants = grants.unwrap_or_else(|| {
        vec![
            "ScalarFn".to_owned(),
            "AggregateFn".to_owned(),
            "Procedure".to_owned(),
        ]
    });
    for g in &grants {
        let cap = Capability::parse_grant(g)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        cap_set.insert(cap);
    }
    Ok(cap_set)
}

/// M8 F2 helper: serialize a [`uni_plugin_pyo3::LoadOutcome`] into a
/// metadata dict matching `Database::load_rhai_plugin`'s return shape.
pub(crate) fn load_outcome_to_pydict(
    py: Python<'_>,
    outcome: &uni_plugin_pyo3::LoadOutcome,
) -> PyResult<Py<PyAny>> {
    use pyo3::types::PyDict;
    let dict = PyDict::new(py);
    dict.set_item("plugin_id", outcome.plugin_id.as_str())?;
    dict.set_item("version", outcome.version.clone())?;
    dict.set_item("scalars_registered", outcome.scalars_registered.clone())?;
    dict.set_item(
        "aggregates_registered",
        outcome.aggregates_registered.clone(),
    )?;
    dict.set_item(
        "procedures_registered",
        outcome.procedures_registered.clone(),
    )?;
    dict.set_item(
        "algorithms_registered",
        outcome.algorithms_registered.clone(),
    )?;
    let denied: Vec<String> = outcome
        .denied_capabilities
        .iter()
        .map(|c| format!("{c:?}"))
        .collect();
    dict.set_item("denied_capabilities", denied)?;
    Ok(dict.into())
}

// ============================================================================
// DatabaseBuilder
// ============================================================================

/// Builder for creating and configuring a Uni instance.
#[pyclass(name = "UniBuilder", from_py_object)]
#[derive(Debug, Clone)]
pub struct DatabaseBuilder {
    pub(crate) uri: String,
    pub(crate) mode: OpenMode,
    pub(crate) hybrid_local: Option<String>,
    pub(crate) hybrid_remote: Option<String>,
    pub(crate) cache_size: Option<usize>,
    pub(crate) parallelism: Option<usize>,
    pub(crate) schema_file: Option<String>,
    pub(crate) xervo_catalog_json: Option<String>,
    pub(crate) xervo_catalog_file: Option<String>,
    pub(crate) xervo_runtime: Option<crate::types::PyModelRuntime>,
    pub(crate) cloud_config: Option<uni_common::CloudStorageConfig>,
    pub(crate) uni_config: Option<uni_common::UniConfig>,
    pub(crate) read_only: bool,
    pub(crate) write_lease: Option<crate::types::PyWriteLease>,
    pub(crate) skip_invalid_locy_rules: bool,
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

    /// Create a temporary database that is deleted when dropped.
    #[staticmethod]
    pub fn temporary() -> Self {
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
        config: HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.cloud_config = Some(convert::extract_cloud_config(py, &config)?);
        Ok(slf)
    }

    /// Configure database options (query_timeout, max_query_memory, etc.).
    fn config(
        mut slf: PyRefMut<'_, Self>,
        config: HashMap<String, Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let py = slf.py();
        slf.uni_config = Some(convert::extract_uni_config(py, &config)?);
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

    /// Enforce strict schema mode (default false).
    ///
    /// When enabled, writes that reference labels or edge types not declared
    /// in the schema are rejected with an error.
    fn strict_schema(mut slf: PyRefMut<'_, Self>, enabled: bool) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .strict_schema = enabled;
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

    /// Phase 4b — cap on total fork count (Active + Pending + Tombstoned).
    /// `None` means unbounded.
    fn max_forks(mut slf: PyRefMut<'_, Self>, cap: Option<usize>) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .max_forks = cap;
        slf
    }

    /// Phase 4b — default TTL applied to forks when the user does not
    /// supply one via `session.fork(name).ttl(...)`.
    fn fork_default_ttl<'py>(
        mut slf: PyRefMut<'py, Self>,
        ttl: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let bound = ttl.bind(py);
        let dur = if bound.is_none() {
            None
        } else {
            Some(convert::py_timedelta_to_duration(bound)?)
        };
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .fork_default_ttl = dur;
        Ok(slf)
    }

    /// Phase 4b — how often the background TTL sweeper polls the registry.
    fn fork_sweeper_interval<'py>(
        mut slf: PyRefMut<'py, Self>,
        interval: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let dur = convert::py_timedelta_to_duration(interval.bind(py))?;
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .fork_sweeper_interval = dur;
        Ok(slf)
    }

    /// Phase 4b — skip spawning the TTL sweeper. Tests that race against
    /// TTL should set this to `True`.
    fn disable_fork_sweeper(mut slf: PyRefMut<'_, Self>, disabled: bool) -> PyRefMut<'_, Self> {
        slf.uni_config
            .get_or_insert_with(uni_common::UniConfig::default)
            .disable_fork_sweeper = disabled;
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

    /// Build and return the Database instance.
    fn build(&self, py: Python<'_>) -> PyResult<crate::sync_api::Database> {
        // `Custom` is read-back only: the Rust variant wraps a
        // `Box<dyn WriteLeaseProvider>` that Python cannot supply. It reaches a
        // `PyWriteLease` only by being read off a database configured in Rust,
        // so handing one back to a builder is a caller error rather than a
        // conversion.
        let rust_write_lease = match self.write_lease.as_ref() {
            None => None,
            Some(wl) => Some(match &wl.variant {
                crate::types::WriteLeaseVariant::Local => {
                    ::uni_db::api::multi_agent::WriteLease::Local
                }
                crate::types::WriteLeaseVariant::DynamoDB { table } => {
                    ::uni_db::api::multi_agent::WriteLease::DynamoDB {
                        table: table.clone(),
                    }
                }
                crate::types::WriteLeaseVariant::Custom => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "WriteLease.CUSTOM cannot be configured from Python: a custom \
                         lease provider is a Rust trait object. It is only ever read \
                         back from a database configured in Rust.",
                    ));
                }
            }),
        };
        let uni = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(core::build_database_core(
                    &self.uri,
                    self.mode,
                    self.hybrid_local.as_deref(),
                    self.hybrid_remote.as_deref(),
                    self.cache_size,
                    self.parallelism,
                    self.schema_file.as_deref(),
                    self.xervo_catalog_json.as_deref(),
                    self.xervo_catalog_file.as_deref(),
                    self.cloud_config.clone(),
                    self.uni_config.clone(),
                    self.read_only,
                    rust_write_lease,
                    self.skip_invalid_locy_rules,
                    self.xervo_runtime.as_ref().map(|rt| rt.inner.clone()),
                ))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

        Ok(crate::sync_api::Database {
            inner: Arc::new(uni),
        })
    }
}

// ============================================================================
// SchemaBuilder, LabelBuilder, EdgeTypeBuilder
// ============================================================================

/// Builder for defining and modifying the graph schema.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct SchemaBuilder {
    pub(crate) inner: Arc<Uni>,
    pub(crate) pending_labels: Vec<crate::core::PendingLabel>,
    pub(crate) pending_edge_types: Vec<crate::core::PendingEdgeType>,
    pub(crate) pending_properties: Vec<crate::core::PendingProperty>,
    pub(crate) pending_indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl SchemaBuilder {
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
    fn label(&self, name: &str, description: Option<String>) -> PyResult<LabelBuilder> {
        Ok(LabelBuilder {
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
    ) -> PyResult<EdgeTypeBuilder> {
        Ok(EdgeTypeBuilder {
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

    /// Apply all pending schema changes.
    fn apply(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(core::apply_schema_core(
                &self.inner,
                &self.pending_labels,
                &self.pending_edge_types,
                &self.pending_properties,
                &self.pending_indexes,
            ))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(())
    }
}

/// Parse an index config from either a string or dict Python object.
pub(crate) fn parse_index_config(
    py: Python<'_>,
    label: &str,
    property: &str,
    index_type: &Py<PyAny>,
) -> PyResult<IndexDefinition> {
    let bound = index_type.bind(py);
    if let Ok(type_str) = bound.extract::<String>() {
        core::create_index_definition(label, property, &type_str)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
    } else if bound.is_instance_of::<pyo3::types::PyDict>() {
        let dict: HashMap<String, Py<PyAny>> = bound.extract()?;
        let mut config = std::collections::HashMap::new();
        for (k, v) in &dict {
            let val = crate::convert::py_object_to_json(py, v)?;
            config.insert(k.clone(), val);
        }
        core::create_index_definition_from_config(label, property, &config)
            .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
    } else {
        Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "index_type must be a string or dict",
        ))
    }
}

/// Builder for defining a label with its properties and indexes.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct LabelBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<crate::core::PendingLabel>,
    parent_edge_types: Vec<crate::core::PendingEdgeType>,
    parent_properties: Vec<crate::core::PendingProperty>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    description: Option<String>,
    properties: Vec<(String, DataType, bool, Option<String>)>,
    indexes: Vec<IndexDefinition>,
}

#[pymethods]
impl LabelBuilder {
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

    /// Add a vector property (shorthand for vector type + index).
    fn vector(mut slf: PyRefMut<'_, Self>, name: String, dimensions: usize) -> PyRefMut<'_, Self> {
        slf.properties
            .push((name, DataType::Vector { dimensions }, false, None));
        slf
    }

    /// Add an index on a property.
    ///
    /// `index_type` can be a string (e.g., `"btree"`, `"vector"`, `"fulltext"`, `"inverted"`)
    /// or a dict with a `"type"` key and additional configuration.
    ///
    /// For `"type": "vector"`, the optional `"algorithm"` selects the ANN —
    /// `"flat"`, `"ivf_flat"`, `"ivf_pq"` (the default), `"ivf_sq"`, `"ivf_rq"`,
    /// `"hnsw_flat"`, `"hnsw_sq"`, `"hnsw_pq"`, or `"muvera"` — and `"metric"` is
    /// `"cosine"` (default), `"l2"`, or `"dot"`. `"muvera"` (ColBERT / MaxSim
    /// Fixed-Dimensional Encoding over a multi-vector `list[Vector[N]]` column) also
    /// accepts `"k_sim"`, `"reps"`, `"d_proj"`, `"seed"`, and `"inner"` (the ANN built
    /// over the derived FDE column, default `"ivf_pq"`).
    ///
    /// ```python
    /// # Simple (string) — a "vector" index defaults to the IVF_PQ algorithm:
    /// builder.index("name", "btree")
    /// builder.index("embedding", "vector")
    ///
    /// # Rich (dict):
    /// builder.index("embedding", {
    ///     "type": "vector", "algorithm": "hnsw_sq",
    ///     "m": 32, "ef_construction": 400, "metric": "l2"
    /// })
    /// # MUVERA over a multi-vector column:
    /// builder.index("tokens", {
    ///     "type": "vector", "algorithm": "muvera",
    ///     "k_sim": 4, "reps": 20, "d_proj": 16, "inner": "ivf_pq"
    /// })
    /// builder.index("content", {
    ///     "type": "fulltext", "tokenizer": "ngram",
    ///     "ngram_min": 2, "ngram_max": 4
    /// })
    /// ```
    fn index<'py>(
        mut slf: PyRefMut<'py, Self>,
        property: String,
        index_type: Py<PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        let label = slf.name.clone();
        let idx = parse_index_config(py, &label, &property, &index_type)?;
        slf.indexes.push(idx);
        Ok(slf)
    }

    /// Finish this label and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
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

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: labels,
            pending_edge_types: self.parent_edge_types.clone(),
            pending_properties: properties,
            pending_indexes: indexes,
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self, py: Python<'_>) -> PyResult<()> {
        self.done()?.apply(py)
    }
}

/// Builder for defining an edge type with its properties.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct EdgeTypeBuilder {
    parent_inner: Arc<Uni>,
    parent_labels: Vec<crate::core::PendingLabel>,
    parent_edge_types: Vec<crate::core::PendingEdgeType>,
    parent_properties: Vec<crate::core::PendingProperty>,
    parent_indexes: Vec<IndexDefinition>,
    name: String,
    description: Option<String>,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    properties: Vec<(String, DataType, bool, Option<String>)>,
}

#[pymethods]
impl EdgeTypeBuilder {
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

    /// Finish this edge type and return to SchemaBuilder.
    fn done(&self) -> PyResult<SchemaBuilder> {
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

        Ok(SchemaBuilder {
            inner: self.parent_inner.clone(),
            pending_labels: self.parent_labels.clone(),
            pending_edge_types: edge_types,
            pending_properties: properties,
            pending_indexes: self.parent_indexes.clone(),
        })
    }

    /// Apply schema changes immediately.
    fn apply(&self, py: Python<'_>) -> PyResult<()> {
        self.done()?.apply(py)
    }
}

// ============================================================================
// Session
// ============================================================================

/// A query session with scoped variables.
///
/// Sessions are the primary scope for reads and the factory for transactions.
/// Create via `db.session()`.
#[pyclass]
pub struct Session {
    pub(crate) inner: ::uni_db::Session,
    /// M8 F2: per-session decorator-accumulator. Each `@db.scalar_fn`
    /// / `@db.aggregate_fn` / `@db.procedure` decoration pushes into
    /// this builder; `Session.finalize_plugin(plugin_id)` drains it
    /// and registers the accumulated entries into the session's
    /// local plugin registry (proposal §5.4.2 default).
    pub(crate) pending_plugin_builder: std::sync::Arc<uni_plugin_pyo3::ManifestBuilder>,
}

#[pymethods]
impl Session {
    /// Access the session-scoped parameter store.
    fn params(&self) -> crate::sync_api::PyParams {
        crate::sync_api::PyParams {
            inner: self.inner.params().clone_store_arc(),
        }
    }

    /// Execute a read query within this session.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    /// The result also implements the sequence protocol (`for row in result` works).
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyQueryResult> {
        let result = if let Some(p) = params {
            let mut builder = self.inner.query_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(k, val);
            }
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.fetch_all()))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            py.detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.query(cypher))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        convert::query_result_to_py_class(py, result)
    }

    /// Create a new transaction for multi-statement writes.
    fn tx(&self, py: Python<'_>) -> PyResult<super::sync_api::Transaction> {
        let tx = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.tx()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(super::sync_api::Transaction { inner: Some(tx) })
    }

    // ── Forks (Phase 4b) ────────────────────────────────────────────────

    /// Open or create a fork. Returns a builder; chain `.new_()` to
    /// require fresh creation, `.ttl(timedelta)` to set a wall-clock
    /// TTL, then `.build()` to drive open-or-create.
    ///
    /// Parent inference: forking a primary session creates a child of
    /// primary; forking a forked session creates a nested child.
    fn fork(&self, name: String) -> PyForkBuilder {
        PyForkBuilder {
            parent: self.inner.clone(),
            name,
            must_create: false,
            ttl: None,
        }
    }

    /// Fork-local schema mutation builder. Adds labels and edge types
    /// to the fork's overlay only — primary is unaffected. Required
    /// under `strict_schema=True` to introduce fork-only entities.
    fn fork_schema(&self) -> PyForkSchemaBuilder {
        PyForkSchemaBuilder {
            parent: self.inner.clone(),
            pending: Vec::new(),
        }
    }

    /// Flush this session's writer to L1.
    ///
    /// On a forked session this flushes the fork's L0 buffer to its
    /// Lance branches.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.flush()))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// `True` when this session was returned by a `fork()` call.
    fn is_forked(&self) -> bool {
        self.inner.is_forked()
    }

    /// Get the session ID.
    fn id(&self) -> &str {
        self.inner.id()
    }

    /// Add a named session hook.
    #[allow(deprecated)] // Python binding still uses per-session hooks; migration to BuiltinHookPlugin tracked alongside the v2.0 ABI break.
    fn add_hook(&mut self, name: &str, hook: Py<PyAny>) {
        self.inner.add_hook(name, PySessionHook { py_obj: hook });
    }

    /// Remove a hook by name.
    #[allow(deprecated)]
    fn remove_hook(&mut self, name: &str) -> bool {
        self.inner.remove_hook(name)
    }

    /// List names of all registered hooks.
    #[allow(deprecated)]
    fn list_hooks(&self) -> Vec<String> {
        self.inner.list_hooks()
    }

    /// Remove all hooks.
    #[allow(deprecated)]
    fn clear_hooks(&mut self) {
        self.inner.clear_hooks();
    }

    /// Get session capabilities.
    fn capabilities(&self) -> crate::types::PySessionCapabilities {
        let caps = self.inner.capabilities();
        let write_lease = caps.write_lease.map(|wl| format!("{:?}", wl));
        crate::types::PySessionCapabilities {
            can_write: caps.can_write,
            can_pin: caps.can_pin,
            isolation: caps.isolation.to_string(),
            has_notifications: caps.has_notifications,
            write_lease,
        }
    }

    /// Evaluate a Locy program within this session.
    #[pyo3(signature = (program, params=None))]
    fn locy(
        &self,
        py: Python,
        program: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyLocyResult> {
        // Release the GIL across `block_on`: the Locy executor may call
        // back into Python (e.g. a registered neural classifier from
        // `LocyConfig::classifier_registry`). Holding the GIL while
        // blocking on tokio would deadlock the callback's GIL acquisition
        // on a worker thread.
        let result = if let Some(p) = params {
            let mut builder = self.inner.locy_with(program);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            py.detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.locy(program))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        convert::locy_result_to_py_class(py, result)
    }

    /// Access the session-scoped rule registry.
    fn rules(&self) -> crate::sync_api::PyRuleRegistry {
        crate::sync_api::PyRuleRegistry {
            registry: self.inner.rules().clone_registry_arc(),
            // Session-scoped rules are ephemeral.
            persister: None,
        }
    }

    /// Prepare a Cypher query for repeated execution.
    fn prepare(&self, py: Python<'_>, cypher: &str) -> PyResult<crate::types::PyPreparedQuery> {
        let prepared = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.prepare(cypher))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyPreparedQuery {
            inner: std::sync::Arc::new(prepared),
        })
    }

    /// Explain a query plan without executing.
    ///
    /// Returns a typed `ExplainOutput` with `.plan_text`, `.warnings`, `.cost_estimates`,
    /// `.index_usage`, `.suggestions`.
    /// Compile a Locy program without executing it.
    fn compile_locy(&self, program: &str) -> PyResult<crate::types::PyCompiledProgram> {
        let compiled = self
            .inner
            .compile_locy(program)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyCompiledProgram { inner: compiled })
    }

    /// Get session metrics.
    fn metrics(&self) -> crate::types::PySessionMetrics {
        let m = self.inner.metrics();
        crate::types::PySessionMetrics {
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
        }
    }

    /// Register a user-defined function callable from Cypher/Locy.
    /// Get a cancellation token for this session.
    fn cancellation_token(&self) -> crate::types::PyCancellationToken {
        crate::types::PyCancellationToken {
            inner: self.inner.cancellation_token(),
        }
    }

    /// Cancel in-progress operations on this session.
    fn cancel(&mut self) {
        self.inner.cancel();
    }

    /// Pin this session to a specific snapshot version.
    fn pin_to_version(&mut self, py: Python<'_>, snapshot_id: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.pin_to_version(snapshot_id))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Pin this session to a specific timestamp (seconds since epoch).
    fn pin_to_timestamp(&mut self, py: Python<'_>, epoch_secs: f64) -> PyResult<()> {
        let ts = chrono::DateTime::from_timestamp(
            epoch_secs as i64,
            ((epoch_secs.fract()) * 1_000_000_000.0) as u32,
        )
        .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid timestamp"))?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.pin_to_timestamp(ts))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Refresh session to latest database version (unpins if pinned).
    fn refresh(&mut self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.refresh()))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    // ── PyO3 plugin decorator surface (M8 follow-up F2) ──────────────
    //
    // Per proposal §5.4: Python notebook users register session-scoped
    // UDFs via `@session.scalar_fn("name", ...)` (or aggregate_fn /
    // procedure). Each decoration appends to the per-session
    // `pending_plugin_builder`; `session.finalize_plugin(plugin_id)`
    // commits the accumulated entries into the session's local
    // plugin registry (proposal §5.4.2 default scope).

    /// `@session.scalar_fn(name, args=[...], returns=..., vectorized=False, determinism="pure")`
    ///
    /// Returns a Python decorator that, when applied to a function,
    /// captures it as a session-scoped scalar UDF. Call
    /// `session.finalize_plugin("plugin.id")` to commit accumulated
    /// decorations.
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
            std::sync::Arc::clone(&self.pending_plugin_builder),
            name,
            args,
            returns,
            vectorized,
            determinism,
        )
    }

    /// `@session.aggregate_fn(name, args=[...], returns=..., determinism="pure")`
    ///
    /// The wrapped target must be a dict with `init` / `accumulate` /
    /// `merge` / `finalize` callables (or a class exposing those as
    /// methods).
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
            std::sync::Arc::clone(&self.pending_plugin_builder),
            name,
            args,
            returns,
            determinism,
        )
    }

    /// `@session.procedure(name, args=[...], yields=[...], mode="read")`
    ///
    /// The wrapped callable receives the procedure args and must
    /// return an iterable of dicts mapping the yield column names to
    /// values. (M8.7 contract.)
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
            std::sync::Arc::clone(&self.pending_plugin_builder),
            name,
            args,
            yields,
            mode,
        )
    }

    /// `session.set_plugin_id(id)` — sets the plugin id used by the
    /// next `finalize_plugin()` (the per-decorator path).
    fn set_plugin_id(&self, plugin_id: String) {
        self.pending_plugin_builder.set_id(plugin_id);
    }

    /// `session.set_plugin_version(version)` — sets the version used
    /// by the next `finalize_plugin()`.
    fn set_plugin_version(&self, version: String) {
        self.pending_plugin_builder.set_version(version);
    }

    /// `session.finalize_plugin(plugin_id, version=None, grants=None)`
    /// — drain accumulated decorator entries and register them into
    /// this session's local plugin registry. Returns a metadata dict
    /// (`plugin_id`, `version`, `scalars_registered`, etc.) mirroring
    /// the shape of `load_rhai_plugin`'s return value.
    #[pyo3(signature = (plugin_id, version=None, grants=None))]
    fn finalize_plugin(
        &self,
        py: Python<'_>,
        plugin_id: &str,
        version: Option<&str>,
        grants: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        if self.pending_plugin_builder.entry_count() == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "session.finalize_plugin: no pending decorators — call \
                 @session.scalar_fn / aggregate_fn / procedure first",
            ));
        }
        self.pending_plugin_builder.set_id(plugin_id);
        if let Some(v) = version {
            self.pending_plugin_builder.set_version(v);
        }
        let caps = build_capability_set(grants);
        let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id(plugin_id);
        let outcome = self
            .inner
            .finalize_python_plugin(&loader, &self.pending_plugin_builder, &caps)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        load_outcome_to_pydict(py, &outcome)
    }

    /// `session.load_python_plugin(module_src, module_name, grants=None)`
    /// — load a Python plugin from a source string. The module body
    /// uses `@db.scalar_fn(...)` etc. on the host-injected `db`
    /// global, identical to the source-load form used by
    /// `Uni::load_python_plugin`. Registers session-scoped.
    #[pyo3(signature = (module_src, module_name, grants=None))]
    fn load_python_plugin(
        &self,
        py: Python<'_>,
        module_src: &str,
        module_name: &str,
        grants: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        let caps = build_capability_set(grants);
        let loader = uni_plugin_pyo3::PythonPluginLoader::with_default_plugin_id(module_name);
        let outcome = self
            .inner
            .add_python_plugin(py, &loader, module_src, module_name, &caps)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        load_outcome_to_pydict(py, &outcome)
    }

    /// Create a query builder for parameterized queries.
    fn query_with(slf: Py<Self>, cypher: &str) -> SessionQueryBuilder {
        SessionQueryBuilder {
            session: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_memory: None,
            cancellation_token: None,
        }
    }

    /// Create a builder for parameterized Locy evaluation.
    fn locy_with(slf: Py<Self>, program: &str) -> SessionLocyBuilder {
        SessionLocyBuilder {
            session: slf,
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            locy_config: None,
            cancellation_token: None,
        }
    }

    /// Check if this session is pinned to a specific version.
    fn is_pinned(&self) -> bool {
        self.inner.is_pinned()
    }

    /// Prepare a Locy program for repeated execution.
    fn prepare_locy(
        &self,
        py: Python<'_>,
        program: &str,
    ) -> PyResult<crate::types::PyPreparedLocy> {
        let prepared = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.prepare_locy(program))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyPreparedLocy {
            inner: std::sync::Arc::new(prepared),
        })
    }

    /// Watch for commit notifications (returns a synchronous CommitStream).
    fn watch(&self) -> crate::types::PyCommitStream {
        let stream = self.inner.watch();
        crate::types::PyCommitStream {
            inner: std::sync::Mutex::new(Some(stream)),
            closed: std::sync::atomic::AtomicBool::new(false),
            close_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Create a WatchBuilder for configuring commit notification filters.
    fn watch_with(&self) -> crate::types::PyWatchBuilder {
        let builder = self.inner.watch_with();
        crate::types::PyWatchBuilder {
            inner: Some(builder),
        }
    }

    /// Create a transaction builder for configuring transaction options.
    fn tx_with(slf: Py<Self>) -> PyTransactionBuilder {
        PyTransactionBuilder {
            session: slf,
            timeout_secs: None,
            isolation_level: None,
        }
    }

    /// Context manager enter.
    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Context manager exit — cancel in-progress operations.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> bool {
        self.inner.cancel();
        false
    }
}

// ============================================================================
// Session builders (sync)
// ============================================================================

/// Builder for parameterized queries on a Session.
#[pyclass(name = "SessionQueryBuilder")]
pub struct SessionQueryBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_memory: Option<usize>,
    pub(crate) cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl SessionQueryBuilder {
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

    /// Attach a cancellation token to this query.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Fetch all results as a `QueryResult`.
    fn fetch_all(&self, py: Python) -> PyResult<crate::types::PyQueryResult> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(m) = self.max_memory {
            builder = builder.max_memory(m);
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.fetch_all()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::query_result_to_py_class(py, result)
    }

    /// Fetch a single row or None.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let result = self.fetch_all(py)?;
        Ok(result.rows.into_iter().next())
    }

    /// Open a streaming cursor.
    fn cursor(&self, py: Python) -> PyResult<crate::sync_api::QueryCursor> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(m) = self.max_memory {
            builder = builder.max_memory(m);
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let cursor = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.cursor()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let columns = cursor.columns().to_vec();
        Ok(crate::sync_api::QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(VecDeque::new()),
            columns,
        })
    }

    /// Explain the query plan without executing it.
    fn explain(&self, py: Python) -> PyResult<crate::types::PyExplainOutput> {
        let session = self.session.borrow(py);
        let builder = session.inner.query_with(&self.cypher);
        let output = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.explain()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::explain_output_to_py_class(py, output)
    }

    /// Profile the query execution, returning results with profiling output.
    fn profile(
        &self,
        py: Python,
    ) -> PyResult<(crate::types::PyQueryResult, crate::types::PyProfileOutput)> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        let (results, profile) = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.profile()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let query_result = convert::query_result_to_py_class(py, results)?;
        let profile_output = convert::profile_output_to_py_class(py, profile)?;
        Ok((query_result, profile_output))
    }
}

/// Builder for Locy evaluation on a Session.
#[pyclass(name = "SessionLocyBuilder")]
pub struct SessionLocyBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) program: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_iterations: Option<usize>,
    pub(crate) locy_config: Option<::uni_locy::LocyConfig>,
    pub(crate) cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl SessionLocyBuilder {
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

    /// Attach a cancellation token to this evaluation.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation.
    fn run(&self, py: Python) -> PyResult<crate::types::PyLocyResult> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.locy_with(&self.program);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(n) = self.max_iterations {
            builder = builder.max_iterations(n);
        }
        if let Some(ref config) = self.locy_config {
            builder = builder.with_config(config.clone());
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        // Release the GIL across `block_on`: the Locy executor may call
        // back into Python (e.g. a registered neural classifier). Holding
        // the GIL through tokio would deadlock the callback's reacquire.
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::locy_result_to_py_class(py, result)
    }

    /// Explain the Locy program without executing it.
    fn explain(&self, py: Python) -> PyResult<crate::types::PyLocyExplainOutput> {
        let session = self.session.borrow(py);
        let builder = session.inner.locy_with(&self.program);
        let result = builder
            .explain()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(convert::locy_explain_to_py_class(result))
    }

    /// Profile the Locy program: run it and return `(result, profile)`.
    ///
    /// The Locy analog of `query.profile()` — `profile` is a
    /// [`LocyProfileOutput`](crate::types::PyLocyProfile) carrying the
    /// stratum → rule → fixpoint-iteration timing and per-operator metrics.
    fn profile(
        &self,
        py: Python,
    ) -> PyResult<(crate::types::PyLocyResult, crate::types::PyLocyProfile)> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.locy_with(&self.program);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(n) = self.max_iterations {
            builder = builder.max_iterations(n);
        }
        if let Some(ref config) = self.locy_config {
            builder = builder.with_config(config.clone());
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        // Release the GIL across `block_on` (the executor may call back into
        // Python; see `run`).
        let (result, profile) = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.profile()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok((
            convert::locy_result_to_py_class(py, result)?,
            convert::locy_profile_to_py_class(profile),
        ))
    }
}

/// Builder for transaction configuration.
#[pyclass(name = "TransactionBuilder")]
pub struct PyTransactionBuilder {
    pub(crate) session: Py<Session>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) isolation_level: Option<String>,
}

#[pymethods]
impl PyTransactionBuilder {
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
    fn start(&self, py: Python) -> PyResult<super::sync_api::Transaction> {
        let session = self.session.borrow(py);
        let mut builder = session.inner.tx_with();
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(ref level) = self.isolation_level {
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
        let tx = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.start()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(super::sync_api::Transaction { inner: Some(tx) })
    }
}

// ============================================================================
// Transaction builders (sync)
// ============================================================================

/// Builder for parameterized queries on a Transaction.
#[pyclass(name = "TxQueryBuilder")]
pub struct PyTxQueryBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    /// Mirrors `SessionQueryBuilder`. Its absence made the whole transaction
    /// surface uncancellable from Python even though the Rust builder this
    /// wraps has carried the field all along.
    pub(crate) cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl PyTxQueryBuilder {
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

    /// Fetch all results as a `QueryResult`.
    fn fetch_all(&self, py: Python) -> PyResult<crate::types::PyQueryResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.fetch_all()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::query_result_to_py_class(py, result)
    }

    /// Fetch a single row or None.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let result = self.fetch_all(py)?;
        Ok(result.rows.into_iter().next())
    }

    /// Execute as a mutation and return ExecuteResult.
    fn execute(&self, py: Python) -> PyResult<crate::types::PyExecuteResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.execute_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::execute_result_to_py(py, result)
    }

    /// Open a streaming cursor.
    fn cursor(&self, py: Python) -> PyResult<crate::sync_api::QueryCursor> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.query_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let cursor = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.cursor()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let columns = cursor.columns().to_vec();
        Ok(crate::sync_api::QueryCursor {
            cursor: std::sync::Mutex::new(Some(cursor)),
            buffer: std::sync::Mutex::new(VecDeque::new()),
            columns,
        })
    }
}

/// Builder for parameterized mutations on a Transaction.
#[pyclass(name = "TxExecuteBuilder")]
pub struct PyTxExecuteBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) cypher: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
}

#[pymethods]
impl PyTxExecuteBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set execution timeout.
    fn timeout(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.timeout_secs = Some(seconds);
        slf
    }

    /// Execute the mutation.
    fn run(&self, py: Python) -> PyResult<crate::types::PyExecuteResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.execute_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::execute_result_to_py(py, result)
    }

    /// Execute the mutation with profiling. Returns
    /// `(ExecuteResult, ProfileOutput)`: the first carries mutation
    /// counters from the transaction's private L0, the second carries
    /// per-operator timings/memory. Mirrors `SessionQueryBuilder.profile`
    /// for the write path.
    fn profile(
        &self,
        py: Python,
    ) -> PyResult<(crate::types::PyExecuteResult, crate::types::PyProfileOutput)> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.execute_with(&self.cypher);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        let (result, profile) = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.profile()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        let exec_result = convert::execute_result_to_py(py, result)?;
        let profile_output = convert::profile_output_to_py_class(py, profile)?;
        Ok((exec_result, profile_output))
    }
}

/// Builder for Locy evaluation on a Transaction.
#[pyclass(name = "TxLocyBuilder")]
pub struct PyTxLocyBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) program: String,
    pub(crate) params: HashMap<String, Py<PyAny>>,
    pub(crate) timeout_secs: Option<f64>,
    pub(crate) max_iterations: Option<usize>,
    pub(crate) locy_config: Option<::uni_locy::LocyConfig>,
    pub(crate) cancellation_token: Option<crate::types::PyCancellationToken>,
}

#[pymethods]
impl PyTxLocyBuilder {
    /// Bind a parameter.
    fn param(mut slf: PyRefMut<'_, Self>, name: String, value: Py<PyAny>) -> PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Set evaluation timeout.
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

    /// Attach a cancellation token to this evaluation.
    fn cancellation_token(
        mut slf: PyRefMut<'_, Self>,
        token: crate::types::PyCancellationToken,
    ) -> PyRefMut<'_, Self> {
        slf.cancellation_token = Some(token);
        slf
    }

    /// Execute the Locy evaluation.
    fn run(&self, py: Python) -> PyResult<crate::types::PyLocyResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.locy_with(&self.program);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(n) = self.max_iterations {
            builder = builder.max_iterations(n);
        }
        if let Some(ref config) = self.locy_config {
            builder = builder.with_config(config.clone());
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        // Release the GIL across `block_on`: the Locy executor may call
        // back into Python (e.g. a registered neural classifier). Holding
        // the GIL through tokio would deadlock the callback's reacquire.
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::locy_result_to_py_class(py, result)
    }

    /// Profile the Locy program in this transaction: run it and return
    /// `(result, profile)`. Transaction-level analog of
    /// [`SessionLocyBuilder::profile`].
    fn profile(
        &self,
        py: Python,
    ) -> PyResult<(crate::types::PyLocyResult, crate::types::PyLocyProfile)> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let mut builder = tx.locy_with(&self.program);
        for (k, v) in &self.params {
            let val = convert::py_object_to_value(py, v)?;
            builder = builder.param(k, val);
        }
        if let Some(t) = self.timeout_secs {
            builder = builder.timeout(std::time::Duration::from_secs_f64(t));
        }
        if let Some(n) = self.max_iterations {
            builder = builder.max_iterations(n);
        }
        if let Some(ref config) = self.locy_config {
            builder = builder.with_config(config.clone());
        }
        if let Some(ref ct) = self.cancellation_token {
            builder = builder.cancellation_token(ct.inner.clone());
        }
        let (result, profile) = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.profile()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok((
            convert::locy_result_to_py_class(py, result)?,
            convert::locy_profile_to_py_class(profile),
        ))
    }
}

/// Builder for applying a DerivedFactSet with options.
///
/// Defaults to fresh-required: a version gap > 0 fails with a
/// stale-derived-facts error unless `allow_stale()` or
/// `max_version_gap(n)` is chained.
#[pyclass(name = "ApplyBuilder")]
pub struct PyApplyBuilder {
    pub(crate) tx: Py<super::sync_api::Transaction>,
    pub(crate) derived: Option<uni_locy::DerivedFactSet>,
    pub(crate) allow_stale: bool,
    pub(crate) max_version_gap: Option<u64>,
}

#[pymethods]
impl PyApplyBuilder {
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

    /// Execute the apply.
    fn run(&mut self, py: Python) -> PyResult<crate::types::PyApplyResult> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.inner.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Transaction already completed")
        })?;
        let dfs = self.derived.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let mut builder = tx.apply_with(dfs);
        if self.allow_stale && self.max_version_gap.is_none() {
            builder = builder.allow_stale();
        }
        if let Some(gap) = self.max_version_gap {
            builder = builder.max_version_gap(gap);
        }
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::types::PyApplyResult {
            facts_applied: result.facts_applied,
            version_gap: result.version_gap,
        })
    }
}

// ============================================================================
// Python hook bridge
// ============================================================================

/// Bridge from Python hook objects to Rust SessionHook trait.
pub(crate) struct PySessionHook {
    pub(crate) py_obj: Py<PyAny>,
}

// Safety: The Py<PyAny> handle is GIL-independent (reference-counted).
// All access to the Python object goes through `Python::attach`.
unsafe impl Send for PySessionHook {}
unsafe impl Sync for PySessionHook {}

impl ::uni_db::SessionHook for PySessionHook {
    fn before_query(&self, ctx: &::uni_db::HookContext) -> uni_common::Result<()> {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "before_query") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("query_text", &ctx.query_text).ok();
                py_ctx
                    .set_item("query_type", format!("{:?}", ctx.query_type))
                    .ok();
                // Enrich: pass params dict
                let params_dict = pyo3::types::PyDict::new(py);
                for (k, v) in &ctx.params {
                    if let Ok(py_v) = convert::value_to_py(py, v) {
                        params_dict.set_item(k, py_v).ok();
                    }
                }
                py_ctx.set_item("params", params_dict).ok();
                if let Err(e) = method.call1(py, (py_ctx,)) {
                    return Err(uni_common::UniError::HookRejected {
                        message: e.to_string(),
                    });
                }
            }
            Ok(())
        })
    }

    fn after_query(&self, ctx: &::uni_db::HookContext, metrics: &::uni_db::QueryMetrics) {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "after_query") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("query_text", &ctx.query_text).ok();
                // Enrich: pass metrics as second argument (with fallback to 1-arg)
                if let Ok(py_metrics) = convert::query_metrics_to_py_class(py, metrics) {
                    if method
                        .call1(py, (py_ctx.as_any(), py_metrics.bind(py).as_any()))
                        .is_err()
                    {
                        // Fallback: try 1-arg call for backward compat
                        let py_ctx2 = pyo3::types::PyDict::new(py);
                        py_ctx2.set_item("session_id", &ctx.session_id).ok();
                        py_ctx2.set_item("query_text", &ctx.query_text).ok();
                        let _ = method.call1(py, (py_ctx2,));
                    }
                } else {
                    let _ = method.call1(py, (py_ctx,));
                }
            }
        });
    }

    fn before_commit(&self, ctx: &::uni_db::CommitHookContext) -> uni_common::Result<()> {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "before_commit") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("tx_id", &ctx.tx_id).ok();
                py_ctx.set_item("mutation_count", ctx.mutation_count).ok();
                if let Err(e) = method.call1(py, (py_ctx,)) {
                    return Err(uni_common::UniError::HookRejected {
                        message: e.to_string(),
                    });
                }
            }
            Ok(())
        })
    }

    fn after_commit(&self, ctx: &::uni_db::CommitHookContext, result: &::uni_db::CommitResult) {
        Python::attach(|py| {
            if let Ok(method) = self.py_obj.getattr(py, "after_commit") {
                let py_ctx = pyo3::types::PyDict::new(py);
                py_ctx.set_item("session_id", &ctx.session_id).ok();
                py_ctx.set_item("tx_id", &ctx.tx_id).ok();
                py_ctx.set_item("mutation_count", ctx.mutation_count).ok();
                // Enrich: pass commit result as second argument (with fallback)
                let py_result = crate::types::PyCommitResult {
                    mutations_committed: result.mutations_committed,
                    rules_promoted: result.rules_promoted,
                    version: result.version,
                    started_at_version: result.started_at_version,
                    wal_lsn: result.wal_lsn,
                    duration_secs: result.duration.as_secs_f64(),
                    rule_promotion_errors: result
                        .rule_promotion_errors
                        .iter()
                        .map(|e| crate::types::PyRulePromotionError {
                            rule_text: e.rule_text.clone(),
                            error: e.error.clone(),
                        })
                        .collect(),
                };
                if let Ok(bound) = Py::new(py, py_result) {
                    if method
                        .call1(py, (py_ctx.as_any(), bound.bind(py).as_any()))
                        .is_err()
                    {
                        let py_ctx2 = pyo3::types::PyDict::new(py);
                        py_ctx2.set_item("session_id", &ctx.session_id).ok();
                        py_ctx2.set_item("tx_id", &ctx.tx_id).ok();
                        py_ctx2.set_item("mutation_count", ctx.mutation_count).ok();
                        let _ = method.call1(py, (py_ctx2,));
                    }
                } else {
                    let _ = method.call1(py, (py_ctx,));
                }
            }
        });
    }
}

// ============================================================================
// StreamingAppender
// ============================================================================

/// Convert a PyArrow `RecordBatch` into an Arrow [`RecordBatch`], zero-copy.
///
/// Uses the Arrow PyCapsule C Data Interface (`__arrow_c_array__`). Shared by
/// the sync and async streaming appenders so the FFI extraction lives in one
/// place.
///
/// # Errors
/// Returns a `TypeError` if `batch` does not expose `__arrow_c_array__`, or a
/// `ValueError` if the Arrow FFI import fails.
pub(crate) fn record_batch_from_pyarrow(
    batch: &Bound<'_, PyAny>,
) -> PyResult<arrow_array::RecordBatch> {
    let capsule_tuple = batch.call_method0("__arrow_c_array__").map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Expected a PyArrow RecordBatch with __arrow_c_array__ support: {}",
            e
        ))
    })?;
    let schema_capsule = capsule_tuple.get_item(0)?;
    let array_capsule = capsule_tuple.get_item(1)?;

    // Safety: the capsules are produced by the Arrow C Data Interface, which
    // guarantees valid `arrow_schema` / `arrow_array` pointers. Per that
    // interface the consumer takes ownership AND must mark the source structs
    // as released so the producer (pyarrow's capsule destructor) does not free
    // the same buffers. `FFI_Arrow{Schema,Array}::from_raw` does exactly this:
    // it moves the value out and overwrites the source in place with an empty
    // struct (`release == NULL`), leaving the capsule destructor a no-op. Using
    // `ptr::read` here instead would leave `release` non-NULL and cause a
    // double-free / use-after-free of the ingested buffers.
    let (ffi_schema, ffi_array) = unsafe {
        let schema_ptr =
            pyo3::ffi::PyCapsule_GetPointer(schema_capsule.as_ptr(), c"arrow_schema".as_ptr())
                as *mut arrow_array::ffi::FFI_ArrowSchema;
        let array_ptr =
            pyo3::ffi::PyCapsule_GetPointer(array_capsule.as_ptr(), c"arrow_array".as_ptr())
                as *mut arrow_array::ffi::FFI_ArrowArray;
        (
            arrow_array::ffi::FFI_ArrowSchema::from_raw(schema_ptr),
            arrow_array::ffi::FFI_ArrowArray::from_raw(array_ptr),
        )
    };

    // Safety: `ffi_array` / `ffi_schema` were just read from valid Arrow C Data
    // Interface capsules and describe a consistent array/schema pair.
    let array_data = unsafe {
        arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(
            |e: arrow_schema::ArrowError| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
            },
        )?
    };
    let struct_array = arrow_array::StructArray::from(array_data);
    let schema = arrow_schema::Schema::new(
        struct_array
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    );
    arrow_array::RecordBatch::try_new(std::sync::Arc::new(schema), struct_array.columns().to_vec())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

/// A streaming appender for single-label data loading.
///
/// Rows are buffered and flushed in batches. Use as a context manager:
/// ```python
/// with session.appender("Person") as app:
///     app.append({"name": "Alice", "age": 30})
///     stats = app.finish()
/// ```
#[pyclass]
pub struct StreamingAppender {
    pub(crate) inner: std::sync::Mutex<Option<::uni_db::StreamingAppender>>,
}

#[pymethods]
impl StreamingAppender {
    /// Append a single row of properties.
    fn append(&self, py: Python, properties: HashMap<String, Py<PyAny>>) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();
        let appender = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })?;
        let mut rust_props = HashMap::new();
        for (k, v) in properties {
            rust_props.insert(k, convert::py_object_to_value(py, &v)?);
        }
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(appender.append(rust_props))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Flush remaining rows and commit.
    fn finish(&self, py: Python<'_>) -> PyResult<BulkStats> {
        let mut guard = self.inner.lock().unwrap();
        let appender = guard.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })?;
        let stats = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(appender.finish()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(BulkStats {
            vertices_inserted: stats.vertices_inserted,
            edges_inserted: stats.edges_inserted,
            indexes_rebuilt: stats.indexes_rebuilt,
            duration_secs: stats.duration.as_secs_f64(),
            index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
            index_task_ids: stats.index_task_ids.clone(),
            indexes_pending: stats.indexes_pending,
        })
    }

    /// Abort without committing.
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        let mut guard = self.inner.lock().unwrap();
        if let Some(appender) = guard.take() {
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(appender.abort()))
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
        }
        Ok(())
    }

    /// Write an Arrow RecordBatch of rows.
    ///
    /// Accepts a PyArrow RecordBatch. Uses the Arrow PyCapsule C Data Interface
    /// (`__arrow_c_array__`) for zero-copy transfer.
    fn write_batch(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let record_batch = record_batch_from_pyarrow(batch)?;
        let mut guard = self.inner.lock().unwrap();
        let appender = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })?;
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(appender.write_batch(&record_batch))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Number of rows currently buffered.
    fn buffered_count(&self) -> PyResult<usize> {
        let guard = self.inner.lock().unwrap();
        guard.as_ref().map(|a| a.buffered_count()).ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Appender already finished")
        })
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let guard = self.inner.lock().unwrap();
        if guard.is_some() {
            drop(guard);
            self.abort(py)?;
        }
        Ok(false)
    }
}

// ============================================================================
// SessionTemplateBuilder
// ============================================================================

/// Builder for creating pre-configured session templates.
#[pyclass]
pub struct SessionTemplateBuilder {
    pub(crate) inner: Option<::uni_db::SessionTemplateBuilder>,
}

#[pymethods]
impl SessionTemplateBuilder {
    /// Bind a parameter that all sessions created from this template will inherit.
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
        slf.inner = Some(builder.hook(name, PySessionHook { py_obj: hook }));
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
    fn build(&mut self) -> PyResult<SessionTemplate> {
        let builder = self.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Builder already consumed")
        })?;
        let template = builder
            .build()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(SessionTemplate {
            inner: std::sync::Arc::new(template),
        })
    }
}

// ============================================================================
// SessionTemplate
// ============================================================================

/// A pre-configured session factory.
///
/// Create sessions cheaply from pre-compiled templates:
/// ```python
/// template = db.session_template().param("tenant", 42).rules("...").build()
/// session = template.create()
/// ```
#[pyclass]
pub struct SessionTemplate {
    inner: std::sync::Arc<::uni_db::SessionTemplate>,
}

#[pymethods]
impl SessionTemplate {
    /// Create a new session from this template (cheap, no I/O).
    fn create(&self) -> Session {
        Session {
            inner: self.inner.create(),
            pending_plugin_builder: uni_plugin_pyo3::ManifestBuilder::new(),
        }
    }
}

// ============================================================================
// BulkWriter (wrapping real Rust BulkWriter)
// ============================================================================

/// Bulk writer for high-throughput data ingestion.
///
/// Wraps the real Rust `BulkWriter` via `Mutex<Option<T>>` ownership pattern.
#[pyclass]
pub struct BulkWriter {
    pub(crate) inner: std::sync::Mutex<Option<::uni_db::api::bulk::BulkWriter>>,
}

impl BulkWriter {
    fn with_writer<F, R>(&self, op: &str, f: F) -> PyResult<R>
    where
        F: FnOnce(&mut ::uni_db::api::bulk::BulkWriter) -> PyResult<R>,
    {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "BulkWriter already completed ({})",
                op
            ))
        })?;
        f(writer)
    }
}

#[pymethods]
impl BulkWriter {
    /// Insert vertices in bulk, returning allocated VIDs.
    fn insert_vertices(
        &self,
        py: Python,
        label: &str,
        vertices: Vec<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<Vec<u64>> {
        let mut rust_props: Vec<HashMap<String, ::uni_db::Value>> =
            Vec::with_capacity(vertices.len());
        for v in vertices {
            let mut map = HashMap::new();
            for (k, val) in v {
                map.insert(k, convert::py_object_to_value(py, &val)?);
            }
            rust_props.push(map);
        }
        self.with_writer("insert_vertices", |writer| {
            let vids = py
                .detach(|| {
                    pyo3_async_runtimes::tokio::get_runtime()
                        .block_on(writer.insert_vertices(label, rust_props))
                })
                .map_err(crate::exceptions::anyhow_to_pyerr)?;
            Ok(vids.into_iter().map(|v| v.as_u64()).collect())
        })
    }

    /// Insert edges in bulk.
    fn insert_edges(
        &self,
        py: Python,
        edge_type: &str,
        edges: Vec<(u64, u64, HashMap<String, Py<PyAny>>)>,
    ) -> PyResult<()> {
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
        self.with_writer("insert_edges", |writer| {
            py.detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(writer.insert_edges(edge_type, rust_edges))
            })
            .map_err(crate::exceptions::anyhow_to_pyerr)?;
            Ok(())
        })
    }

    /// Get current bulk load statistics.
    fn stats(&self) -> PyResult<BulkStats> {
        self.with_writer("stats", |writer| {
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
        })
    }

    /// Get labels that have been written to.
    fn touched_labels(&self) -> PyResult<Vec<String>> {
        self.with_writer("touched_labels", |writer| Ok(writer.touched_labels()))
    }

    /// Get edge types that have been written to.
    fn touched_edge_types(&self) -> PyResult<Vec<String>> {
        self.with_writer("touched_edge_types", |writer| {
            Ok(writer.touched_edge_types())
        })
    }

    /// Commit all pending data and rebuild indexes.
    fn commit(&self, py: Python<'_>) -> PyResult<BulkStats> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        let writer = guard.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("BulkWriter already completed")
        })?;
        let stats = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(writer.commit()))
            .map_err(crate::exceptions::anyhow_to_pyerr)?;
        Ok(BulkStats {
            vertices_inserted: stats.vertices_inserted,
            edges_inserted: stats.edges_inserted,
            indexes_rebuilt: stats.indexes_rebuilt,
            duration_secs: stats.duration.as_secs_f64(),
            index_build_duration_secs: stats.index_build_duration.as_secs_f64(),
            index_task_ids: stats.index_task_ids.clone(),
            indexes_pending: stats.indexes_pending,
        })
    }

    /// Abort bulk loading and discard uncommitted changes.
    fn abort(&self, py: Python<'_>) -> PyResult<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        if let Some(writer) = guard.take() {
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(writer.abort()))
                .map_err(crate::exceptions::anyhow_to_pyerr)?;
        }
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Auto-abort on exception if not committed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        let guard = self.inner.lock().unwrap();
        if guard.is_some() {
            drop(guard);
            self.abort(py)?;
        }
        Ok(false)
    }
}

// ============================================================================
// Phase 4b — Fork builder + Fork-schema builder
// ============================================================================

/// Builder returned by `Session.fork(name)`. Drive it via `.build()`
/// after chaining configuration methods.
///
/// Example:
/// ```python
/// fork = session.fork("scenario_1").ttl(timedelta(hours=1)).build()
/// ```
#[pyclass(name = "ForkBuilder")]
pub struct PyForkBuilder {
    pub(crate) parent: ::uni_db::Session,
    pub(crate) name: String,
    pub(crate) must_create: bool,
    pub(crate) ttl: Option<std::time::Duration>,
}

#[pymethods]
impl PyForkBuilder {
    /// Require fresh creation; errors with `UniForkAlreadyExistsError`
    /// if the fork name is already taken.
    fn new_(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.must_create = true;
        slf
    }

    /// Stamp a wall-clock TTL on the fork. Has no effect when the
    /// fork already exists (open-or-create returns it unchanged).
    fn ttl<'py>(mut slf: PyRefMut<'py, Self>, ttl: Py<PyAny>) -> PyResult<PyRefMut<'py, Self>> {
        let py = slf.py();
        slf.ttl = Some(convert::py_timedelta_to_duration(ttl.bind(py))?);
        Ok(slf)
    }

    /// Drive the open-or-create flow and return a forked `Session`.
    fn build(&self, py: Python<'_>) -> PyResult<Session> {
        let parent = self.parent.clone();
        let name = self.name.clone();
        let must_create = self.must_create;
        let ttl = self.ttl;
        let forked = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(async move {
                    let mut b = parent.fork(name);
                    if must_create {
                        b = b.new_();
                    }
                    if let Some(d) = ttl {
                        b = b.ttl(d);
                    }
                    b.await
                })
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Session {
            inner: forked,
            pending_plugin_builder: uni_plugin_pyo3::ManifestBuilder::new(),
        })
    }
}

/// Builder returned by `Session.fork_schema()`. Chain `.label(...)`
/// and `.edge_type(...)` calls, then `.apply()` to persist the
/// fork-local overlay.
#[pyclass(name = "ForkSchemaBuilder")]
pub struct PyForkSchemaBuilder {
    pub(crate) parent: ::uni_db::Session,
    pub(crate) pending: Vec<ForkSchemaPending>,
}

#[derive(Clone)]
pub enum ForkSchemaPending {
    Label {
        name: String,
        description: Option<String>,
    },
    EdgeType {
        name: String,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
        description: Option<String>,
    },
}

#[pymethods]
impl PyForkSchemaBuilder {
    /// Add a fork-local label.
    #[pyo3(signature = (name, description=None))]
    fn label(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        description: Option<String>,
    ) -> PyRefMut<'_, Self> {
        slf.pending
            .push(ForkSchemaPending::Label { name, description });
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
        slf.pending.push(ForkSchemaPending::EdgeType {
            name,
            from_labels,
            to_labels,
            description,
        });
        slf
    }

    /// Persist the pending entries to the fork's overlay file and the
    /// fork's in-memory `SchemaManager`. Errors with
    /// `UniInvalidArgumentError` on a non-forked session.
    fn apply(&self, py: Python<'_>) -> PyResult<()> {
        let parent = self.parent.clone();
        let pending = self.pending.clone();
        if pending.is_empty() {
            return Ok(());
        }
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(apply_fork_schema_pending(parent, pending))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(())
    }
}

/// Drive the Rust `ForkSchemaBuilder` from a buffered list of pending
/// changes. The Rust builder consumes by-value at each chain step;
/// only the final cursor's `.apply()` lands every entry in one
/// persisted overlay update. We model the cursor as an enum because
/// `ForkSchemaBuilder`, `ForkLabelBuilder`, and `ForkEdgeTypeBuilder`
/// all share the same `.label()` / `.edge_type()` continuation
/// methods but have distinct types.
pub(crate) async fn apply_fork_schema_pending(
    parent: ::uni_db::Session,
    pending: Vec<ForkSchemaPending>,
) -> ::uni_common::api::error::Result<()> {
    use ::uni_db::api::fork_schema::{ForkEdgeTypeBuilder, ForkLabelBuilder, ForkSchemaBuilder};

    enum Cursor<'a> {
        Schema(ForkSchemaBuilder<'a>),
        Label(ForkLabelBuilder<'a>),
        EdgeType(ForkEdgeTypeBuilder<'a>),
    }

    fn step_label<'a>(cursor: Cursor<'a>, name: &str, desc: Option<&str>) -> Cursor<'a> {
        let mut lb = match cursor {
            Cursor::Schema(b) => b.label(name),
            Cursor::Label(b) => b.label(name),
            Cursor::EdgeType(b) => b.label(name),
        };
        if let Some(d) = desc {
            lb = lb.description(d);
        }
        Cursor::Label(lb)
    }

    fn step_edge<'a>(
        cursor: Cursor<'a>,
        name: &str,
        from: &[&str],
        to: &[&str],
        desc: Option<&str>,
    ) -> Cursor<'a> {
        let mut eb = match cursor {
            Cursor::Schema(b) => b.edge_type(name, from, to),
            Cursor::Label(b) => b.edge_type(name, from, to),
            Cursor::EdgeType(b) => b.edge_type(name, from, to),
        };
        if let Some(d) = desc {
            eb = eb.description(d);
        }
        Cursor::EdgeType(eb)
    }

    let mut cursor = Cursor::Schema(parent.fork_schema());
    for change in &pending {
        cursor = match change {
            ForkSchemaPending::Label { name, description } => {
                step_label(cursor, name, description.as_deref())
            }
            ForkSchemaPending::EdgeType {
                name,
                from_labels,
                to_labels,
                description,
            } => {
                let from_refs: Vec<&str> = from_labels.iter().map(String::as_str).collect();
                let to_refs: Vec<&str> = to_labels.iter().map(String::as_str).collect();
                step_edge(cursor, name, &from_refs, &to_refs, description.as_deref())
            }
        };
    }

    match cursor {
        Cursor::Schema(b) => b.apply().await,
        Cursor::Label(b) => b.apply().await,
        Cursor::EdgeType(b) => b.apply().await,
    }
}
