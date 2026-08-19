// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Python data classes for query results, schema info, and statistics.

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySlice};
use std::collections::HashMap;

// ============================================================================
// Query result types (Phase 1)
// ============================================================================

/// Query performance metrics returned with every query result.
#[pyclass(get_all, name = "QueryMetrics", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQueryMetrics {
    /// Time spent parsing the query in milliseconds.
    pub parse_time_ms: f64,
    /// Time spent planning the query in milliseconds.
    pub plan_time_ms: f64,
    /// Time spent executing the query in milliseconds.
    pub exec_time_ms: f64,
    /// Total query time in milliseconds.
    pub total_time_ms: f64,
    /// Number of rows in the result set.
    pub rows_returned: usize,
    /// Number of rows scanned during execution.
    pub rows_scanned: usize,
    /// Number of bytes read from storage.
    pub bytes_read: usize,
    /// Whether the query plan was served from cache.
    pub plan_cache_hit: bool,
    /// Number of L0 (in-memory) reads.
    pub l0_reads: usize,
    /// Number of persistent storage reads.
    pub storage_reads: usize,
    /// Number of cache hits during execution.
    pub cache_hits: usize,
    /// Number of scans that executed against a fork's Lance branch.
    ///
    /// Counts what executed, not what was configured: a forked session that
    /// reads a label the fork never wrote falls back to primary and reports 0.
    pub branch_scans: u64,
    /// Number of reads that executed against a pinned time-travel snapshot.
    pub snapshot_reads: u64,
    /// Number of Lance scans whose executed plan reported index work.
    ///
    /// Nonzero proves a scalar index was searched; zero does not prove one was
    /// not (an index already in memory is not re-counted, and a lookup outside
    /// every page range performs no comparisons).
    pub index_scans: u64,
    /// Sum of Lance's index comparisons across those scans. Presence is
    /// meaningful; the magnitude is index-type dependent.
    pub index_comparisons: u64,
    /// Number of Lance scans for which the stats callback fired — the
    /// denominator that makes `index_scans == 0` unambiguous.
    pub scans_reported: u64,
}

#[pymethods]
impl PyQueryMetrics {
    fn __repr__(&self) -> String {
        format!(
            "QueryMetrics(total={:.2}ms, rows_returned={}, rows_scanned={})",
            self.total_time_ms, self.rows_returned, self.rows_scanned
        )
    }
}

/// A query warning emitted during execution (e.g., missing index).
#[pyclass(get_all, name = "QueryWarning", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQueryWarning {
    /// Warning code string (e.g., "index_unavailable", "no_index_for_filter").
    pub code: String,
    /// Human-readable warning message.
    pub message: String,
}

#[pymethods]
impl PyQueryWarning {
    fn __repr__(&self) -> String {
        format!(
            "QueryWarning(code='{}', message='{}')",
            self.code, self.message
        )
    }
}

/// Rich query result containing rows, metrics, warnings, and column names.
///
/// Implements the sequence protocol for backward compatibility:
/// `for row in result`, `result[0]`, `len(result)` all work.
#[pyclass(name = "QueryResult")]
pub struct PyQueryResult {
    pub(crate) rows: Vec<Py<PyAny>>,
    #[pyo3(get)]
    pub metrics: Py<PyQueryMetrics>,
    #[pyo3(get)]
    pub warnings: Vec<PyQueryWarning>,
    #[pyo3(get)]
    pub columns: Vec<String>,
}

#[pymethods]
impl PyQueryResult {
    /// Return the list of row dicts.
    #[getter]
    fn rows(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::new(py, self.rows.iter().map(|r| r.bind(py)))?;
        Ok(list.unbind())
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(slice) = key.cast::<PySlice>() {
            let indices = slice.indices(self.rows.len() as isize)?;
            let mut items: Vec<Py<PyAny>> = Vec::new();
            let mut i = indices.start;
            while (indices.step > 0 && i < indices.stop) || (indices.step < 0 && i > indices.stop) {
                items.push(self.rows[i as usize].clone_ref(py));
                i += indices.step;
            }
            let list = PyList::new(py, items.iter().map(|r| r.bind(py)))?;
            return Ok(list.into_any().unbind());
        }
        let idx: isize = key.extract()?;
        let len = self.rows.len() as isize;
        let actual = if idx < 0 { len + idx } else { idx };
        if actual < 0 || actual >= len {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "index out of range",
            ));
        }
        Ok(self.rows[actual as usize].clone_ref(py))
    }

    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, slf.rows.iter().map(|r| r.bind(py)))?;
        list.call_method0("__iter__").map(|i| i.unbind())
    }

    fn __bool__(&self) -> bool {
        !self.rows.is_empty()
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryResult(rows={}, columns={:?})",
            self.rows.len(),
            self.columns
        )
    }
}

// ============================================================================
// Graph element types
// ============================================================================

/// Emit a Python DeprecationWarning.
fn deprecation_warning(py: Python, msg: &str) -> PyResult<()> {
    let warnings = py.import("warnings")?;
    warnings.call_method1(
        "warn",
        (msg, py.get_type::<pyo3::exceptions::PyDeprecationWarning>()),
    )?;
    Ok(())
}

/// A graph node returned from a Cypher query.
///
/// Provides graph-native attributes (`.id`, `.labels`, `.properties`) and
/// dict-like property access (`node["name"]`, `"name" in node`).
#[pyclass(frozen, name = "Node")]
pub struct PyNode {
    pub(crate) id: u64,
    pub(crate) labels: Vec<String>,
    pub(crate) properties: HashMap<String, Py<PyAny>>,
}

#[pymethods]
impl PyNode {
    /// Internal vertex identifier.
    #[getter]
    fn id(&self) -> PyVid {
        PyVid {
            inner: uni_common::Vid::new(self.id),
        }
    }

    /// Alias for `id` (Neo4j driver compatibility).
    #[getter]
    fn element_id(&self) -> PyVid {
        PyVid {
            inner: uni_common::Vid::new(self.id),
        }
    }

    /// Node labels.
    #[getter]
    fn labels(&self) -> Vec<String> {
        self.labels.clone()
    }

    /// Property dictionary.
    #[getter]
    fn properties(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.properties {
            dict.set_item(k, v.bind(py))?;
        }
        Ok(dict.unbind())
    }

    /// Get a property by name, returning *default* if absent.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        self.properties
            .get(key)
            .map(|v| v.clone_ref(py))
            .unwrap_or_else(|| default.unwrap_or_else(|| py.None()))
    }

    /// Property names.
    fn keys(&self) -> Vec<String> {
        self.properties.keys().cloned().collect()
    }

    /// Property values.
    fn values(&self, py: Python<'_>) -> Vec<Py<PyAny>> {
        self.properties.values().map(|v| v.clone_ref(py)).collect()
    }

    /// (key, value) pairs.
    fn items(&self, py: Python<'_>) -> Vec<(String, Py<PyAny>)> {
        self.properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect()
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        // Deprecated magic-key backward compatibility
        match key {
            "_id" => {
                deprecation_warning(py, "Node['_id'] is deprecated, use Node.id")?;
                return self.id.into_py_any(py);
            }
            "_labels" => {
                deprecation_warning(py, "Node['_labels'] is deprecated, use Node.labels")?;
                return self.labels.clone().into_py_any(py);
            }
            _ => {}
        }
        self.properties
            .get(key)
            .map(|v| v.clone_ref(py))
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    fn __len__(&self) -> usize {
        self.properties.len()
    }

    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let keys: Vec<&str> = slf.properties.keys().map(|k| k.as_str()).collect();
        let list = PyList::new(py, &keys)?;
        list.call_method0("__iter__").map(|i| i.unbind())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.id == other.id
    }

    fn __hash__(&self) -> u64 {
        self.id
    }

    fn __bool__(&self) -> bool {
        true
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let props: Vec<String> = self
            .properties
            .iter()
            .map(|(k, v)| {
                let val_repr = v
                    .bind(py)
                    .repr()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|_| "?".into());
                format!("'{}': {}", k, val_repr)
            })
            .collect();
        format!(
            "Node(id={}, labels={:?}, properties={{{}}})",
            self.id,
            self.labels,
            props.join(", ")
        )
    }
}

/// A graph edge (relationship) returned from a Cypher query.
///
/// Provides graph-native attributes (`.id`, `.type`, `.start_id`, `.end_id`,
/// `.properties`) and dict-like property access.
#[pyclass(frozen, name = "Edge")]
pub struct PyEdge {
    pub(crate) id: u64,
    pub(crate) type_name: String,
    pub(crate) start_id: u64,
    pub(crate) end_id: u64,
    pub(crate) properties: HashMap<String, Py<PyAny>>,
}

#[pymethods]
impl PyEdge {
    /// Internal edge identifier.
    #[getter]
    fn id(&self) -> PyEid {
        PyEid {
            inner: uni_common::Eid::new(self.id),
        }
    }

    /// Alias for `id` (Neo4j driver compatibility).
    #[getter]
    fn element_id(&self) -> PyEid {
        PyEid {
            inner: uni_common::Eid::new(self.id),
        }
    }

    /// Relationship type name.
    #[getter]
    #[pyo3(name = "type")]
    fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Source vertex identifier.
    #[getter]
    fn start_id(&self) -> PyVid {
        PyVid {
            inner: uni_common::Vid::new(self.start_id),
        }
    }

    /// Destination vertex identifier.
    #[getter]
    fn end_id(&self) -> PyVid {
        PyVid {
            inner: uni_common::Vid::new(self.end_id),
        }
    }

    /// Property dictionary.
    #[getter]
    fn properties(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.properties {
            dict.set_item(k, v.bind(py))?;
        }
        Ok(dict.unbind())
    }

    /// Get a property by name, returning *default* if absent.
    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        self.properties
            .get(key)
            .map(|v| v.clone_ref(py))
            .unwrap_or_else(|| default.unwrap_or_else(|| py.None()))
    }

    /// Property names.
    fn keys(&self) -> Vec<String> {
        self.properties.keys().cloned().collect()
    }

    /// Property values.
    fn values(&self, py: Python<'_>) -> Vec<Py<PyAny>> {
        self.properties.values().map(|v| v.clone_ref(py)).collect()
    }

    /// (key, value) pairs.
    fn items(&self, py: Python<'_>) -> Vec<(String, Py<PyAny>)> {
        self.properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone_ref(py)))
            .collect()
    }

    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "_id" => {
                deprecation_warning(py, "Edge['_id'] is deprecated, use Edge.id")?;
                return self.id.into_py_any(py);
            }
            "_type" => {
                deprecation_warning(py, "Edge['_type'] is deprecated, use Edge.type")?;
                return self.type_name.clone().into_py_any(py);
            }
            "_src" => {
                deprecation_warning(py, "Edge['_src'] is deprecated, use Edge.start_id")?;
                return self.start_id.to_string().into_py_any(py);
            }
            "_dst" => {
                deprecation_warning(py, "Edge['_dst'] is deprecated, use Edge.end_id")?;
                return self.end_id.to_string().into_py_any(py);
            }
            _ => {}
        }
        self.properties
            .get(key)
            .map(|v| v.clone_ref(py))
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    fn __len__(&self) -> usize {
        self.properties.len()
    }

    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let keys: Vec<&str> = slf.properties.keys().map(|k| k.as_str()).collect();
        let list = PyList::new(py, &keys)?;
        list.call_method0("__iter__").map(|i| i.unbind())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.id == other.id
    }

    fn __hash__(&self) -> u64 {
        self.id
    }

    fn __bool__(&self) -> bool {
        true
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let props: Vec<String> = self
            .properties
            .iter()
            .map(|(k, v)| {
                let val_repr = v
                    .bind(py)
                    .repr()
                    .map(|r| r.to_string())
                    .unwrap_or_else(|_| "?".into());
                format!("'{}': {}", k, val_repr)
            })
            .collect();
        format!(
            "Edge(id={}, type='{}', start={}, end={}, properties={{{}}})",
            self.id,
            self.type_name,
            self.start_id,
            self.end_id,
            props.join(", ")
        )
    }
}

/// A graph path (alternating sequence of nodes and edges) returned from a
/// Cypher query.
///
/// Supports `len()` (number of edges/hops), interleaved indexing
/// (`path[0]` → first node, `path[1]` → first edge, …), and iteration.
#[pyclass(frozen, name = "Path")]
pub struct PyPath {
    pub(crate) nodes: Vec<Py<PyNode>>,
    pub(crate) edges: Vec<Py<PyEdge>>,
}

#[pymethods]
impl PyPath {
    /// Nodes along the path.
    #[getter]
    fn nodes(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::new(py, self.nodes.iter().map(|n| n.bind(py)))?;
        Ok(list.unbind())
    }

    /// Edges connecting the nodes.
    #[getter]
    fn edges(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::new(py, self.edges.iter().map(|e| e.bind(py)))?;
        Ok(list.unbind())
    }

    /// First node in the path, or ``None`` if empty.
    #[getter]
    fn start(&self, py: Python<'_>) -> Option<Py<PyNode>> {
        self.nodes.first().map(|n| n.clone_ref(py))
    }

    /// Last node in the path, or ``None`` if empty.
    #[getter]
    fn end(&self, py: Python<'_>) -> Option<Py<PyNode>> {
        self.nodes.last().map(|n| n.clone_ref(py))
    }

    /// True if the path contains no edges.
    fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Number of edges (hops) in the path.
    fn __len__(&self) -> usize {
        self.edges.len()
    }

    /// Interleaved access: even indices → nodes, odd indices → edges.
    fn __getitem__(&self, py: Python<'_>, idx: isize) -> PyResult<Py<PyAny>> {
        let total = self.nodes.len() + self.edges.len();
        let actual = if idx < 0 {
            (total as isize + idx) as usize
        } else {
            idx as usize
        };
        if actual >= total {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "path index out of range",
            ));
        }
        if actual % 2 == 0 {
            // Even index → node
            Ok(self.nodes[actual / 2].clone_ref(py).into_any())
        } else {
            // Odd index → edge
            Ok(self.edges[actual / 2].clone_ref(py).into_any())
        }
    }

    /// Iterate over interleaved nodes and edges.
    fn __iter__(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        for (i, node) in slf.nodes.iter().enumerate() {
            list.append(node.bind(py))?;
            if i < slf.edges.len() {
                list.append(slf.edges[i].bind(py))?;
            }
        }
        list.call_method0("__iter__").map(|i| i.unbind())
    }

    fn __eq__(&self, other: &Self) -> bool {
        if self.nodes.len() != other.nodes.len() || self.edges.len() != other.edges.len() {
            return false;
        }
        let self_node_ids: Vec<u64> = self.nodes.iter().map(|n| n.get().id).collect();
        let other_node_ids: Vec<u64> = other.nodes.iter().map(|n| n.get().id).collect();
        let self_edge_ids: Vec<u64> = self.edges.iter().map(|e| e.get().id).collect();
        let other_edge_ids: Vec<u64> = other.edges.iter().map(|e| e.get().id).collect();
        self_node_ids == other_node_ids && self_edge_ids == other_edge_ids
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for n in &self.nodes {
            n.get().id.hash(&mut hasher);
        }
        for e in &self.edges {
            e.get().id.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn __bool__(&self) -> bool {
        !self.nodes.is_empty()
    }

    fn __repr__(&self) -> String {
        format!(
            "Path(nodes={}, edges={})",
            self.nodes.len(),
            self.edges.len()
        )
    }
}

/// Typed output from `session.explain()`.
#[pyclass(get_all, name = "ExplainOutput")]
#[derive(Debug)]
pub struct PyExplainOutput {
    /// Human-readable query plan text.
    pub plan_text: String,
    /// Warnings from the planner.
    pub warnings: Vec<String>,
    /// Cost estimates as a dict with `estimated_rows` and `estimated_cost`.
    pub cost_estimates: Py<PyAny>,
    /// List of index usage details.
    pub index_usage: Py<PyAny>,
    /// List of index suggestions from the planner.
    pub suggestions: Py<PyAny>,
}

#[pymethods]
impl PyExplainOutput {
    fn __repr__(&self) -> String {
        format!(
            "ExplainOutput(warnings={}, plan_text='{}...')",
            self.warnings.len(),
            self.plan_text.chars().take(60).collect::<String>()
        )
    }
}

/// Typed output from `session.profile()`.
#[pyclass(get_all, name = "ProfileOutput")]
#[derive(Debug)]
pub struct PyProfileOutput {
    /// Total execution time in milliseconds.
    pub total_time_ms: u64,
    /// Peak memory usage in bytes.
    pub peak_memory_bytes: usize,
    /// Human-readable query plan text.
    pub plan_text: String,
    /// Operator-level statistics (list of dicts).
    pub operators: Py<PyAny>,
}

#[pymethods]
impl PyProfileOutput {
    fn __repr__(&self) -> String {
        format!(
            "ProfileOutput(total_time={}ms, peak_memory={}B)",
            self.total_time_ms, self.peak_memory_bytes
        )
    }
}

/// Typed output from `session.locy_with(program).explain()`.
#[pyclass(get_all, name = "LocyExplainOutput", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyLocyExplainOutput {
    /// Human-readable evaluation plan text.
    pub plan_text: String,
    /// Number of strata in the program.
    pub strata_count: usize,
    /// Names of all rules in the program.
    pub rule_names: Vec<String>,
    /// Whether any stratum is recursive.
    pub has_recursive_strata: bool,
    /// Warnings from the planner.
    pub warnings: Vec<String>,
    /// Number of Cypher commands in the program.
    pub command_count: usize,
}

#[pymethods]
impl PyLocyExplainOutput {
    fn __repr__(&self) -> String {
        format!(
            "LocyExplainOutput(strata={}, rules={}, recursive={})",
            self.strata_count,
            self.rule_names.len(),
            self.has_recursive_strata
        )
    }
}

/// Structured execution profile from `session.locy_with(...).profile()`.
///
/// The Locy analog of Cypher's profile: a stratum → rule → fixpoint-iteration
/// tree, each iteration carrying per-operator metrics. Scalar summaries are
/// exposed as attributes; the full tree is available via `strata` as nested
/// dicts/lists, and `str(profile)` renders a readable report.
#[pyclass(name = "LocyProfileOutput", skip_from_py_object)]
#[derive(Clone)]
pub struct PyLocyProfile {
    pub(crate) inner: uni_db::api::locy_result::LocyProfileOutput,
}

#[pymethods]
impl PyLocyProfile {
    /// Total program evaluation wall-clock time, in milliseconds.
    #[getter]
    fn total_time_ms(&self) -> f64 {
        self.inner.total_time_ms
    }

    /// Peak derived-fact memory, in bytes.
    #[getter]
    fn peak_memory_bytes(&self) -> usize {
        self.inner.peak_memory_bytes
    }

    /// Compile-time plan text (identical to `explain()`).
    #[getter]
    fn plan_text(&self) -> String {
        self.inner.explain.plan_text.clone()
    }

    /// The per-stratum / per-rule / per-iteration profile as nested Python
    /// dicts and lists (snake_case keys; operators carry `operator`,
    /// `actual_rows`, `time_ms`).
    #[getter]
    fn strata(&self, py: Python) -> PyResult<Py<PyAny>> {
        let json = serde_json::to_value(&self.inner.profile.strata)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        crate::convert::json_value_to_py(py, &json)
    }

    fn __str__(&self) -> String {
        format!("{}", self.inner)
    }

    fn __repr__(&self) -> String {
        format!(
            "LocyProfileOutput(total_time_ms={:.3}, peak_memory_bytes={}, strata={})",
            self.inner.total_time_ms,
            self.inner.peak_memory_bytes,
            self.inner.profile.strata.len()
        )
    }
}

/// Information about a vertex label in the schema.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct LabelInfo {
    /// Label name.
    pub name: String,
    /// Approximate count of vertices with this label.
    pub count: usize,
    /// Properties defined on this label.
    pub properties: Vec<PropertyInfo>,
    /// Indexes defined on this label.
    pub indexes: Vec<IndexInfo>,
    /// Constraints defined on this label.
    pub constraints: Vec<ConstraintInfo>,
    /// Optional human-readable description.
    pub description: Option<String>,
}

#[pymethods]
impl LabelInfo {
    fn __repr__(&self) -> String {
        format!(
            "LabelInfo(name='{}', count={}, properties={}, indexes={})",
            self.name,
            self.count,
            self.properties.len(),
            self.indexes.len()
        )
    }
}

/// Information about a property in the schema.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct PropertyInfo {
    /// Property name.
    pub name: String,
    /// Data type (e.g., "String", "Int64", "Vector{128}").
    pub data_type: String,
    /// Whether null values are allowed.
    pub nullable: bool,
    /// Whether an index exists on this property.
    pub is_indexed: bool,
    /// Optional human-readable description.
    pub description: Option<String>,
}

#[pymethods]
impl PropertyInfo {
    fn __repr__(&self) -> String {
        format!(
            "PropertyInfo(name='{}', type='{}', nullable={})",
            self.name, self.data_type, self.nullable
        )
    }
}

/// Information about an index in the schema.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Type of index (SCALAR, VECTOR, FULLTEXT).
    pub index_type: String,
    /// Properties covered by the index.
    pub properties: Vec<String>,
    /// Current status (ONLINE, BUILDING, FAILED).
    pub status: String,
}

#[pymethods]
impl IndexInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexInfo(name='{}', type='{}', properties={:?})",
            self.name, self.index_type, self.properties
        )
    }
}

/// Information about a constraint in the schema.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct ConstraintInfo {
    /// Constraint name.
    pub name: String,
    /// Type of constraint (UNIQUE, EXISTS, CHECK).
    pub constraint_type: String,
    /// Properties covered by the constraint.
    pub properties: Vec<String>,
    /// Whether the constraint is currently enforced.
    pub enabled: bool,
}

#[pymethods]
impl ConstraintInfo {
    fn __repr__(&self) -> String {
        format!(
            "ConstraintInfo(name='{}', type='{}', enabled={})",
            self.name, self.constraint_type, self.enabled
        )
    }
}

/// Information about an edge type in the schema.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct EdgeTypeInfo {
    /// Edge type name.
    pub name: String,
    /// Approximate count of edges with this type.
    pub count: usize,
    /// Allowed source labels.
    pub source_labels: Vec<String>,
    /// Allowed target labels.
    pub target_labels: Vec<String>,
    /// Properties defined on this edge type.
    pub properties: Vec<PropertyInfo>,
    /// Indexes defined on this edge type.
    pub indexes: Vec<IndexInfo>,
    /// Constraints defined on this edge type.
    pub constraints: Vec<ConstraintInfo>,
    /// Optional human-readable description.
    pub description: Option<String>,
}

#[pymethods]
impl EdgeTypeInfo {
    fn __repr__(&self) -> String {
        format!(
            "EdgeTypeInfo(name='{}', count={}, source_labels={:?}, target_labels={:?}, properties={}, indexes={})",
            self.name,
            self.count,
            self.source_labels,
            self.target_labels,
            self.properties.len(),
            self.indexes.len()
        )
    }
}

impl From<::uni_db::api::schema::PropertyInfo> for PropertyInfo {
    fn from(p: ::uni_db::api::schema::PropertyInfo) -> Self {
        PropertyInfo {
            name: p.name,
            data_type: p.data_type,
            nullable: p.nullable,
            is_indexed: p.is_indexed,
            description: p.description,
        }
    }
}

impl From<::uni_db::api::schema::IndexInfo> for IndexInfo {
    fn from(idx: ::uni_db::api::schema::IndexInfo) -> Self {
        IndexInfo {
            name: idx.name,
            index_type: idx.index_type,
            properties: idx.properties,
            status: idx.status,
        }
    }
}

impl From<::uni_db::api::schema::ConstraintInfo> for ConstraintInfo {
    fn from(c: ::uni_db::api::schema::ConstraintInfo) -> Self {
        ConstraintInfo {
            name: c.name,
            constraint_type: c.constraint_type,
            properties: c.properties,
            enabled: c.enabled,
        }
    }
}

impl From<::uni_db::api::schema::LabelInfo> for LabelInfo {
    fn from(i: ::uni_db::api::schema::LabelInfo) -> Self {
        LabelInfo {
            name: i.name,
            count: i.count,
            properties: i.properties.into_iter().map(Into::into).collect(),
            indexes: i.indexes.into_iter().map(Into::into).collect(),
            constraints: i.constraints.into_iter().map(Into::into).collect(),
            description: i.description,
        }
    }
}

impl From<::uni_db::api::schema::EdgeTypeInfo> for EdgeTypeInfo {
    fn from(i: ::uni_db::api::schema::EdgeTypeInfo) -> Self {
        EdgeTypeInfo {
            name: i.name,
            count: i.count,
            source_labels: i.source_labels,
            target_labels: i.target_labels,
            properties: i.properties.into_iter().map(Into::into).collect(),
            indexes: i.indexes.into_iter().map(Into::into).collect(),
            constraints: i.constraints.into_iter().map(Into::into).collect(),
            description: i.description,
        }
    }
}

/// Statistics from a bulk loading operation.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone, Default)]
pub struct BulkStats {
    /// Number of vertices inserted.
    pub vertices_inserted: usize,
    /// Number of edges inserted.
    pub edges_inserted: usize,
    /// Number of indexes rebuilt.
    pub indexes_rebuilt: usize,
    /// Total duration in seconds.
    pub duration_secs: f64,
    /// Duration spent building indexes in seconds.
    pub index_build_duration_secs: f64,
    /// Task IDs for async index rebuilds.
    pub index_task_ids: Vec<String>,
    /// Whether indexes are still building in background.
    pub indexes_pending: bool,
}

#[pymethods]
impl BulkStats {
    fn __repr__(&self) -> String {
        format!(
            "BulkStats(vertices={}, edges={}, duration={:.2}s)",
            self.vertices_inserted, self.edges_inserted, self.duration_secs
        )
    }
}

/// Progress callback data during bulk loading.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct BulkProgress {
    /// Current phase of bulk loading.
    pub phase: String,
    /// Number of rows processed so far.
    pub rows_processed: usize,
    /// Total rows if known.
    pub total_rows: Option<usize>,
    /// Current label being processed.
    pub current_label: Option<String>,
    /// Elapsed time in seconds since bulk loading started.
    pub elapsed_secs: f64,
}

#[pymethods]
impl BulkProgress {
    fn __repr__(&self) -> String {
        format!(
            "BulkProgress(phase='{}', processed={}, elapsed={:.2}s)",
            self.phase, self.rows_processed, self.elapsed_secs
        )
    }
}

/// Statistics from a Locy program evaluation.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct LocyStats {
    /// Number of strata evaluated.
    pub strata_evaluated: usize,
    /// Total fixpoint iterations across all strata.
    pub total_iterations: usize,
    /// Total derived nodes across all rules.
    pub derived_nodes: usize,
    /// Total derived edges across all rules.
    pub derived_edges: usize,
    /// Total evaluation time in seconds.
    pub evaluation_time_secs: f64,
    /// Number of Cypher queries executed.
    pub queries_executed: usize,
    /// Number of mutations executed.
    pub mutations_executed: usize,
    /// Peak memory used by derived relations in bytes.
    pub peak_memory_bytes: usize,
}

#[pymethods]
impl LocyStats {
    fn __repr__(&self) -> String {
        format!(
            "LocyStats(strata={}, iterations={}, time={:.3}s)",
            self.strata_evaluated, self.total_iterations, self.evaluation_time_secs
        )
    }
}

/// Result of a Locy program evaluation, mirroring the Rust `LocyResult`.
#[pyclass(get_all, name = "LocyResult")]
#[derive(Debug)]
pub struct PyLocyResult {
    /// Derived relations as a dict of relation-name → list[dict].
    pub derived: Py<PyAny>,
    /// Evaluation statistics (`LocyStats`).
    pub stats: Py<PyAny>,
    /// Command results (goal queries, explanations, etc.).
    pub command_results: Py<PyAny>,
    /// Runtime warnings emitted during evaluation.
    pub warnings: Py<PyAny>,
    /// Compile-time warnings from rule compilation, as a list of dicts with
    /// `code`, `message` and `rule_name`.
    ///
    /// These are emitted before evaluation starts and cover static hazards the
    /// compiler can see — an aggregate inside a recursive stratum, an
    /// uncalibrated neural predicate, a possibly-negative `MSUM` argument.
    /// They were previously dropped at the PyO3 boundary, so a Python caller
    /// had no way to see them (issue #159 was filed against a program the
    /// compiler had already warned about).
    pub compile_warnings: Py<PyAny>,
    /// Groups flagged as approximate (shared-proof detection).
    pub approximate_groups: Py<PyAny>,
    /// Opaque derived fact set, pass to `tx.apply()` to materialize.
    pub derived_fact_set: Py<PyAny>,
    /// `True` when evaluation was cut short (timeout or iteration limit). Only
    /// ever set on the `allow_partial=True` path — by default an over-budget
    /// evaluation raises `UniLocyIncompleteError` instead of returning a result.
    pub timed_out: bool,
    /// Diagnostics dict for a partial evaluation, or `None` when complete. Keys:
    /// `reason`, `elapsed_ms`, `limit_ms`, `max_iterations`, `completed_strata`,
    /// `total_strata`, `incomplete_rules`, `skipped_rules`,
    /// `complement_rules_affected`.
    pub incomplete: Py<PyAny>,
}

#[pymethods]
impl PyLocyResult {
    fn __repr__(&self, py: Python<'_>) -> String {
        let n_warnings = self.warnings.bind(py).len().unwrap_or(0);
        format!(
            "LocyResult(stats={}, warnings={})",
            self.stats
                .bind(py)
                .repr()
                .map_or_else(|_| "?".to_string(), |r| r.to_string(),),
            n_warnings,
        )
    }

    /// Check whether any warning with the given code string was emitted.
    fn has_warning(&self, py: Python<'_>, code: &str) -> PyResult<bool> {
        let list = self.warnings.bind(py);
        let len = list.len()?;
        for i in 0..len {
            let item = list.get_item(i)?;
            // Each warning is expected to have a `.code` attribute (string).
            if let Ok(c) = item.getattr("code")
                && let Ok(s) = c.extract::<String>()
                && s == code
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Return the list of runtime warnings.
    fn warnings_list(&self, py: Python<'_>) -> Py<PyAny> {
        self.warnings.clone_ref(py)
    }

    /// Get derived facts for a specific rule name.
    ///
    /// Returns the list of fact dicts for the given rule, or `None` if the rule
    /// produced no derived facts.
    fn derived_facts(&self, py: Python<'_>, rule: &str) -> PyResult<Option<Py<PyAny>>> {
        let result = self.derived.bind(py).call_method1("get", (rule,))?;
        if result.is_none() {
            Ok(None)
        } else {
            Ok(Some(result.unbind()))
        }
    }

    /// Get rows from the first Query/Assume/Cypher command result, if any.
    fn rows(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let list = self.command_results.bind(py);
        for i in 0..list.len()? {
            let item = list.get_item(i)?;
            // Command results are dicts; Query/Assume/Cypher variants have a "rows" key.
            if let Ok(rows) = item.get_item("rows") {
                return Ok(Some(rows.unbind()));
            }
        }
        Ok(None)
    }

    /// Get column names from the first row of command results.
    fn columns(&self, py: Python<'_>) -> PyResult<Option<Vec<String>>> {
        if let Some(rows_obj) = self.rows(py)? {
            let rows = rows_obj.bind(py);
            if rows.len()? > 0 {
                let first_row = rows.get_item(0)?;
                let keys = first_row.call_method0("keys")?;
                return Ok(Some(keys.extract::<Vec<String>>()?));
            }
        }
        Ok(None)
    }

    /// Total number of fixpoint iterations.
    ///
    /// Shorthand for `self.stats.total_iterations`.
    #[getter]
    fn iterations(&self, py: Python<'_>) -> PyResult<usize> {
        self.stats.bind(py).getattr("total_iterations")?.extract()
    }
}

/// A compiled Locy program ready for evaluation.
#[pyclass(name = "CompiledProgram")]
pub struct PyCompiledProgram {
    pub(crate) inner: uni_locy::CompiledProgram,
}

#[pymethods]
impl PyCompiledProgram {
    fn __repr__(&self) -> String {
        format!(
            "CompiledProgram(strata={}, rules={})",
            self.inner.strata.len(),
            self.inner.rule_catalog.len(),
        )
    }

    /// Number of strata in the compiled program.
    #[getter]
    fn num_strata(&self) -> usize {
        self.inner.strata.len()
    }

    /// Number of compiled rules.
    #[getter]
    fn num_rules(&self) -> usize {
        self.inner.rule_catalog.len()
    }

    /// Names of all compiled rules.
    #[getter]
    fn rule_names(&self) -> Vec<String> {
        self.inner.rule_catalog.keys().cloned().collect()
    }
}

// ============================================================================
// Xervo types
// ============================================================================

/// A message in a conversation (role + text content).
#[pyclass(get_all, name = "Message", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyMessage {
    /// Role: "user", "assistant", or "system".
    pub role: String,
    /// Text content of the message.
    pub content: String,
}

#[pymethods]
impl PyMessage {
    #[new]
    fn new(role: String, content: String) -> Self {
        Self { role, content }
    }

    /// Create a user message.
    #[staticmethod]
    fn user(text: String) -> Self {
        Self {
            role: "user".to_string(),
            content: text,
        }
    }

    /// Create an assistant message.
    #[staticmethod]
    fn assistant(text: String) -> Self {
        Self {
            role: "assistant".to_string(),
            content: text,
        }
    }

    /// Create a system message.
    #[staticmethod]
    fn system(text: String) -> Self {
        Self {
            role: "system".to_string(),
            content: text,
        }
    }

    fn __repr__(&self) -> String {
        format!("Message(role='{}', content='{}')", self.role, self.content)
    }
}

/// Token usage statistics from a generation call.
#[pyclass(get_all, name = "TokenUsage", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyTokenUsage {
    pub prompt_tokens: usize,
    pub completion_tokens: usize,
    pub total_tokens: usize,
}

#[pymethods]
impl PyTokenUsage {
    fn __repr__(&self) -> String {
        format!(
            "TokenUsage(prompt={}, completion={}, total={})",
            self.prompt_tokens, self.completion_tokens, self.total_tokens
        )
    }
}

/// Result of a Xervo generation call.
#[pyclass(get_all, name = "GenerationResult")]
#[derive(Debug)]
pub struct PyGenerationResult {
    /// The generated text.
    pub text: String,
    /// Token usage statistics, if available.
    pub usage: Option<Py<PyTokenUsage>>,
}

#[pymethods]
impl PyGenerationResult {
    fn __repr__(&self) -> String {
        format!(
            "GenerationResult(text='{}...')",
            self.text.chars().take(40).collect::<String>()
        )
    }
}

// ============================================================================
// Snapshot + Index types
// ============================================================================

/// Information about a database snapshot.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    /// Unique snapshot identifier.
    pub snapshot_id: String,
    /// Optional human-readable name.
    pub name: Option<String>,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
    /// Version high-water mark at snapshot time.
    pub version_hwm: u64,
}

#[pymethods]
impl SnapshotInfo {
    fn __repr__(&self) -> String {
        format!(
            "SnapshotInfo(id='{}', name={:?}, created_at='{}')",
            self.snapshot_id, self.name, self.created_at
        )
    }
}

/// Status of a background index rebuild task.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct IndexRebuildTaskInfo {
    pub id: String,
    pub label: String,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub error: Option<String>,
    pub retry_count: u32,
}

#[pymethods]
impl IndexRebuildTaskInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexRebuildTaskInfo(id='{}', label='{}', status='{}')",
            self.id, self.label, self.status
        )
    }
}

/// Definition of an index in the schema.
#[pyclass(get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct IndexDefinitionInfo {
    pub name: String,
    pub index_type: String,
    pub label: String,
    pub properties: Vec<String>,
    pub state: String,
}

#[pymethods]
impl IndexDefinitionInfo {
    fn __repr__(&self) -> String {
        format!(
            "IndexDefinitionInfo(name='{}', type='{}', label='{}')",
            self.name, self.index_type, self.label
        )
    }
}

// ============================================================================
// Commit notification
// ============================================================================

/// A commit notification describing the effects of a committed transaction.
#[pyclass(get_all, name = "CommitNotification", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCommitNotification {
    /// Database version after commit.
    pub version: u64,
    /// Number of mutations in the committed transaction.
    pub mutation_count: usize,
    /// Vertex labels affected by the commit.
    pub labels_affected: Vec<String>,
    /// Edge types affected by the commit.
    pub edge_types_affected: Vec<String>,
    /// Number of Locy rules promoted.
    pub rules_promoted: usize,
    /// ISO 8601 timestamp of the commit.
    pub timestamp: String,
    /// Transaction ID.
    pub tx_id: String,
    /// Session ID that committed the transaction.
    pub session_id: String,
    /// Database version when the transaction started.
    pub causal_version: u64,
}

#[pymethods]
impl PyCommitNotification {
    fn __repr__(&self) -> String {
        format!(
            "CommitNotification(version={}, mutations={}, labels={:?})",
            self.version, self.mutation_count, self.labels_affected
        )
    }
}

impl From<::uni_db::CommitNotification> for PyCommitNotification {
    fn from(n: ::uni_db::CommitNotification) -> Self {
        Self {
            version: n.version,
            mutation_count: n.mutation_count,
            labels_affected: n.labels_affected,
            edge_types_affected: n.edge_types_affected,
            rules_promoted: n.rules_promoted,
            timestamp: n.timestamp.to_rfc3339(),
            tx_id: n.tx_id,
            session_id: n.session_id,
            causal_version: n.causal_version,
        }
    }
}

/// Session capabilities snapshot.
#[pyclass(get_all, name = "SessionCapabilities", from_py_object)]
#[derive(Debug, Clone)]
pub struct PySessionCapabilities {
    /// Whether the session can create transactions and execute writes.
    pub can_write: bool,
    /// Whether the session supports version pinning.
    pub can_pin: bool,
    /// The isolation level used for transactions.
    pub isolation: String,
    /// Whether commit notifications are available.
    pub has_notifications: bool,
    /// Write lease configuration, if any (e.g., "local", "dynamodb:table_name").
    pub write_lease: Option<String>,
}

/// Metadata about a registered Locy rule.
#[pyclass(get_all, name = "RuleInfo", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyRuleInfo {
    /// Rule name.
    pub name: String,
    /// Number of clauses in the rule.
    pub clause_count: usize,
    /// Whether the rule is recursive.
    pub is_recursive: bool,
}

/// Statistics from a compaction operation.
#[pyclass(get_all, name = "CompactionStats", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCompactionStats {
    /// Number of files compacted.
    pub files_compacted: usize,
    /// Total bytes before compaction.
    pub bytes_before: u64,
    /// Total bytes after compaction.
    pub bytes_after: u64,
    /// Duration in seconds.
    pub duration_secs: f64,
    /// Number of CRDT merge operations performed.
    pub crdt_merges: usize,
}

#[pymethods]
impl PyCompactionStats {
    fn __repr__(&self) -> String {
        format!(
            "CompactionStats(files={}, before={}B, after={}B, duration={:.2}s)",
            self.files_compacted, self.bytes_before, self.bytes_after, self.duration_secs
        )
    }
}

#[pymethods]
impl PySessionCapabilities {
    fn __repr__(&self) -> String {
        format!(
            "SessionCapabilities(can_write={}, has_notifications={})",
            self.can_write, self.has_notifications
        )
    }
}

// ============================================================================
// Transaction commit result
// ============================================================================

/// A rule promotion error from a transaction commit.
#[pyclass(get_all, name = "RulePromotionError", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyRulePromotionError {
    /// The rule text that failed.
    pub rule_text: String,
    /// The error message.
    pub error: String,
}

#[pymethods]
impl PyRulePromotionError {
    fn __repr__(&self) -> String {
        format!(
            "RulePromotionError(rule='{}...', error='{}')",
            self.rule_text.chars().take(40).collect::<String>(),
            self.error
        )
    }
}

/// Result of committing a transaction.
#[pyclass(name = "CommitResult")]
pub struct PyCommitResult {
    /// Number of mutations committed.
    #[pyo3(get)]
    pub mutations_committed: usize,
    /// Number of rules promoted to the parent session.
    #[pyo3(get)]
    pub rules_promoted: usize,
    /// Database version after commit.
    #[pyo3(get)]
    pub version: u64,
    /// Database version when the transaction was created.
    #[pyo3(get)]
    pub started_at_version: u64,
    /// WAL log sequence number.
    #[pyo3(get)]
    pub wal_lsn: u64,
    /// Duration of the commit operation in seconds.
    #[pyo3(get)]
    pub duration_secs: f64,
    /// Rule promotion errors (empty if all rules promoted successfully).
    #[pyo3(get)]
    pub rule_promotion_errors: Vec<PyRulePromotionError>,
}

impl From<::uni_db::CommitResult> for PyCommitResult {
    fn from(r: ::uni_db::CommitResult) -> Self {
        Self {
            mutations_committed: r.mutations_committed,
            rules_promoted: r.rules_promoted,
            version: r.version,
            started_at_version: r.started_at_version,
            wal_lsn: r.wal_lsn,
            duration_secs: r.duration.as_secs_f64(),
            rule_promotion_errors: r
                .rule_promotion_errors
                .into_iter()
                .map(|e| PyRulePromotionError {
                    rule_text: e.rule_text,
                    error: e.error,
                })
                .collect(),
        }
    }
}

#[pymethods]
impl PyCommitResult {
    /// Number of versions between start and commit (0 means no concurrent commits).
    fn version_gap(&self) -> u64 {
        self.version.saturating_sub(self.started_at_version + 1)
    }

    fn __repr__(&self) -> String {
        format!(
            "CommitResult(mutations={}, version={}, duration={:.3}s)",
            self.mutations_committed, self.version, self.duration_secs
        )
    }
}

// ============================================================================
// PreparedQuery
// ============================================================================

/// A prepared Cypher query that can be executed multiple times with different parameters.
#[pyclass]
pub struct PyPreparedQuery {
    // Shared, lock-free handle. `PreparedQuery` is `Send + Sync` with its own
    // internal `RwLock`, so an outer `Mutex` is unnecessary — and holding a
    // `Mutex` guard across the GIL-releasing `py.detach(block_on(..))` below
    // created a GIL/mutex ABBA deadlock (correctness-scan R13). `Arc` shares
    // without a lock, so nothing is held across the await.
    pub inner: std::sync::Arc<::uni_db::PreparedQuery>,
}

#[pymethods]
impl PyPreparedQuery {
    /// Execute the prepared query with optional parameter bindings.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    #[pyo3(signature = (params=None))]
    fn execute(
        &self,
        py: pyo3::Python,
        params: Option<std::collections::HashMap<String, pyo3::Py<pyo3::PyAny>>>,
    ) -> pyo3::PyResult<PyQueryResult> {
        let rust_params: Vec<(String, ::uni_db::Value)> = if let Some(p) = params {
            p.into_iter()
                .map(|(k, v)| {
                    let val = crate::convert::py_object_to_value(py, &v)?;
                    Ok((k, val))
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let param_refs: Vec<(&str, ::uni_db::Value)> = rust_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        // Clone the shared handle (not a lock) so nothing is held across the
        // GIL-releasing `py.detach` — see the ABBA note on `inner` (R13).
        let prepared = std::sync::Arc::clone(&self.inner);
        let result = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(prepared.execute(&param_refs))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        crate::convert::query_result_to_py_class(py, result)
    }

    /// Get the original query text.
    fn query_text(&self) -> pyo3::PyResult<String> {
        Ok(self.inner.query_text().to_string())
    }

    fn __repr__(&self) -> String {
        format!("PreparedQuery({:?})", self.inner.query_text())
    }

    /// Create a fluent binder for this prepared query.
    fn bind(slf: Py<Self>) -> PyPreparedQueryBinder {
        PyPreparedQueryBinder {
            prepared: slf,
            params: std::collections::HashMap::new(),
        }
    }
}

// ============================================================================
// ExecuteResult
// ============================================================================

/// Result of a transaction.execute() call.
#[pyclass(get_all, name = "ExecuteResult")]
#[derive(Debug)]
pub struct PyExecuteResult {
    pub affected_rows: usize,
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
    pub metrics: Py<pyo3::types::PyDict>,
}

#[pymethods]
impl PyExecuteResult {
    fn __repr__(&self) -> String {
        format!(
            "ExecuteResult(affected={}, nodes_created={})",
            self.affected_rows, self.nodes_created
        )
    }
}

// ============================================================================
// ApplyResult
// ============================================================================

/// Result of applying a DerivedFactSet to a transaction.
#[pyclass(get_all, name = "ApplyResult", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyApplyResult {
    pub facts_applied: usize,
    pub version_gap: u64,
}

#[pymethods]
impl PyApplyResult {
    fn __repr__(&self) -> String {
        format!(
            "ApplyResult(facts={}, version_gap={})",
            self.facts_applied, self.version_gap
        )
    }
}

// ============================================================================
// DerivedFactSet
// ============================================================================

/// Opaque wrapper around a Locy-derived fact set.
///
/// Obtained from `LocyResult.derived_fact_set` and passed to `tx.apply()`.
#[pyclass(name = "DerivedFactSet")]
pub struct PyDerivedFactSet {
    pub(crate) inner: Option<uni_locy::DerivedFactSet>,
}

#[pymethods]
impl PyDerivedFactSet {
    /// Database version at evaluation time.
    #[getter]
    fn evaluated_at_version(&self) -> PyResult<u64> {
        self.inner
            .as_ref()
            .map(|d| d.evaluated_at_version)
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
            })
    }

    /// Number of derived vertices.
    #[getter]
    fn vertex_count(&self) -> PyResult<usize> {
        self.inner
            .as_ref()
            .map(|d| d.vertices.values().map(|v| v.len()).sum())
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
            })
    }

    /// Number of derived edges.
    #[getter]
    fn edge_count(&self) -> PyResult<usize> {
        self.inner.as_ref().map(|d| d.edges.len()).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
        })
    }

    /// Total number of derived facts.
    #[getter]
    fn fact_count(&self) -> PyResult<usize> {
        self.inner.as_ref().map(|d| d.fact_count()).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
        })
    }

    /// Check if no facts were derived.
    fn is_empty(&self) -> PyResult<bool> {
        self.inner
            .as_ref()
            .map(|d| d.fact_count() == 0)
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
            })
    }

    /// Derived vertices grouped by label.
    ///
    /// Returns a dict mapping label names to lists of property dicts.
    #[getter]
    fn vertices(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
        })?;
        let dict = pyo3::types::PyDict::new(py);
        for (label, props_list) in &inner.vertices {
            let py_list = pyo3::types::PyList::empty(py);
            for props in props_list {
                let row_dict = pyo3::types::PyDict::new(py);
                for (k, v) in props.iter() {
                    row_dict.set_item(k, crate::convert::value_to_py(py, v)?)?;
                }
                py_list.append(row_dict)?;
            }
            dict.set_item(label.as_str(), py_list)?;
        }
        Ok(dict.into_any().unbind())
    }

    /// Derived edges as a list of edge dicts.
    ///
    /// Each dict has keys: `edge_type`, `source_label`, `source_properties`,
    /// `target_label`, `target_properties`, `edge_properties`.
    #[getter]
    fn edges(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("DerivedFactSet already consumed")
        })?;
        let py_list = pyo3::types::PyList::empty(py);
        for edge in &inner.edges {
            let d = pyo3::types::PyDict::new(py);
            d.set_item("edge_type", &edge.edge_type)?;
            d.set_item("source_label", &edge.source_label)?;
            d.set_item("target_label", &edge.target_label)?;
            let src = pyo3::types::PyDict::new(py);
            for (k, v) in edge.source_properties.iter() {
                src.set_item(k, crate::convert::value_to_py(py, v)?)?;
            }
            d.set_item("source_properties", src)?;
            let tgt = pyo3::types::PyDict::new(py);
            for (k, v) in edge.target_properties.iter() {
                tgt.set_item(k, crate::convert::value_to_py(py, v)?)?;
            }
            d.set_item("target_properties", tgt)?;
            let ep = pyo3::types::PyDict::new(py);
            for (k, v) in edge.edge_properties.iter() {
                ep.set_item(k, crate::convert::value_to_py(py, v)?)?;
            }
            d.set_item("edge_properties", ep)?;
            py_list.append(d)?;
        }
        Ok(py_list.into_any().unbind())
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            Some(d) => format!(
                "DerivedFactSet(facts={}, version={})",
                d.fact_count(),
                d.evaluated_at_version
            ),
            None => "DerivedFactSet(<consumed>)".to_string(),
        }
    }
}

// ============================================================================
// SessionMetrics
// ============================================================================

/// Metrics for a session's lifetime.
#[pyclass(get_all, name = "SessionMetrics", from_py_object)]
#[derive(Debug, Clone)]
pub struct PySessionMetrics {
    pub session_id: String,
    /// Seconds since the session was created.
    pub active_since_secs: f64,
    pub queries_executed: u64,
    pub locy_evaluations: u64,
    pub total_query_time_secs: f64,
    pub transactions_committed: u64,
    pub transactions_rolled_back: u64,
    pub total_rows_returned: u64,
    pub total_rows_scanned: u64,
    pub plan_cache_hits: u64,
    pub plan_cache_misses: u64,
    pub plan_cache_size: usize,
}

#[pymethods]
impl PySessionMetrics {
    fn __repr__(&self) -> String {
        format!(
            "SessionMetrics(session='{}', queries={}, txns={})",
            self.session_id, self.queries_executed, self.transactions_committed
        )
    }
}

// ============================================================================
// PreparedLocy
// ============================================================================

/// A prepared Locy program that can be executed multiple times.
#[pyclass(name = "PreparedLocy")]
pub struct PyPreparedLocy {
    // Shared, lock-free handle — see `PyPreparedQuery::inner` (correctness-scan R13).
    pub(crate) inner: std::sync::Arc<::uni_db::PreparedLocy>,
}

#[pymethods]
impl PyPreparedLocy {
    /// Execute the prepared Locy program with optional parameter bindings.
    #[pyo3(signature = (params=None))]
    fn execute(
        &self,
        py: pyo3::Python,
        params: Option<std::collections::HashMap<String, Py<PyAny>>>,
    ) -> pyo3::PyResult<PyLocyResult> {
        let rust_params: Vec<(String, ::uni_db::Value)> = if let Some(p) = params {
            p.into_iter()
                .map(|(k, v)| {
                    let val = crate::convert::py_object_to_value(py, &v)?;
                    Ok((k, val))
                })
                .collect::<pyo3::PyResult<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let param_refs: Vec<(&str, ::uni_db::Value)> = rust_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        // Clone the shared handle (not a lock); nothing held across `py.detach` (R13).
        let prepared = std::sync::Arc::clone(&self.inner);
        let result = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(prepared.execute(&param_refs))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        crate::convert::locy_result_to_py_class(py, result)
    }

    /// Get the original program text.
    fn program_text(&self) -> pyo3::PyResult<String> {
        Ok(self.inner.program_text().to_string())
    }

    fn __repr__(&self) -> String {
        let t = self.inner.program_text();
        // Truncate by CHARACTER, not byte index: `&t[..60]` panics when
        // byte 60 falls inside a multibyte UTF-8 codepoint.
        let text = match t.char_indices().nth(60) {
            Some((byte_idx, _)) => format!("{}...", &t[..byte_idx]),
            None => t.to_string(),
        };
        format!("PreparedLocy({:?})", text)
    }

    /// Create a fluent binder for this prepared Locy program.
    fn bind(slf: Py<Self>) -> PyPreparedLocyBinder {
        PyPreparedLocyBinder {
            prepared: slf,
            params: std::collections::HashMap::new(),
        }
    }
}

// ============================================================================
// CancellationToken
// ============================================================================

/// A cooperative cancellation token for long-running operations.
///
/// Call `cancel()` to request cancellation; operations holding a reference
/// to the same token will observe the cancellation and terminate early.
#[pyclass(name = "CancellationToken", from_py_object)]
#[derive(Clone)]
pub struct PyCancellationToken {
    pub(crate) inner: tokio_util::sync::CancellationToken,
}

#[pymethods]
impl PyCancellationToken {
    /// Request cancellation.
    fn cancel(&self) {
        self.inner.cancel();
    }

    /// Check whether cancellation has been requested.
    fn is_cancelled(&self) -> bool {
        self.inner.is_cancelled()
    }

    fn __repr__(&self) -> String {
        format!("CancellationToken(cancelled={})", self.inner.is_cancelled())
    }
}

// ============================================================================
// PreparedQueryBinder / PreparedLocyBinder
// ============================================================================

/// A fluent binder for executing a prepared Cypher query with named parameters.
#[pyclass(name = "PreparedQueryBinder")]
pub struct PyPreparedQueryBinder {
    pub(crate) prepared: Py<PyPreparedQuery>,
    pub(crate) params: std::collections::HashMap<String, Py<PyAny>>,
}

#[pymethods]
impl PyPreparedQueryBinder {
    /// Bind a named parameter.
    fn param(
        mut slf: pyo3::PyRefMut<'_, Self>,
        name: String,
        value: Py<PyAny>,
    ) -> pyo3::PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Execute with bound parameters.
    fn execute(&self, py: pyo3::Python) -> pyo3::PyResult<PyQueryResult> {
        let prepared = self.prepared.borrow(py);
        let rust_params: Vec<(String, ::uni_db::Value)> = self
            .params
            .iter()
            .map(|(k, v)| {
                let val = crate::convert::py_object_to_value(py, v)?;
                Ok((k.clone(), val))
            })
            .collect::<pyo3::PyResult<Vec<_>>>()?;
        let param_refs: Vec<(&str, ::uni_db::Value)> = rust_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        // Clone the shared handle (not a lock); nothing held across `py.detach` (R13).
        let handle = std::sync::Arc::clone(&prepared.inner);
        let result = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(handle.execute(&param_refs))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        crate::convert::query_result_to_py_class(py, result)
    }
}

/// A fluent binder for executing a prepared Locy program with named parameters.
#[pyclass(name = "PreparedLocyBinder")]
pub struct PyPreparedLocyBinder {
    pub(crate) prepared: Py<PyPreparedLocy>,
    pub(crate) params: std::collections::HashMap<String, Py<PyAny>>,
}

#[pymethods]
impl PyPreparedLocyBinder {
    /// Bind a named parameter.
    fn param(
        mut slf: pyo3::PyRefMut<'_, Self>,
        name: String,
        value: Py<PyAny>,
    ) -> pyo3::PyRefMut<'_, Self> {
        slf.params.insert(name, value);
        slf
    }

    /// Execute with bound parameters.
    fn execute(&self, py: pyo3::Python) -> pyo3::PyResult<PyLocyResult> {
        let prepared = self.prepared.borrow(py);
        let rust_params: Vec<(String, ::uni_db::Value)> = self
            .params
            .iter()
            .map(|(k, v)| {
                let val = crate::convert::py_object_to_value(py, v)?;
                Ok((k.clone(), val))
            })
            .collect::<pyo3::PyResult<Vec<_>>>()?;
        let param_refs: Vec<(&str, ::uni_db::Value)> = rust_params
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();
        // Clone the shared handle (not a lock); nothing held across `py.detach` (R13).
        let handle = std::sync::Arc::clone(&prepared.inner);
        let result = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(handle.execute(&param_refs))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        crate::convert::locy_result_to_py_class(py, result)
    }
}

// ============================================================================
// WriteLease
// ============================================================================

/// Write lease configuration for multi-agent coordination.
#[pyclass(name = "WriteLease", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyWriteLease {
    pub(crate) variant: WriteLeaseVariant,
}

#[derive(Debug, Clone)]
pub(crate) enum WriteLeaseVariant {
    Local,
    DynamoDB { table: String },
}

#[pymethods]
impl PyWriteLease {
    /// Create a local (single-process) write lease.
    #[staticmethod]
    #[pyo3(name = "LOCAL")]
    fn local() -> Self {
        Self {
            variant: WriteLeaseVariant::Local,
        }
    }

    /// Create a DynamoDB-based distributed write lease.
    #[staticmethod]
    #[pyo3(name = "DYNAMODB")]
    fn dynamodb(table: String) -> Self {
        Self {
            variant: WriteLeaseVariant::DynamoDB { table },
        }
    }

    fn __repr__(&self) -> String {
        match &self.variant {
            WriteLeaseVariant::Local => "WriteLease.LOCAL".to_string(),
            WriteLeaseVariant::DynamoDB { table } => {
                format!("WriteLease.DYNAMODB(table={:?})", table)
            }
        }
    }
}

// ============================================================================
// DatabaseMetrics
// ============================================================================

/// Database-wide metrics snapshot.
#[pyclass(get_all, name = "DatabaseMetrics", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyDatabaseMetrics {
    pub l0_mutation_count: usize,
    pub l0_estimated_size_bytes: usize,
    pub schema_version: u64,
    pub uptime_secs: f64,
    pub active_sessions: usize,
    pub l1_run_count: usize,
    pub write_throttle_pressure: f64,
    pub compaction_in_progress: bool,
    pub wal_size_bytes: u64,
    pub wal_lsn: u64,
    pub total_queries: u64,
    pub total_commits: u64,
}

#[pymethods]
impl PyDatabaseMetrics {
    fn __repr__(&self) -> String {
        format!(
            "DatabaseMetrics(queries={}, commits={}, sessions={}, uptime={:.1}s)",
            self.total_queries, self.total_commits, self.active_sessions, self.uptime_secs
        )
    }
}

// ============================================================================
// CommitStream (sync iterator)
// ============================================================================

/// A synchronous iterator over commit notifications.
///
/// Use as a context manager or call `close()` when done.
#[pyclass(name = "CommitStream")]
pub struct PyCommitStream {
    pub(crate) inner: std::sync::Mutex<Option<::uni_db::CommitStream>>,
    // `close()` used to block acquiring `inner` while `__next__` held it across
    // a GIL-releasing `block_on(stream.next())` that parks indefinitely waiting
    // for a commit — a GIL/mutex deadlock (correctness-scan R13). We now (a) take
    // the stream out of the mutex for the await so `inner` is never held across
    // it, and (b) let `close()` actively interrupt an in-flight `next()` via a
    // wakeup, so a parked reader returns promptly instead of hanging.
    pub(crate) closed: std::sync::atomic::AtomicBool,
    pub(crate) close_notify: std::sync::Arc<tokio::sync::Notify>,
}

#[pymethods]
impl PyCommitStream {
    fn __iter__(slf: pyo3::PyRef<'_, Self>) -> pyo3::PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> pyo3::PyResult<Option<PyCommitNotification>> {
        use std::sync::atomic::Ordering;
        // Take the stream out of the mutex so `inner` is NOT held across the
        // GIL-releasing await (R13). A concurrent `__next__` sees `None` and
        // stops — single-stream concurrent iteration is unordered anyway.
        let mut stream = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            if self.closed.load(Ordering::SeqCst) {
                return Ok(None);
            }
            match guard.take() {
                Some(s) => s,
                None => return Ok(None),
            }
        };
        let notify = std::sync::Arc::clone(&self.close_notify);
        let closed = &self.closed;
        // Await the next notification, but race it against `close()`'s wakeup so
        // a parked reader returns promptly. `stream.next()` (a broadcast recv)
        // is cancel-safe, so dropping it when `close()` wins loses no message.
        let notification = py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let notified = notify.notified();
                tokio::pin!(notified);
                // Register the waiter BEFORE re-checking `closed`, so a `close()`
                // racing between the take above and here cannot be lost.
                notified.as_mut().enable();
                if closed.load(Ordering::SeqCst) {
                    return None;
                }
                tokio::select! {
                    n = stream.next() => n,
                    _ = notified => None,
                }
            })
        });
        // Re-lock briefly: put the stream back unless `close()` ran during the
        // await (then drop it). Either way, surface any item already pulled.
        {
            let mut guard = self
                .inner
                .lock()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            if !self.closed.load(Ordering::SeqCst) {
                *guard = Some(stream);
            }
        }
        Ok(notification.map(PyCommitNotification::from))
    }

    /// Close the stream, releasing resources.
    fn close(&self) -> pyo3::PyResult<()> {
        use std::sync::atomic::Ordering;
        // Signal first (no lock held) so a reader parked in `__next__` wakes
        // promptly, then take the stream — `inner` is now uncontended (R13).
        self.closed.store(true, Ordering::SeqCst);
        self.close_notify.notify_waiters();
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        guard.take();
        Ok(())
    }

    fn __enter__(slf: pyo3::PyRef<'_, Self>) -> pyo3::PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<pyo3::Py<pyo3::PyAny>>,
        _exc_val: Option<pyo3::Py<pyo3::PyAny>>,
        _exc_tb: Option<pyo3::Py<pyo3::PyAny>>,
    ) -> pyo3::PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

// ============================================================================
// WatchBuilder
// ============================================================================

/// Builder for configuring a commit watch stream.
#[pyclass(name = "WatchBuilder")]
pub struct PyWatchBuilder {
    pub(crate) inner: Option<::uni_db::WatchBuilder>,
}

#[pymethods]
impl PyWatchBuilder {
    /// Filter notifications to only include commits affecting these labels.
    fn labels<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        labels: Vec<String>,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
        slf.inner = Some(builder.labels(&label_refs));
        Ok(slf)
    }

    /// Filter notifications to only include commits affecting these edge types.
    fn edge_types<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        types: Vec<String>,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        let type_refs: Vec<&str> = types.iter().map(|s| s.as_str()).collect();
        slf.inner = Some(builder.edge_types(&type_refs));
        Ok(slf)
    }

    /// Set a debounce interval in seconds.
    fn debounce<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        seconds: f64,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        slf.inner = Some(builder.debounce(std::time::Duration::from_secs_f64(seconds)));
        Ok(slf)
    }

    /// Exclude notifications from a specific session.
    fn exclude_session<'py>(
        mut slf: pyo3::PyRefMut<'py, Self>,
        session_id: &str,
    ) -> pyo3::PyResult<pyo3::PyRefMut<'py, Self>> {
        let builder = slf.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        slf.inner = Some(builder.exclude_session(session_id));
        Ok(slf)
    }

    /// Build and return a synchronous CommitStream.
    fn build(&mut self) -> pyo3::PyResult<PyCommitStream> {
        let builder = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        Ok(PyCommitStream {
            inner: std::sync::Mutex::new(Some(builder.build())),
            closed: std::sync::atomic::AtomicBool::new(false),
            close_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Build and return an async CommitStream.
    fn build_async(&mut self) -> pyo3::PyResult<crate::async_api::AsyncCommitStream> {
        let builder = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("WatchBuilder already consumed")
        })?;
        Ok(crate::async_api::AsyncCommitStream {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(Some(builder.build()))),
        })
    }
}

// ============================================================================
// ModelRuntime (shared Xervo runtime handle)
// ============================================================================

/// An opaque handle to a Xervo model runtime.
///
/// A runtime owns its provider set, its model catalog, and — importantly — its
/// cache of loaded models. Passing one handle to several ``UniBuilder``s makes
/// those databases share a single resident copy of the weights instead of each
/// deserializing its own, which is the difference between one model footprint
/// and N of them.
///
/// Obtain one either standalone via :meth:`from_catalog_str` /
/// :meth:`from_catalog_file`, or from an existing database via
/// ``db.xervo().raw_runtime()``.
///
/// Equality and hashing are *identity*, not catalog contents: two handles are
/// equal exactly when they wrap the same underlying runtime. Two runtimes built
/// separately from the same catalog JSON compare unequal, because they hold
/// separate model caches.
///
/// Example::
///
///     rt = uni_db.ModelRuntime.from_catalog_str(catalog_json)
///     a = uni_db.Uni.builder().open("/data/a").xervo_runtime(rt).build()
///     b = uni_db.Uni.builder().open("/data/b").xervo_runtime(rt).build()
///     assert a.xervo().raw_runtime() == b.xervo().raw_runtime()
#[pyclass(name = "ModelRuntime", frozen, eq, hash, from_py_object)]
#[derive(Clone)]
pub struct PyModelRuntime {
    pub(crate) inner: std::sync::Arc<::uni_db::ModelRuntime>,
}

// `uni_xervo::runtime::ModelRuntime` carries no derives, so `Debug` cannot be
// derived through the `Arc`. The builders that hold this handle do derive
// `Debug`, so it is supplied here once rather than hand-writing `Debug` for two
// fourteen-field builder structs.
impl std::fmt::Debug for PyModelRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelRuntime")
            .field("ptr", &std::sync::Arc::as_ptr(&self.inner))
            .finish()
    }
}

// Identity comparison: this is what lets a caller assert that two databases
// really do share one runtime, without loading a model to observe it.
impl PartialEq for PyModelRuntime {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.inner, &other.inner)
    }
}
impl Eq for PyModelRuntime {}
impl std::hash::Hash for PyModelRuntime {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(std::sync::Arc::as_ptr(&self.inner), state);
    }
}

impl PyModelRuntime {
    /// Parses a catalog JSON string into specs, mapping errors for Python.
    fn parse_str(json: &str) -> PyResult<Vec<::uni_db::ModelAliasSpec>> {
        ::uni_db::xervo_catalog_from_str(json)
            .map_err(|e| ::uni_db::UniError::Internal(anyhow::anyhow!(e.to_string())))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Parses a catalog JSON file into specs, mapping errors for Python.
    fn parse_file(path: &str) -> PyResult<Vec<::uni_db::ModelAliasSpec>> {
        ::uni_db::xervo_catalog_from_file(path)
            .map_err(|e| ::uni_db::UniError::Internal(anyhow::anyhow!(e.to_string())))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }
}

#[pymethods]
impl PyModelRuntime {
    /// Build a runtime from a catalog JSON string.
    ///
    /// Blocks until the runtime is ready. With ``"warmup": "eager"`` on any
    /// alias that includes loading the model, so prefer
    /// :meth:`from_catalog_str_async` from an event loop.
    #[staticmethod]
    fn from_catalog_str(py: Python<'_>, json: &str) -> PyResult<Self> {
        let catalog = Self::parse_str(json)?;
        let inner = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(::uni_db::xervo::build_model_runtime(catalog))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Self { inner })
    }

    /// Build a runtime from a catalog JSON file path.
    ///
    /// Blocks; see :meth:`from_catalog_file_async`.
    #[staticmethod]
    fn from_catalog_file(py: Python<'_>, path: &str) -> PyResult<Self> {
        let catalog = Self::parse_file(path)?;
        let inner = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(::uni_db::xervo::build_model_runtime(catalog))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Self { inner })
    }

    /// Build a runtime from a catalog JSON string, awaitable.
    #[staticmethod]
    fn from_catalog_str_async<'py>(py: Python<'py>, json: &str) -> PyResult<Bound<'py, PyAny>> {
        let catalog = Self::parse_str(json)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = ::uni_db::xervo::build_model_runtime(catalog)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(Self { inner })
        })
    }

    /// Build a runtime from a catalog JSON file path, awaitable.
    #[staticmethod]
    fn from_catalog_file_async<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
        let catalog = Self::parse_file(path)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let inner = ::uni_db::xervo::build_model_runtime(catalog)
                .await
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            Ok(Self { inner })
        })
    }

    /// Whether the given alias is present in this runtime's catalog.
    fn contains_alias(&self, py: Python<'_>, alias: &str) -> bool {
        let inner = self.inner.clone();
        let alias = alias.to_string();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(async move { inner.contains_alias(&alias).await })
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "ModelRuntime(0x{:x})",
            std::sync::Arc::as_ptr(&self.inner) as usize
        )
    }
}

// ============================================================================
// ID Types (Vid, Eid, UniId)
// ============================================================================

/// Vertex identifier (64-bit sequential ID).
#[pyclass(name = "Vid", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyVid {
    pub(crate) inner: uni_common::Vid,
}

#[pymethods]
impl PyVid {
    #[new]
    fn new(id: u64) -> Self {
        Self {
            inner: uni_common::Vid::new(id),
        }
    }

    /// Return the raw integer value.
    fn as_int(&self) -> u64 {
        self.inner.as_u64()
    }

    fn __int__(&self) -> u64 {
        self.inner.as_u64()
    }

    fn __repr__(&self) -> String {
        format!("Vid({})", self.inner.as_u64())
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(v) = other.extract::<PyVid>() {
            Ok(self.inner == v.inner)
        } else if let Ok(i) = other.extract::<u64>() {
            Ok(self.inner.as_u64() == i)
        } else {
            Ok(false)
        }
    }

    fn __hash__(&self) -> u64 {
        self.inner.as_u64()
    }

    fn __index__(&self) -> u64 {
        self.inner.as_u64()
    }
}

/// Edge identifier (64-bit sequential ID).
#[pyclass(name = "Eid", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyEid {
    pub(crate) inner: uni_common::Eid,
}

#[pymethods]
impl PyEid {
    #[new]
    fn new(id: u64) -> Self {
        Self {
            inner: uni_common::Eid::new(id),
        }
    }

    /// Return the raw integer value.
    fn as_int(&self) -> u64 {
        self.inner.as_u64()
    }

    fn __int__(&self) -> u64 {
        self.inner.as_u64()
    }

    fn __repr__(&self) -> String {
        format!("Eid({})", self.inner.as_u64())
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(e) = other.extract::<PyEid>() {
            Ok(self.inner == e.inner)
        } else if let Ok(i) = other.extract::<u64>() {
            Ok(self.inner.as_u64() == i)
        } else {
            Ok(false)
        }
    }

    fn __hash__(&self) -> u64 {
        self.inner.as_u64()
    }

    fn __index__(&self) -> u64 {
        self.inner.as_u64()
    }
}

/// Universal content-addressed identifier (SHA3-256, multibase-encoded).
#[pyclass(name = "UniId", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyUniId {
    pub(crate) inner: uni_common::UniId,
}

#[pymethods]
impl PyUniId {
    /// Create from a multibase-encoded string.
    #[new]
    fn new(multibase: &str) -> PyResult<Self> {
        let id = uni_common::UniId::from_multibase(multibase)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: id })
    }

    /// Return the multibase-encoded string representation.
    fn to_multibase(&self) -> String {
        self.inner.to_multibase()
    }

    /// Return the raw 32 bytes.
    fn as_bytes(&self) -> Vec<u8> {
        self.inner.as_bytes().to_vec()
    }

    fn __repr__(&self) -> String {
        format!("UniId('{}')", self.inner.to_multibase())
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.inner.hash(&mut h);
        h.finish()
    }

    fn __str__(&self) -> String {
        self.inner.to_multibase()
    }
}

// ============================================================================
// CrdtType enum
// ============================================================================

/// CRDT type for conflict-free replicated data.
#[pyclass(name = "CrdtType", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCrdtType {
    pub(crate) inner: uni_common::CrdtType,
}

#[pymethods]
impl PyCrdtType {
    #[staticmethod]
    #[pyo3(name = "G_COUNTER")]
    fn g_counter() -> Self {
        Self {
            inner: uni_common::CrdtType::GCounter,
        }
    }
    #[staticmethod]
    #[pyo3(name = "G_SET")]
    fn g_set() -> Self {
        Self {
            inner: uni_common::CrdtType::GSet,
        }
    }
    #[staticmethod]
    #[pyo3(name = "OR_SET")]
    fn or_set() -> Self {
        Self {
            inner: uni_common::CrdtType::ORSet,
        }
    }
    #[staticmethod]
    #[pyo3(name = "LWW_REGISTER")]
    fn lww_register() -> Self {
        Self {
            inner: uni_common::CrdtType::LWWRegister,
        }
    }
    #[staticmethod]
    #[pyo3(name = "LWW_MAP")]
    fn lww_map() -> Self {
        Self {
            inner: uni_common::CrdtType::LWWMap,
        }
    }
    #[staticmethod]
    #[pyo3(name = "RGA")]
    fn rga() -> Self {
        Self {
            inner: uni_common::CrdtType::Rga,
        }
    }
    #[staticmethod]
    #[pyo3(name = "VECTOR_CLOCK")]
    fn vector_clock() -> Self {
        Self {
            inner: uni_common::CrdtType::VectorClock,
        }
    }
    #[staticmethod]
    #[pyo3(name = "VC_REGISTER")]
    fn vc_register() -> Self {
        Self {
            inner: uni_common::CrdtType::VCRegister,
        }
    }

    fn __repr__(&self) -> String {
        let name = match &self.inner {
            uni_common::CrdtType::GCounter => "G_COUNTER",
            uni_common::CrdtType::GSet => "G_SET",
            uni_common::CrdtType::ORSet => "OR_SET",
            uni_common::CrdtType::LWWRegister => "LWW_REGISTER",
            uni_common::CrdtType::LWWMap => "LWW_MAP",
            uni_common::CrdtType::Rga => "RGA",
            uni_common::CrdtType::VectorClock => "VECTOR_CLOCK",
            uni_common::CrdtType::VCRegister => "VC_REGISTER",
            _ => "UNKNOWN",
        };
        format!("CrdtType.{}", name)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::mem::discriminant(&self.inner).hash(&mut h);
        h.finish()
    }
}

// ============================================================================
// DataType enum
// ============================================================================

/// Data type for schema property definitions.
#[pyclass(name = "DataType", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyDataType {
    pub(crate) inner: uni_common::DataType,
}

#[pymethods]
impl PyDataType {
    // Simple variants
    #[staticmethod]
    #[pyo3(name = "STRING")]
    fn string() -> Self {
        Self {
            inner: uni_common::DataType::String,
        }
    }
    #[staticmethod]
    #[pyo3(name = "INT32")]
    fn int32() -> Self {
        Self {
            inner: uni_common::DataType::Int32,
        }
    }
    #[staticmethod]
    #[pyo3(name = "INT64")]
    fn int64() -> Self {
        Self {
            inner: uni_common::DataType::Int64,
        }
    }
    #[staticmethod]
    #[pyo3(name = "FLOAT32")]
    fn float32() -> Self {
        Self {
            inner: uni_common::DataType::Float32,
        }
    }
    #[staticmethod]
    #[pyo3(name = "FLOAT64")]
    fn float64() -> Self {
        Self {
            inner: uni_common::DataType::Float64,
        }
    }
    #[staticmethod]
    #[pyo3(name = "BOOL")]
    fn bool_() -> Self {
        Self {
            inner: uni_common::DataType::Bool,
        }
    }
    #[staticmethod]
    #[pyo3(name = "TIMESTAMP")]
    fn timestamp() -> Self {
        Self {
            inner: uni_common::DataType::Timestamp,
        }
    }
    #[staticmethod]
    #[pyo3(name = "DATE")]
    fn date() -> Self {
        Self {
            inner: uni_common::DataType::Date,
        }
    }
    #[staticmethod]
    #[pyo3(name = "TIME")]
    fn time() -> Self {
        Self {
            inner: uni_common::DataType::Time,
        }
    }
    #[staticmethod]
    #[pyo3(name = "DATETIME")]
    fn datetime() -> Self {
        Self {
            inner: uni_common::DataType::DateTime,
        }
    }
    #[staticmethod]
    #[pyo3(name = "DURATION")]
    fn duration() -> Self {
        Self {
            inner: uni_common::DataType::Duration,
        }
    }
    #[staticmethod]
    #[pyo3(name = "JSON")]
    fn json() -> Self {
        Self {
            inner: uni_common::DataType::CypherValue,
        }
    }
    #[staticmethod]
    #[pyo3(name = "BTIC")]
    fn btic() -> Self {
        Self {
            inner: uni_common::DataType::Btic,
        }
    }
    #[staticmethod]
    #[pyo3(name = "BYTES")]
    fn bytes() -> Self {
        Self {
            inner: uni_common::DataType::Bytes,
        }
    }

    // Parameterized variants
    #[staticmethod]
    fn vector(dimensions: usize) -> Self {
        Self {
            inner: uni_common::DataType::Vector { dimensions },
        }
    }

    #[staticmethod]
    fn sparse_vector(dimensions: usize) -> Self {
        Self {
            inner: uni_common::DataType::SparseVector { dimensions },
        }
    }

    #[staticmethod]
    fn list(element_type: &PyDataType) -> Self {
        Self {
            inner: uni_common::DataType::List(Box::new(element_type.inner.clone())),
        }
    }

    #[staticmethod]
    fn map(key_type: &PyDataType, value_type: &PyDataType) -> Self {
        Self {
            inner: uni_common::DataType::Map(
                Box::new(key_type.inner.clone()),
                Box::new(value_type.inner.clone()),
            ),
        }
    }

    #[staticmethod]
    fn crdt(crdt_type: &PyCrdtType) -> Self {
        Self {
            inner: uni_common::DataType::Crdt(crdt_type.inner.clone()),
        }
    }

    fn __repr__(&self) -> String {
        let name = match &self.inner {
            uni_common::DataType::String => "STRING".to_string(),
            uni_common::DataType::Int32 => "INT32".to_string(),
            uni_common::DataType::Int64 => "INT64".to_string(),
            uni_common::DataType::Float32 => "FLOAT32".to_string(),
            uni_common::DataType::Float64 => "FLOAT64".to_string(),
            uni_common::DataType::Bool => "BOOL".to_string(),
            uni_common::DataType::Timestamp => "TIMESTAMP".to_string(),
            uni_common::DataType::Date => "DATE".to_string(),
            uni_common::DataType::Time => "TIME".to_string(),
            uni_common::DataType::DateTime => "DATETIME".to_string(),
            uni_common::DataType::Duration => "DURATION".to_string(),
            uni_common::DataType::CypherValue => "JSON".to_string(),
            uni_common::DataType::Vector { dimensions } => format!("vector({})", dimensions),
            uni_common::DataType::SparseVector { dimensions } => {
                format!("sparse_vector({})", dimensions)
            }
            uni_common::DataType::List(inner) => {
                let py_inner = PyDataType {
                    inner: *inner.clone(),
                };
                format!("list({})", py_inner.__repr__())
            }
            uni_common::DataType::Map(k, v) => {
                let py_k = PyDataType { inner: *k.clone() };
                let py_v = PyDataType { inner: *v.clone() };
                format!("map({}, {})", py_k.__repr__(), py_v.__repr__())
            }
            uni_common::DataType::Crdt(ct) => {
                let py_ct = PyCrdtType { inner: ct.clone() };
                format!("crdt({})", py_ct.__repr__())
            }
            uni_common::DataType::Btic => "BTIC".to_string(),
            uni_common::DataType::Bytes => "BYTES".to_string(),
            uni_common::DataType::Point(_) => "POINT".to_string(),
            _ => "UNKNOWN".to_string(),
        };
        format!("DataType.{}", name)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        format!("{:?}", self.inner).hash(&mut h);
        h.finish()
    }
}

// ============================================================================
// Value (opt-in tagged union)
// ============================================================================

/// A tagged Uni value — opt-in wrapper for when you need to distinguish types.
///
/// Query results still return native Python types by default. Use `Value` for
/// explicit type tagging in parameters or custom logic.
#[pyclass(name = "Value", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyValue {
    pub(crate) inner: ::uni_db::Value,
}

#[pymethods]
impl PyValue {
    // Constructors
    #[staticmethod]
    fn null() -> Self {
        Self {
            inner: ::uni_db::Value::Null,
        }
    }
    #[staticmethod]
    fn bool(v: bool) -> Self {
        Self {
            inner: ::uni_db::Value::Bool(v),
        }
    }
    #[staticmethod]
    fn int(v: i64) -> Self {
        Self {
            inner: ::uni_db::Value::Int(v),
        }
    }
    #[staticmethod]
    fn float(v: f64) -> Self {
        Self {
            inner: ::uni_db::Value::Float(v),
        }
    }
    #[staticmethod]
    fn string(v: String) -> Self {
        Self {
            inner: ::uni_db::Value::String(v),
        }
    }
    #[staticmethod]
    fn bytes(v: Vec<u8>) -> Self {
        Self {
            inner: ::uni_db::Value::Bytes(v),
        }
    }
    #[staticmethod]
    fn vector(v: Vec<f32>) -> Self {
        Self {
            inner: ::uni_db::Value::Vector(v),
        }
    }
    #[staticmethod]
    fn binary_vector(v: Vec<u8>) -> Self {
        Self {
            inner: ::uni_db::Value::BinaryVector(v),
        }
    }

    /// Create a BTIC temporal interval from a string literal.
    #[staticmethod]
    fn btic(literal: &str) -> PyResult<Self> {
        let b = uni_common::uni_btic::parse::parse_btic_literal(literal).map_err(|e| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid BTIC literal: {e}"))
        })?;
        Ok(Self {
            inner: ::uni_db::Value::Temporal(uni_common::value::TemporalValue::Btic {
                lo: b.lo(),
                hi: b.hi(),
                meta: b.meta(),
            }),
        })
    }

    /// The type discriminator name.
    #[getter]
    fn type_name(&self) -> &str {
        match &self.inner {
            ::uni_db::Value::Null => "null",
            ::uni_db::Value::Bool(_) => "bool",
            ::uni_db::Value::Int(_) => "int",
            ::uni_db::Value::Float(_) => "float",
            ::uni_db::Value::String(_) => "string",
            ::uni_db::Value::Bytes(_) => "bytes",
            ::uni_db::Value::List(_) => "list",
            ::uni_db::Value::Map(_) => "map",
            ::uni_db::Value::Node(_) => "node",
            ::uni_db::Value::Edge(_) => "edge",
            ::uni_db::Value::Path(_) => "path",
            ::uni_db::Value::Vector(_) => "vector",
            ::uni_db::Value::BinaryVector(_) => "binary_vector",
            ::uni_db::Value::Temporal(uni_common::value::TemporalValue::Btic { .. }) => "btic",
            ::uni_db::Value::Temporal(_) => "temporal",
            _ => "unknown",
        }
    }

    fn is_null(&self) -> bool {
        matches!(&self.inner, ::uni_db::Value::Null)
    }
    fn is_bool(&self) -> bool {
        matches!(&self.inner, ::uni_db::Value::Bool(_))
    }
    fn is_int(&self) -> bool {
        matches!(&self.inner, ::uni_db::Value::Int(_))
    }
    fn is_float(&self) -> bool {
        matches!(&self.inner, ::uni_db::Value::Float(_))
    }
    fn is_string(&self) -> bool {
        matches!(&self.inner, ::uni_db::Value::String(_))
    }
    fn is_btic(&self) -> bool {
        matches!(
            &self.inner,
            ::uni_db::Value::Temporal(uni_common::value::TemporalValue::Btic { .. })
        )
    }

    /// Convert to the corresponding native Python type.
    fn to_python(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::convert::value_to_py(py, &self.inner)
    }

    fn __repr__(&self) -> String {
        format!("Value.{}({})", self.type_name(), self.inner)
    }

    fn __bool__(&self) -> bool {
        !matches!(&self.inner, ::uni_db::Value::Null)
    }
}

// ============================================================================
// Row type
// ============================================================================

/// A query result row with named columns.
///
/// Supports dict-like access (``row["name"]``, ``row[0]``) and conversion
/// to a plain dict via ``row.to_dict()``.
#[pyclass(name = "Row", frozen)]
pub struct PyRow {
    pub(crate) columns: Vec<String>,
    pub(crate) values: Vec<Py<PyAny>>,
}

#[pymethods]
impl PyRow {
    /// Column names in this row.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Get value by column name, returning *default* if absent.
    #[pyo3(signature = (column, default=None))]
    fn get(&self, py: Python<'_>, column: &str, default: Option<Py<PyAny>>) -> Py<PyAny> {
        for (i, col) in self.columns.iter().enumerate() {
            if col == column {
                return self.values[i].clone_ref(py);
            }
        }
        default.unwrap_or_else(|| py.None())
    }

    /// Convert to a plain Python dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        for (col, val) in self.columns.iter().zip(self.values.iter()) {
            dict.set_item(col, val.bind(py))?;
        }
        Ok(dict.unbind())
    }

    fn __getitem__(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        if let Ok(idx) = key.extract::<isize>() {
            let len = self.values.len() as isize;
            let actual = if idx < 0 { len + idx } else { idx };
            if actual < 0 || actual >= len {
                return Err(pyo3::exceptions::PyIndexError::new_err(
                    "Row index out of range",
                ));
            }
            Ok(self.values[actual as usize].clone_ref(py))
        } else if let Ok(col) = key.extract::<String>() {
            for (i, c) in self.columns.iter().enumerate() {
                if *c == col {
                    return Ok(self.values[i].clone_ref(py));
                }
            }
            Err(pyo3::exceptions::PyKeyError::new_err(format!(
                "Column '{}' not found",
                col
            )))
        } else {
            Err(pyo3::exceptions::PyTypeError::new_err(
                "Row indices must be integers or column name strings",
            ))
        }
    }

    fn __contains__(&self, key: &str) -> bool {
        self.columns.contains(&key.to_string())
    }

    fn __len__(&self) -> usize {
        self.columns.len()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::new(py, &self.columns)?;
        Ok(list.call_method0("__iter__")?.unbind())
    }

    fn __repr__(&self) -> String {
        let pairs: Vec<String> = self
            .columns
            .iter()
            .zip(self.values.iter())
            .map(|(k, _v)| format!("{}=...", k))
            .collect();
        format!("Row({})", pairs.join(", "))
    }
}

// ============================================================================
// LocyConfig
// ============================================================================

/// Configuration for Locy program evaluation.
///
/// All parameters are optional — unset values use engine defaults.
#[pyclass(name = "LocyConfig", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyLocyConfig {
    pub(crate) inner: ::uni_locy::LocyConfig,
}

#[pymethods]
impl PyLocyConfig {
    #[new]
    #[pyo3(signature = (
        max_iterations=None, timeout_secs=None, allow_partial=None, max_explain_depth=None,
        max_slg_depth=None, max_abduce_candidates=None, max_abduce_results=None,
        max_derived_bytes=None, deterministic_best_by=None,
        strict_probability_domain=None, probability_epsilon=None,
        exact_probability=None, max_bdd_variables=None,
        top_k_proofs=None, top_k_proofs_training=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_iterations: Option<usize>,
        timeout_secs: Option<f64>,
        allow_partial: Option<bool>,
        max_explain_depth: Option<usize>,
        max_slg_depth: Option<usize>,
        max_abduce_candidates: Option<usize>,
        max_abduce_results: Option<usize>,
        max_derived_bytes: Option<usize>,
        deterministic_best_by: Option<bool>,
        strict_probability_domain: Option<bool>,
        probability_epsilon: Option<f64>,
        exact_probability: Option<bool>,
        max_bdd_variables: Option<usize>,
        top_k_proofs: Option<usize>,
        top_k_proofs_training: Option<usize>,
    ) -> Self {
        let mut cfg = ::uni_locy::LocyConfig::default();
        if let Some(v) = max_iterations {
            cfg.max_iterations = v;
        }
        if let Some(v) = timeout_secs {
            cfg.timeout = std::time::Duration::from_secs_f64(v);
        }
        if let Some(v) = allow_partial {
            cfg.allow_partial = v;
        }
        if let Some(v) = max_explain_depth {
            cfg.max_explain_depth = v;
        }
        if let Some(v) = max_slg_depth {
            cfg.max_slg_depth = v;
        }
        if let Some(v) = max_abduce_candidates {
            cfg.max_abduce_candidates = v;
        }
        if let Some(v) = max_abduce_results {
            cfg.max_abduce_results = v;
        }
        if let Some(v) = max_derived_bytes {
            cfg.max_derived_bytes = v;
        }
        if let Some(v) = deterministic_best_by {
            cfg.deterministic_best_by = v;
        }
        if let Some(v) = strict_probability_domain {
            cfg.strict_probability_domain = v;
        }
        if let Some(v) = probability_epsilon {
            cfg.probability_epsilon = v;
        }
        if let Some(v) = exact_probability {
            cfg.exact_probability = v;
        }
        if let Some(v) = max_bdd_variables {
            cfg.max_bdd_variables = v;
        }
        if let Some(v) = top_k_proofs {
            cfg.top_k_proofs = v;
        }
        cfg.top_k_proofs_training = top_k_proofs_training;
        Self { inner: cfg }
    }

    #[getter]
    fn max_iterations(&self) -> usize {
        self.inner.max_iterations
    }
    #[getter]
    fn timeout_secs(&self) -> f64 {
        self.inner.timeout.as_secs_f64()
    }
    #[getter]
    fn max_explain_depth(&self) -> usize {
        self.inner.max_explain_depth
    }
    #[getter]
    fn max_slg_depth(&self) -> usize {
        self.inner.max_slg_depth
    }
    #[getter]
    fn max_abduce_candidates(&self) -> usize {
        self.inner.max_abduce_candidates
    }
    #[getter]
    fn max_abduce_results(&self) -> usize {
        self.inner.max_abduce_results
    }
    #[getter]
    fn max_derived_bytes(&self) -> usize {
        self.inner.max_derived_bytes
    }
    #[getter]
    fn deterministic_best_by(&self) -> bool {
        self.inner.deterministic_best_by
    }
    #[getter]
    fn strict_probability_domain(&self) -> bool {
        self.inner.strict_probability_domain
    }
    #[getter]
    fn probability_epsilon(&self) -> f64 {
        self.inner.probability_epsilon
    }
    #[getter]
    fn exact_probability(&self) -> bool {
        self.inner.exact_probability
    }
    #[getter]
    fn max_bdd_variables(&self) -> usize {
        self.inner.max_bdd_variables
    }
    #[getter]
    fn top_k_proofs(&self) -> usize {
        self.inner.top_k_proofs
    }
    #[getter]
    fn top_k_proofs_training(&self) -> Option<usize> {
        self.inner.top_k_proofs_training
    }

    /// Register a Python callable as the neural classifier for `alias`.
    ///
    /// The callable receives `list[dict[str, Any]]` (one dict per row,
    /// keyed by the `FEATURES (...)` identifiers) and must return
    /// `list[float]` of the same length, with values in `[0, 1]`. Any
    /// Python exception, length mismatch, NaN, or out-of-range value
    /// surfaces as a Locy runtime error at the first invocation of the
    /// associated `CREATE MODEL`.
    ///
    /// Calling this method multiple times with the same alias replaces
    /// the previous registration. The registry is keyed by alias, not
    /// by callable identity, so re-registering is the supported way to
    /// swap classifiers between runs.
    #[pyo3(text_signature = "($self, alias, callable)")]
    fn register_classifier(
        &mut self,
        py: Python<'_>,
        alias: &str,
        callable: Py<PyAny>,
    ) -> PyResult<()> {
        let bound = callable.bind(py);
        if !bound.is_callable() {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "register_classifier('{alias}'): expected a callable, got {ty}",
                ty = bound
                    .get_type()
                    .name()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| "<unknown>".to_string()),
            )));
        }
        self.inner.classifier_registry.insert(
            alias.to_string(),
            std::sync::Arc::new(crate::classifier::PyClassifier::new(alias, callable)),
        );
        Ok(())
    }

    /// Names of currently-registered classifiers, sorted alphabetically.
    fn classifier_aliases(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner.classifier_registry.keys().cloned().collect();
        names.sort();
        names
    }

    fn __repr__(&self) -> String {
        format!(
            "LocyConfig(max_iterations={}, timeout={:.1}s, strict_prob={}, classifiers={})",
            self.inner.max_iterations,
            self.inner.timeout.as_secs_f64(),
            self.inner.strict_probability_domain,
            self.inner.classifier_registry.len(),
        )
    }
}

// ============================================================================
// Schema type
// ============================================================================

/// A read-only snapshot of the database schema.
#[pyclass(name = "Schema", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PySchema {
    pub(crate) inner: std::sync::Arc<uni_common::core::schema::Schema>,
}

#[pymethods]
impl PySchema {
    /// Schema version number.
    #[getter]
    fn version(&self) -> u32 {
        self.inner.schema_version
    }

    /// List of all label names.
    #[getter]
    fn label_names(&self) -> Vec<String> {
        self.inner.labels.keys().cloned().collect()
    }

    /// List of all edge type names.
    #[getter]
    fn edge_type_names(&self) -> Vec<String> {
        self.inner.edge_types.keys().cloned().collect()
    }

    /// Number of labels in the schema.
    #[getter]
    fn label_count(&self) -> usize {
        self.inner.labels.len()
    }

    /// Number of edge types in the schema.
    #[getter]
    fn edge_type_count(&self) -> usize {
        self.inner.edge_types.len()
    }

    /// Get information about a specific label, or None if not found.
    fn label_info(&self, name: &str) -> Option<LabelInfo> {
        let label_info_result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            // We need a Uni instance to call get_label_info, but Schema is standalone.
            // Instead, build from the schema directly.
            Ok::<_, ()>(())
        });
        let _ = label_info_result;
        // Build LabelInfo directly from the schema metadata.
        let meta = self.inner.labels.get(name)?;
        let props = self.inner.properties.get(name).cloned().unwrap_or_default();
        let property_infos: Vec<PropertyInfo> = props
            .iter()
            .map(|(pname, pmeta)| PropertyInfo {
                name: pname.clone(),
                data_type: format!("{:?}", pmeta.r#type),
                nullable: pmeta.nullable,
                is_indexed: false,
                description: pmeta.description.clone(),
            })
            .collect();
        Some(LabelInfo {
            name: name.to_string(),
            description: meta.description.clone(),
            count: 0, // Not available from schema alone
            properties: property_infos,
            indexes: vec![],
            constraints: vec![],
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Schema(version={}, labels={}, edge_types={})",
            self.inner.schema_version,
            self.inner.labels.len(),
            self.inner.edge_types.len(),
        )
    }
}

// ============================================================================
// CommandResult classes
// ============================================================================

/// A Locy QUERY command result.
#[pyclass(name = "QueryCommandResult", frozen)]
pub struct PyQueryCommandResult {
    pub(crate) rows: Py<PyAny>,
}

#[pymethods]
impl PyQueryCommandResult {
    #[getter]
    fn command_type(&self) -> &str {
        "query"
    }
    #[getter]
    fn rows(&self, py: Python<'_>) -> Py<PyAny> {
        self.rows.clone_ref(py)
    }
    fn __repr__(&self) -> String {
        "QueryCommandResult(...)".to_string()
    }
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "type" => Ok("query".into_pyobject(py)?.into_any().unbind()),
            "rows" => Ok(self.rows.clone_ref(py)),
            _ => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
        }
    }
}

/// A Locy ASSUME command result.
#[pyclass(name = "AssumeCommandResult", frozen)]
pub struct PyAssumeCommandResult {
    pub(crate) rows: Py<PyAny>,
}

#[pymethods]
impl PyAssumeCommandResult {
    #[getter]
    fn command_type(&self) -> &str {
        "assume"
    }
    #[getter]
    fn rows(&self, py: Python<'_>) -> Py<PyAny> {
        self.rows.clone_ref(py)
    }
    fn __repr__(&self) -> String {
        "AssumeCommandResult(...)".to_string()
    }
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "type" => Ok("assume".into_pyobject(py)?.into_any().unbind()),
            "rows" => Ok(self.rows.clone_ref(py)),
            _ => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
        }
    }
}

/// A Locy EXPLAIN command result.
#[pyclass(name = "ExplainCommandResult", frozen)]
pub struct PyExplainCommandResult {
    pub(crate) tree: Py<PyAny>,
}

#[pymethods]
impl PyExplainCommandResult {
    #[getter]
    fn command_type(&self) -> &str {
        "explain"
    }
    #[getter]
    fn tree(&self, py: Python<'_>) -> Py<PyAny> {
        self.tree.clone_ref(py)
    }
    fn __repr__(&self) -> String {
        "ExplainCommandResult(...)".to_string()
    }
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "type" => Ok("explain".into_pyobject(py)?.into_any().unbind()),
            "tree" => Ok(self.tree.clone_ref(py)),
            _ => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
        }
    }
}

/// A Locy ABDUCE command result.
#[pyclass(name = "AbduceCommandResult", frozen)]
pub struct PyAbduceCommandResult {
    pub(crate) modifications: Py<PyAny>,
}

#[pymethods]
impl PyAbduceCommandResult {
    #[getter]
    fn command_type(&self) -> &str {
        "abduce"
    }
    #[getter]
    fn modifications(&self, py: Python<'_>) -> Py<PyAny> {
        self.modifications.clone_ref(py)
    }
    fn __repr__(&self) -> String {
        "AbduceCommandResult(...)".to_string()
    }
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "type" => Ok("abduce".into_pyobject(py)?.into_any().unbind()),
            "modifications" => Ok(self.modifications.clone_ref(py)),
            _ => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
        }
    }
}

/// A Locy DERIVE command result.
#[pyclass(name = "DeriveCommandResult", frozen, get_all)]
pub struct PyDeriveCommandResult {
    pub affected: usize,
}

#[pymethods]
impl PyDeriveCommandResult {
    #[getter]
    fn command_type(&self) -> &str {
        "derive"
    }
    fn __repr__(&self) -> String {
        format!("DeriveCommandResult(affected={})", self.affected)
    }
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "type" => Ok("derive".into_pyobject(py)?.into_any().unbind()),
            "affected" => Ok(self.affected.into_pyobject(py)?.into_any().unbind()),
            _ => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
        }
    }
}

/// A Locy CYPHER command result.
#[pyclass(name = "CypherCommandResult", frozen)]
pub struct PyCypherCommandResult {
    pub(crate) rows: Py<PyAny>,
}

#[pymethods]
impl PyCypherCommandResult {
    #[getter]
    fn command_type(&self) -> &str {
        "cypher"
    }
    #[getter]
    fn rows(&self, py: Python<'_>) -> Py<PyAny> {
        self.rows.clone_ref(py)
    }
    fn __repr__(&self) -> String {
        "CypherCommandResult(...)".to_string()
    }
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        match key {
            "type" => Ok("cypher".into_pyobject(py)?.into_any().unbind()),
            "rows" => Ok(self.rows.clone_ref(py)),
            _ => Err(pyo3::exceptions::PyKeyError::new_err(key.to_string())),
        }
    }
}

// ============================================================================
// Calibrator wrapper
// ============================================================================

/// A fitted calibrator returned by a CALIBRATE Locy command. Wraps the
/// Rust `Arc<dyn Calibrator>` so Python callers can apply it to raw
/// classifier outputs (e.g. to rescore a maintenance queue with the
/// calibrated probability rather than the raw classifier value).
#[pyclass(name = "Calibrator", frozen)]
pub struct PyCalibrator {
    pub inner: std::sync::Arc<dyn ::uni_locy::calibration::Calibrator>,
}

#[pymethods]
impl PyCalibrator {
    /// The calibration method name (e.g. "PlattScaling").
    #[getter]
    fn method(&self) -> String {
        format!("{:?}", self.inner.method())
    }

    /// Apply the calibrator to a single raw probability.
    fn apply(&self, raw: f64) -> f64 {
        self.inner.apply(raw)
    }

    /// Apply the calibrator to a batch of raw probabilities.
    fn apply_batch(&self, raws: Vec<f64>) -> Vec<f64> {
        self.inner.apply_batch(&raws)
    }

    fn __repr__(&self) -> String {
        format!("Calibrator(method={:?})", self.inner.method())
    }

    fn __call__(&self, raw: f64) -> f64 {
        self.inner.apply(raw)
    }
}

// ============================================================================
// Hook Context types
// ============================================================================

/// Context passed to session hooks before/after query execution.
#[pyclass(name = "HookContext", frozen, get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyHookContext {
    pub session_id: String,
    pub query_text: String,
    pub query_type: String,
}

#[pymethods]
impl PyHookContext {
    fn __repr__(&self) -> String {
        format!(
            "HookContext(session={}, type={})",
            self.session_id, self.query_type
        )
    }
}

/// Context passed to session hooks before/after transaction commit.
#[pyclass(name = "CommitHookContext", frozen, get_all, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyCommitHookContext {
    pub session_id: String,
    pub tx_id: String,
    pub mutation_count: usize,
}

#[pymethods]
impl PyCommitHookContext {
    fn __repr__(&self) -> String {
        format!(
            "CommitHookContext(tx={}, mutations={})",
            self.tx_id, self.mutation_count
        )
    }
}

/// Query type discriminator for hook contexts.
#[pyclass(name = "QueryType", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyQueryType;

#[pymethods]
impl PyQueryType {
    #[staticmethod]
    #[pyo3(name = "CYPHER")]
    fn cypher() -> String {
        "cypher".to_string()
    }
    #[staticmethod]
    #[pyo3(name = "LOCY")]
    fn locy() -> String {
        "locy".to_string()
    }
    #[staticmethod]
    #[pyo3(name = "EXECUTE")]
    fn execute() -> String {
        "execute".to_string()
    }
}

// ============================================================================
// Fork types (Phase 4b)
// ============================================================================

/// Stable identifier for a fork (ULID-backed).
#[pyclass(name = "ForkId", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub struct PyForkId {
    pub(crate) inner: uni_common::core::fork::ForkId,
}

#[pymethods]
impl PyForkId {
    /// Parse a `ForkId` from its canonical 26-character base32 ULID string.
    #[staticmethod]
    fn parse(s: &str) -> PyResult<Self> {
        uni_common::core::fork::ForkId::parse(s)
            .map(|id| Self { inner: id })
            .map_err(|e| crate::exceptions::UniInvalidArgumentError::new_err(e.to_string()))
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }
    fn __repr__(&self) -> String {
        format!("ForkId({})", self.inner)
    }
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(v) = other.extract::<PyForkId>() {
            Ok(self.inner == v.inner)
        } else if let Ok(s) = other.extract::<String>() {
            Ok(self.inner.to_string() == s)
        } else {
            Ok(false)
        }
    }
    fn __hash__(&self) -> isize {
        // ULIDs are 128-bit; fold to isize for Python's hash protocol.
        let raw = self.inner.0.0;
        (raw as i64 ^ ((raw >> 64) as i64)) as isize
    }
}

/// Lifecycle status of a fork in the registry.
#[pyclass(eq, eq_int, name = "ForkStatus", from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyForkStatus {
    Pending,
    Active,
    Tombstoned,
    /// A lifecycle state this build of the binding does not know about.
    ///
    /// `ForkStatus` is `#[non_exhaustive]` upstream, so the wildcard below
    /// cannot be removed. Reporting an unknown state as `Unknown` rather than
    /// `Active` matters: recovery resumes any non-`Active` state, so
    /// non-`Active` is precisely the "mid-lifecycle, do not use" signal, and
    /// Python code branching on `status == ForkStatus.Active` would otherwise
    /// open sessions against a fork the engine considers unsafe.
    Unknown,
}

impl PyForkStatus {
    pub(crate) fn from_rust(status: uni_common::core::fork::ForkStatus) -> Self {
        use uni_common::core::fork::ForkStatus;
        match status {
            ForkStatus::Pending => Self::Pending,
            ForkStatus::Active => Self::Active,
            ForkStatus::Tombstoned => Self::Tombstoned,
            // Fail closed, not open — see the `Unknown` variant's doc.
            _ => Self::Unknown,
        }
    }
}

/// Registry record for a single fork.
///
/// All fields are exposed as Python attributes via `#[pyo3(get)]`.
#[pyclass(name = "ForkInfo", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyForkInfo {
    #[pyo3(get)]
    pub id: PyForkId,
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub parent_fork_id: Option<PyForkId>,
    #[pyo3(get)]
    pub parent_snapshot_id: String,
    /// `datetime.datetime` (UTC) — fork creation time.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// `Optional[datetime.datetime]` — wall-clock TTL expiry. `None` ⇒ no TTL.
    pub ttl_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    #[pyo3(get)]
    pub schema_version_at_creation: u32,
    /// Map of dataset name → branch name.
    pub datasets: std::collections::BTreeMap<String, String>,
    #[pyo3(get)]
    pub status: PyForkStatus,
}

#[pymethods]
impl PyForkInfo {
    /// Fork creation time as a UTC `datetime.datetime`.
    #[getter]
    fn created_at<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        crate::convert::utc_datetime_to_py(py, self.created_at)
    }

    /// Wall-clock TTL expiry, or `None` if the fork has no TTL.
    #[getter]
    fn ttl_expires_at<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.ttl_expires_at {
            Some(t) => Ok(Some(crate::convert::utc_datetime_to_py(py, t)?)),
            None => Ok(None),
        }
    }

    /// Map of `dataset_name` → `branch_name` for every Lance dataset
    /// this fork owns.
    #[getter]
    fn datasets<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.datasets {
            dict.set_item(k, v)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "ForkInfo(id={}, name={:?}, status={:?}, datasets={})",
            self.id.inner,
            self.name,
            self.status,
            self.datasets.len(),
        )
    }
}

impl PyForkInfo {
    pub(crate) fn from_rust(info: uni_common::core::fork::ForkInfo) -> Self {
        Self {
            id: PyForkId { inner: info.id },
            name: info.name,
            parent_fork_id: info.parent_fork_id.map(|id| PyForkId { inner: id }),
            parent_snapshot_id: info.parent_snapshot_id,
            created_at: info.created_at,
            ttl_expires_at: info.ttl_expires_at,
            schema_version_at_creation: info.schema_version_at_creation,
            datasets: info.datasets,
            status: PyForkStatus::from_rust(info.status),
        }
    }
}

// ============================================================================
// Phase 7 — Diff & promote pyclasses (Phase 6/6b feature surface)
// ============================================================================

/// A single property's before/after pair within a diff.
#[pyclass(name = "PropertyChange", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyPropertyChange {
    inner: uni_db::PropertyChange,
}

#[pymethods]
impl PyPropertyChange {
    #[getter]
    fn key(&self) -> &str {
        &self.inner.key
    }

    /// Value on the *before* side, or `None` if absent.
    #[getter]
    fn before(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match &self.inner.before {
            Some(v) => Ok(Some(crate::convert::value_to_py(py, v)?)),
            None => Ok(None),
        }
    }

    /// Value on the *after* side, or `None` if absent.
    #[getter]
    fn after(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match &self.inner.after {
            Some(v) => Ok(Some(crate::convert::value_to_py(py, v)?)),
            None => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "PropertyChange(key={:?}, before={:?}, after={:?})",
            self.inner.key, self.inner.before, self.inner.after
        )
    }
}

impl PyPropertyChange {
    pub(crate) fn from_rust(p: uni_db::PropertyChange) -> Self {
        Self { inner: p }
    }
}

/// A vertex row from one side of a diff.
#[pyclass(name = "DiffVertex", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyDiffVertex {
    inner: uni_db::DiffVertex,
}

#[pymethods]
impl PyDiffVertex {
    #[getter]
    fn label(&self) -> &str {
        &self.inner.label
    }

    /// Content-addressed UID as a base32-multibase string.
    #[getter]
    fn uid(&self) -> String {
        self.inner.uid.to_string()
    }

    /// Informational VID this row carried on the side it was scanned
    /// from. `None` when the underlying node lacked a VID, which
    /// shouldn't happen in practice.
    #[getter]
    fn vid(&self) -> Option<u64> {
        self.inner.vid.map(|v| v.as_u64())
    }

    /// User properties as a Python dict.
    #[getter]
    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.inner.properties {
            dict.set_item(k, crate::convert::value_to_py(py, v)?)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "DiffVertex(label={:?}, uid={}, vid={:?})",
            self.inner.label, self.inner.uid, self.inner.vid
        )
    }
}

/// A vertex property change (paired across both sides by UID).
#[pyclass(name = "VertexPropertyChange", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyVertexPropertyChange {
    inner: uni_db::VertexPropertyChange,
}

#[pymethods]
impl PyVertexPropertyChange {
    #[getter]
    fn label(&self) -> &str {
        &self.inner.label
    }

    #[getter]
    fn uid(&self) -> String {
        self.inner.uid.to_string()
    }

    #[getter]
    fn changes(&self) -> Vec<PyPropertyChange> {
        self.inner
            .changes
            .iter()
            .cloned()
            .map(PyPropertyChange::from_rust)
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "VertexPropertyChange(label={:?}, uid={}, changes={})",
            self.inner.label,
            self.inner.uid,
            self.inner.changes.len()
        )
    }
}

/// An edge row from one side of a diff.
#[pyclass(name = "DiffEdge", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyDiffEdge {
    inner: uni_db::DiffEdge,
}

#[pymethods]
impl PyDiffEdge {
    #[getter]
    fn edge_type(&self) -> &str {
        &self.inner.edge_type
    }

    /// Content-addressed edge UID (Phase 7d) — computed over
    /// `(src_uid, dst_uid, edge_type, sorted_properties)`. Two
    /// parallel edges between the same endpoints with different
    /// property bags have different `edge_uid`s.
    #[getter]
    fn edge_uid(&self) -> String {
        self.inner.edge_uid.to_string()
    }

    #[getter]
    fn src_uid(&self) -> String {
        self.inner.src_uid.to_string()
    }

    #[getter]
    fn dst_uid(&self) -> String {
        self.inner.dst_uid.to_string()
    }

    #[getter]
    fn properties<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.inner.properties {
            dict.set_item(k, crate::convert::value_to_py(py, v)?)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "DiffEdge(type={:?}, src_uid={}, dst_uid={})",
            self.inner.edge_type, self.inner.src_uid, self.inner.dst_uid
        )
    }
}

/// An edge property change (paired across both sides by `(src_uid,
/// dst_uid, edge_type)`).
#[pyclass(name = "EdgePropertyChange", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyEdgePropertyChange {
    inner: uni_db::EdgePropertyChange,
}

#[pymethods]
impl PyEdgePropertyChange {
    #[getter]
    fn edge_type(&self) -> &str {
        &self.inner.edge_type
    }

    #[getter]
    fn src_uid(&self) -> String {
        self.inner.src_uid.to_string()
    }

    #[getter]
    fn dst_uid(&self) -> String {
        self.inner.dst_uid.to_string()
    }

    #[getter]
    fn changes(&self) -> Vec<PyPropertyChange> {
        self.inner
            .changes
            .iter()
            .cloned()
            .map(PyPropertyChange::from_rust)
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "EdgePropertyChange(type={:?}, src_uid={}, dst_uid={})",
            self.inner.edge_type, self.inner.src_uid, self.inner.dst_uid
        )
    }
}

/// Generate a `*Diff` pyclass wrapping a Rust diff struct.
///
/// `PyVertexDiff` and `PyEdgeDiff` are the same shape — `added` / `deleted` /
/// `changed` getters over element wrappers, plus `is_empty`, `total_rows` and
/// `__repr__` — differing only in the wrapped type, the element wrappers and
/// the repr name.
///
/// Doc strings are supplied per invocation rather than baked into the macro:
/// the two families document different things to Python's `help()`, and a
/// shared body would silently give the edge type the vertex type's wording.
macro_rules! diff_pyclass {
    (
        $(#[$struct_meta:meta])*
        $struct:ident, name = $py_name:literal, inner = $inner:ty,
        element = $elem:ident, change = $change:ident,
        added = $added_doc:literal,
        deleted = $deleted_doc:literal,
        changed = $changed_doc:literal,
    ) => {
        $(#[$struct_meta])*
        #[pyclass(name = $py_name, from_py_object)]
        #[derive(Debug, Clone)]
        pub struct $struct {
            inner: $inner,
        }

        #[pymethods]
        impl $struct {
            #[doc = $added_doc]
            #[getter]
            fn added(&self) -> Vec<$elem> {
                self.inner
                    .added
                    .iter()
                    .cloned()
                    .map(|v| $elem { inner: v })
                    .collect()
            }

            #[doc = $deleted_doc]
            #[getter]
            fn deleted(&self) -> Vec<$elem> {
                self.inner
                    .deleted
                    .iter()
                    .cloned()
                    .map(|v| $elem { inner: v })
                    .collect()
            }

            #[doc = $changed_doc]
            #[getter]
            fn changed(&self) -> Vec<$change> {
                self.inner
                    .changed
                    .iter()
                    .cloned()
                    .map(|c| $change { inner: c })
                    .collect()
            }

            fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            fn total_rows(&self) -> usize {
                self.inner.total_rows()
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!($py_name, "(added={}, deleted={}, changed={})"),
                    self.inner.added.len(),
                    self.inner.deleted.len(),
                    self.inner.changed.len()
                )
            }
        }
    };
}

diff_pyclass! {
    /// Vertex-side of [`PyForkDiff`].
    PyVertexDiff, name = "VertexDiff", inner = uni_db::VertexDiff,
    element = PyDiffVertex, change = PyVertexPropertyChange,
    added = "Rows present in `b` but not `a`.",
    deleted = "Rows present in `a` but not `b`.",
    changed = "Rows with matching UID and differing properties.",
}

diff_pyclass! {
    /// Edge-side of [`PyForkDiff`].
    PyEdgeDiff, name = "EdgeDiff", inner = uni_db::EdgeDiff,
    element = PyDiffEdge, change = PyEdgePropertyChange,
    added = "Edges present in `b` but not `a`.",
    deleted = "Edges present in `a` but not `b`.",
    changed = "Edges with matching endpoints and differing properties.",
}

/// Structural delta between two fork views (or a fork and primary).
///
/// Build via [`PyUni.diff_fork_primary`] or [`PyUni.diff_forks`].
#[pyclass(name = "ForkDiff", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyForkDiff {
    inner: uni_db::ForkDiff,
}

#[pymethods]
impl PyForkDiff {
    #[getter]
    fn vertices(&self) -> PyVertexDiff {
        PyVertexDiff {
            inner: self.inner.vertices.clone(),
        }
    }

    #[getter]
    fn edges(&self) -> PyEdgeDiff {
        PyEdgeDiff {
            inner: self.inner.edges.clone(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn total_rows(&self) -> usize {
        self.inner.total_rows()
    }

    /// Returns the inverse: `diff(a, b).invert() == diff(b, a)`.
    fn invert(&self) -> Self {
        Self {
            inner: self.inner.clone().invert(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ForkDiff(vertices=added={}/deleted={}/changed={}, edges=added={}/deleted={}/changed={})",
            self.inner.vertices.added.len(),
            self.inner.vertices.deleted.len(),
            self.inner.vertices.changed.len(),
            self.inner.edges.added.len(),
            self.inner.edges.deleted.len(),
            self.inner.edges.changed.len(),
        )
    }
}

impl PyForkDiff {
    pub(crate) fn from_rust(d: uni_db::ForkDiff) -> Self {
        Self { inner: d }
    }
}

/// Selector for [`PyUni.promote_from_fork`].
///
/// Construct via the class methods:
///   - `PromotePattern.label("Person", where_clause="n.age > 30")`
///   - `PromotePattern.edge_type("KNOWS", where_clause="r.since > 2020")`
/// How a baseline-aware merge resolves a concurrent divergent edit
/// (the fork and primary both changed a vertex since the fork point).
#[pyclass(eq, eq_int, name = "ConflictPolicy", from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PyConflictPolicy {
    /// Leave primary's value; count the conflict. The safe default.
    Skip,
    /// Apply the fork's value over primary's.
    Overwrite,
}

impl PyConflictPolicy {
    fn to_rust(self) -> uni_db::ConflictPolicy {
        match self {
            Self::Skip => uni_db::ConflictPolicy::Skip,
            Self::Overwrite => uni_db::ConflictPolicy::Overwrite,
        }
    }
}

/// Options for `Uni.promote_from_fork_with_options`.
///
/// `upsert=True` updates an existing primary vertex (matched by
/// `(label, ext_id)`) in place instead of inserting a twin.
/// `delete_promotion=True` additionally propagates fork deletions and
/// detects concurrent conflicts against the fork-point baseline
/// (resolved per `on_conflict`).
#[pyclass(name = "PromoteOptions", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyPromoteOptions {
    pub(crate) inner: uni_db::PromoteOptions,
}

#[pymethods]
impl PyPromoteOptions {
    #[new]
    #[pyo3(signature = (upsert=false, delete_promotion=false, on_conflict=PyConflictPolicy::Skip))]
    fn new(upsert: bool, delete_promotion: bool, on_conflict: PyConflictPolicy) -> Self {
        let mut inner = if delete_promotion {
            uni_db::PromoteOptions::with_merge()
        } else if upsert {
            uni_db::PromoteOptions::with_upsert()
        } else {
            uni_db::PromoteOptions::insert_only()
        };
        inner = inner.on_conflict(on_conflict.to_rust());
        // Honor an explicit `upsert`/`delete_promotion` combination even
        // when it doesn't match a named preset.
        inner.upsert = upsert || delete_promotion;
        inner.delete_promotion = delete_promotion;
        Self { inner }
    }

    #[getter]
    fn upsert(&self) -> bool {
        self.inner.upsert
    }

    #[getter]
    fn delete_promotion(&self) -> bool {
        self.inner.delete_promotion
    }

    fn __repr__(&self) -> String {
        format!(
            "PromoteOptions(upsert={}, delete_promotion={})",
            self.inner.upsert, self.inner.delete_promotion
        )
    }
}

#[pyclass(name = "PromotePattern", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyPromotePattern {
    pub(crate) inner: uni_db::PromotePattern,
}

#[pymethods]
impl PyPromotePattern {
    /// Match every vertex with this label. `where_clause`, if given,
    /// is interpolated verbatim into a Cypher `WHERE` on the
    /// fork-side scan.
    #[classmethod]
    #[pyo3(signature = (label, where_clause=None))]
    fn label(
        _cls: &Bound<'_, pyo3::types::PyType>,
        label: &str,
        where_clause: Option<&str>,
    ) -> Self {
        let mut p = uni_db::PromotePattern::label(label);
        if let Some(w) = where_clause {
            p = p.where_clause(w);
        }
        Self { inner: p }
    }

    /// Match every edge of this type. Endpoints must already exist on
    /// primary (or be promoted earlier in the same call by a vertex
    /// pattern); otherwise the edge is counted in
    /// `PromoteReport.edges_skipped_no_endpoint`. Bound names are
    /// `a` (source), `r` (edge), `b` (destination) inside
    /// `where_clause`.
    #[classmethod]
    #[pyo3(signature = (edge_type, where_clause=None))]
    fn edge_type(
        _cls: &Bound<'_, pyo3::types::PyType>,
        edge_type: &str,
        where_clause: Option<&str>,
    ) -> Self {
        let mut p = uni_db::PromotePattern::edge_type(edge_type);
        if let Some(w) = where_clause {
            p = p.where_clause(w);
        }
        Self { inner: p }
    }

    /// `"vertex"` or `"edge"`.
    #[getter]
    fn kind(&self) -> &'static str {
        if self.inner.is_edge() {
            "edge"
        } else {
            "vertex"
        }
    }

    fn __repr__(&self) -> String {
        format!("PromotePattern({})", self.inner)
    }
}

/// Outcome of [`PyUni.promote_from_fork`].
#[pyclass(get_all, name = "PromoteReport", from_py_object)]
#[derive(Debug, Clone)]
pub struct PyPromoteReport {
    pub vertices_inserted: usize,
    pub vertices_updated: usize,
    pub vertices_skipped_no_op: usize,
    pub vertices_inserted_unverified: usize,
    pub vertices_deleted: usize,
    pub vertices_skipped_no_ext_id_for_delete: usize,
    pub vertices_conflicting: usize,
    pub vertices_skipped_uid_conflict: usize,
    pub vertices_skipped_no_uid: usize,
    pub edges_inserted: usize,
    pub edges_skipped_duplicate: usize,
    pub edges_skipped_no_endpoint: usize,
    pub edges_skipped: usize,
    pub per_pattern_inserted: Vec<usize>,
}

#[pymethods]
impl PyPromoteReport {
    fn __repr__(&self) -> String {
        format!(
            "PromoteReport(vertices_inserted={}, vertices_skipped_uid_conflict={}, \
             edges_inserted={}, edges_skipped_duplicate={}, edges_skipped_no_endpoint={})",
            self.vertices_inserted,
            self.vertices_skipped_uid_conflict,
            self.edges_inserted,
            self.edges_skipped_duplicate,
            self.edges_skipped_no_endpoint,
        )
    }
}

impl PyPromoteReport {
    pub(crate) fn from_rust(r: uni_db::PromoteReport) -> Self {
        Self {
            vertices_inserted: r.vertices_inserted,
            vertices_updated: r.vertices_updated,
            vertices_skipped_no_op: r.vertices_skipped_no_op,
            vertices_inserted_unverified: r.vertices_inserted_unverified,
            vertices_deleted: r.vertices_deleted,
            vertices_skipped_no_ext_id_for_delete: r.vertices_skipped_no_ext_id_for_delete,
            vertices_conflicting: r.vertices_conflicting,
            vertices_skipped_uid_conflict: r.vertices_skipped_uid_conflict,
            // Reserved field on the Python side — the Rust `PromoteReport`
            // no longer carries it; it was always 0.
            vertices_skipped_no_uid: 0,
            edges_inserted: r.edges_inserted,
            edges_skipped_duplicate: r.edges_skipped_duplicate,
            edges_skipped_no_endpoint: r.edges_skipped_no_endpoint,
            edges_skipped: r.edges_skipped,
            per_pattern_inserted: r.per_pattern_inserted,
        }
    }
}
