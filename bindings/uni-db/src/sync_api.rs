// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Synchronous Python API — `Database`, `Transaction`, and `LocyEngine`.

use crate::builders::SchemaBuilder;
use crate::convert;
use crate::core;
use crate::types::*;
use ::uni_db::Uni;
use pyo3::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

// ============================================================================
// QueryCursor (synchronous)
// ============================================================================

/// Cursor-based result streaming for large query result sets.
///
/// Implements Python's iterator protocol (`__iter__`/`__next__`) and context
/// manager protocol (`__enter__`/`__exit__`).  Rows are yielded one at a time
/// from the underlying batch stream.
#[pyclass]
pub struct QueryCursor {
    pub(crate) cursor: std::sync::Mutex<Option<core::QueryCursor>>,
    pub(crate) buffer: std::sync::Mutex<VecDeque<core::Row>>,
    #[pyo3(get)]
    pub(crate) columns: Vec<String>,
}

impl QueryCursor {
    /// Pull the next single row, refilling from the batch stream as needed.
    fn next_row(&self, py: Python<'_>) -> PyResult<Option<core::Row>> {
        // Buffer fast-path in its own scope so the guard is dropped before any await.
        {
            let mut buf = self.buffer.lock().unwrap();
            if let Some(row) = buf.pop_front() {
                return Ok(Some(row));
            }
        }
        // Take the cursor out by value so NO mutex is held across the
        // GIL-releasing `block_on` — holding buffer+cursor guards across it was
        // the ABBA deadlock (correctness-scan R13). Mirrors `fetch_all` below,
        // but writes the cursor back afterwards (we stream it repeatedly).
        let mut cursor = match self.cursor.lock().unwrap().take() {
            Some(c) => c,
            None => return Ok(None), // closed / exhausted
        };
        let batch =
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(cursor.next_batch()));
        // Put the cursor back for the next call, then stash any leftover rows —
        // both under brief, non-overlapping re-locks (no lock spans the await).
        *self.cursor.lock().unwrap() = Some(cursor);
        match batch {
            Some(Ok(rows)) => {
                let mut iter = rows.into_iter();
                let first = iter.next();
                self.buffer.lock().unwrap().extend(iter);
                Ok(first)
            }
            Some(Err(e)) => Err(crate::exceptions::uni_error_to_pyerr(e)),
            None => Ok(None),
        }
    }
}

#[pymethods]
impl QueryCursor {
    /// Fetch a single row, or `None` if exhausted.
    fn fetch_one(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        match self.next_row(py)? {
            Some(row) => Ok(Some(convert::row_to_dict(py, &row)?.into())),
            None => Ok(None),
        }
    }

    /// Fetch up to `n` rows.
    #[pyo3(signature = (n))]
    fn fetch_many(&self, py: Python, n: usize) -> PyResult<Vec<Py<PyAny>>> {
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            match self.next_row(py)? {
                Some(row) => result.push(convert::row_to_dict(py, &row)?.into()),
                None => break,
            }
        }
        Ok(result)
    }

    /// Fetch all remaining rows.
    fn fetch_all(&self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        // Drain buffer first, then collect remaining from cursor.
        let mut rows: Vec<core::Row> = {
            let mut buf = self.buffer.lock().unwrap();
            buf.drain(..).collect()
        };

        let cursor_opt = {
            let mut guard = self.cursor.lock().unwrap();
            guard.take()
        };
        if let Some(cursor) = cursor_opt {
            let remaining = py
                .detach(|| {
                    pyo3_async_runtimes::tokio::get_runtime().block_on(cursor.collect_remaining())
                })
                .map_err(crate::exceptions::uni_error_to_pyerr)?;
            rows.extend(remaining);
        }

        convert::rows_to_py(py, rows)
    }

    /// Close the cursor, releasing resources.
    fn close(&self) -> PyResult<()> {
        let _ = self.cursor.lock().unwrap().take();
        self.buffer.lock().unwrap().clear();
        Ok(())
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        self.fetch_one(py)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        self.close()?;
        Ok(false) // don't suppress exceptions
    }
}

// ============================================================================
// Transaction
// ============================================================================

/// A database transaction for atomic operations.
///
/// Wraps a Rust `Transaction` which provides ACID guarantees.
/// Use as a context manager for automatic rollback on error.
#[pyclass]
pub struct Transaction {
    pub(crate) inner: Option<::uni_db::Transaction>,
}

impl Transaction {
    fn check_active(&self) -> PyResult<&::uni_db::Transaction> {
        self.inner.as_ref().ok_or_else(|| {
            crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                "Transaction already completed",
            )
        })
    }
}

#[pymethods]
impl Transaction {
    /// Execute a read query within this transaction.
    ///
    /// Returns a `QueryResult` with `.rows`, `.metrics`, `.warnings`, `.columns`.
    #[pyo3(signature = (cypher, params=None))]
    fn query(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<crate::types::PyQueryResult> {
        let tx = self.check_active()?;
        let result = if let Some(p) = params {
            let mut builder = tx.query_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.fetch_all()))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(tx.query(cypher)))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        convert::query_result_to_py_class(py, result)
    }

    /// Execute a mutation query within this transaction.
    #[pyo3(signature = (cypher, params=None))]
    fn execute(
        &self,
        py: Python,
        cypher: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<PyExecuteResult> {
        let tx = self.check_active()?;
        let result = if let Some(p) = params {
            let mut builder = tx.execute_with(cypher);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(tx.execute(cypher)))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        convert::execute_result_to_py(py, result)
    }

    /// Evaluate a Locy program within this transaction.
    #[pyo3(signature = (program, params=None))]
    fn locy(
        &self,
        py: Python,
        program: &str,
        params: Option<HashMap<String, Py<PyAny>>>,
    ) -> PyResult<PyLocyResult> {
        let tx = self.check_active()?;
        // Release the GIL across `block_on`: the Locy executor may call
        // back into Python (e.g. a registered neural classifier). Holding
        // the GIL through tokio would deadlock the callback's reacquire.
        let result = if let Some(p) = params {
            let mut builder = tx.locy_with(program);
            for (k, v) in p {
                let val = convert::py_object_to_value(py, &v)?;
                builder = builder.param(&k, val);
            }
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(builder.run()))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        } else {
            py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(tx.locy(program)))
                .map_err(crate::exceptions::uni_error_to_pyerr)?
        };
        convert::locy_result_to_py_class(py, result)
    }

    /// Apply a DerivedFactSet to this transaction.
    ///
    /// Freshness is required: a commit between DERIVE evaluation and apply
    /// raises a stale-derived-facts error. Use `apply_with(...)` +
    /// `allow_stale()` / `max_version_gap(n)` to opt out.
    fn apply(&self, py: Python, derived: &mut PyDerivedFactSet) -> PyResult<PyApplyResult> {
        let tx = self.check_active()?;
        let dfs = derived.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(tx.apply(dfs)))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(PyApplyResult {
            facts_applied: result.facts_applied,
            version_gap: result.version_gap,
        })
    }

    /// Access the transaction-scoped rule registry.
    fn rules(&self) -> PyResult<PyRuleRegistry> {
        let tx = self.check_active()?;
        Ok(PyRuleRegistry {
            registry: tx.rules().clone_registry_arc(),
            // Transaction-scoped rules are ephemeral.
            persister: None,
        })
    }

    /// Cancel in-progress operations.
    fn cancel(&self) -> PyResult<()> {
        let tx = self.check_active()?;
        tx.cancel();
        Ok(())
    }

    /// Prepare a Cypher query for repeated execution within this transaction.
    fn prepare(&self, py: Python<'_>, cypher: &str) -> PyResult<PyPreparedQuery> {
        let tx = self.check_active()?;
        let prepared = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(tx.prepare(cypher)))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(PyPreparedQuery {
            inner: std::sync::Arc::new(prepared),
        })
    }

    /// Prepare a Locy program for repeated execution within this transaction.
    fn prepare_locy(&self, py: Python<'_>, program: &str) -> PyResult<PyPreparedLocy> {
        let tx = self.check_active()?;
        let prepared = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(tx.prepare_locy(program)))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(PyPreparedLocy {
            inner: std::sync::Arc::new(prepared),
        })
    }

    /// Create a query builder for this transaction.
    fn query_with(slf: Py<Self>, cypher: &str) -> crate::builders::PyTxQueryBuilder {
        crate::builders::PyTxQueryBuilder {
            tx: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            cancellation_token: None,
        }
    }

    /// Create an execute builder for this transaction.
    fn execute_with(slf: Py<Self>, cypher: &str) -> crate::builders::PyTxExecuteBuilder {
        crate::builders::PyTxExecuteBuilder {
            tx: slf,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
        }
    }

    /// Create a Locy builder for this transaction.
    fn locy_with(slf: Py<Self>, program: &str) -> crate::builders::PyTxLocyBuilder {
        crate::builders::PyTxLocyBuilder {
            tx: slf,
            program: program.to_string(),
            params: HashMap::new(),
            timeout_secs: None,
            max_iterations: None,
            locy_config: None,
            cancellation_token: None,
        }
    }

    /// Create an apply builder for this transaction.
    ///
    /// Defaults to fresh-required; chain `allow_stale()` or
    /// `max_version_gap(n)` to opt out.
    fn apply_with(
        slf: Py<Self>,
        derived: &mut PyDerivedFactSet,
    ) -> PyResult<crate::builders::PyApplyBuilder> {
        let dfs = derived.inner.take().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("DerivedFactSet already consumed")
        })?;
        Ok(crate::builders::PyApplyBuilder {
            tx: slf,
            derived: Some(dfs),
            allow_stale: false,
            max_version_gap: None,
        })
    }

    /// Commit the transaction, returning a CommitResult.
    fn commit(&mut self, py: Python<'_>) -> PyResult<PyCommitResult> {
        let tx = self.inner.take().ok_or_else(|| {
            crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                "Transaction already completed",
            )
        })?;
        let result = py
            .detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(tx.commit()))
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(PyCommitResult::from(result))
    }

    /// Rollback the transaction.
    fn rollback(&mut self) -> PyResult<()> {
        let tx = self.inner.take().ok_or_else(|| {
            crate::exceptions::UniTransactionAlreadyCompletedError::new_err(
                "Transaction already completed",
            )
        })?;
        tx.rollback();
        Ok(())
    }

    /// Get the transaction ID.
    fn id(&self) -> PyResult<String> {
        Ok(self.check_active()?.id().to_string())
    }

    /// Database version when this transaction was started.
    fn started_at_version(&self) -> PyResult<u64> {
        Ok(self.check_active()?.started_at_version())
    }

    /// Check if the transaction has uncommitted changes.
    fn is_dirty(&self) -> PyResult<bool> {
        Ok(self.check_active()?.is_dirty())
    }

    /// Check if the transaction has been completed (committed or rolled back).
    fn is_completed(&self) -> bool {
        self.inner.is_none()
    }

    /// Get a cancellation token for this transaction.
    fn cancellation_token(&self) -> PyResult<crate::types::PyCancellationToken> {
        let tx = self.check_active()?;
        Ok(crate::types::PyCancellationToken {
            inner: tx.cancellation_token(),
        })
    }

    /// Create a bulk writer builder for high-throughput data ingestion.
    fn bulk_writer(slf: Py<Self>) -> TxBulkWriterBuilder {
        TxBulkWriterBuilder {
            tx: slf,
            defer_vector_indexes: true,
            defer_scalar_indexes: true,
            batch_size: None,
            async_indexes: false,
            validate_constraints: None,
            max_buffer_size_bytes: None,
            on_progress: None,
        }
    }

    /// Create a streaming appender for the given label.
    fn appender(&self, label: &str) -> PyResult<crate::builders::StreamingAppender> {
        let tx = self.check_active()?;
        let builder = tx.appender(label);
        let appender = builder
            .build()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::builders::StreamingAppender {
            inner: std::sync::Mutex::new(Some(appender)),
        })
    }

    /// Create a configurable appender builder for the given label.
    fn appender_builder(slf: Py<Self>, label: &str) -> TxAppenderBuilder {
        TxAppenderBuilder {
            tx: slf,
            label: label.to_string(),
            batch_size: None,
            defer_vector_indexes: None,
            max_buffer_size_bytes: None,
        }
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Context manager exit — rolls back if not committed.
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        // If the transaction is still active, roll it back.
        // The Rust Drop impl will also auto-rollback, but explicit is clearer.
        if let Some(tx) = self.inner.take() {
            tx.rollback();
        }
        Ok(false) // don't suppress exceptions
    }
}

// ============================================================================
// Transaction Bulk Writer Builder
// ============================================================================

/// Builder for configuring bulk data loading within a transaction.
#[pyclass(name = "TxBulkWriterBuilder")]
pub struct TxBulkWriterBuilder {
    tx: Py<Transaction>,
    defer_vector_indexes: bool,
    defer_scalar_indexes: bool,
    batch_size: Option<usize>,
    async_indexes: bool,
    validate_constraints: Option<bool>,
    max_buffer_size_bytes: Option<usize>,
    on_progress: Option<Py<PyAny>>,
}

#[pymethods]
impl TxBulkWriterBuilder {
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

    fn build(&self, py: Python) -> PyResult<crate::builders::BulkWriter> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.check_active()?;
        let mut builder = tx.bulk_writer();
        builder = builder
            .defer_vector_indexes(self.defer_vector_indexes)
            .defer_scalar_indexes(self.defer_scalar_indexes)
            .async_indexes(self.async_indexes);
        crate::apply_opt!(builder, self.batch_size, batch_size);
        crate::apply_opt!(builder, self.validate_constraints, validate_constraints);
        crate::apply_opt!(builder, self.max_buffer_size_bytes, max_buffer_size_bytes);
        if let Some(ref callback) = self.on_progress {
            builder = builder.on_progress(convert::make_progress_callback(callback.clone_ref(py)));
        }
        let real_writer = builder
            .build()
            .map_err(crate::exceptions::anyhow_to_pyerr)?;
        Ok(crate::builders::BulkWriter {
            inner: std::sync::Mutex::new(Some(real_writer)),
        })
    }
}

// ============================================================================
// Transaction Appender Builder
// ============================================================================

/// Builder for configuring a StreamingAppender within a transaction.
#[pyclass(name = "TxAppenderBuilder")]
pub struct TxAppenderBuilder {
    tx: Py<Transaction>,
    label: String,
    batch_size: Option<usize>,
    defer_vector_indexes: Option<bool>,
    max_buffer_size_bytes: Option<usize>,
}

#[pymethods]
impl TxAppenderBuilder {
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

    fn build(&self, py: Python) -> PyResult<crate::builders::StreamingAppender> {
        let tx_ref = self.tx.borrow(py);
        let tx = tx_ref.check_active()?;
        let mut builder = tx.appender(&self.label);
        crate::apply_opt!(builder, self.batch_size, batch_size);
        crate::apply_opt!(builder, self.defer_vector_indexes, defer_vector_indexes);
        crate::apply_opt!(builder, self.max_buffer_size_bytes, max_buffer_size_bytes);
        let appender = builder
            .build()
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(crate::builders::StreamingAppender {
            inner: std::sync::Mutex::new(Some(appender)),
        })
    }
}

/// Marshal a WASM / Extism `LoadOutcome` into the dict shape the Python
/// plugin-load APIs return (the WASM/Extism outcomes already carry
/// `Vec<String>` capability lists, so no `Debug`-formatting is needed).
#[cfg(any(feature = "wasm-plugins", feature = "extism-plugins"))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn wasm_outcome_to_pydict(
    py: Python<'_>,
    plugin_id: String,
    version: String,
    scalars_registered: Vec<String>,
    aggregates_registered: Vec<String>,
    procedures_registered: Vec<String>,
    algorithms_registered: Vec<String>,
    effective_capabilities: Vec<String>,
    denied_capabilities: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("plugin_id", plugin_id)?;
    dict.set_item("version", version)?;
    dict.set_item("scalars_registered", scalars_registered)?;
    dict.set_item("aggregates_registered", aggregates_registered)?;
    dict.set_item("procedures_registered", procedures_registered)?;
    dict.set_item("algorithms_registered", algorithms_registered)?;
    dict.set_item("effective_capabilities", effective_capabilities)?;
    dict.set_item("denied_capabilities", denied_capabilities)?;
    Ok(dict.into())
}

// ============================================================================
// Database (main entry point)
// ============================================================================

/// Main entry point for the Uni embedded graph database.
#[pyclass(name = "Uni")]
pub struct Database {
    pub(crate) inner: Arc<Uni>,
}

#[pymethods]
impl Database {
    // ========================================================================
    // Static Factory Methods
    // ========================================================================

    /// Open or create a database at the given path.
    #[staticmethod]
    fn open(py: Python<'_>, path: &str) -> PyResult<Self> {
        let uni = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(async { Uni::open(path).build().await })
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Create a temporary in-memory database.
    #[staticmethod]
    fn temporary(py: Python<'_>) -> PyResult<Self> {
        let uni = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(async { Uni::temporary().build().await })
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Create an in-memory database (alias for temporary).
    #[staticmethod]
    fn in_memory(py: Python<'_>) -> PyResult<Self> {
        Self::temporary(py)
    }

    /// Create a new database. Fails if it already exists.
    #[staticmethod]
    fn create(py: Python<'_>, path: &str) -> PyResult<Self> {
        let uni = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(async { Uni::create(path).build().await })
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Open an existing database. Fails if it does not exist.
    #[staticmethod]
    fn open_existing(py: Python<'_>, path: &str) -> PyResult<Self> {
        let uni = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(async { Uni::open_existing(path).build().await })
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        Ok(Database {
            inner: Arc::new(uni),
        })
    }

    /// Return a DatabaseBuilder for advanced configuration.
    #[staticmethod]
    fn builder() -> crate::builders::DatabaseBuilder {
        crate::builders::DatabaseBuilder::temporary()
    }

    // ========================================================================
    // Context Manager
    // ========================================================================

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
        // Shutdown on exit.
        let _ = self.shutdown(py);
        Ok(false)
    }

    fn __repr__(&self) -> String {
        "Uni(open)".to_string()
    }

    /// The filesystem path or URI backing this database.
    ///
    /// For a `temporary()` / `in_memory()` database this is the `uni_mem_*`
    /// scratch directory. Exposed so a caller who never reaches the
    /// context-manager teardown can still account for the directory — without
    /// it there is no way to discover the path short of globbing `$TMPDIR`,
    /// which is unsafe when another process owns one.
    #[getter]
    fn uri(&self) -> String {
        self.inner.uri().to_string()
    }

    /// Flush all uncommitted changes to persistent storage.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(core::flush_core(&self.inner))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)?;
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

    /// Check if a label exists.
    fn label_exists(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::label_exists_core(&self.inner, name))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Check if an edge type exists.
    fn edge_type_exists(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::edge_type_exists_core(&self.inner, name))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get all label names.
    fn list_labels(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(core::list_labels_core(&self.inner))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get all edge type names.
    fn list_edge_types(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::list_edge_types_core(&self.inner))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get detailed information about a label.
    fn get_label_info(&self, py: Python<'_>, name: &str) -> PyResult<Option<LabelInfo>> {
        let info = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(core::get_label_info_core(&self.inner, name))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

        Ok(info.map(LabelInfo::from))
    }

    /// Get detailed information about an edge type.
    fn get_edge_type_info(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<Option<crate::types::EdgeTypeInfo>> {
        let info = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(core::get_edge_type_info_core(&self.inner, name))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

        Ok(info.map(crate::types::EdgeTypeInfo::from))
    }

    /// Load schema from a JSON file.
    fn load_schema(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::load_schema_core(&self.inner, path))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Save schema to a JSON file.
    fn save_schema(&self, py: Python<'_>, path: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::save_schema_core(&self.inner, path))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    // ========================================================================
    // Plugin Methods
    // ========================================================================

    /// Load a Rhai-script plugin from source text.
    ///
    /// `script` is the Rhai source; the script must export a
    /// `uni_manifest()` function returning a map declaring its scalar /
    /// aggregate / procedure entries (see proposal §5.6).
    ///
    /// `grants` is a list of capability variant names the host is
    /// willing to give the plugin. Recognised values:
    /// `"ScalarFn"`, `"AggregateFn"`, `"Procedure"`, `"Filesystem"`,
    /// `"Network"`, `"HostQuery"`, `"Kms"`, `"Secret"`. Pattern-
    /// narrowed grants (specific paths / URLs) are not yet exposed
    /// from Python; the variant grant gives the script's host fns
    /// permission to call out, and the host's runtime enforcement
    /// (e.g., glob validation) is unchanged.
    ///
    /// Returns a dict with keys `plugin_id`, `version`,
    /// `scalars_registered`, `aggregates_registered`,
    /// `procedures_registered`, `denied_capabilities`.
    #[pyo3(signature = (script, grants=None))]
    fn load_rhai_plugin(
        &self,
        py: Python<'_>,
        script: &str,
        grants: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        let cap_set = crate::builders::build_capability_set_strict(grants)?;

        let mut loader = uni_plugin_rhai::RhaiLoader::new();
        uni_plugin_rhai::host_fn_impls::register_default_host_fns(&mut loader);
        let outcome = self
            .inner
            .load_rhai_plugin(&loader, script, &cap_set)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;

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
        Ok(dict.into())
    }

    /// Load a WASM Component Model plugin from raw bytes.
    ///
    /// Thin passthrough to [`Uni::load_wasm_component`](uni_db::Uni). `grants`
    /// uses the same variant names as [`Self::load_rhai_plugin`]
    /// (`ScalarFn` / `AggregateFn` / `Procedure` / `Filesystem` / `Network`
    /// / `HostQuery` / `Kms` / `Secret`) and drives both the surface
    /// registration gate and the host-fn grant set. Defaults to
    /// scalar / aggregate / procedure when omitted.
    ///
    /// Returns a dict with `plugin_id`, `version`, `scalars_registered`,
    /// `aggregates_registered`, `procedures_registered`, `algorithms_registered`,
    /// `effective_capabilities`, `denied_capabilities`.
    #[cfg(feature = "wasm-plugins")]
    #[pyo3(signature = (wasm_bytes, grants=None))]
    fn load_wasm_component(
        &self,
        py: Python<'_>,
        wasm_bytes: &[u8],
        grants: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        // `cap_set` is the rich capability set derived from `grants` (names →
        // attenuated `Capability`; None → default scalar/agg/proc). It drives
        // both the registration gate and the guest host-fn grant set.
        let cap_set = crate::builders::build_capability_set(grants);
        // Wire a fresh GraphCompute registry so guest `algorithm` entries that
        // drive `host-graph` kernels can register; harmless for scalar-only
        // plugins (the registry is touched only when an algorithm is present).
        let loader = uni_plugin_wasm::WasmLoader::new().with_graph(std::sync::Arc::new(
            uni_plugin_builtin::algorithms::graph_compute::GraphComputeRegistry::new(),
        ));
        let outcome = self
            .inner
            .load_wasm_component(&loader, wasm_bytes, &cap_set, &cap_set)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        wasm_outcome_to_pydict(
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
    }

    /// Load an Extism WASM plugin from raw bytes.
    ///
    /// Thin passthrough to [`Uni::load_wasm_extism`](uni_db::Uni); see
    /// [`Self::load_wasm_component`] for the `grants` and return shape.
    /// Extism host-grant-backed host functions require registered
    /// implementations on the loader; this v1 wrapper covers surface-grant
    /// plugins (scalar / aggregate / procedure).
    #[cfg(feature = "extism-plugins")]
    #[pyo3(signature = (wasm_bytes, grants=None))]
    fn load_wasm_extism(
        &self,
        py: Python<'_>,
        wasm_bytes: &[u8],
        grants: Option<Vec<String>>,
    ) -> PyResult<Py<PyAny>> {
        let cap_set = crate::builders::build_capability_set(grants);
        let mut loader = uni_plugin_extism::ExtismLoader::new();
        uni_plugin_extism::register_default_host_svc(&mut loader);
        // Wire a fresh GraphCompute registry so guest `algorithm` entries that
        // drive `uni_graph_call` can register; harmless for scalar-only plugins.
        let loader = loader.with_graph(std::sync::Arc::new(
            uni_plugin_builtin::algorithms::graph_compute::GraphComputeRegistry::new(),
        ));
        let outcome = self
            .inner
            .load_wasm_extism(&loader, wasm_bytes, &cap_set, &cap_set)
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        wasm_outcome_to_pydict(
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
    }

    // ========================================================================
    // Session Methods
    // ========================================================================

    /// Create a new session.
    ///
    /// Sessions are the primary scope for reads and the factory for transactions.
    fn session(&self) -> crate::builders::Session {
        crate::builders::Session {
            inner: self.inner.session(),
            pending_plugin_builder: uni_plugin_pyo3::ManifestBuilder::new(),
        }
    }

    /// Create a session template builder for pre-configured session factories.
    fn session_template(&self) -> crate::builders::SessionTemplateBuilder {
        crate::builders::SessionTemplateBuilder {
            inner: Some(self.inner.session_template()),
        }
    }

    /// Access the rule registry for managing pre-compiled Locy rules.
    fn rules(&self) -> PyRuleRegistry {
        PyRuleRegistry {
            registry: self.inner.rules().clone_registry_arc(),
            persister: self.inner.rules().clone_persister_arc(),
        }
    }

    /// Get a Xervo facade for embedding and generation operations.
    fn xervo(&self) -> PyResult<Xervo> {
        Ok(Xervo {
            inner: self.inner.clone(),
        })
    }

    // -----------------------------------------------------------------------
    // Snapshot management
    // -----------------------------------------------------------------------

    /// Create a point-in-time snapshot. Returns the snapshot ID.
    fn create_snapshot(&self, py: Python<'_>, name: &str) -> PyResult<String> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::create_snapshot_core(&self.inner, name))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// List all available snapshots.
    fn list_snapshots(&self, py: Python) -> PyResult<Vec<crate::types::SnapshotInfo>> {
        let manifests = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(core::list_snapshots_core(&self.inner))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        manifests
            .into_iter()
            .map(|m| convert::snapshot_manifest_to_py(py, m))
            .collect()
    }

    /// Restore the database to a specific snapshot.
    fn restore_snapshot(&self, py: Python<'_>, snapshot_id: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::restore_snapshot_core(&self.inner, snapshot_id))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    // -----------------------------------------------------------------------
    // Fork management (Phase 4b)
    // -----------------------------------------------------------------------

    /// List all currently-Active forks across the database.
    fn list_forks(&self, py: Python<'_>) -> Vec<crate::types::PyForkInfo> {
        py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.list_forks()))
            .into_iter()
            .map(crate::types::PyForkInfo::from_rust)
            .collect()
    }

    /// Look up a single fork by name. Returns `None` if the fork
    /// doesn't exist (rather than raising `UniForkNotFoundError` —
    /// matches the typical Python `dict.get`-style ergonomics).
    fn fork_info(&self, py: Python<'_>, name: &str) -> PyResult<Option<crate::types::PyForkInfo>> {
        match py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.fork_info(name))
        }) {
            Ok(info) => Ok(Some(crate::types::PyForkInfo::from_rust(info))),
            Err(uni_common::UniError::ForkNotFound { .. }) => Ok(None),
            Err(e) => Err(crate::exceptions::uni_error_to_pyerr(e)),
        }
    }

    /// Drop a fork.
    ///
    /// Errors with `UniForkInUseError`, `UniForkInflightTxError`, or
    /// `UniForkHasChildrenError` (use `drop_fork_cascade` for the last).
    fn drop_fork(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.drop_fork(name)))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Drop a fork and every descendant in its subtree.
    ///
    /// Pre-validates the whole subtree for live sessions / open
    /// transactions before tombstoning anything.
    fn drop_fork_cascade(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.drop_fork_cascade(name))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Tag a fork with a Lance tag (GC-exempt; survives drop).
    fn tag_fork(&self, py: Python<'_>, fork_name: &str, tag: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.tag_fork(fork_name, tag))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Remove a previously-applied tag from a fork. Idempotent per dataset.
    fn untag_fork(&self, py: Python<'_>, fork_name: &str, tag: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.untag_fork(fork_name, tag))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// List the unique user-visible tag names applied to this fork.
    fn list_fork_tags(&self, py: Python<'_>, fork_name: &str) -> PyResult<Vec<String>> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.list_fork_tags(fork_name))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    // -----------------------------------------------------------------------
    // Fork diff & promote (Phase 7 — Phase 6/6b feature surface)
    // -----------------------------------------------------------------------

    /// Structural diff between primary and a named fork.
    ///
    /// Returns a `ForkDiff` describing the rows the fork has that
    /// primary doesn't (`added`), the rows primary has that the fork
    /// has dropped (`deleted`), and the rows with matching UID and
    /// differing properties (`changed`).
    fn diff_fork_primary(
        &self,
        py: Python<'_>,
        fork_name: &str,
    ) -> PyResult<crate::types::PyForkDiff> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.diff_fork_primary(fork_name))
        })
        .map(crate::types::PyForkDiff::from_rust)
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Structural diff between two named forks. `diff(a, b)` is the
    /// delta that, if applied to `a`, produces `b`.
    fn diff_forks(&self, py: Python<'_>, a: &str, b: &str) -> PyResult<crate::types::PyForkDiff> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.diff_forks(a, b))
        })
        .map(crate::types::PyForkDiff::from_rust)
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Promote matched fork rows onto primary.
    ///
    /// `patterns` is a list of `PromotePattern` objects built via
    /// `PromotePattern.label(...)` or `PromotePattern.edge_type(...)`.
    /// All inserts run in a single primary transaction that commits
    /// at the end.
    fn promote_from_fork(
        &self,
        py: Python<'_>,
        fork_name: &str,
        patterns: Vec<crate::types::PyPromotePattern>,
    ) -> PyResult<crate::types::PyPromoteReport> {
        let rust_patterns: Vec<uni_db::PromotePattern> =
            patterns.into_iter().map(|p| p.inner).collect();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(self.inner.promote_from_fork(fork_name, &rust_patterns))
        })
        .map(crate::types::PyPromoteReport::from_rust)
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Promote matched fork rows onto primary with explicit merge options.
    ///
    /// `options` is a `PromoteOptions` (enable `upsert` to update existing
    /// primary vertices in place by `(label, ext_id)`, or
    /// `delete_promotion` to also propagate fork deletions and surface
    /// conflicts). The default options reproduce `promote_from_fork`.
    fn promote_from_fork_with_options(
        &self,
        py: Python<'_>,
        fork_name: &str,
        patterns: Vec<crate::types::PyPromotePattern>,
        options: crate::types::PyPromoteOptions,
    ) -> PyResult<crate::types::PyPromoteReport> {
        let rust_patterns: Vec<uni_db::PromotePattern> =
            patterns.into_iter().map(|p| p.inner).collect();
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(
                self.inner.promote_from_fork_with_options(
                    fork_name,
                    &rust_patterns,
                    &options.inner,
                ),
            )
        })
        .map(crate::types::PyPromoteReport::from_rust)
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Access compaction operations.
    fn compaction(&self) -> PyCompaction {
        PyCompaction {
            inner: self.inner.clone(),
        }
    }

    /// Access index management operations.
    fn indexes(&self) -> PyIndexes {
        PyIndexes {
            inner: self.inner.clone(),
        }
    }

    /// Flush data and prepare for shutdown.
    ///
    /// Calls `flush()` for data safety. The actual shutdown occurs when the
    /// last reference is dropped (Python GC triggers Rust `Drop`).
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        // Really shut down, rather than flushing and calling it one. Flushing
        // left the background tasks running and the teardown to `Drop` at GC
        // time, which only *signals* them — so a temporary database's scratch
        // directory was removed while writers were still finishing and survived
        // a few percent of the time. `shutdown_in_place` awaits them first.
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.inner.shutdown_in_place())
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
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
}

// ============================================================================
// Xervo (synchronous)
// ============================================================================

/// Synchronous facade for Uni-Xervo embedding and generation.
#[pyclass]
pub struct Xervo {
    inner: Arc<Uni>,
}

#[pymethods]
impl Xervo {
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

    /// Embed texts using a configured model alias. Returns a list of float vectors.
    fn embed(&self, py: Python<'_>, alias: &str, texts: Vec<String>) -> PyResult<Vec<Vec<f32>>> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(core::xervo_embed_core(
                &self.inner,
                alias,
                texts,
            ))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Generate text using structured messages. Returns a GenerationResult.
    ///
    /// Each message may be a `Message` instance or a dict with `"role"` and `"content"` keys.
    #[pyo3(signature = (alias, messages, max_tokens=None, temperature=None, top_p=None))]
    fn generate(
        &self,
        py: Python,
        alias: &str,
        messages: Vec<Py<PyAny>>,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<crate::types::PyGenerationResult> {
        let msg_pairs = convert::extract_messages(py, messages)?;
        let result = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(core::xervo_generate_core(
                    &self.inner,
                    alias,
                    msg_pairs,
                    max_tokens,
                    temperature,
                    top_p,
                ))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::generation_result_to_py(py, result)
    }

    /// Generate text from a single user prompt. Convenience wrapper around `generate()`.
    #[pyo3(signature = (alias, prompt, max_tokens=None, temperature=None, top_p=None))]
    fn generate_text(
        &self,
        py: Python,
        alias: &str,
        prompt: String,
        max_tokens: Option<usize>,
        temperature: Option<f32>,
        top_p: Option<f32>,
    ) -> PyResult<crate::types::PyGenerationResult> {
        let msg_pairs = vec![("user".to_string(), prompt)];
        let result = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime().block_on(core::xervo_generate_core(
                    &self.inner,
                    alias,
                    msg_pairs,
                    max_tokens,
                    temperature,
                    top_p,
                ))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        convert::generation_result_to_py(py, result)
    }

    /// Pre-load and cache specific model aliases so first inference is instant.
    fn prefetch(&self, py: Python<'_>, aliases: Vec<String>) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::xervo_prefetch_core(&self.inner, aliases))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Pre-load and cache every model in the Xervo catalog.
    fn prefetch_all(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::xervo_prefetch_all_core(&self.inner))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }
}

// ============================================================================
// Rule Registry (synchronous)
// ============================================================================

/// Facade for managing pre-compiled Locy rules.
///
/// Obtained via `db.rules()`, `session.rules()`, or `tx.rules()`.
#[pyclass(name = "RuleRegistry")]
pub struct PyRuleRegistry {
    pub(crate) registry: std::sync::Arc<std::sync::RwLock<uni_db::LocyRuleRegistry>>,
    /// Durable persister; `Some` only for the database-level registry, so
    /// session-, transaction-, and fork-scoped registries stay ephemeral.
    pub(crate) persister: Option<std::sync::Arc<uni_db::LocyRulePersister>>,
}

impl PyRuleRegistry {
    /// Builds the borrowed Rust facade, wiring the persister when present.
    fn facade(&self) -> uni_db::RuleRegistry<'_> {
        match &self.persister {
            Some(persister) => uni_db::RuleRegistry::with_persister(&self.registry, persister),
            None => uni_db::RuleRegistry::new(&self.registry),
        }
    }
}

#[pymethods]
impl PyRuleRegistry {
    /// Register Locy rules from a program string.
    fn register(&self, py: Python<'_>, program: &str) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(self.facade().register(program))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Remove a rule by name. Returns True if found and removed.
    fn remove(&self, py: Python<'_>, name: &str) -> PyResult<bool> {
        py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(self.facade().remove(name)))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// List names of all registered rules.
    fn list(&self) -> Vec<String> {
        self.facade().list()
    }

    /// Get metadata about a registered rule.
    fn get(&self, name: &str) -> Option<crate::types::PyRuleInfo> {
        self.facade()
            .get(name)
            .map(|info| crate::types::PyRuleInfo {
                name: info.name,
                clause_count: info.clause_count,
                is_recursive: info.is_recursive,
            })
    }

    /// Clear all registered rules.
    fn clear(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| pyo3_async_runtimes::tokio::get_runtime().block_on(self.facade().clear()))
            .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get the number of registered rules.
    fn count(&self) -> usize {
        self.facade().count()
    }
}

// ============================================================================
// Compaction (synchronous)
// ============================================================================

/// Facade for compaction operations.
///
/// Obtained via `db.compaction()`.
#[pyclass(name = "Compaction")]
pub struct PyCompaction {
    inner: Arc<Uni>,
}

#[pymethods]
impl PyCompaction {
    /// Compact data for a label or edge type.
    fn compact(&self, py: Python<'_>, name: &str) -> PyResult<crate::types::PyCompactionStats> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::compact_core(&self.inner, name))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Wait for all background compaction to complete.
    fn wait(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::wait_for_compaction_core(&self.inner))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }
}

// ============================================================================
// Indexes (synchronous)
// ============================================================================

/// Facade for index management operations.
///
/// Obtained via `db.indexes()`.
#[pyclass(name = "Indexes")]
pub struct PyIndexes {
    inner: Arc<Uni>,
}

#[pymethods]
impl PyIndexes {
    /// List index definitions, optionally filtered by label.
    #[pyo3(signature = (label=None))]
    fn list(
        &self,
        py: Python,
        label: Option<&str>,
    ) -> PyResult<Vec<crate::types::IndexDefinitionInfo>> {
        let indexes = match label {
            Some(l) => core::list_indexes_core(&self.inner, l),
            None => core::list_all_indexes_core(&self.inner),
        };
        indexes
            .into_iter()
            .map(|i| convert::index_definition_to_py(py, i))
            .collect()
    }

    /// Rebuild indexes for a label. If background=true, returns a task ID.
    #[pyo3(signature = (label, background=false))]
    fn rebuild(&self, py: Python<'_>, label: &str, background: bool) -> PyResult<Option<String>> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime().block_on(core::rebuild_indexes_core(
                &self.inner,
                label,
                background,
            ))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }

    /// Get status of all rebuild tasks.
    fn rebuild_status(&self, py: Python) -> PyResult<Vec<crate::types::IndexRebuildTaskInfo>> {
        let tasks = py
            .detach(|| {
                pyo3_async_runtimes::tokio::get_runtime()
                    .block_on(core::index_rebuild_status_core(&self.inner))
            })
            .map_err(crate::exceptions::uni_error_to_pyerr)?;
        tasks
            .into_iter()
            .map(|t| convert::index_rebuild_task_to_py(py, t))
            .collect()
    }

    /// Retry failed rebuild tasks. Returns task IDs scheduled for retry.
    fn retry_failed(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| {
            pyo3_async_runtimes::tokio::get_runtime()
                .block_on(core::retry_index_rebuilds_core(&self.inner))
        })
        .map_err(crate::exceptions::uni_error_to_pyerr)
    }
}

// ============================================================================
// Params (synchronous)
// ============================================================================

/// Facade for session-scoped parameters.
///
/// Obtained via `session.params()`.
#[pyclass(name = "Params")]
pub struct PyParams {
    pub(crate) inner:
        std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, uni_db::Value>>>,
}

#[pymethods]
impl PyParams {
    /// Set a parameter.
    fn set(&self, py: Python, key: String, value: Py<PyAny>) -> PyResult<()> {
        let val = convert::py_object_to_value(py, &value)?;
        self.inner.write().unwrap().insert(key, val);
        Ok(())
    }

    /// Get a parameter value by key.
    fn get(&self, py: Python, key: &str) -> PyResult<Option<Py<PyAny>>> {
        let store = self.inner.read().unwrap();
        match store.get(key) {
            Some(v) => {
                let py_val = convert::value_to_py(py, v)?;
                Ok(Some(py_val))
            }
            None => Ok(None),
        }
    }

    /// Remove a parameter. Returns the previous value if it existed.
    fn unset(&self, py: Python, key: &str) -> PyResult<Option<Py<PyAny>>> {
        let mut store = self.inner.write().unwrap();
        match store.remove(key) {
            Some(v) => {
                let py_val = convert::value_to_py(py, &v)?;
                Ok(Some(py_val))
            }
            None => Ok(None),
        }
    }

    /// Get a snapshot of all parameters as a dict.
    fn get_all(&self, py: Python) -> PyResult<std::collections::HashMap<String, Py<PyAny>>> {
        let store = self.inner.read().unwrap();
        let mut result = std::collections::HashMap::new();
        for (k, v) in store.iter() {
            result.insert(k.clone(), convert::value_to_py(py, v)?);
        }
        Ok(result)
    }

    /// Set multiple parameters at once.
    fn set_all(
        &self,
        py: Python,
        params: std::collections::HashMap<String, Py<PyAny>>,
    ) -> PyResult<()> {
        let mut store = self.inner.write().unwrap();
        for (k, v) in params {
            let val = convert::py_object_to_value(py, &v)?;
            store.insert(k, val);
        }
        Ok(())
    }
}
