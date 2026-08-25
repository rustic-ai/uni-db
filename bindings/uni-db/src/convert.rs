// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Conversion utilities between Python objects and Rust/Uni types.

use ::uni_db::Value;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::collections::HashMap;
use uni_common::value::TemporalValue;

use crate::types::{PyEdge, PyNode, PyPath};

/// Split a nanos-since-midnight count into `(hour, minute, second, microsecond)`.
///
/// Shared by the `LocalTime` and `Time` temporal arms, which differ only in
/// whether they attach a timezone.
fn split_time_of_day(nanos_since_midnight: i64) -> (i64, i64, i64, i64) {
    let total_micros = nanos_since_midnight / 1_000;
    let hour = total_micros / 3_600_000_000;
    let minute = (total_micros % 3_600_000_000) / 60_000_000;
    let second = (total_micros % 60_000_000) / 1_000_000;
    let microsecond = total_micros % 1_000_000;
    (hour, minute, second, microsecond)
}

/// Wall-clock nanoseconds since the Unix epoch for a Python date/datetime.
///
/// Treats the object's naive calendar and clock components as if they were UTC,
/// using exact integer arithmetic. This matches the Uni core's
/// `LocalDateTime.nanos_since_epoch` semantics (wall-clock-as-if-UTC) and avoids
/// both the local-timezone shift and the f64 precision loss of `.timestamp()`.
///
/// # Errors
///
/// Returns an error if any component attribute cannot be read from `dt`.
fn wall_clock_nanos_since_epoch(dt: &Bound<'_, PyAny>) -> PyResult<i64> {
    let ordinal: i64 = dt.call_method0("toordinal")?.extract()?;
    let epoch_ordinal: i64 = 719163; // date(1970, 1, 1).toordinal()
    let days = ordinal - epoch_ordinal;
    let hour: i64 = dt.getattr("hour")?.extract()?;
    let minute: i64 = dt.getattr("minute")?.extract()?;
    let second: i64 = dt.getattr("second")?.extract()?;
    let microsecond: i64 = dt.getattr("microsecond")?.extract()?;
    Ok(days * 86_400_000_000_000
        + hour * 3_600_000_000_000
        + minute * 60_000_000_000
        + second * 1_000_000_000
        + microsecond * 1_000)
}

/// Build a Python `datetime.timezone` with the given UTC offset in seconds.
fn fixed_timezone<'py>(
    py: Python<'py>,
    datetime_module: &Bound<'py, PyModule>,
    offset_seconds: i32,
) -> PyResult<Bound<'py, PyAny>> {
    let tz_class = datetime_module.getattr("timezone")?;
    let td_class = datetime_module.getattr("timedelta")?;
    let td = td_class.call1(pyo3::types::PyTuple::new(
        py,
        &[
            0i32.into_pyobject(py)?.into_any(),
            offset_seconds.into_pyobject(py)?.into_any(),
        ],
    )?)?;
    tz_class.call1((td,))
}

/// Convert a Rust `Node` to a Python `Node` object.
pub fn node_to_py(py: Python, n: &::uni_db::Node) -> PyResult<Py<PyNode>> {
    let mut properties = HashMap::new();
    for (k, v) in &n.properties {
        properties.insert(k.clone(), value_to_py(py, v)?);
    }
    Py::new(
        py,
        PyNode {
            id: n.vid.as_u64(),
            labels: n.labels.clone(),
            properties,
        },
    )
}

/// Convert a Rust `Edge` to a Python `Edge` object.
pub fn edge_to_py(py: Python, e: &::uni_db::Edge) -> PyResult<Py<PyEdge>> {
    let mut properties = HashMap::new();
    for (k, v) in &e.properties {
        properties.insert(k.clone(), value_to_py(py, v)?);
    }
    Py::new(
        py,
        PyEdge {
            id: e.eid.as_u64(),
            type_name: e.edge_type.clone(),
            start_id: e.src.as_u64(),
            end_id: e.dst.as_u64(),
            properties,
        },
    )
}

/// Convert a Uni Value to a Python object.
pub fn value_to_py(py: Python, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_py_any(py)?),
        Value::Int(i) => Ok(i.into_py_any(py)?),
        Value::Float(f) => Ok(f.into_py_any(py)?),
        Value::String(s) => Ok(s.into_py_any(py)?),
        Value::Bytes(b) => Ok(PyBytes::new(py, b).into()),
        Value::List(l) => {
            let list = PyList::empty(py);
            for item in l {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        Value::Map(m) => {
            let dict = PyDict::new(py);
            for (k, v) in m {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Vector(v) => Ok(v.clone().into_py_any(py)?),
        Value::Node(n) => Ok(node_to_py(py, n)?.into_any()),
        Value::Edge(e) => Ok(edge_to_py(py, e)?.into_any()),
        Value::Path(p) => {
            let nodes: Vec<Py<PyNode>> = p
                .nodes()
                .iter()
                .map(|n| node_to_py(py, n))
                .collect::<PyResult<_>>()?;
            let edges: Vec<Py<PyEdge>> = p
                .edges()
                .iter()
                .map(|e| edge_to_py(py, e))
                .collect::<PyResult<_>>()?;
            Ok(Py::new(py, PyPath { nodes, edges })?.into_any())
        }
        Value::Temporal(tv) => {
            let datetime_module = py.import("datetime")?;
            match tv {
                TemporalValue::Date { days_since_epoch } => {
                    let date_class = datetime_module.getattr("date")?;
                    let epoch_ordinal: i64 = 719163; // date(1970,1,1).toordinal()
                    let result = date_class
                        .call_method1("fromordinal", (epoch_ordinal + *days_since_epoch as i64,))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::LocalTime {
                    nanos_since_midnight,
                } => {
                    let (hour, minute, second, microsecond) =
                        split_time_of_day(*nanos_since_midnight);
                    let time_class = datetime_module.getattr("time")?;
                    let result = time_class.call1((hour, minute, second, microsecond))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::Time {
                    nanos_since_midnight,
                    offset_seconds,
                } => {
                    let (hour, minute, second, microsecond) =
                        split_time_of_day(*nanos_since_midnight);
                    let tz = fixed_timezone(py, &datetime_module, *offset_seconds)?;
                    let time_class = datetime_module.getattr("time")?;
                    let result = time_class.call1((hour, minute, second, microsecond, tz))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::LocalDateTime { nanos_since_epoch } => {
                    // Wall-clock-as-UTC: rebuild the naive datetime with exact
                    // integer arithmetic (`epoch + timedelta(microseconds=..)`),
                    // never `fromtimestamp`, which applies the host's local
                    // timezone and loses precision through f64 scaling.
                    let total_micros = nanos_since_epoch.div_euclid(1_000);
                    let dt_class = datetime_module.getattr("datetime")?;
                    let td_class = datetime_module.getattr("timedelta")?;
                    let epoch = dt_class.call1((1970, 1, 1))?;
                    let delta = td_class.call1((0i64, 0i64, total_micros))?;
                    let result = epoch.call_method1("__add__", (delta,))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::DateTime {
                    nanos_since_epoch,
                    offset_seconds,
                    ..
                } => {
                    // True UTC instant: build the aware UTC datetime from the
                    // exact microsecond offset, then convert into the stored
                    // fixed offset for local rendering. Integer arithmetic only.
                    let total_micros = nanos_since_epoch.div_euclid(1_000);
                    let tz = fixed_timezone(py, &datetime_module, *offset_seconds)?;
                    let dt_class = datetime_module.getattr("datetime")?;
                    let td_class = datetime_module.getattr("timedelta")?;
                    let utc_tz = datetime_module.getattr("timezone")?.getattr("utc")?;
                    let epoch_utc = dt_class.call1((1970, 1, 1, 0, 0, 0, 0, utc_tz))?;
                    let delta = td_class.call1((0i64, 0i64, total_micros))?;
                    let instant_utc = epoch_utc.call_method1("__add__", (delta,))?;
                    let result = instant_utc.call_method1("astimezone", (tz,))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::Duration {
                    months,
                    days,
                    nanos,
                } => {
                    let total_days = months * 30 + days;
                    let total_secs = nanos / 1_000_000_000;
                    let remaining_micros = (nanos % 1_000_000_000) / 1_000;
                    let td_class = datetime_module.getattr("timedelta")?;
                    let result = td_class.call1((total_days, total_secs, remaining_micros))?;
                    Ok(result.into_py_any(py)?)
                }
                TemporalValue::Btic { lo, hi, meta } => {
                    let btic = uni_common::uni_btic::Btic::new(*lo, *hi, *meta).map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!("invalid BTIC: {e}"))
                    })?;
                    Ok(Py::new(py, crate::btic::PyBtic { inner: btic })?.into_any())
                }
            }
        }
        // Learned-sparse vector → `SparseVector` (without this arm it would fall
        // through to `py.None()`, silently dropping any returned sparse property).
        Value::SparseVector { indices, values } => {
            let inner =
                uni_common::uni_sparse_vector::SparseVector::new(indices.clone(), values.clone())
                    .map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid stored sparse vector: {e}"
                    ))
                })?;
            Ok(Py::new(py, crate::sparse::PySparseVector { inner })?.into_any())
        }
        // Binary vector → Python list of lane ints (`0..=255`). Without this arm it
        // would fall through to `py.None()`, silently dropping the property.
        Value::BinaryVector(bytes) => {
            let lanes: Vec<i64> = bytes.iter().map(|&b| i64::from(b)).collect();
            Ok(lanes.into_py_any(py)?)
        }
        // `uni_common::Value` is `#[non_exhaustive]` and this is a different
        // crate, so the wildcard is mandatory — rustc can never warn here. It
        // used to return `py.None()`, which is how the `SparseVector` and
        // `BinaryVector` arms above came to exist: each was added only after a
        // release had already shipped silently dropping that property. Failing
        // loudly converts the next such variant from lost data into a visible
        // error at the boundary.
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "uni_db: no Python conversion for Value variant {}; this is a \
             uni-db bug — the binding needs an arm for it",
            truncated_variant_debug(other)
        ))),
    }
}

/// Render a `Value` for an error message without pasting a whole vector into it.
fn truncated_variant_debug(value: &Value) -> String {
    const MAX: usize = 120;
    let rendered = format!("{value:?}");
    if rendered.len() <= MAX {
        return rendered;
    }
    let mut cut = MAX;
    while cut > 0 && !rendered.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}… ({} bytes total)", &rendered[..cut], rendered.len())
}

/// Convert a Python object to a serde_json::Value.
pub fn py_object_to_json(py: Python, obj: &Py<PyAny>) -> PyResult<serde_json::Value> {
    if obj.is_none(py) {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(b) = obj.extract::<bool>(py) {
        return Ok(serde_json::Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>(py) {
        return Ok(serde_json::json!(i));
    }
    if let Ok(f) = obj.extract::<f64>(py) {
        return Ok(serde_json::json!(f));
    }
    if let Ok(s) = obj.extract::<String>(py) {
        return Ok(serde_json::Value::String(s));
    }

    let bound = obj.bind(py);
    if let Ok(l) = bound.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in l {
            vec.push(py_object_to_json(py, &item.into())?);
        }
        return Ok(serde_json::Value::Array(vec));
    }

    if let Ok(d) = bound.cast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (k, v) in d {
            let key = k.extract::<String>()?;
            let val = py_object_to_json(py, &v.into())?;
            map.insert(key, val);
        }
        return Ok(serde_json::Value::Object(map));
    }

    // Unrecognized Python types must NOT be silently coerced to JSON `null`
    // (that would drop the caller's data without warning). Raise `TypeError`.
    let type_name = bound.get_type().name()?;
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "cannot convert Python object of type '{type_name}' to JSON"
    )))
}

/// Convert a serde_json::Value to a Python object.
pub fn json_value_to_py(py: Python, val: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match val {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py).unwrap().into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py).unwrap().into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py).unwrap().into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                list.append(json_value_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
    }
}

/// Convert a Python object to a Uni Value.
pub fn py_object_to_value(py: Python, obj: &Py<PyAny>) -> PyResult<Value> {
    if obj.is_none(py) {
        return Ok(Value::Null);
    }

    let bound = obj.bind(py);

    // Check for datetime types FIRST (before int/float extraction)
    // datetime is a subclass of date, so check datetime first
    let datetime_module = py.import("datetime")?;
    let datetime_class = datetime_module.getattr("datetime")?;
    let date_class = datetime_module.getattr("date")?;
    let time_class = datetime_module.getattr("time")?;

    if bound.is_instance(&datetime_class)? {
        // Wall-clock nanoseconds from the calendar/clock components, computed
        // with exact integer arithmetic (never Python's local-timezone-dependent
        // `.timestamp()`, which both shifts naive values by the host offset and
        // loses microsecond precision through f64 scaling at modern epochs).
        let wall_nanos = wall_clock_nanos_since_epoch(bound)?;
        let tzinfo = bound.getattr("tzinfo")?;
        if tzinfo.is_none() {
            // Naive: the wall-clock reading is stored as-if-UTC, matching the
            // core's `LocalDateTime.nanos_since_epoch` semantics.
            return Ok(Value::Temporal(TemporalValue::LocalDateTime {
                nanos_since_epoch: wall_nanos,
            }));
        } else {
            // Aware: `DateTime.nanos_since_epoch` is the true UTC instant, i.e.
            // the local wall-clock minus the UTC offset.
            let utcoffset = bound.call_method0("utcoffset")?;
            let offset_seconds: i32 =
                utcoffset.call_method0("total_seconds")?.extract::<f64>()? as i32;
            let tz_name: Option<String> =
                bound.call_method0("tzname")?.extract::<Option<String>>()?;
            let instant_nanos = wall_nanos - (offset_seconds as i64) * 1_000_000_000;
            return Ok(Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch: instant_nanos,
                offset_seconds,
                timezone_name: tz_name,
            }));
        }
    }

    if bound.is_instance(&date_class)? {
        // Convert to days since epoch using Python's own toordinal
        let ordinal: i64 = bound.call_method0("toordinal")?.extract()?;
        let epoch_ordinal: i64 = 719163; // date(1970,1,1).toordinal()
        let days = (ordinal - epoch_ordinal) as i32;
        return Ok(Value::Temporal(TemporalValue::Date {
            days_since_epoch: days,
        }));
    }

    if bound.is_instance(&time_class)? {
        let hour: i64 = bound.getattr("hour")?.extract()?;
        let minute: i64 = bound.getattr("minute")?.extract()?;
        let second: i64 = bound.getattr("second")?.extract()?;
        let microsecond: i64 = bound.getattr("microsecond")?.extract()?;
        let nanos = hour * 3_600_000_000_000
            + minute * 60_000_000_000
            + second * 1_000_000_000
            + microsecond * 1_000;
        let tzinfo = bound.getattr("tzinfo")?;
        if tzinfo.is_none() {
            return Ok(Value::Temporal(TemporalValue::LocalTime {
                nanos_since_midnight: nanos,
            }));
        } else {
            // `datetime.time.utcoffset()` takes NO arguments (unlike
            // `datetime.datetime.utcoffset()`); passing one raises `TypeError`.
            let utcoffset = bound.call_method0("utcoffset")?;
            let offset_seconds: i32 =
                utcoffset.call_method0("total_seconds")?.extract::<f64>()? as i32;
            return Ok(Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: nanos,
                offset_seconds,
            }));
        }
    }

    // Check for BTIC temporal interval (PyBtic instance)
    if let Ok(btic) = bound.extract::<crate::btic::PyBtic>() {
        return Ok(Value::Temporal(TemporalValue::Btic {
            lo: btic.inner.lo(),
            hi: btic.inner.hi(),
            meta: btic.inner.meta(),
        }));
    }

    // Check for a learned-sparse vector (PySparseVector instance). This MUST come
    // before the `PyDict` branch below: a sparse vector is conceptually a
    // `{term_id: weight}` mapping, so without an explicit class extraction first a
    // user-built `SparseVector` would be mis-ingested as a `Value::Map`.
    if let Ok(sv) = bound.extract::<crate::sparse::PySparseVector>() {
        return Ok(Value::SparseVector {
            indices: sv.inner.indices().to_vec(),
            values: sv.inner.values().to_vec(),
        });
    }

    // Check primitive types in order of specificity
    if let Ok(b) = obj.extract::<bool>(py) {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>(py) {
        return Ok(Value::Int(i));
    }
    if let Ok(f) = obj.extract::<f64>(py) {
        return Ok(Value::Float(f));
    }
    if let Ok(s) = obj.extract::<String>(py) {
        return Ok(Value::String(s));
    }

    // Python `bytes`/`bytearray` map to raw `Value::Bytes` (round-trips with the
    // `Value::Bytes => PyBytes` direction); without this they fall through to Null.
    if let Ok(b) = bound.cast::<PyBytes>() {
        return Ok(Value::Bytes(b.as_bytes().to_vec()));
    }
    if let Ok(b) = bound.cast::<pyo3::types::PyByteArray>() {
        return Ok(Value::Bytes(b.to_vec()));
    }

    if let Ok(l) = bound.cast::<PyList>() {
        let mut vec = Vec::new();
        for item in l {
            vec.push(py_object_to_value(py, &item.into())?);
        }
        return Ok(Value::List(vec));
    }

    if let Ok(d) = bound.cast::<PyDict>() {
        let mut map = HashMap::new();
        for (k, v) in d {
            let key = k.extract::<String>()?;
            let val = py_object_to_value(py, &v.into())?;
            map.insert(key, val);
        }
        return Ok(Value::Map(map));
    }

    // Unrecognized Python types must NOT be silently coerced to `Value::Null`
    // (that would drop the caller's data without warning). Raise `TypeError`.
    let type_name = bound.get_type().name()?;
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "cannot convert Python object of type '{type_name}' to a Uni value"
    )))
}

/// Convert Python params dict to Rust params.
pub fn prepare_params(
    py: Python,
    params: Option<HashMap<String, Py<PyAny>>>,
) -> PyResult<HashMap<String, Value>> {
    let mut rust_params = HashMap::new();
    if let Some(p) = params {
        for (k, v) in p {
            let val = py_object_to_value(py, &v)?;
            rust_params.insert(k, val);
        }
    }
    Ok(rust_params)
}

/// Convert an optional Python params dict to Rust params, preserving the
/// `None`/`Some` distinction so callers can pick the parameterized vs.
/// parameter-free query path.
pub fn convert_params(
    py: Python,
    params: Option<HashMap<String, Py<PyAny>>>,
) -> PyResult<Option<HashMap<String, Value>>> {
    match params {
        Some(p) => {
            let mut map = HashMap::with_capacity(p.len());
            for (k, v) in p {
                map.insert(k, py_object_to_value(py, &v)?);
            }
            Ok(Some(map))
        }
        None => Ok(None),
    }
}

/// Build a bulk-writer progress callback that forwards each `BulkProgress`
/// update to the given Python callable.
///
/// The Python object is not `Send`, but the bulk-writer requires a `Send`
/// closure; the returned closure always re-attaches the GIL before touching it,
/// so the `unsafe impl Send` wrapper is sound (the object is only ever used
/// while the GIL is held).
pub fn make_progress_callback(
    cb: Py<PyAny>,
) -> impl Fn(::uni_db::api::bulk::BulkProgress) + Send + 'static {
    struct PyProgressWrapper {
        py_obj: Py<PyAny>,
    }
    // SAFETY: `py_obj` is only dereferenced inside `Python::attach`, i.e. with
    // the GIL held, so it is never accessed concurrently from Rust threads.
    unsafe impl Send for PyProgressWrapper {}

    let wrapper = PyProgressWrapper { py_obj: cb };
    move |progress: ::uni_db::api::bulk::BulkProgress| {
        Python::attach(|py| {
            let py_progress = crate::types::BulkProgress {
                phase: format!("{:?}", progress.phase),
                rows_processed: progress.rows_processed,
                total_rows: progress.total_rows,
                current_label: progress.current_label.clone(),
                elapsed_secs: progress.elapsed.as_secs_f64(),
            };
            if let Ok(bound) = Py::new(py, py_progress) {
                let _ = wrapper.py_obj.call1(py, (bound,));
            }
        });
    }
}

/// Map a Rust `WriteLease` to its Python representation.
///
/// `WriteLease` is `#[non_exhaustive]`; the catch-all (covering `Custom` and any
/// future variant) reports `Local`, matching the pre-existing behavior.
pub fn write_lease_to_py(
    wl: &::uni_db::api::multi_agent::WriteLease,
) -> crate::types::PyWriteLease {
    match wl {
        ::uni_db::api::multi_agent::WriteLease::DynamoDB { table } => crate::types::PyWriteLease {
            variant: crate::types::WriteLeaseVariant::DynamoDB {
                table: table.clone(),
            },
        },
        _ => crate::types::PyWriteLease {
            variant: crate::types::WriteLeaseVariant::Local,
        },
    }
}

/// Convert a borrowed Python params dict to Rust params.
pub fn convert_params_ref(
    py: Python,
    params: &HashMap<String, Py<PyAny>>,
) -> PyResult<HashMap<String, Value>> {
    let mut map = HashMap::with_capacity(params.len());
    for (k, v) in params {
        map.insert(k.clone(), py_object_to_value(py, v)?);
    }
    Ok(map)
}

/// Convert a single query result row to a Python `{column: value}` dict.
///
/// Shared by the sync and async cursor `fetch_*` / iteration paths.
pub fn row_to_dict<'py>(py: Python<'py>, row: &::uni_db::Row) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (col, val) in row.as_map() {
        dict.set_item(col, value_to_py(py, val)?)?;
    }
    Ok(dict)
}

/// Convert query result rows to Python Row objects.
pub fn rows_to_py(py: Python, rows: Vec<::uni_db::Row>) -> PyResult<Vec<Py<PyAny>>> {
    let mut result = Vec::new();
    for row in rows {
        let columns: Vec<String> = row.columns().to_vec();
        let mut values = Vec::with_capacity(columns.len());
        for col in &columns {
            let val = row.value(col).unwrap_or(&::uni_db::Value::Null);
            values.push(value_to_py(py, val)?);
        }
        let py_row = crate::types::PyRow { columns, values };
        result.push(Py::new(py, py_row)?.into_any());
    }
    Ok(result)
}

/// Convert Locy rows (HashMap<String, Value>) to a Python list of dicts.
fn locy_rows_to_py(py: Python, rows: Vec<HashMap<String, Value>>) -> PyResult<Vec<Py<PyAny>>> {
    let mut result = Vec::new();
    for row in rows {
        let dict = PyDict::new(py);
        for (col_name, val) in row {
            dict.set_item(&col_name, value_to_py(py, &val)?)?;
        }
        result.push(dict.into());
    }
    Ok(result)
}

/// Convert a Locy DerivationNode to a Python dict.
fn derivation_node_to_py(py: Python, node: uni_locy::DerivationNode) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("rule", &node.rule)?;
    dict.set_item("clause_index", node.clause_index)?;
    dict.set_item("priority", node.priority)?;

    let bindings_dict = PyDict::new(py);
    for (k, v) in node.bindings {
        bindings_dict.set_item(&k, value_to_py(py, &v)?)?;
    }
    dict.set_item("bindings", bindings_dict)?;

    let along_dict = PyDict::new(py);
    for (k, v) in node.along_values {
        along_dict.set_item(&k, value_to_py(py, &v)?)?;
    }
    dict.set_item("along_values", along_dict)?;

    let children = PyList::empty(py);
    for child in node.children {
        children.append(derivation_node_to_py(py, child)?)?;
    }
    dict.set_item("children", children)?;
    dict.set_item("graph_fact", node.graph_fact)?;
    dict.set_item("proof_probability", node.proof_probability)?;

    let neural_calls = PyList::empty(py);
    for call in node.neural_calls {
        let call_dict = PyDict::new(py);
        call_dict.set_item("model_name", &call.model_name)?;
        call_dict.set_item("raw_probability", call.raw_probability)?;
        call_dict.set_item("calibrated_probability", call.calibrated_probability)?;
        if let Some(band) = call.confidence_band {
            let band_dict = PyDict::new(py);
            band_dict.set_item("lower", band.lower)?;
            band_dict.set_item("upper", band.upper)?;
            let (source_name, source_params) = match band.source {
                uni_locy::ConfidenceSource::Conformal { alpha } => {
                    let p = PyDict::new(py);
                    p.set_item("alpha", alpha)?;
                    ("conformal", p)
                }
                uni_locy::ConfidenceSource::EnsembleVariance { n_estimators } => {
                    let p = PyDict::new(py);
                    p.set_item("n_estimators", n_estimators)?;
                    ("ensemble_variance", p)
                }
                uni_locy::ConfidenceSource::Credal {
                    lower_prior,
                    upper_prior,
                } => {
                    let p = PyDict::new(py);
                    p.set_item("lower_prior", lower_prior)?;
                    p.set_item("upper_prior", upper_prior)?;
                    ("credal", p)
                }
            };
            band_dict.set_item("source", source_name)?;
            band_dict.set_item("source_params", source_params)?;
            call_dict.set_item("confidence_band", band_dict)?;
        } else {
            call_dict.set_item("confidence_band", py.None())?;
        }
        neural_calls.append(call_dict)?;
    }
    dict.set_item("neural_calls", neural_calls)?;

    Ok(dict.into())
}

/// Convert a Locy Modification to a Python dict.
fn modification_to_py(py: Python, m: uni_locy::Modification) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    match m {
        uni_locy::Modification::RemoveEdge {
            source_var,
            target_var,
            edge_var,
            edge_type,
            match_properties,
        } => {
            dict.set_item("type", "remove_edge")?;
            dict.set_item("source_var", source_var)?;
            dict.set_item("target_var", target_var)?;
            dict.set_item("edge_var", edge_var)?;
            dict.set_item("edge_type", edge_type)?;
            let props_dict = PyDict::new(py);
            for (k, v) in match_properties {
                props_dict.set_item(&k, value_to_py(py, &v)?)?;
            }
            dict.set_item("match_properties", props_dict)?;
        }
        uni_locy::Modification::ChangeProperty {
            element_var,
            property,
            old_value,
            new_value,
        } => {
            dict.set_item("type", "change_property")?;
            dict.set_item("element_var", element_var)?;
            dict.set_item("property", property)?;
            dict.set_item("old_value", value_to_py(py, &old_value)?)?;
            dict.set_item("new_value", value_to_py(py, &new_value)?)?;
        }
        uni_locy::Modification::AddEdge {
            source_var,
            target_var,
            edge_type,
            properties,
        } => {
            dict.set_item("type", "add_edge")?;
            dict.set_item("source_var", source_var)?;
            dict.set_item("target_var", target_var)?;
            dict.set_item("edge_type", edge_type)?;
            let props_dict = PyDict::new(py);
            for (k, v) in properties {
                props_dict.set_item(&k, value_to_py(py, &v)?)?;
            }
            dict.set_item("properties", props_dict)?;
        }
    }
    Ok(dict.into())
}

/// Convert a Locy CommandResult to a Python dict.
fn command_result_to_py(py: Python, cmd: uni_locy::CommandResult) -> PyResult<Py<PyAny>> {
    use crate::types::*;
    match cmd {
        uni_locy::CommandResult::Query(rows) => {
            let rows_py = locy_rows_to_py(py, rows)?;
            let list = PyList::new(py, &rows_py)?;
            Ok(Py::new(py, PyQueryCommandResult { rows: list.into() })?.into_any())
        }
        uni_locy::CommandResult::Assume(rows) => {
            let rows_py = locy_rows_to_py(py, rows)?;
            let list = PyList::new(py, &rows_py)?;
            Ok(Py::new(py, PyAssumeCommandResult { rows: list.into() })?.into_any())
        }
        uni_locy::CommandResult::Explain(node) => {
            let tree = derivation_node_to_py(py, node)?;
            Ok(Py::new(py, PyExplainCommandResult { tree })?.into_any())
        }
        uni_locy::CommandResult::Abduce(result) => {
            let mods = PyList::empty(py);
            for vm in result.modifications {
                let mod_dict = PyDict::new(py);
                mod_dict.set_item("modification", modification_to_py(py, vm.modification)?)?;
                mod_dict.set_item("validated", vm.validated)?;
                mod_dict.set_item("cost", vm.cost)?;
                mods.append(mod_dict)?;
            }
            Ok(Py::new(
                py,
                PyAbduceCommandResult {
                    modifications: mods.into(),
                },
            )?
            .into_any())
        }
        uni_locy::CommandResult::Derive { affected } => {
            Ok(Py::new(py, PyDeriveCommandResult { affected })?.into_any())
        }
        uni_locy::CommandResult::Cypher(rows) => {
            let rows_py = locy_rows_to_py(py, rows)?;
            let list = PyList::new(py, &rows_py)?;
            Ok(Py::new(py, PyCypherCommandResult { rows: list.into() })?.into_any())
        }
        uni_locy::CommandResult::Calibrate(c) => {
            let d = PyDict::new(py);
            d.set_item("type", "calibrate")?;
            d.set_item("model_name", &c.model_name)?;
            d.set_item("method", format!("{:?}", c.method))?;
            d.set_item("n_samples", c.n_samples)?;
            d.set_item("holdout_size", c.holdout_size)?;
            d.set_item("raw_brier", c.raw_brier)?;
            d.set_item("raw_ece", c.raw_ece)?;
            d.set_item("calibrated_brier", c.calibrated_brier)?;
            d.set_item("calibrated_ece", c.calibrated_ece)?;
            match c.confidence_band_quantile {
                Some(q) => d.set_item("confidence_band_quantile", q)?,
                None => d.set_item("confidence_band_quantile", py.None())?,
            }
            // Surface the fitted calibrator so Python callers can apply
            // it to raw classifier outputs (e.g. to rescore a ranked
            // queue with calibrated probabilities).
            let py_cal = Py::new(
                py,
                crate::types::PyCalibrator {
                    inner: c.calibrator.clone(),
                },
            )?;
            d.set_item("calibrator", py_cal)?;
            Ok(d.into_any().unbind())
        }
        uni_locy::CommandResult::Validate(v) => {
            let d = PyDict::new(py);
            d.set_item("type", "validate")?;
            d.set_item("rule_name", &v.rule_name)?;
            d.set_item("prob_column", &v.prob_column)?;
            d.set_item("n_samples", v.n_samples)?;
            let metrics = PyDict::new(py);
            for (m, val) in &v.metrics {
                metrics.set_item(format!("{:?}", m), *val)?;
            }
            d.set_item("metrics", metrics)?;
            Ok(d.into_any().unbind())
        }
    }
}

/// Extract a LocyConfig from a Python config dict.
pub fn extract_locy_config(
    py: Python,
    config: HashMap<String, Py<PyAny>>,
) -> PyResult<::uni_db::locy::LocyConfig> {
    let mut locy_config = ::uni_db::locy::LocyConfig::default();
    if let Some(v) = config.get("max_iterations") {
        locy_config.max_iterations = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("timeout") {
        locy_config.timeout = std::time::Duration::from_secs_f64(v.extract::<f64>(py)?);
    }
    if let Some(v) = config.get("allow_partial") {
        locy_config.allow_partial = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("max_explain_depth") {
        locy_config.max_explain_depth = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_slg_depth") {
        locy_config.max_slg_depth = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_abduce_candidates") {
        locy_config.max_abduce_candidates = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_abduce_results") {
        locy_config.max_abduce_results = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_derived_bytes") {
        locy_config.max_derived_bytes = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("deterministic_best_by") {
        locy_config.deterministic_best_by = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("strict_probability_domain") {
        locy_config.strict_probability_domain = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("probability_epsilon") {
        locy_config.probability_epsilon = v.extract::<f64>(py)?;
    }
    if let Some(v) = config.get("exact_probability") {
        locy_config.exact_probability = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("max_bdd_variables") {
        locy_config.max_bdd_variables = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("top_k_proofs") {
        locy_config.top_k_proofs = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("top_k_proofs_training") {
        locy_config.top_k_proofs_training = Some(v.extract::<usize>(py)?);
    }
    if let Some(v) = config.get("params") {
        let params_map = v.extract::<HashMap<String, Py<PyAny>>>(py)?;
        locy_config.params = prepare_params(py, Some(params_map))?;
    }
    if let Some(v) = config.get("classifier_registry") {
        let raw = v.extract::<HashMap<String, Py<PyAny>>>(py)?;
        let registry = crate::classifier::build_classifier_registry(py, raw)?;
        locy_config.classifier_registry = registry;
    }
    // Always-on NeuralProvenance side-channel store. Without it, EXPLAIN's
    // neural_calls list comes back empty for Python-registered classifiers
    // (the fallback re-invocation path in collect_neural_calls_for_row only
    // populates entries when the planner's pre-rewrite model_invocations
    // survive into the executed clause; the store side-channel is what
    // apply_model_invocations actually writes into).
    locy_config.classifier_provenance_store =
        Some(std::sync::Arc::new(::uni_locy::NeuralProvenanceStore::new()));
    Ok(locy_config)
}

/// Convert the optional incompleteness diagnostics to a Python dict (or `None`).
///
/// Present only on the `allow_partial` path; `None` for a complete evaluation.
fn locy_incomplete_to_py(
    py: Python,
    incomplete: Option<&uni_db::LocyIncomplete>,
) -> PyResult<Py<PyAny>> {
    let Some(d) = incomplete else {
        return Ok(py.None());
    };
    let dict = PyDict::new(py);
    dict.set_item("reason", d.reason.as_str())?;
    dict.set_item("elapsed_ms", d.elapsed_ms)?;
    dict.set_item("limit_ms", d.limit_ms)?;
    dict.set_item("max_iterations", d.max_iterations)?;
    dict.set_item("completed_strata", d.completed_strata)?;
    dict.set_item("total_strata", d.total_strata)?;
    dict.set_item("incomplete_rules", PyList::new(py, &d.incomplete_rules)?)?;
    dict.set_item("skipped_rules", PyList::new(py, &d.skipped_rules)?)?;
    dict.set_item(
        "complement_rules_affected",
        PyList::new(py, &d.complement_rules_affected)?,
    )?;
    Ok(dict.into())
}

/// Convert compile-time warnings to a Python list of dicts.
///
/// Mirrors the `RuntimeWarning` shape (`code` / `message` / `rule_name`);
/// `CompilerWarning` carries no `variable_count` or `key_group`.
///
/// These were previously dropped at the PyO3 boundary entirely, so warnings the
/// compiler had already emitted were invisible from Python. Issue #159 was
/// filed against a program that warns at compile time.
///
/// The match is deliberately exhaustive rather than using a `_` arm, so a new
/// `WarningCode` variant fails the build here instead of silently reaching
/// Python under a wrong or generic name.
fn compile_warnings_to_py(
    py: Python,
    warnings: &[uni_locy::types::CompilerWarning],
) -> PyResult<Py<PyAny>> {
    use uni_locy::types::WarningCode;

    let list = PyList::empty(py);
    for w in warnings {
        let wd = PyDict::new(py);
        let code_str = match w.code {
            WarningCode::MsumNonNegativity => "msum_non_negativity",
            WarningCode::ProbabilityDomainViolation => "probability_domain_violation",
            WarningCode::FoldInRecursivePath => "fold_in_recursive_path",
            WarningCode::EceBinningBias => "ece_binning_bias",
            WarningCode::UncalibratedLLMLogprobs => "uncalibrated_llm_logprobs",
            WarningCode::UncalibratedNeuralPredicate => "uncalibrated_neural_predicate",
            WarningCode::SharedNeuralInputArgument => "shared_neural_input_argument",
            WarningCode::SharedNeuralFeatureValue => "shared_neural_feature_value",
            WarningCode::PositiveComplementCorrelation => "positive_complement_correlation",
            WarningCode::CrossPredicateCorrelation => "cross_predicate_correlation",
            WarningCode::SharedRetrievalContext => "shared_retrieval_context",
        };
        wd.set_item("code", code_str)?;
        wd.set_item("message", &w.message)?;
        wd.set_item("rule_name", &w.rule_name)?;
        list.append(wd)?;
    }
    Ok(list.into())
}

/// Convert a LocyResult to a Python dict.
pub fn locy_result_to_py(py: Python, result: uni_db::locy::LocyResult) -> PyResult<Py<PyAny>> {
    let result = result.into_inner();
    // Capture before the by-value field moves below — `timed_out()`
    // borrows `&result`, which would conflict afterwards.
    let timed_out = result.timed_out();
    let dict = PyDict::new(py);

    // derived: HashMap<String, Vec<Row>> -> Python dict of lists of dicts
    let derived_dict = PyDict::new(py);
    for (rule_name, rows) in result.derived {
        derived_dict.set_item(&rule_name, locy_rows_to_py(py, rows)?)?;
    }
    dict.set_item("derived", derived_dict)?;

    // stats
    let stats = crate::types::LocyStats {
        strata_evaluated: result.stats.strata_evaluated,
        total_iterations: result.stats.total_iterations,
        derived_nodes: result.stats.derived_nodes,
        derived_edges: result.stats.derived_edges,
        evaluation_time_secs: result.stats.evaluation_time.as_secs_f64(),
        queries_executed: result.stats.queries_executed,
        mutations_executed: result.stats.mutations_executed,
        peak_memory_bytes: result.stats.peak_memory_bytes,
    };
    dict.set_item("stats", stats.into_py_any(py)?)?;

    // command_results
    let cmd_list = PyList::empty(py);
    for cmd in result.command_results {
        cmd_list.append(command_result_to_py(py, cmd)?)?;
    }
    dict.set_item("command_results", cmd_list)?;

    // warnings: Vec<RuntimeWarning> -> list of dicts
    let warn_list = PyList::empty(py);
    for w in result.warnings {
        let wd = PyDict::new(py);
        let code_str = match w.code {
            uni_locy::RuntimeWarningCode::SharedProbabilisticDependency => {
                "shared_probabilistic_dependency"
            }
            uni_locy::RuntimeWarningCode::BddLimitExceeded => "bdd_limit_exceeded",
            uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact => {
                "cross_group_correlation_not_exact"
            }
            uni_locy::RuntimeWarningCode::FuzzyNotProbabilistic => "fuzzy_not_probabilistic",
            uni_locy::RuntimeWarningCode::TopKPruningCrossedDependency => {
                "top_k_pruning_crossed_dependency"
            }
        };
        wd.set_item("code", code_str)?;
        wd.set_item("message", &w.message)?;
        wd.set_item("rule_name", &w.rule_name)?;
        match w.variable_count {
            Some(n) => wd.set_item("variable_count", n)?,
            None => wd.set_item("variable_count", py.None())?,
        }
        match w.key_group {
            Some(ref g) => wd.set_item("key_group", g)?,
            None => wd.set_item("key_group", py.None())?,
        }
        warn_list.append(wd)?;
    }
    dict.set_item("warnings", warn_list)?;

    // compile_warnings: Vec<CompilerWarning> -> list of dicts
    dict.set_item(
        "compile_warnings",
        compile_warnings_to_py(py, &result.compile_warnings)?,
    )?;

    // approximate_groups: HashMap<String, Vec<String>> -> Python dict of lists
    let approx_dict = PyDict::new(py);
    for (rule_name, groups) in result.approximate_groups {
        let group_list = PyList::new(py, groups.iter().map(|s| s.as_str()))?;
        approx_dict.set_item(&rule_name, group_list)?;
    }
    dict.set_item("approximate_groups", approx_dict)?;

    dict.set_item("timed_out", timed_out)?;
    dict.set_item(
        "incomplete",
        locy_incomplete_to_py(py, result.incomplete.as_ref())?,
    )?;

    Ok(dict.into())
}

/// Convert QueryMetrics to a Python dict.
pub fn query_metrics_to_py(py: Python, m: &::uni_db::QueryMetrics) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("parse_time_ms", m.parse_time.as_secs_f64() * 1000.0)?;
    dict.set_item("plan_time_ms", m.plan_time.as_secs_f64() * 1000.0)?;
    dict.set_item("exec_time_ms", m.exec_time.as_secs_f64() * 1000.0)?;
    dict.set_item("total_time_ms", m.total_time.as_secs_f64() * 1000.0)?;
    dict.set_item("rows_returned", m.rows_returned)?;
    dict.set_item("rows_scanned", m.rows_scanned)?;
    dict.set_item("bytes_read", m.bytes_read)?;
    dict.set_item("plan_cache_hit", m.plan_cache_hit)?;
    dict.set_item("l0_reads", m.l0_reads)?;
    dict.set_item("branch_scans", m.branch_scans)?;
    dict.set_item("snapshot_reads", m.snapshot_reads)?;
    dict.set_item("index_scans", m.index_scans)?;
    dict.set_item("index_comparisons", m.index_comparisons)?;
    dict.set_item("scans_reported", m.scans_reported)?;
    dict.set_item("storage_reads", m.storage_reads)?;
    dict.set_item("cache_hits", m.cache_hits)?;
    Ok(dict.into())
}

/// Convert an ExecuteResult to a Python ExecuteResult.
pub fn execute_result_to_py(
    py: Python,
    r: ::uni_db::query_crate::ExecuteResult,
) -> PyResult<crate::types::PyExecuteResult> {
    Ok(crate::types::PyExecuteResult {
        affected_rows: r.affected_rows(),
        nodes_created: r.nodes_created(),
        nodes_deleted: r.nodes_deleted(),
        relationships_created: r.relationships_created(),
        relationships_deleted: r.relationships_deleted(),
        properties_set: r.properties_set(),
        labels_added: r.labels_added(),
        labels_removed: r.labels_removed(),
        metrics: query_metrics_to_py(py, r.metrics())?,
    })
}

/// Convert a LocyResult to a Python LocyResult class instance.
pub fn locy_result_to_py_class(
    py: Python,
    result: uni_db::locy::LocyResult,
) -> PyResult<crate::types::PyLocyResult> {
    let result = result.into_inner();
    // Capture before the by-value field moves below — `timed_out()`
    // borrows `&result`, which would conflict afterwards.
    let timed_out = result.timed_out();
    // Reuse the existing dict-based conversion for the inner fields
    let derived_dict = pyo3::types::PyDict::new(py);
    for (rule_name, rows) in result.derived {
        derived_dict.set_item(&rule_name, locy_rows_to_py(py, rows)?)?;
    }

    let stats = crate::types::LocyStats {
        strata_evaluated: result.stats.strata_evaluated,
        total_iterations: result.stats.total_iterations,
        derived_nodes: result.stats.derived_nodes,
        derived_edges: result.stats.derived_edges,
        evaluation_time_secs: result.stats.evaluation_time.as_secs_f64(),
        queries_executed: result.stats.queries_executed,
        mutations_executed: result.stats.mutations_executed,
        peak_memory_bytes: result.stats.peak_memory_bytes,
    };

    let cmd_list = pyo3::types::PyList::empty(py);
    for cmd in result.command_results {
        cmd_list.append(command_result_to_py(py, cmd)?)?;
    }

    // Built before `result.warnings` is moved below.
    let compile_warn_list = compile_warnings_to_py(py, &result.compile_warnings)?;

    let warn_list = pyo3::types::PyList::empty(py);
    for w in result.warnings {
        let wd = pyo3::types::PyDict::new(py);
        let code_str = match w.code {
            uni_locy::RuntimeWarningCode::SharedProbabilisticDependency => {
                "shared_probabilistic_dependency"
            }
            uni_locy::RuntimeWarningCode::BddLimitExceeded => "bdd_limit_exceeded",
            uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact => {
                "cross_group_correlation_not_exact"
            }
            uni_locy::RuntimeWarningCode::FuzzyNotProbabilistic => "fuzzy_not_probabilistic",
            uni_locy::RuntimeWarningCode::TopKPruningCrossedDependency => {
                "top_k_pruning_crossed_dependency"
            }
        };
        wd.set_item("code", code_str)?;
        wd.set_item("message", &w.message)?;
        wd.set_item("rule_name", &w.rule_name)?;
        match w.variable_count {
            Some(n) => wd.set_item("variable_count", n)?,
            None => wd.set_item("variable_count", py.None())?,
        }
        match w.key_group {
            Some(ref g) => wd.set_item("key_group", g)?,
            None => wd.set_item("key_group", py.None())?,
        }
        warn_list.append(wd)?;
    }

    let approx_dict = pyo3::types::PyDict::new(py);
    for (rule_name, groups) in result.approximate_groups {
        let group_list = pyo3::types::PyList::new(py, groups.iter().map(|s| s.as_str()))?;
        approx_dict.set_item(&rule_name, group_list)?;
    }

    // Wrap the derived fact set in the opaque PyDerivedFactSet type
    let derived_fact_set: Py<pyo3::PyAny> = match result.derived_fact_set {
        Some(dfs) => {
            let py_dfs = crate::types::PyDerivedFactSet { inner: Some(dfs) };
            py_dfs.into_py_any(py)?
        }
        None => py.None(),
    };

    let incomplete = locy_incomplete_to_py(py, result.incomplete.as_ref())?;

    Ok(crate::types::PyLocyResult {
        derived: derived_dict.into(),
        stats: stats.into_py_any(py)?,
        command_results: cmd_list.into(),
        warnings: warn_list.into(),
        compile_warnings: compile_warn_list,
        approximate_groups: approx_dict.into(),
        derived_fact_set,
        timed_out,
        incomplete,
    })
}

/// Convert a `LocyExplainOutput` to a Python dict.
pub fn locy_explain_to_py(
    py: Python,
    output: uni_db::api::locy_result::LocyExplainOutput,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("plan_text", &output.plan_text)?;
    dict.set_item("strata_count", output.strata_count)?;
    dict.set_item(
        "rule_names",
        pyo3::types::PyList::new(py, output.rule_names.iter().map(|s| s.as_str()))?,
    )?;
    dict.set_item("has_recursive_strata", output.has_recursive_strata)?;
    dict.set_item(
        "warnings",
        pyo3::types::PyList::new(py, output.warnings.iter().map(|s| s.as_str()))?,
    )?;
    dict.set_item("command_count", output.command_count)?;
    dict.into_py_any(py)
}

/// Convert ExplainOutput to a Python dict.
pub fn explain_output_to_py(
    py: Python,
    output: ::uni_db::ExplainOutput,
) -> PyResult<Py<pyo3::PyAny>> {
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

/// Convert ProfileOutput to a Python dict.
pub fn profile_output_to_py(
    py: Python,
    profile: ::uni_db::ProfileOutput,
) -> PyResult<Py<pyo3::PyAny>> {
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

    Ok(profile_dict.into())
}

// ============================================================================
// Typed conversion functions (Phase 1) — return pyclass instances
// ============================================================================

/// Convert QueryMetrics to a typed PyQueryMetrics instance.
pub fn query_metrics_to_py_class(
    py: Python,
    m: &::uni_db::QueryMetrics,
) -> PyResult<Py<crate::types::PyQueryMetrics>> {
    let metrics = crate::types::PyQueryMetrics {
        parse_time_ms: m.parse_time.as_secs_f64() * 1000.0,
        plan_time_ms: m.plan_time.as_secs_f64() * 1000.0,
        exec_time_ms: m.exec_time.as_secs_f64() * 1000.0,
        total_time_ms: m.total_time.as_secs_f64() * 1000.0,
        rows_returned: m.rows_returned,
        rows_scanned: m.rows_scanned,
        bytes_read: m.bytes_read,
        plan_cache_hit: m.plan_cache_hit,
        l0_reads: m.l0_reads,
        storage_reads: m.storage_reads,
        cache_hits: m.cache_hits,
        branch_scans: m.branch_scans,
        snapshot_reads: m.snapshot_reads,
        index_scans: m.index_scans,
        index_comparisons: m.index_comparisons,
        scans_reported: m.scans_reported,
    };
    Py::new(py, metrics)
}

/// Convert a QueryWarning enum to a PyQueryWarning.
pub fn query_warning_to_py(w: &::uni_db::QueryWarning) -> crate::types::PyQueryWarning {
    match w {
        ::uni_db::QueryWarning::IndexUnavailable {
            label,
            index_name,
            reason,
        } => crate::types::PyQueryWarning {
            code: "index_unavailable".to_string(),
            message: format!(
                "Index '{}' on label '{}' unavailable: {}",
                index_name, label, reason
            ),
        },
        ::uni_db::QueryWarning::NoIndexForFilter { label, property } => {
            crate::types::PyQueryWarning {
                code: "no_index_for_filter".to_string(),
                message: format!("No index on '{}.{}' — full scan required", label, property),
            }
        }
        ::uni_db::QueryWarning::RrfPointContext => crate::types::PyQueryWarning {
            code: "rrf_point_context".to_string(),
            message: "RRF used in point-query context".to_string(),
        },
        ::uni_db::QueryWarning::Other(msg) => crate::types::PyQueryWarning {
            code: "other".to_string(),
            message: msg.clone(),
        },
    }
}

/// Convert a full QueryResult to a typed PyQueryResult.
pub fn query_result_to_py_class(
    py: Python,
    result: ::uni_db::QueryResult,
) -> PyResult<crate::types::PyQueryResult> {
    let columns = result.columns().to_vec();
    let warnings: Vec<crate::types::PyQueryWarning> =
        result.warnings().iter().map(query_warning_to_py).collect();
    let metrics = query_metrics_to_py_class(py, result.metrics())?;
    let rows = rows_to_py(py, result.into_rows())?;
    Ok(crate::types::PyQueryResult {
        rows,
        metrics,
        warnings,
        columns,
    })
}

/// Convert ExplainOutput to a typed PyExplainOutput.
pub fn explain_output_to_py_class(
    py: Python,
    output: ::uni_db::ExplainOutput,
) -> PyResult<crate::types::PyExplainOutput> {
    let cost_dict = PyDict::new(py);
    cost_dict.set_item("estimated_rows", output.cost_estimates.estimated_rows)?;
    cost_dict.set_item("estimated_cost", output.cost_estimates.estimated_cost)?;

    let index_usage = PyList::empty(py);
    for usage in &output.index_usage {
        let d = PyDict::new(py);
        d.set_item("label_or_type", &usage.label_or_type)?;
        d.set_item("property", &usage.property)?;
        d.set_item("index_type", &usage.index_type)?;
        d.set_item("used", usage.used)?;
        if let Some(reason) = &usage.reason {
            d.set_item("reason", reason)?;
        }
        index_usage.append(d)?;
    }

    let suggestions = PyList::empty(py);
    for s in &output.suggestions {
        let d = PyDict::new(py);
        d.set_item("label_or_type", &s.label_or_type)?;
        d.set_item("property", &s.property)?;
        d.set_item("index_type", &s.index_type)?;
        d.set_item("reason", &s.reason)?;
        d.set_item("create_statement", &s.create_statement)?;
        suggestions.append(d)?;
    }

    Ok(crate::types::PyExplainOutput {
        plan_text: output.plan_text,
        warnings: output.warnings,
        cost_estimates: cost_dict.into(),
        index_usage: index_usage.into(),
        suggestions: suggestions.into(),
    })
}

/// Convert ProfileOutput to a typed PyProfileOutput.
pub fn profile_output_to_py_class(
    py: Python,
    profile: ::uni_db::ProfileOutput,
) -> PyResult<crate::types::PyProfileOutput> {
    let ops = PyList::empty(py);
    for op in &profile.runtime_stats {
        let d = PyDict::new(py);
        d.set_item("operator", &op.operator)?;
        d.set_item("actual_rows", op.actual_rows)?;
        d.set_item("time_ms", op.time_ms)?;
        d.set_item("memory_bytes", op.memory_bytes)?;
        if let Some(hits) = op.index_hits {
            d.set_item("index_hits", hits)?;
        }
        if let Some(misses) = op.index_misses {
            d.set_item("index_misses", misses)?;
        }
        ops.append(d)?;
    }

    Ok(crate::types::PyProfileOutput {
        total_time_ms: profile.total_time_ms,
        peak_memory_bytes: profile.peak_memory_bytes,
        plan_text: profile.explain.plan_text,
        operators: ops.into(),
    })
}

/// Convert LocyExplainOutput to a typed PyLocyExplainOutput.
pub fn locy_explain_to_py_class(
    output: uni_db::api::locy_result::LocyExplainOutput,
) -> crate::types::PyLocyExplainOutput {
    crate::types::PyLocyExplainOutput {
        plan_text: output.plan_text,
        strata_count: output.strata_count,
        rule_names: output.rule_names,
        has_recursive_strata: output.has_recursive_strata,
        warnings: output.warnings,
        command_count: output.command_count,
    }
}

/// Wrap a Rust [`LocyProfileOutput`] in its Python class.
///
/// [`LocyProfileOutput`]: uni_db::api::locy_result::LocyProfileOutput
pub fn locy_profile_to_py_class(
    output: uni_db::api::locy_result::LocyProfileOutput,
) -> crate::types::PyLocyProfile {
    crate::types::PyLocyProfile { inner: output }
}

/// Extract a CloudStorageConfig from a Python dict.
///
/// The dict must have a `"provider"` key: `"s3"`, `"gcs"`, or `"azure"`.
pub fn extract_cloud_config(
    py: Python,
    config: &HashMap<String, Py<PyAny>>,
) -> PyResult<uni_common::CloudStorageConfig> {
    let provider = config
        .get("provider")
        .ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "cloud_config must contain a 'provider' key",
            )
        })?
        .extract::<String>(py)?;

    match provider.to_lowercase().as_str() {
        "s3" => {
            let bucket = config
                .get("bucket")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("S3 config requires 'bucket'")
                })?
                .extract::<String>(py)?;
            let region = config
                .get("region")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let endpoint = config
                .get("endpoint")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let access_key_id = config
                .get("access_key_id")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let secret_access_key = config
                .get("secret_access_key")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let session_token = config
                .get("session_token")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let virtual_hosted_style = config
                .get("virtual_hosted_style")
                .map(|v| v.extract::<bool>(py))
                .transpose()?
                .unwrap_or(false);
            Ok(uni_common::CloudStorageConfig::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                session_token,
                virtual_hosted_style,
            })
        }
        "gcs" => {
            let bucket = config
                .get("bucket")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>("GCS config requires 'bucket'")
                })?
                .extract::<String>(py)?;
            let service_account_path = config
                .get("service_account_path")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let service_account_key = config
                .get("service_account_key")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            Ok(uni_common::CloudStorageConfig::Gcs {
                bucket,
                service_account_path,
                service_account_key,
            })
        }
        "azure" => {
            let container = config
                .get("container")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Azure config requires 'container'",
                    )
                })?
                .extract::<String>(py)?;
            let account = config
                .get("account")
                .ok_or_else(|| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Azure config requires 'account'",
                    )
                })?
                .extract::<String>(py)?;
            let access_key = config
                .get("access_key")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            let sas_token = config
                .get("sas_token")
                .map(|v| v.extract::<String>(py))
                .transpose()?;
            Ok(uni_common::CloudStorageConfig::Azure {
                container,
                account,
                access_key,
                sas_token,
            })
        }
        other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Unknown cloud provider '{}'. Expected 's3', 'gcs', or 'azure'.",
            other
        ))),
    }
}

/// Extract a UniConfig from a Python dict.
///
/// Supports: `query_timeout` (float, seconds), `max_query_memory` (int, bytes),
/// `parallelism` (int), `cache_size` (int, bytes), `max_transaction_memory` (int, bytes),
/// `batch_size` (int), `wal_enabled` (bool).
pub fn extract_uni_config(
    py: Python,
    config: &HashMap<String, Py<PyAny>>,
) -> PyResult<uni_common::UniConfig> {
    let mut uni_config = uni_common::UniConfig::default();
    if let Some(v) = config.get("query_timeout") {
        uni_config.query_timeout = std::time::Duration::from_secs_f64(v.extract::<f64>(py)?);
    }
    if let Some(v) = config.get("max_query_memory") {
        uni_config.max_query_memory = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("parallelism") {
        uni_config.parallelism = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("cache_size") {
        uni_config.cache_size = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("max_transaction_memory") {
        uni_config.max_transaction_memory = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("batch_size") {
        uni_config.batch_size = v.extract::<usize>(py)?;
    }
    if let Some(v) = config.get("wal_enabled") {
        uni_config.wal_enabled = v.extract::<bool>(py)?;
    }
    if let Some(v) = config.get("strict_schema") {
        uni_config.strict_schema = v.extract::<bool>(py)?;
    }
    // Phase 4b — fork lifecycle config
    if let Some(v) = config.get("max_forks") {
        // Allow None to mean unbounded; otherwise extract as usize.
        if v.bind(py).is_none() {
            uni_config.max_forks = None;
        } else {
            uni_config.max_forks = Some(v.extract::<usize>(py)?);
        }
    }
    if let Some(v) = config.get("fork_default_ttl") {
        if v.bind(py).is_none() {
            uni_config.fork_default_ttl = None;
        } else {
            uni_config.fork_default_ttl = Some(py_timedelta_to_duration(v.bind(py))?);
        }
    }
    if let Some(v) = config.get("fork_sweeper_interval") {
        uni_config.fork_sweeper_interval = py_timedelta_to_duration(v.bind(py))?;
    }
    if let Some(v) = config.get("disable_fork_sweeper") {
        uni_config.disable_fork_sweeper = v.extract::<bool>(py)?;
    }
    Ok(uni_config)
}

/// Convert a SnapshotManifest to a Python SnapshotInfo object.
pub fn snapshot_manifest_to_py(
    _py: Python,
    manifest: uni_common::core::snapshot::SnapshotManifest,
) -> PyResult<crate::types::SnapshotInfo> {
    Ok(crate::types::SnapshotInfo {
        snapshot_id: manifest.snapshot_id,
        name: manifest.name,
        created_at: manifest.created_at.to_rfc3339(),
        version_hwm: manifest.version_high_water_mark,
    })
}

/// Convert an IndexRebuildTask to a Python IndexRebuildTaskInfo object.
pub fn index_rebuild_task_to_py(
    _py: Python,
    task: uni_store::storage::IndexRebuildTask,
) -> PyResult<crate::types::IndexRebuildTaskInfo> {
    let status = format!("{:?}", task.status).to_lowercase();
    Ok(crate::types::IndexRebuildTaskInfo {
        id: task.id,
        label: task.label,
        status,
        created_at: task.created_at.to_rfc3339(),
        started_at: task.started_at.map(|t| t.to_rfc3339()),
        completed_at: task.completed_at.map(|t| t.to_rfc3339()),
        error: task.error,
        retry_count: task.retry_count,
    })
}

/// Convert an IndexDefinition to a Python IndexDefinitionInfo object.
pub fn index_definition_to_py(
    _py: Python,
    idx: uni_common::core::schema::IndexDefinition,
) -> PyResult<crate::types::IndexDefinitionInfo> {
    match idx {
        uni_common::core::schema::IndexDefinition::Scalar(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: format!("{:?}", cfg.index_type).to_lowercase(),
                label: cfg.label,
                properties: cfg.properties,
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::Vector(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "vector".to_string(),
                label: cfg.label,
                properties: vec![cfg.property],
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::FullText(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "fulltext".to_string(),
                label: cfg.label,
                properties: cfg.properties,
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::Inverted(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "inverted".to_string(),
                label: cfg.label,
                properties: vec![cfg.property],
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::JsonFullText(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "json_fulltext".to_string(),
                label: cfg.label,
                properties: vec![cfg.column],
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        uni_common::core::schema::IndexDefinition::Sparse(cfg) => {
            Ok(crate::types::IndexDefinitionInfo {
                name: cfg.name,
                index_type: "sparse".to_string(),
                label: cfg.label,
                properties: vec![cfg.property],
                state: format!("{:?}", cfg.metadata.status).to_lowercase(),
            })
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Unknown index definition type",
        )),
    }
}

/// Extract a list of (role, content) pairs from Python objects.
///
/// Each element may be a `Message` instance or a dict with `"role"` and `"content"` keys.
pub fn extract_messages(py: Python, messages: Vec<Py<PyAny>>) -> PyResult<Vec<(String, String)>> {
    messages
        .into_iter()
        .enumerate()
        .map(|(i, obj)| {
            let bound = obj.bind(py);
            // Try as PyMessage instance first
            if let Ok(msg) = bound.extract::<crate::types::PyMessage>() {
                return Ok((msg.role, msg.content));
            }
            // Try as dict
            if let Ok(dict) = bound.cast::<pyo3::types::PyDict>() {
                let role: String = dict
                    .get_item("role")?
                    .ok_or_else(|| {
                        pyo3::exceptions::PyTypeError::new_err(format!(
                            "messages[{}]: dict missing 'role' key",
                            i
                        ))
                    })?
                    .extract()?;
                let content: String = dict
                    .get_item("content")?
                    .ok_or_else(|| {
                        pyo3::exceptions::PyTypeError::new_err(format!(
                            "messages[{}]: dict missing 'content' key",
                            i
                        ))
                    })?
                    .extract()?;
                return Ok((role, content));
            }
            Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "messages[{}]: expected Message or dict, got {}",
                i,
                bound.get_type().name()?
            )))
        })
        .collect()
}

/// Convert a GenerationResult to Python types.
pub fn generation_result_to_py(
    py: Python,
    result: ::uni_db::api::xervo::GenerationResult,
) -> PyResult<crate::types::PyGenerationResult> {
    let usage = result
        .usage
        .map(|u| {
            let tu = crate::types::PyTokenUsage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
                total_tokens: u.total_tokens,
            };
            Py::new(py, tu)
        })
        .transpose()?;
    Ok(crate::types::PyGenerationResult {
        text: result.text,
        usage,
    })
}

/// Convert a `DatabaseMetrics` to a Python `PyDatabaseMetrics`.
pub fn database_metrics_to_py(
    _py: Python,
    m: ::uni_db::DatabaseMetrics,
) -> PyResult<crate::types::PyDatabaseMetrics> {
    Ok(crate::types::PyDatabaseMetrics {
        l0_mutation_count: m.l0_mutation_count,
        l0_estimated_size_bytes: m.l0_estimated_size_bytes,
        schema_version: m.schema_version,
        uptime_secs: m.uptime.as_secs_f64(),
        active_sessions: m.active_sessions,
        l1_run_count: m.l1_run_count,
        write_throttle_pressure: m.write_throttle_pressure.value(),
        compaction_in_progress: m.compaction_status.compaction_in_progress,
        wal_size_bytes: m.wal_size_bytes,
        wal_lsn: m.wal_lsn,
        total_queries: m.total_queries,
        total_commits: m.total_commits,
    })
}

/// Convert a `&UniConfig` to a Python dict.
pub fn uni_config_to_py(py: Python, config: &uni_common::UniConfig) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("cache_size", config.cache_size)?;
    dict.set_item("parallelism", config.parallelism)?;
    dict.set_item("query_timeout", config.query_timeout.as_secs_f64())?;
    dict.set_item("max_query_memory", config.max_query_memory)?;
    dict.set_item("max_transaction_memory", config.max_transaction_memory)?;
    dict.set_item("batch_size", config.batch_size)?;
    dict.set_item("auto_flush_threshold", config.auto_flush_threshold)?;
    dict.set_item(
        "auto_flush_interval",
        config.auto_flush_interval.map(|d| d.as_secs_f64()),
    )?;
    dict.set_item("wal_enabled", config.wal_enabled)?;
    dict.set_item(
        "max_recursive_cte_iterations",
        config.max_recursive_cte_iterations,
    )?;
    // Phase 4b — fork lifecycle config
    dict.set_item("max_forks", config.max_forks)?;
    dict.set_item(
        "fork_default_ttl",
        config.fork_default_ttl.map(|d| d.as_secs_f64()),
    )?;
    dict.set_item(
        "fork_sweeper_interval",
        config.fork_sweeper_interval.as_secs_f64(),
    )?;
    dict.set_item("disable_fork_sweeper", config.disable_fork_sweeper)?;
    Ok(dict.into())
}

// ============================================================================
// Phase 4b — fork helpers
// ============================================================================

/// Convert a `chrono::DateTime<Utc>` to a Python `datetime.datetime`
/// in the UTC timezone. Mirrors the existing `TemporalValue::DateTime`
/// converter above but takes a `chrono` value directly so non-Value
/// call sites (like `ForkInfo.created_at`) stay readable.
pub fn utc_datetime_to_py<'py>(
    py: Python<'py>,
    dt: chrono::DateTime<chrono::Utc>,
) -> PyResult<Bound<'py, PyAny>> {
    let datetime_module = py.import("datetime")?;
    let tz_class = datetime_module.getattr("timezone")?;
    let utc = tz_class.getattr("utc")?;
    let dt_class = datetime_module.getattr("datetime")?;
    let secs = dt.timestamp();
    let micros = dt.timestamp_subsec_micros() as f64 / 1_000_000.0;
    let result = dt_class.call_method1("fromtimestamp", (secs as f64 + micros, utc))?;
    Ok(result)
}

/// Convert a Python `datetime.timedelta` to a `std::time::Duration`.
/// Used by `ForkBuilder.ttl(...)` and the fork-related `UniConfig` fields.
/// Negative timedeltas error.
pub fn py_timedelta_to_duration(obj: &Bound<'_, PyAny>) -> PyResult<std::time::Duration> {
    let total_secs: f64 = obj.call_method0("total_seconds")?.extract().map_err(|e| {
        crate::exceptions::UniInvalidArgumentError::new_err(format!(
            "expected datetime.timedelta or compatible duration: {e}"
        ))
    })?;
    if total_secs < 0.0 {
        return Err(crate::exceptions::UniInvalidArgumentError::new_err(
            "timedelta must be non-negative",
        ));
    }
    Ok(std::time::Duration::from_secs_f64(total_secs))
}

#[cfg(test)]
mod value_to_py_coverage {
    use super::*;

    /// A Rust-side exhaustiveness test is not possible here: the crate builds
    /// with pyo3's `extension-module`, so libpython is not linked and
    /// `Python::initialize()` is unavailable in a unit test. Coverage of the
    /// conversion itself lives in `tests/test_value_conversion.py`, which runs
    /// under a real interpreter.
    ///
    /// What is testable without Python is that the new error path stays bounded
    /// — a vector-shaped variant would otherwise paste kilobytes into the
    /// exception message.
    #[test]
    fn variant_debug_is_truncated() {
        let big = Value::Vector(vec![1.234_567; 4096]);
        let rendered = truncated_variant_debug(&big);
        assert!(
            rendered.len() < 200,
            "error text must stay bounded, got {} bytes",
            rendered.len()
        );
        assert!(rendered.contains("bytes total"));
    }

    #[test]
    fn short_variant_debug_is_left_alone() {
        let small = Value::Int(7);
        assert_eq!(truncated_variant_debug(&small), "Int(7)");
    }
}
