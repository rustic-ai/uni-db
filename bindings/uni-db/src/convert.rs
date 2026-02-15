// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Conversion utilities between Python objects and Rust/Uni types.

use ::uni_db::Value;
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::collections::HashMap;

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
        Value::Node(n) => {
            let dict = PyDict::new(py);
            dict.set_item("_id", n.vid.to_string())?;
            dict.set_item("_labels", &n.labels)?;
            for (k, v) in &n.properties {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Edge(e) => {
            let dict = PyDict::new(py);
            dict.set_item("_id", e.eid.as_u64())?;
            dict.set_item("_type", &e.edge_type)?;
            dict.set_item("_src", e.src.to_string())?;
            dict.set_item("_dst", e.dst.to_string())?;
            for (k, v) in &e.properties {
                dict.set_item(k, value_to_py(py, v)?)?;
            }
            Ok(dict.into())
        }
        Value::Path(p) => {
            let dict = PyDict::new(py);
            let nodes = PyList::empty(py);
            for n in p.nodes() {
                nodes.append(value_to_py(py, &Value::Node(n.clone()))?)?;
            }
            dict.set_item("nodes", nodes)?;

            let edges = PyList::empty(py);
            for e in p.edges() {
                edges.append(value_to_py(py, &Value::Edge(e.clone()))?)?;
            }
            dict.set_item("edges", edges)?;
            Ok(dict.into())
        }
        _ => Ok(py.None()),
    }
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

    Ok(serde_json::Value::Null)
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
        let timestamp_secs: f64 = bound.call_method0("timestamp")?.extract()?;
        let micros = (timestamp_secs * 1_000_000.0) as i64;
        return Ok(Value::Int(micros));
    }

    if bound.is_instance(&date_class)? {
        let time_min = time_class.getattr("min")?;
        let dt = datetime_class.call_method1("combine", (bound.clone(), time_min))?;
        let timestamp_secs: f64 = dt.call_method0("timestamp")?.extract()?;
        let days = (timestamp_secs / 86400.0) as i64;
        return Ok(Value::Int(days));
    }

    if bound.is_instance(&time_class)? {
        let hour: i64 = bound.getattr("hour")?.extract()?;
        let minute: i64 = bound.getattr("minute")?.extract()?;
        let second: i64 = bound.getattr("second")?.extract()?;
        let microsecond: i64 = bound.getattr("microsecond")?.extract()?;
        let micros = hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + microsecond;
        return Ok(Value::Int(micros));
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

    Ok(Value::Null)
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

/// Convert query result rows to Python dicts.
pub fn rows_to_py(py: Python, rows: Vec<::uni_db::Row>) -> PyResult<Vec<Py<PyAny>>> {
    let mut result = Vec::new();
    for row in rows {
        let dict = PyDict::new(py);
        for (col_name, val) in row.as_map() {
            dict.set_item(col_name, value_to_py(py, val)?)?;
        }
        result.push(dict.into());
    }
    Ok(result)
}
