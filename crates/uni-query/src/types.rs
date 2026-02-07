// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use base64::Engine;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use uni_common::core::id::{Eid, Vid};
use uni_common::{Result, UniError}; // Needed for encode

/// Dynamic value type for query parameters and results
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// ... (omitting rest for brevity in old_string match, will include full in actual call)
#[serde(untagged)]
#[non_exhaustive]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(HashMap<String, Value>),

    // Graph-specific
    Node(Node),
    Edge(Edge),
    Path(Path),

    // Vector
    Vector(Vec<f32>),
}

// ... (imports)

/// Trait for converting from Value
pub trait FromValue: Sized {
    fn from_value(value: &Value) -> Result<Self>;
}

// Default implementation of FromValue for types that implement TryFrom<&Value>
impl<T> FromValue for T
where
    T: for<'a> TryFrom<&'a Value, Error = UniError>,
{
    fn from_value(value: &Value) -> Result<Self> {
        Self::try_from(value)
    }
}

/// Macro to implement TryFrom<Value> that delegates to TryFrom<&Value>.
///
/// This eliminates the boilerplate of writing:
/// ```ignore
/// impl TryFrom<Value> for T {
///     type Error = UniError;
///     fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
///         Self::try_from(&value)
///     }
/// }
/// ```
macro_rules! impl_try_from_value_owned {
    ($($t:ty),+ $(,)?) => {
        $(
            impl TryFrom<Value> for $t {
                type Error = UniError;
                fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
                    Self::try_from(&value)
                }
            }
        )+
    };
}

// Implement TryFrom<Value> for standard types using the macro
impl_try_from_value_owned!(
    String,
    i64,
    i32,
    f64,
    bool,
    Vid,
    Eid,
    Vec<f32>,
    Path,
    Node,
    Edge
);

// Implement TryFrom<&Value> for standard types

impl TryFrom<&Value> for String {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(s.clone()),
            Value::Int(i) => Ok(i.to_string()),
            Value::Float(f) => Ok(f.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            _ => Err(UniError::Type {
                expected: "String".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(*i),
            Value::Float(f) => Ok(*f as i64),
            _ => Err(UniError::Type {
                expected: "Int".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for i32 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Int(i) => i32::try_from(*i).map_err(|_| UniError::Type {
                expected: "i32".to_string(),
                actual: format!("Integer {} out of range", i),
            }),
            Value::Float(f) => {
                if *f < i32::MIN as f64 || *f > i32::MAX as f64 {
                    return Err(UniError::Type {
                        expected: "i32".to_string(),
                        actual: format!("Float {} out of range", f),
                    });
                }
                if f.fract() != 0.0 {
                    return Err(UniError::Type {
                        expected: "i32".to_string(),
                        actual: format!("Float {} has fractional part", f),
                    });
                }
                Ok(*f as i32)
            }
            _ => Err(UniError::Type {
                expected: "Int".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for f64 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err(UniError::Type {
                expected: "Float".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for bool {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Bool(b) => Ok(*b),
            _ => Err(UniError::Type {
                expected: "Bool".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for Vid {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Node(n) => Ok(n.vid),
            Value::String(s) => {
                if let Ok(id) = s.parse::<u64>() {
                    return Ok(Vid::new(id));
                }
                Err(UniError::Type {
                    expected: "Vid".into(),
                    actual: s.clone(),
                })
            }
            Value::Int(i) => Ok(Vid::new(*i as u64)),
            _ => Err(UniError::Type {
                expected: "Vid".into(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for Eid {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Edge(e) => Ok(e.eid),
            Value::String(s) => {
                if let Ok(id) = s.parse::<u64>() {
                    return Ok(Eid::new(id));
                }
                Err(UniError::Type {
                    expected: "Eid".into(),
                    actual: s.clone(),
                })
            }
            Value::Int(i) => Ok(Eid::new(*i as u64)),
            _ => Err(UniError::Type {
                expected: "Eid".into(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for Vec<f32> {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Vector(v) => Ok(v.clone()),
            Value::List(l) => {
                let mut vec = Vec::with_capacity(l.len());
                for item in l {
                    match item {
                        Value::Float(f) => vec.push(*f as f32),
                        Value::Int(i) => vec.push(*i as f32),
                        _ => {
                            return Err(UniError::Type {
                                expected: "Float".to_string(),
                                actual: format!("{:?}", item),
                            });
                        }
                    }
                }
                Ok(vec)
            }
            _ => Err(UniError::Type {
                expected: "Vector".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl<T> TryFrom<&Value> for Option<T>
where
    T: for<'a> TryFrom<&'a Value, Error = UniError>,
{
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(None),
            _ => T::try_from(value).map(Some),
        }
    }
}

impl<T> TryFrom<Value> for Option<T>
where
    T: TryFrom<Value, Error = UniError>,
{
    type Error = UniError;
    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Null => Ok(None),
            _ => T::try_from(value).map(Some),
        }
    }
}

impl<T> TryFrom<&Value> for Vec<T>
where
    T: for<'a> TryFrom<&'a Value, Error = UniError>,
{
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::List(l) => {
                let mut vec = Vec::with_capacity(l.len());
                for item in l {
                    vec.push(T::try_from(item)?);
                }
                Ok(vec)
            }
            _ => Err(UniError::Type {
                expected: "List".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl<T> TryFrom<Value> for Vec<T>
where
    T: TryFrom<Value, Error = UniError>,
{
    type Error = UniError;
    fn try_from(value: Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::List(l) => {
                let mut vec = Vec::with_capacity(l.len());
                for item in l {
                    vec.push(T::try_from(item)?);
                }
                Ok(vec)
            }
            _ => Err(UniError::Type {
                expected: "List".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl TryFrom<&Value> for Path {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Path(p) => Ok(p.clone()),
            Value::Map(m) => {
                if let (Some(Value::List(nodes_list)), Some(Value::List(rels_list))) =
                    (m.get("nodes"), m.get("relationships"))
                {
                    let mut nodes = Vec::new();
                    for n in nodes_list {
                        if let Value::Node(node) = n {
                            nodes.push(node.clone());
                        } else if let Value::Map(node_map) = n {
                            // Reconstruct Node from Map manually matching serialization
                            let vid_val = node_map.get("_id").or_else(|| node_map.get("vid"));
                            let label_val =
                                node_map.get("_label").or_else(|| node_map.get("label"));
                            let props_val = node_map.get("properties");

                            if let (Some(v), Some(l), Some(p)) = (vid_val, label_val, props_val) {
                                let vid: Vid = Vid::try_from(v)?;
                                let label: String = String::try_from(l)?;
                                let properties: HashMap<String, Value> = if let Value::Map(m) = p {
                                    m.clone()
                                } else {
                                    HashMap::new()
                                };
                                nodes.push(Node {
                                    vid,
                                    label,
                                    properties,
                                });
                            } else {
                                return Err(UniError::Type {
                                    expected: "Node Map".into(),
                                    actual: format!("{:?}", n),
                                });
                            }
                        }
                    }

                    let mut edges = Vec::new();
                    for e in rels_list {
                        if let Value::Edge(edge) = e {
                            edges.push(edge.clone());
                        } else if let Value::Map(edge_map) = e {
                            let eid_val = edge_map.get("_id").or_else(|| edge_map.get("eid"));
                            let type_val =
                                edge_map.get("_type").or_else(|| edge_map.get("edge_type"));
                            let src_val = edge_map.get("_src").or_else(|| edge_map.get("src"));
                            let dst_val = edge_map.get("_dst").or_else(|| edge_map.get("dst"));
                            let props_val = edge_map.get("properties");

                            if let (Some(id), Some(t), Some(s), Some(d), Some(p)) =
                                (eid_val, type_val, src_val, dst_val, props_val)
                            {
                                let eid: Eid = Eid::try_from(id)?; // Eid doesn't implement FromValue yet? check
                                let edge_type: String = String::try_from(t)?;
                                let src: Vid = Vid::try_from(s)?;
                                let dst: Vid = Vid::try_from(d)?;
                                let properties: HashMap<String, Value> = if let Value::Map(m) = p {
                                    m.clone()
                                } else {
                                    HashMap::new()
                                };
                                edges.push(Edge {
                                    eid,
                                    edge_type,
                                    src,
                                    dst,
                                    properties,
                                });
                            } else {
                                return Err(UniError::Type {
                                    expected: "Edge Map".into(),
                                    actual: format!("{:?}", e),
                                });
                            }
                        }
                    }

                    Ok(Path { nodes, edges })
                } else {
                    Err(UniError::Type {
                        expected: "Path (Map with nodes/relationships)".to_string(),
                        actual: format!("{:?}", value),
                    })
                }
            }
            _ => Err(UniError::Type {
                expected: "Path".to_string(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

/// Node returned from query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub vid: Vid,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

impl Node {
    pub fn get<T: FromValue>(&self, property: &str) -> Result<T> {
        let val = self
            .properties
            .get(property)
            .ok_or_else(|| UniError::Query {
                message: format!("Property '{}' not found on node {}", property, self.vid),
                query: None,
            })?;
        T::from_value(val)
    }

    pub fn try_get<T: FromValue>(&self, property: &str) -> Option<T> {
        self.properties
            .get(property)
            .and_then(|v| T::from_value(v).ok())
    }
}

impl TryFrom<&Value> for Node {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Node(n) => Ok(n.clone()),
            Value::Map(m) => {
                let vid_val = m.get("_id").or_else(|| m.get("vid"));
                let label_val = m.get("_label").or_else(|| m.get("label"));
                let props_val = m.get("properties");

                if let (Some(v), Some(l), Some(p)) = (vid_val, label_val, props_val) {
                    let vid: Vid = Vid::try_from(v)?;
                    let label: String = String::try_from(l)?;
                    let properties: HashMap<String, Value> = if let Value::Map(m) = p {
                        m.clone()
                    } else {
                        HashMap::new()
                    };
                    Ok(Node {
                        vid,
                        label,
                        properties,
                    })
                } else {
                    Err(UniError::Type {
                        expected: "Node Map".into(),
                        actual: format!("{:?}", value),
                    })
                }
            }
            _ => Err(UniError::Type {
                expected: "Node".into(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

/// Edge returned from query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub eid: Eid,
    pub edge_type: String,
    pub src: Vid,
    pub dst: Vid,
    pub properties: HashMap<String, Value>,
}

impl TryFrom<&Value> for Edge {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Edge(e) => Ok(e.clone()),
            Value::Map(m) => {
                let eid_val = m.get("_id").or_else(|| m.get("eid"));
                let type_val = m.get("_type").or_else(|| m.get("edge_type"));
                let src_val = m.get("_src").or_else(|| m.get("src"));
                let dst_val = m.get("_dst").or_else(|| m.get("dst"));
                let props_val = m.get("properties");

                if let (Some(id), Some(t), Some(s), Some(d), Some(p)) =
                    (eid_val, type_val, src_val, dst_val, props_val)
                {
                    let eid: Eid = Eid::try_from(id)?;
                    let edge_type: String = String::try_from(t)?;
                    let src: Vid = Vid::try_from(s)?;
                    let dst: Vid = Vid::try_from(d)?;
                    let properties: HashMap<String, Value> = if let Value::Map(m) = p {
                        m.clone()
                    } else {
                        HashMap::new()
                    };
                    Ok(Edge {
                        eid,
                        edge_type,
                        src,
                        dst,
                        properties,
                    })
                } else {
                    Err(UniError::Type {
                        expected: "Edge Map".into(),
                        actual: format!("{:?}", value),
                    })
                }
            }
            _ => Err(UniError::Type {
                expected: "Edge".into(),
                actual: format!("{:?}", value),
            }),
        }
    }
}

impl Edge {
    pub fn get<T: FromValue>(&self, property: &str) -> Result<T> {
        let val = self
            .properties
            .get(property)
            .ok_or_else(|| UniError::Query {
                message: format!("Property '{}' not found on edge {}", property, self.eid),
                query: None,
            })?;
        T::from_value(val)
    }
}

/// Path returned from query
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Path {
    pub nodes: Vec<Node>,
    #[serde(rename = "relationships")]
    pub edges: Vec<Edge>,
}

impl Path {
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Returns the starting node of the path, or None if empty.
    pub fn start(&self) -> Option<&Node> {
        self.nodes.first()
    }

    /// Returns the ending node of the path, or None if empty.
    pub fn end(&self) -> Option<&Node> {
        self.nodes.last()
    }
}

/// Single result row
#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Arc<Vec<String>>,
    pub values: Vec<Value>,
}

impl Row {
    /// Get value by column name
    pub fn get<T: FromValue>(&self, column: &str) -> Result<T> {
        let idx = self
            .columns
            .iter()
            .position(|c| c == column)
            .ok_or_else(|| UniError::Query {
                message: format!("Column '{}' not found", column),
                query: None,
            })?;
        self.get_idx(idx)
    }

    /// Get value by column index
    pub fn get_idx<T: FromValue>(&self, index: usize) -> Result<T> {
        if index >= self.values.len() {
            return Err(UniError::Query {
                message: format!("Column index {} out of bounds", index),
                query: None,
            });
        }
        T::from_value(&self.values[index])
    }

    /// Try to get value (returns None if null or missing)
    pub fn try_get<T: FromValue>(&self, column: &str) -> Option<T> {
        self.get(column).ok()
    }

    /// Get raw Value by column name
    pub fn value(&self, column: &str) -> Option<&Value> {
        let idx = self.columns.iter().position(|c| c == column)?;
        self.values.get(idx)
    }

    /// Get all values as a map
    pub fn as_map(&self) -> HashMap<&str, &Value> {
        let mut map = HashMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            if let Some(val) = self.values.get(i) {
                map.insert(col.as_str(), val);
            }
        }
        map
    }

    /// Convert to JSON object
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self.as_map()).unwrap_or(serde_json::Value::Null)
    }
}

impl std::ops::Index<usize> for Row {
    type Output = Value;
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index]
    }
}

/// Warnings that may be emitted during query execution.
///
/// Warnings indicate potential issues or suboptimal query execution
/// but do not prevent the query from completing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryWarning {
    /// An index is unavailable (e.g., still being rebuilt).
    IndexUnavailable {
        /// The label that the index is for.
        label: String,
        /// The name of the unavailable index.
        index_name: String,
        /// Reason the index is unavailable.
        reason: String,
    },
    /// A property filter could not use an index.
    NoIndexForFilter {
        /// The label being filtered.
        label: String,
        /// The property being filtered.
        property: String,
    },
    /// Generic warning message.
    Other(String),
}

impl std::fmt::Display for QueryWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryWarning::IndexUnavailable {
                label,
                index_name,
                reason,
            } => {
                write!(
                    f,
                    "Index '{}' on label '{}' is unavailable: {}",
                    index_name, label, reason
                )
            }
            QueryWarning::NoIndexForFilter { label, property } => {
                write!(
                    f,
                    "No index available for filter on {}.{}, using full scan",
                    label, property
                )
            }
            QueryWarning::Other(msg) => write!(f, "{}", msg),
        }
    }
}

/// Collection of query result rows.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Arc<Vec<String>>,
    pub rows: Vec<Row>,
    /// Warnings emitted during query execution.
    pub warnings: Vec<QueryWarning>,
}

impl QueryResult {
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get all rows
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Consume into rows
    pub fn into_rows(self) -> Vec<Row> {
        self.rows
    }

    /// Iterate over rows
    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }

    /// Get warnings emitted during query execution.
    pub fn warnings(&self) -> &[QueryWarning] {
        &self.warnings
    }

    /// Check if the query produced any warnings.
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

impl IntoIterator for QueryResult {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Row>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

// Into<Value> implementations
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}
impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v as i64)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::List(arr.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(obj) => {
                Value::Map(obj.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Int(i) => serde_json::Value::Number(serde_json::Number::from(i)),
            Value::Float(f) => {
                if let Some(n) = serde_json::Number::from_f64(f) {
                    serde_json::Value::Number(n)
                } else {
                    serde_json::Value::Null // NaN/Inf
                }
            }
            Value::String(s) => serde_json::Value::String(s),
            Value::Bytes(b) => {
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
            }
            Value::List(l) => serde_json::Value::Array(l.into_iter().map(|i| i.into()).collect()),
            Value::Map(m) => {
                let mut map = serde_json::Map::new();
                for (k, v) in m {
                    map.insert(k, v.into());
                }
                serde_json::Value::Object(map)
            }
            Value::Node(n) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "_id".to_string(),
                    serde_json::Value::String(n.vid.to_string()),
                );
                map.insert("_label".to_string(), serde_json::Value::String(n.label));
                let props: serde_json::Value = Value::Map(n.properties).into();
                map.insert("properties".to_string(), props);
                serde_json::Value::Object(map)
            }
            Value::Edge(e) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "_id".to_string(),
                    serde_json::Value::String(e.eid.to_string()),
                );
                map.insert("_type".to_string(), serde_json::Value::String(e.edge_type));
                map.insert(
                    "_src".to_string(),
                    serde_json::Value::String(e.src.to_string()),
                );
                map.insert(
                    "_dst".to_string(),
                    serde_json::Value::String(e.dst.to_string()),
                );
                let props: serde_json::Value = Value::Map(e.properties).into();
                map.insert("properties".to_string(), props);
                serde_json::Value::Object(map)
            }
            Value::Path(p) => {
                let mut map = serde_json::Map::new();
                map.insert(
                    "nodes".to_string(),
                    Value::List(p.nodes.iter().map(|n| Value::Node(n.clone())).collect()).into(),
                );
                map.insert(
                    "relationships".to_string(),
                    Value::List(p.edges.iter().map(|e| Value::Edge(e.clone())).collect()).into(),
                );
                serde_json::Value::Object(map)
            }
            Value::Vector(v) => serde_json::Value::Array(
                v.into_iter()
                    .map(|f| {
                        serde_json::Number::from_f64(f as f64)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect(),
            ),
        }
    }
}

#[derive(Debug)]
pub struct ExecuteResult {
    pub affected_rows: usize,
}

/// Cursor-based result streaming
pub struct QueryCursor {
    pub columns: Arc<Vec<String>>,
    pub stream: Pin<Box<dyn Stream<Item = Result<Vec<Row>>> + Send>>,
}

impl QueryCursor {
    /// Get column names
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Fetch next batch of rows
    pub async fn next_batch(&mut self) -> Option<Result<Vec<Row>>> {
        use futures::StreamExt;
        self.stream.next().await
    }

    /// Consume all remaining rows
    pub async fn collect_remaining(mut self) -> Result<Vec<Row>> {
        use futures::StreamExt;
        let mut rows = Vec::new();
        while let Some(batch_res) = self.stream.next().await {
            rows.extend(batch_res?);
        }
        Ok(rows)
    }
}
