// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Typed value representation for graph properties and query results.
//!
//! [`Value`] is the canonical internal representation for all property values,
//! query parameters, and expression results. Unlike `serde_json::Value`, it
//! distinguishes integers from floats (`Int(i64)` vs `Float(f64)`) and includes
//! graph-specific variants (`Node`, `Edge`, `Path`, `Vector`).
//!
//! Conversion to/from `serde_json::Value` is provided at the serialization
//! boundary via `From` implementations.

use crate::api::error::UniError;
use crate::core::id::{Eid, Vid};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Dynamic value type for properties, parameters, and results.
///
/// Preserves the distinction between integers and floats, and includes
/// graph-specific variants for nodes, edges, paths, and vectors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum Value {
    /// JSON/Cypher null.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit floating-point number.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw byte buffer.
    Bytes(Vec<u8>),
    /// Ordered list of values.
    List(Vec<Value>),
    /// String-keyed map of values.
    Map(HashMap<String, Value>),

    // Graph-specific
    /// Graph node with VID, label, and properties.
    Node(Node),
    /// Graph edge with EID, type, endpoints, and properties.
    Edge(Edge),
    /// Graph path (alternating nodes and edges).
    Path(Path),

    // Vector
    /// Dense float vector for similarity search.
    Vector(Vec<f32>),
}

// ---------------------------------------------------------------------------
// Accessor methods (mirrors serde_json::Value API for migration ease)
// ---------------------------------------------------------------------------

impl Value {
    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns the boolean if this is `Bool`, otherwise `None`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns the integer if this is `Int`, otherwise `None`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Returns the integer as `u64` if this is a non-negative `Int`, otherwise `None`.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Int(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        }
    }

    /// Returns a float, coercing `Int` to `f64` if needed.
    ///
    /// Returns `None` for non-numeric variants.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Returns the string slice if this is `String`, otherwise `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Returns `true` if this is `Int`.
    pub fn is_i64(&self) -> bool {
        matches!(self, Value::Int(_))
    }

    /// Returns `true` if this is `Float` (not `Int`).
    pub fn is_f64(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    /// Returns `true` if this is `String`.
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Returns `true` if this is `Int` or `Float`.
    pub fn is_number(&self) -> bool {
        matches!(self, Value::Int(_) | Value::Float(_))
    }

    /// Returns the list if this is `List`, otherwise `None`.
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    /// Returns the map if this is `Map`, otherwise `None`.
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Returns `true` if this is `Bool`.
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Returns `true` if this is `List`.
    pub fn is_list(&self) -> bool {
        matches!(self, Value::List(_))
    }

    /// Returns `true` if this is `Map`.
    pub fn is_map(&self) -> bool {
        matches!(self, Value::Map(_))
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(v) => {
                if v.fract() == 0.0 && v.is_finite() {
                    write!(f, "{v:.1}")
                } else {
                    write!(f, "{v}")
                }
            }
            Value::String(s) => write!(f, "{s}"),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Value::List(l) => {
                write!(f, "[")?;
                for (i, item) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
            Value::Node(n) => write!(f, "(:{} {{vid: {}}})", n.label, n.vid),
            Value::Edge(e) => write!(f, "-[:{}]-", e.edge_type),
            Value::Path(p) => write!(f, "<path: {} nodes, {} edges>", p.nodes.len(), p.edges.len()),
            Value::Vector(v) => write!(f, "<vector: {} dims>", v.len()),
        }
    }
}

// ---------------------------------------------------------------------------
// Graph entity types
// ---------------------------------------------------------------------------

/// Graph node with identity, label, and properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Internal vertex identifier.
    pub vid: Vid,
    /// Node label.
    pub label: String,
    /// Property key-value pairs.
    pub properties: HashMap<String, Value>,
}

impl Node {
    /// Gets a typed property by name.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the property is missing,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get<T: FromValue>(&self, property: &str) -> crate::Result<T> {
        let val = self
            .properties
            .get(property)
            .ok_or_else(|| UniError::Query {
                message: format!("Property '{}' not found on node {}", property, self.vid),
                query: None,
            })?;
        T::from_value(val)
    }

    /// Tries to get a typed property, returning `None` on failure.
    pub fn try_get<T: FromValue>(&self, property: &str) -> Option<T> {
        self.properties
            .get(property)
            .and_then(|v| T::from_value(v).ok())
    }
}

/// Graph edge with identity, type, endpoints, and properties.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    /// Internal edge identifier.
    pub eid: Eid,
    /// Relationship type name.
    pub edge_type: String,
    /// Source vertex ID.
    pub src: Vid,
    /// Destination vertex ID.
    pub dst: Vid,
    /// Property key-value pairs.
    pub properties: HashMap<String, Value>,
}

impl Edge {
    /// Gets a typed property by name.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Query` if the property is missing,
    /// or `UniError::Type` if it cannot be converted.
    pub fn get<T: FromValue>(&self, property: &str) -> crate::Result<T> {
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

/// Graph path consisting of alternating nodes and edges.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Path {
    /// Ordered sequence of nodes along the path.
    pub nodes: Vec<Node>,
    /// Ordered sequence of edges connecting the nodes.
    #[serde(rename = "relationships")]
    pub edges: Vec<Edge>,
}

impl Path {
    /// Returns the nodes in this path.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Returns the edges in this path.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Returns the number of edges (path length).
    pub fn len(&self) -> usize {
        self.edges.len()
    }

    /// Returns `true` if the path has no edges.
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Returns the starting node, or `None` if the path is empty.
    pub fn start(&self) -> Option<&Node> {
        self.nodes.first()
    }

    /// Returns the ending node, or `None` if the path is empty.
    pub fn end(&self) -> Option<&Node> {
        self.nodes.last()
    }
}

// ---------------------------------------------------------------------------
// FromValue trait
// ---------------------------------------------------------------------------

/// Trait for fallible conversion from [`Value`].
pub trait FromValue: Sized {
    /// Converts a `Value` reference to `Self`.
    ///
    /// # Errors
    ///
    /// Returns `UniError::Type` if the value cannot be converted.
    fn from_value(value: &Value) -> crate::Result<Self>;
}

/// Blanket implementation: any `T: TryFrom<&Value, Error = UniError>` is `FromValue`.
impl<T> FromValue for T
where
    T: for<'a> TryFrom<&'a Value, Error = UniError>,
{
    fn from_value(value: &Value) -> crate::Result<Self> {
        Self::try_from(value)
    }
}

// ---------------------------------------------------------------------------
// TryFrom<Value> macro for owned values (delegates to &Value)
// ---------------------------------------------------------------------------

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

impl_try_from_value_owned!(String, i64, i32, f64, bool, Vid, Eid, Vec<f32>, Path, Node, Edge);

// ---------------------------------------------------------------------------
// TryFrom<&Value> implementations for standard types
// ---------------------------------------------------------------------------

/// Create a type mismatch error.
fn type_error(expected: &str, value: &Value) -> UniError {
    UniError::Type {
        expected: expected.to_string(),
        actual: format!("{:?}", value),
    }
}

impl TryFrom<&Value> for String {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(s.clone()),
            Value::Int(i) => Ok(i.to_string()),
            Value::Float(f) => Ok(f.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            _ => Err(type_error("String", value)),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Int(i) => Ok(*i),
            Value::Float(f) => Ok(*f as i64),
            _ => Err(type_error("Int", value)),
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
            _ => Err(type_error("Int", value)),
        }
    }
}

impl TryFrom<&Value> for f64 {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err(type_error("Float", value)),
        }
    }
}

impl TryFrom<&Value> for bool {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Bool(b) => Ok(*b),
            _ => Err(type_error("Bool", value)),
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
            _ => Err(type_error("Vid", value)),
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
            _ => Err(type_error("Eid", value)),
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
                        _ => return Err(type_error("Float", item)),
                    }
                }
                Ok(vec)
            }
            _ => Err(type_error("Vector", value)),
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
            _ => Err(type_error("List", value)),
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
            other => Err(type_error("List", &other)),
        }
    }
}

// ---------------------------------------------------------------------------
// TryFrom<&Value> for graph entities (deserialization from Map)
// ---------------------------------------------------------------------------

/// Gets a value from a map trying alternative keys in order.
fn get_with_fallback<'a>(map: &'a HashMap<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|k| map.get(*k))
}

/// Extracts a properties map from a value, defaulting to empty.
fn extract_properties(value: &Value) -> HashMap<String, Value> {
    match value {
        Value::Map(m) => m.clone(),
        _ => HashMap::new(),
    }
}

impl TryFrom<&Value> for Node {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Node(n) => Ok(n.clone()),
            Value::Map(m) => {
                let vid_val = get_with_fallback(m, &["_id", "vid"]);
                let label_val = get_with_fallback(m, &["_label", "label"]);
                let props_val = m.get("properties");

                let (Some(v), Some(l), Some(p)) = (vid_val, label_val, props_val) else {
                    return Err(type_error("Node Map", value));
                };

                Ok(Node {
                    vid: Vid::try_from(v)?,
                    label: String::try_from(l)?,
                    properties: extract_properties(p),
                })
            }
            _ => Err(type_error("Node", value)),
        }
    }
}

impl TryFrom<&Value> for Edge {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Edge(e) => Ok(e.clone()),
            Value::Map(m) => {
                let eid_val = get_with_fallback(m, &["_id", "eid"]);
                let type_val = get_with_fallback(m, &["_type", "edge_type"]);
                let src_val = get_with_fallback(m, &["_src", "src"]);
                let dst_val = get_with_fallback(m, &["_dst", "dst"]);
                let props_val = m.get("properties");

                let (Some(id), Some(t), Some(s), Some(d), Some(p)) =
                    (eid_val, type_val, src_val, dst_val, props_val)
                else {
                    return Err(type_error("Edge Map", value));
                };

                Ok(Edge {
                    eid: Eid::try_from(id)?,
                    edge_type: String::try_from(t)?,
                    src: Vid::try_from(s)?,
                    dst: Vid::try_from(d)?,
                    properties: extract_properties(p),
                })
            }
            _ => Err(type_error("Edge", value)),
        }
    }
}

impl TryFrom<&Value> for Path {
    type Error = UniError;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        match value {
            Value::Path(p) => Ok(p.clone()),
            Value::Map(m) => {
                let (Some(Value::List(nodes_list)), Some(Value::List(rels_list))) =
                    (m.get("nodes"), m.get("relationships"))
                else {
                    return Err(type_error("Path (Map with nodes/relationships)", value));
                };

                let nodes = nodes_list
                    .iter()
                    .map(Node::try_from)
                    .collect::<std::result::Result<Vec<_>, _>>()?;

                let edges = rels_list
                    .iter()
                    .map(Edge::try_from)
                    .collect::<std::result::Result<Vec<_>, _>>()?;

                Ok(Path { nodes, edges })
            }
            _ => Err(type_error("Path", value)),
        }
    }
}

// ---------------------------------------------------------------------------
// From<T> for Value (primitive constructors)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// serde_json::Value ↔ Value conversions (JSONB boundary)
// ---------------------------------------------------------------------------

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
            Value::Float(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null), // NaN/Inf → null
            Value::String(s) => serde_json::Value::String(s),
            Value::Bytes(b) => {
                use base64::Engine;
                serde_json::Value::String(
                    base64::engine::general_purpose::STANDARD.encode(b),
                )
            }
            Value::List(l) => {
                serde_json::Value::Array(l.into_iter().map(serde_json::Value::from).collect())
            }
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
                    Value::List(p.nodes.into_iter().map(Value::Node).collect()).into(),
                );
                map.insert(
                    "relationships".to_string(),
                    Value::List(p.edges.into_iter().map(Value::Edge).collect()).into(),
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accessor_methods() {
        assert!(Value::Null.is_null());
        assert!(!Value::Int(1).is_null());

        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Int(42).as_bool(), None);

        assert_eq!(Value::Int(42).as_i64(), Some(42));
        assert_eq!(Value::Float(3.14).as_i64(), None);

        // as_f64 coerces Int to Float
        assert_eq!(Value::Float(3.14).as_f64(), Some(3.14));
        assert_eq!(Value::Int(42).as_f64(), Some(42.0));
        assert_eq!(Value::String("x".into()).as_f64(), None);

        assert_eq!(Value::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(Value::Int(1).as_str(), None);

        assert!(Value::Int(1).is_i64());
        assert!(!Value::Float(1.0).is_i64());

        assert!(Value::Float(1.0).is_f64());
        assert!(!Value::Int(1).is_f64());

        assert!(Value::Int(1).is_number());
        assert!(Value::Float(1.0).is_number());
        assert!(!Value::String("x".into()).is_number());
    }

    #[test]
    fn test_serde_json_roundtrip() {
        let val = Value::Int(42);
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);

        let val = Value::Float(3.14);
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);

        let val = Value::String("hello".into());
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);

        let val = Value::List(vec![Value::Int(1), Value::Int(2)]);
        let json: serde_json::Value = val.clone().into();
        let back: Value = json.into();
        assert_eq!(val, back);
    }

    #[test]
    fn test_int_float_distinction_preserved() {
        // This is the key property: Int stays Int, Float stays Float
        let int_val = Value::Int(42);
        let float_val = Value::Float(42.0);

        assert!(int_val.is_i64());
        assert!(!int_val.is_f64());

        assert!(float_val.is_f64());
        assert!(!float_val.is_i64());

        // They are NOT equal (different variants)
        assert_ne!(int_val, float_val);
    }
}
