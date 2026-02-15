// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Result normalization - converts internal representations to user-facing types.
//!
//! Enforces type system invariants:
//! - All nodes must be Value::Node (not Value::Map with _vid/_labels)
//! - All edges must be Value::Edge (not Value::Map with _eid/_type)
//! - All paths must be Value::Path
//! - No internal fields exposed in user-facing results

use crate::types::{Edge, Node, Path, Value};
use anyhow::{Result, anyhow};
use std::collections::HashMap;
use uni_common::core::id::{Eid, Vid};

pub struct ResultNormalizer;

impl ResultNormalizer {
    /// Normalize a complete row of results.
    pub fn normalize_row(row: HashMap<String, Value>) -> Result<HashMap<String, Value>> {
        row.into_iter()
            .map(|(k, v)| Ok((k, Self::normalize_value(v)?)))
            .collect()
    }

    /// Recursively normalize a single value.
    pub fn normalize_value(value: Value) -> Result<Value> {
        match value {
            Value::List(items) => {
                let normalized: Result<Vec<_>> =
                    items.into_iter().map(Self::normalize_value).collect();
                Ok(Value::List(normalized?))
            }

            Value::Map(map) => {
                // Check if this map represents a path, node, or edge (order matters: path first)
                if Self::is_path_map(&map) {
                    Self::map_to_path(map)
                } else if Self::is_node_map(&map) {
                    Self::map_to_node(map)
                } else if Self::is_edge_map(&map) {
                    Self::map_to_edge(map)
                } else {
                    let normalized: Result<HashMap<_, _>> = map
                        .into_iter()
                        .map(|(k, v)| Ok((k, Self::normalize_value(v)?)))
                        .collect();
                    Ok(Value::Map(normalized?))
                }
            }

            // Already proper graph types or primitives - pass through unchanged
            _ => Ok(value),
        }
    }

    /// Normalize a property value without structural conversion.
    ///
    /// Recursively processes nested lists and maps but does NOT convert maps to
    /// Node/Edge/Path structures. This prevents user data containing keys like
    /// `_vid` or `_eid` from being incorrectly converted.
    fn normalize_property_value(value: Value) -> Value {
        match value {
            Value::List(items) => Value::List(
                items
                    .into_iter()
                    .map(Self::normalize_property_value)
                    .collect(),
            ),
            Value::Map(map) => Value::Map(
                map.into_iter()
                    .map(|(k, v)| (k, Self::normalize_property_value(v)))
                    .collect(),
            ),
            other => other,
        }
    }

    /// Check if map represents a node.
    ///
    /// Detection is intentionally lenient for top-level result values. Property values
    /// inside nodes/edges use `normalize_property_value` instead, which skips this check.
    fn is_node_map(map: &HashMap<String, Value>) -> bool {
        map.contains_key("_vid") || (map.contains_key("_id") && map.contains_key("label"))
    }

    /// Check if map represents an edge.
    ///
    /// Detection is intentionally lenient for top-level result values. Property values
    /// inside nodes/edges use `normalize_property_value` instead, which skips this check.
    fn is_edge_map(map: &HashMap<String, Value>) -> bool {
        map.contains_key("_eid")
            || (map.contains_key("_id") && map.contains_key("_src") && map.contains_key("_dst"))
    }

    /// Check if map represents a path (has "nodes" and "relationships" or "edges").
    fn is_path_map(map: &HashMap<String, Value>) -> bool {
        map.contains_key("nodes")
            && (map.contains_key("relationships") || map.contains_key("edges"))
    }

    /// Extract a u64 ID from a Value (Int or parseable String).
    fn value_to_u64(value: &Value) -> Option<u64> {
        match value {
            Value::Int(i) => u64::try_from(*i).ok(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }

    /// Extract a string from a Value.
    fn value_to_string(value: &Value) -> Option<String> {
        if let Value::String(s) = value {
            Some(s.clone())
        } else {
            None
        }
    }

    /// Extract properties from a dedicated "properties" field (if present) or from inline fields.
    ///
    /// This handles two property storage formats:
    /// 1. A "properties" field containing LargeBinary (JSON) or a Map
    /// 2. Inline fields in the map (non-underscore fields)
    fn extract_properties_from_field_or_inline(
        map: &HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        // First try to extract from a dedicated "properties" field
        if let Some(props_value) = map.get("properties") {
            match props_value {
                // Properties stored as a Map
                Value::Map(m) => {
                    return Self::prune_null_properties(
                        m.iter()
                            .map(|(k, v)| (k.clone(), Self::normalize_property_value(v.clone())))
                            .collect(),
                    );
                }
                // Properties stored as Bytes (JSON serialized)
                Value::Bytes(bytes) => {
                    if let Ok(props) =
                        serde_json::from_slice::<HashMap<String, serde_json::Value>>(bytes)
                    {
                        return Self::prune_null_properties(
                            props
                                .into_iter()
                                .map(|(k, v)| (k, Self::json_value_to_value(v)))
                                .collect(),
                        );
                    }
                }
                _ => {}
            }
        }

        // Expand _all_props JSONB blob (used by traverse and schemaless scan paths).
        // _all_props is decoded from JSONB to Value::Map by arrow_to_value.
        if let Some(Value::Map(all_props)) = map.get("_all_props") {
            let mut properties: HashMap<String, Value> = all_props
                .iter()
                .map(|(k, v)| (k.clone(), Self::normalize_property_value(v.clone())))
                .collect();
            // Merge any inline non-internal properties (schema-defined props loaded as columns)
            for (k, v) in map.iter() {
                if !k.starts_with('_')
                    && k.as_str() != "properties"
                    && k.as_str() != "label"
                    && k.as_str() != "type"
                    && k.as_str() != "overflow_json"
                {
                    properties
                        .entry(k.clone())
                        .or_insert_with(|| Self::normalize_property_value(v.clone()));
                }
            }
            return Self::prune_null_properties(properties);
        }

        // Fall back to extracting inline properties (excluding _ prefixed and special fields)
        Self::prune_null_properties(
            map.iter()
                .filter(|(k, _)| {
                    !k.starts_with('_')
                        && k.as_str() != "properties"
                        && k.as_str() != "label"
                        && k.as_str() != "type"
                        && k.as_str() != "overflow_json"
                })
                .map(|(k, v)| (k.clone(), Self::normalize_property_value(v.clone())))
                .collect(),
        )
    }

    /// Remove properties with null values from user-facing entity property maps.
    fn prune_null_properties(mut properties: HashMap<String, Value>) -> HashMap<String, Value> {
        properties.retain(|_, v| !v.is_null());
        properties
    }

    /// Convert a serde_json::Value to our Value type.
    fn json_value_to_value(json: serde_json::Value) -> Value {
        match json {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => n
                .as_i64()
                .map(Value::Int)
                .or_else(|| n.as_f64().map(Value::Float))
                .unwrap_or_else(|| Value::String(n.to_string())),
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::List(arr.into_iter().map(Self::json_value_to_value).collect())
            }
            serde_json::Value::Object(obj) => Value::Map(
                obj.into_iter()
                    .map(|(k, v)| (k, Self::json_value_to_value(v)))
                    .collect(),
            ),
        }
    }

    /// Convert map to Node, extracting properties and stripping internal fields.
    fn map_to_node(map: HashMap<String, Value>) -> Result<Value> {
        let vid = map
            .get("_vid")
            .or_else(|| map.get("_id"))
            .and_then(Self::value_to_u64)
            .map(Vid::new)
            .ok_or_else(|| anyhow!("Missing or invalid _vid in node map"))?;

        let labels = if let Some(Value::List(label_list)) = map.get("_labels") {
            label_list
                .iter()
                .filter_map(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else if let Some(Value::String(s)) = map.get("_labels") {
            // Single string fallback for backwards compat within same session
            if s.is_empty() { vec![] } else { vec![s.clone()] }
        } else {
            Vec::new()
        };

        // Try to extract properties from a dedicated "properties" field (LargeBinary/JSON)
        // If not present or not parseable, fall back to extracting from inline fields
        let properties = Self::extract_properties_from_field_or_inline(&map);

        Ok(Value::Node(Node {
            vid,
            labels,
            properties,
        }))
    }

    /// Convert map to Edge, extracting properties and stripping internal fields.
    fn map_to_edge(map: HashMap<String, Value>) -> Result<Value> {
        let eid = map
            .get("_eid")
            .or_else(|| map.get("_id"))
            .and_then(Self::value_to_u64)
            .map(Eid::new)
            .ok_or_else(|| anyhow!("Missing or invalid _eid in edge map"))?;

        // Prefer _type_name (string) over _type (numeric ID) for user-facing output
        let edge_type = ["_type_name", "_type", "type"]
            .iter()
            .find_map(|key| map.get(*key).and_then(Self::value_to_string))
            .filter(|s| !s.is_empty())
            .unwrap_or_default();

        let src = map
            .get("_src")
            .and_then(Self::value_to_u64)
            .map(Vid::new)
            .ok_or_else(|| anyhow!("Missing _src in edge map"))?;

        let dst = map
            .get("_dst")
            .and_then(Self::value_to_u64)
            .map(Vid::new)
            .ok_or_else(|| anyhow!("Missing _dst in edge map"))?;

        // Try to extract properties from a dedicated "properties" field (LargeBinary/JSON)
        // If not present or not parseable, fall back to extracting from inline fields
        let properties = Self::extract_properties_from_field_or_inline(&map);

        Ok(Value::Edge(Edge {
            eid,
            edge_type,
            src,
            dst,
            properties,
        }))
    }

    /// Convert map to Path, handling both "relationships" and "edges" keys.
    fn map_to_path(mut map: HashMap<String, Value>) -> Result<Value> {
        let nodes = Self::extract_path_nodes(
            map.remove("nodes")
                .ok_or_else(|| anyhow!("Missing nodes in path map"))?,
        )?;

        let edges = Self::extract_path_edges(
            map.remove("relationships")
                .or_else(|| map.remove("edges"))
                .ok_or_else(|| anyhow!("Missing relationships/edges in path map"))?,
        )?;

        Ok(Value::Path(Path { nodes, edges }))
    }

    /// Extract nodes from a path's nodes list.
    fn extract_path_nodes(value: Value) -> Result<Vec<Node>> {
        let Value::List(items) = value else {
            return Err(anyhow!("Path nodes must be a list"));
        };

        items
            .into_iter()
            .map(|item| match item {
                Value::Node(n) => Ok(n),
                Value::Map(m) => match Self::map_to_node(m)? {
                    Value::Node(n) => Ok(n),
                    _ => Err(anyhow!("Failed to convert map to node in path")),
                },
                _ => Err(anyhow!("Invalid node type in path nodes list")),
            })
            .collect()
    }

    /// Extract edges from a path's relationships/edges list.
    fn extract_path_edges(value: Value) -> Result<Vec<Edge>> {
        let Value::List(items) = value else {
            return Err(anyhow!("Path relationships/edges must be a list"));
        };

        items
            .into_iter()
            .map(|item| match item {
                Value::Edge(e) => Ok(e),
                Value::Map(m) => match Self::map_to_edge(m)? {
                    Value::Edge(e) => Ok(e),
                    _ => Err(anyhow!("Failed to convert map to edge in path")),
                },
                _ => Err(anyhow!("Invalid edge type in path edges list")),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_node_map() {
        let mut map = HashMap::new();
        map.insert("_vid".to_string(), Value::Int(123));
        map.insert(
            "_labels".to_string(),
            Value::List(vec![Value::String("Person".to_string())]),
        );
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        map.insert("age".to_string(), Value::Int(30));

        let result = ResultNormalizer::normalize_value(Value::Map(map)).unwrap();

        match result {
            Value::Node(node) => {
                assert_eq!(node.vid.as_u64(), 123);
                assert_eq!(node.labels, vec!["Person".to_string()]);
                assert_eq!(
                    node.properties.get("name"),
                    Some(&Value::String("Alice".to_string()))
                );
                assert_eq!(node.properties.get("age"), Some(&Value::Int(30)));
                // Internal fields should be stripped
                assert!(!node.properties.contains_key("_vid"));
                assert!(!node.properties.contains_key("_labels"));
            }
            _ => panic!("Expected Node variant"),
        }
    }

    #[test]
    fn test_normalize_edge_map() {
        let mut map = HashMap::new();
        map.insert("_eid".to_string(), Value::Int(456));
        map.insert("_type".to_string(), Value::String("KNOWS".to_string()));
        map.insert("_src".to_string(), Value::Int(123));
        map.insert("_dst".to_string(), Value::Int(789));
        map.insert("since".to_string(), Value::Int(2020));

        let result = ResultNormalizer::normalize_value(Value::Map(map)).unwrap();

        match result {
            Value::Edge(edge) => {
                assert_eq!(edge.eid.as_u64(), 456);
                assert_eq!(edge.edge_type, "KNOWS");
                assert_eq!(edge.src.as_u64(), 123);
                assert_eq!(edge.dst.as_u64(), 789);
                assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
                // Internal fields should be stripped
                assert!(!edge.properties.contains_key("_eid"));
                assert!(!edge.properties.contains_key("_type"));
            }
            _ => panic!("Expected Edge variant"),
        }
    }

    #[test]
    fn test_normalize_nested_structures() {
        let mut inner_map = HashMap::new();
        inner_map.insert("_vid".to_string(), Value::Int(100));
        inner_map.insert(
            "_labels".to_string(),
            Value::List(vec![Value::String("Node".to_string())]),
        );

        let list = vec![Value::Map(inner_map.clone()), Value::Int(42)];

        let result = ResultNormalizer::normalize_value(Value::List(list)).unwrap();

        match result {
            Value::List(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0], Value::Node(_)));
                assert_eq!(items[1], Value::Int(42));
            }
            _ => panic!("Expected List variant"),
        }
    }

    #[test]
    fn test_normalize_regular_map() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), Value::String("value1".to_string()));
        map.insert("key2".to_string(), Value::Int(42));

        let result = ResultNormalizer::normalize_value(Value::Map(map)).unwrap();

        match result {
            Value::Map(m) => {
                assert_eq!(m.get("key1"), Some(&Value::String("value1".to_string())));
                assert_eq!(m.get("key2"), Some(&Value::Int(42)));
            }
            _ => panic!("Expected Map variant for regular map"),
        }
    }

    #[test]
    fn test_normalize_row() {
        let mut node_map = HashMap::new();
        node_map.insert("_vid".to_string(), Value::Int(123));
        node_map.insert(
            "_labels".to_string(),
            Value::List(vec![Value::String("Person".to_string())]),
        );
        node_map.insert("name".to_string(), Value::String("Alice".to_string()));

        let mut row = HashMap::new();
        row.insert("n".to_string(), Value::Map(node_map));
        row.insert("count".to_string(), Value::Int(5));

        let result = ResultNormalizer::normalize_row(row).unwrap();

        assert!(matches!(result.get("n"), Some(Value::Node(_))));
        assert_eq!(result.get("count"), Some(&Value::Int(5)));
    }

    #[test]
    fn test_map_with_vid_at_top_level_becomes_node() {
        // At top level, a map with _vid is detected as a node
        // (even without _labels - labels defaults to empty vec)
        let mut map = HashMap::new();
        map.insert("_vid".to_string(), Value::Int(123));
        map.insert("name".to_string(), Value::String("test".to_string()));

        let result = ResultNormalizer::normalize_value(Value::Map(map)).unwrap();

        match result {
            Value::Node(node) => {
                assert_eq!(node.vid.as_u64(), 123);
                assert!(node.labels.is_empty()); // Default empty labels
                assert_eq!(
                    node.properties.get("name"),
                    Some(&Value::String("test".to_string()))
                );
            }
            _ => panic!("Expected Node variant, got {:?}", result),
        }
    }

    #[test]
    fn test_normalize_node_with_nested_map_containing_vid_key() {
        // Regression test: user property containing _vid key should NOT be
        // converted to a Node
        let mut nested = HashMap::new();
        nested.insert("_vid".to_string(), Value::String("user-data".to_string()));
        nested.insert("other".to_string(), Value::Int(42));

        let mut node_map = HashMap::new();
        node_map.insert("_vid".to_string(), Value::Int(123));
        node_map.insert(
            "_labels".to_string(),
            Value::List(vec![Value::String("Person".to_string())]),
        );
        node_map.insert("metadata".to_string(), Value::Map(nested));

        let result = ResultNormalizer::normalize_value(Value::Map(node_map)).unwrap();

        match result {
            Value::Node(node) => {
                assert_eq!(node.vid.as_u64(), 123);
                assert_eq!(node.labels, vec!["Person".to_string()]);
                // The nested map should remain a Map, NOT become a Node
                match node.properties.get("metadata") {
                    Some(Value::Map(m)) => {
                        assert_eq!(m.get("_vid"), Some(&Value::String("user-data".to_string())));
                        assert_eq!(m.get("other"), Some(&Value::Int(42)));
                    }
                    other => panic!("Expected metadata to be Map, got {:?}", other),
                }
            }
            _ => panic!("Expected Node variant"),
        }
    }

    #[test]
    fn test_normalize_edge_with_nested_map_containing_eid_key() {
        // Regression test: user property containing _eid key should NOT be
        // converted to an Edge
        let mut nested = HashMap::new();
        nested.insert("_eid".to_string(), Value::String("ref-123".to_string()));

        let mut edge_map = HashMap::new();
        edge_map.insert("_eid".to_string(), Value::Int(456));
        edge_map.insert("_type".to_string(), Value::String("KNOWS".to_string()));
        edge_map.insert("_src".to_string(), Value::Int(123));
        edge_map.insert("_dst".to_string(), Value::Int(789));
        edge_map.insert("reference".to_string(), Value::Map(nested));

        let result = ResultNormalizer::normalize_value(Value::Map(edge_map)).unwrap();

        match result {
            Value::Edge(edge) => {
                assert_eq!(edge.eid.as_u64(), 456);
                // The nested map should remain a Map, NOT become an Edge
                match edge.properties.get("reference") {
                    Some(Value::Map(m)) => {
                        assert_eq!(m.get("_eid"), Some(&Value::String("ref-123".to_string())));
                    }
                    other => panic!("Expected reference to be Map, got {:?}", other),
                }
            }
            _ => panic!("Expected Edge variant"),
        }
    }

    #[test]
    fn test_normalize_node_prunes_null_properties() {
        let mut map = HashMap::new();
        map.insert("_vid".to_string(), Value::Int(1));
        map.insert(
            "_labels".to_string(),
            Value::List(vec![Value::String("Person".to_string())]),
        );
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        map.insert("age".to_string(), Value::Null);

        let result = ResultNormalizer::normalize_value(Value::Map(map)).unwrap();
        let Value::Node(node) = result else {
            panic!("Expected Node variant");
        };

        assert_eq!(
            node.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert!(!node.properties.contains_key("age"));
    }

    #[test]
    fn test_normalize_edge_prunes_null_properties_from_all_props_and_inline() {
        let mut all_props = HashMap::new();
        all_props.insert("since".to_string(), Value::Null);
        all_props.insert("weight".to_string(), Value::Int(7));

        let mut edge_map = HashMap::new();
        edge_map.insert("_eid".to_string(), Value::Int(10));
        edge_map.insert("_type".to_string(), Value::String("REL".to_string()));
        edge_map.insert("_src".to_string(), Value::Int(1));
        edge_map.insert("_dst".to_string(), Value::Int(2));
        edge_map.insert("_all_props".to_string(), Value::Map(all_props));
        edge_map.insert("name".to_string(), Value::Null);

        let result = ResultNormalizer::normalize_value(Value::Map(edge_map)).unwrap();
        let Value::Edge(edge) = result else {
            panic!("Expected Edge variant");
        };

        assert_eq!(edge.properties.get("weight"), Some(&Value::Int(7)));
        assert!(!edge.properties.contains_key("since"));
        assert!(!edge.properties.contains_key("name"));
    }
}
