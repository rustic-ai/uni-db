// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Result normalization - converts internal representations to user-facing types.
//!
//! Enforces type system invariants:
//! - All nodes must be Value::Node (not serde_json::Value::Object with _vid/_label)
//! - All edges must be Value::Edge (not serde_json::Value::Object with _eid/_type)
//! - All paths must be Value::Path
//! - No internal fields exposed in user-facing results

use crate::types::{Edge, Node, Value};
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
            // Recursively normalize lists
            Value::List(items) => {
                let normalized: Result<Vec<_>> =
                    items.into_iter().map(Self::normalize_value).collect();
                Ok(Value::List(normalized?))
            }

            // Recursively normalize maps
            Value::Map(map) => {
                // Check if this map represents a node or edge
                if Self::is_node_map(&map) {
                    Self::map_to_node(map)
                } else if Self::is_edge_map(&map) {
                    Self::map_to_edge(map)
                } else {
                    // Regular map - normalize values recursively
                    let normalized: Result<HashMap<_, _>> = map
                        .into_iter()
                        .map(|(k, v)| Ok((k, Self::normalize_value(v)?)))
                        .collect();
                    Ok(Value::Map(normalized?))
                }
            }

            // Already proper types - pass through
            Value::Node(_) | Value::Edge(_) | Value::Path(_) => Ok(value),

            // Primitives - pass through
            _ => Ok(value),
        }
    }

    /// Check if Map represents a node (has _vid and _label or properties suggesting node).
    fn is_node_map(map: &HashMap<String, Value>) -> bool {
        // A map is a node if it has _vid or both _id and label fields
        map.contains_key("_vid")
            || (map.contains_key("_id") && map.contains_key("label"))
            || (map.contains_key("_vid") && map.contains_key("_label"))
    }

    /// Check if Map represents an edge (has _eid, _type, _src, _dst).
    fn is_edge_map(map: &HashMap<String, Value>) -> bool {
        map.contains_key("_eid")
            || (map.contains_key("_id") && map.contains_key("_src") && map.contains_key("_dst"))
            || (map.contains_key("_eid") && map.contains_key("_type"))
    }

    /// Convert Map to Node, extracting properties and stripping internal fields.
    fn map_to_node(map: HashMap<String, Value>) -> Result<Value> {
        // Extract VID
        let vid = map
            .get("_vid")
            .or_else(|| map.get("_id"))
            .and_then(|v| match v {
                Value::Int(i) => Some(Vid::new(*i as u64)),
                Value::String(s) => s.parse::<u64>().ok().map(Vid::new),
                _ => None,
            })
            .ok_or_else(|| anyhow!("Missing or invalid _vid in node map"))?;

        // Extract label
        let label = map
            .get("_label")
            .or_else(|| map.get("label"))
            .and_then(|v| match v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_default();

        // Extract properties, filtering out internal fields
        let properties: HashMap<String, Value> = map
            .into_iter()
            .filter(|(k, _)| !k.starts_with('_') && k != "properties" && k != "label")
            .collect();

        Ok(Value::Node(Node {
            vid,
            label,
            properties,
        }))
    }

    /// Convert Map to Edge, extracting properties and stripping internal fields.
    fn map_to_edge(map: HashMap<String, Value>) -> Result<Value> {
        let eid = map
            .get("_eid")
            .or_else(|| map.get("_id"))
            .and_then(|v| match v {
                Value::Int(i) => Some(Eid::new(*i as u64)),
                Value::String(s) => s.parse::<u64>().ok().map(Eid::new),
                _ => None,
            })
            .ok_or_else(|| anyhow!("Missing or invalid _eid in edge map"))?;

        // Prefer _type_name (string) over _type (numeric ID) for user-facing output
        let edge_type = map
            .get("_type_name")
            .and_then(|v| match v {
                Value::String(s) if !s.is_empty() => Some(s.clone()),
                _ => None,
            })
            .or_else(|| {
                // Fall back to _type if it's already a string
                map.get("_type")
                    .or_else(|| map.get("type"))
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
            })
            .unwrap_or_default();

        let src = map
            .get("_src")
            .and_then(|v| match v {
                Value::Int(i) => Some(Vid::new(*i as u64)),
                Value::String(s) => s.parse::<u64>().ok().map(Vid::new),
                _ => None,
            })
            .ok_or_else(|| anyhow!("Missing _src in edge map"))?;

        let dst = map
            .get("_dst")
            .and_then(|v| match v {
                Value::Int(i) => Some(Vid::new(*i as u64)),
                Value::String(s) => s.parse::<u64>().ok().map(Vid::new),
                _ => None,
            })
            .ok_or_else(|| anyhow!("Missing _dst in edge map"))?;

        let properties: HashMap<String, Value> = map
            .into_iter()
            .filter(|(k, _)| !k.starts_with('_') && k != "properties" && k != "type")
            .collect();

        Ok(Value::Edge(Edge {
            eid,
            edge_type,
            src,
            dst,
            properties,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_node_map() {
        let mut map = HashMap::new();
        map.insert("_vid".to_string(), Value::Int(123));
        map.insert("_label".to_string(), Value::String("Person".to_string()));
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        map.insert("age".to_string(), Value::Int(30));

        let result = ResultNormalizer::normalize_value(Value::Map(map)).unwrap();

        match result {
            Value::Node(node) => {
                assert_eq!(node.vid.as_u64(), 123);
                assert_eq!(node.label, "Person");
                assert_eq!(
                    node.properties.get("name"),
                    Some(&Value::String("Alice".to_string()))
                );
                assert_eq!(node.properties.get("age"), Some(&Value::Int(30)));
                // Internal fields should be stripped
                assert!(!node.properties.contains_key("_vid"));
                assert!(!node.properties.contains_key("_label"));
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
        inner_map.insert("_label".to_string(), Value::String("Node".to_string()));

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
        node_map.insert("_label".to_string(), Value::String("Person".to_string()));
        node_map.insert("name".to_string(), Value::String("Alice".to_string()));

        let mut row = HashMap::new();
        row.insert("n".to_string(), Value::Map(node_map));
        row.insert("count".to_string(), Value::Int(5));

        let result = ResultNormalizer::normalize_row(row).unwrap();

        assert!(matches!(result.get("n"), Some(Value::Node(_))));
        assert_eq!(result.get("count"), Some(&Value::Int(5)));
    }
}
