// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Path construction for graph traversals.
//!
//! Provides builder pattern for accumulating nodes and edges into Path objects.
//! Used by traverse operators to construct complete paths through multi-hop traversals.

use crate::types::{Edge, Node, Path, Value};

/// Builder for constructing Path objects through multi-hop traversals.
///
/// Maintains a sequence of nodes and edges representing a path through the graph.
/// Each hop adds an edge and a target node, building up the complete path.
#[derive(Debug, Clone)]
pub struct PathBuilder {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl PathBuilder {
    /// Create a new path starting with a source node.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let start_node = Node { vid, label, properties };
    /// let path = PathBuilder::new(start_node);
    /// ```
    pub fn new(start_node: Node) -> Self {
        Self {
            nodes: vec![start_node],
            edges: Vec::new(),
        }
    }

    /// Create from existing path (for extending in nested traversals).
    ///
    /// Useful when continuing a path from a previous traversal result.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let existing_path = Path { nodes: vec![n1, n2], edges: vec![e1] };
    /// let mut builder = PathBuilder::from_path(existing_path);
    /// builder.add_hop(e2, n3); // Extends to (n1)-[e1]->(n2)-[e2]->(n3)
    /// ```
    pub fn from_path(path: Path) -> Self {
        Self {
            nodes: path.nodes,
            edges: path.edges,
        }
    }

    /// Add a hop to the path (edge + target node).
    ///
    /// Appends an edge and the node it leads to, extending the path by one hop.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let mut builder = PathBuilder::new(start_node);
    /// builder.add_hop(edge1, node2); // (start)-[edge1]->(node2)
    /// builder.add_hop(edge2, node3); // (start)-[edge1]->(node2)-[edge2]->(node3)
    /// ```
    pub fn add_hop(&mut self, edge: Edge, target: Node) {
        self.edges.push(edge);
        self.nodes.push(target);
    }

    /// Get the last node in the path (current position).
    ///
    /// Returns a reference to the most recently added node, which represents
    /// the current position in the traversal.
    ///
    /// # Panics
    ///
    /// Panics if the path has no nodes (should never happen with proper construction).
    pub fn current_node(&self) -> &Node {
        self.nodes.last().expect("Path must have at least one node")
    }

    /// Get path length (number of edges).
    ///
    /// Returns the number of hops (edges) in the path. A path with n nodes
    /// has n-1 edges.
    pub fn length(&self) -> usize {
        self.edges.len()
    }

    /// Build the final Path object.
    ///
    /// Consumes the builder and returns the completed Path.
    pub fn build(self) -> Path {
        Path {
            nodes: self.nodes,
            edges: self.edges,
        }
    }

    /// Build as Value::Path for row insertion.
    ///
    /// Convenience method that builds the Path and wraps it in Value::Path,
    /// ready for insertion into a query result row.
    pub fn build_value(self) -> Value {
        Value::Path(self.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use uni_common::core::id::{Eid, Vid};

    fn make_node(vid: u64, label: &str, name: &str) -> Node {
        let mut properties = HashMap::new();
        properties.insert("name".to_string(), Value::String(name.to_string()));
        Node {
            vid: Vid::new(vid),
            labels: vec![label.to_string()],
            properties,
        }
    }

    fn make_edge(eid: u64, edge_type: &str, src: u64, dst: u64) -> Edge {
        Edge {
            eid: Eid::new(eid),
            edge_type: edge_type.to_string(),
            src: Vid::new(src),
            dst: Vid::new(dst),
            properties: HashMap::new(),
        }
    }

    #[test]
    fn test_path_builder_single_hop() {
        // Build (a)-[r]->(b)
        let node_a = make_node(1, "Person", "Alice");
        let node_b = make_node(2, "Person", "Bob");
        let edge_r = make_edge(100, "KNOWS", 1, 2);

        let mut builder = PathBuilder::new(node_a.clone());
        builder.add_hop(edge_r.clone(), node_b.clone());

        let path = builder.build();

        assert_eq!(path.nodes.len(), 2);
        assert_eq!(path.edges.len(), 1);
        assert_eq!(path.nodes[0].vid, Vid::new(1));
        assert_eq!(path.nodes[1].vid, Vid::new(2));
        assert_eq!(path.edges[0].eid, Eid::new(100));
    }

    #[test]
    fn test_path_builder_multi_hop() {
        // Build (a)-[r1]->(b)-[r2]->(c)
        let node_a = make_node(1, "Person", "Alice");
        let node_b = make_node(2, "Person", "Bob");
        let node_c = make_node(3, "Person", "Charlie");
        let edge_r1 = make_edge(100, "KNOWS", 1, 2);
        let edge_r2 = make_edge(101, "KNOWS", 2, 3);

        let mut builder = PathBuilder::new(node_a.clone());
        builder.add_hop(edge_r1.clone(), node_b.clone());
        builder.add_hop(edge_r2.clone(), node_c.clone());

        let path = builder.build();

        assert_eq!(path.nodes.len(), 3);
        assert_eq!(path.edges.len(), 2);
        assert_eq!(path.nodes[0].vid, Vid::new(1));
        assert_eq!(path.nodes[1].vid, Vid::new(2));
        assert_eq!(path.nodes[2].vid, Vid::new(3));
        assert_eq!(path.edges[0].eid, Eid::new(100));
        assert_eq!(path.edges[1].eid, Eid::new(101));
    }

    #[test]
    fn test_path_extension() {
        // Start with existing path (a)-[r1]->(b), extend to (a)-[r1]->(b)-[r2]->(c)
        let node_a = make_node(1, "Person", "Alice");
        let node_b = make_node(2, "Person", "Bob");
        let node_c = make_node(3, "Person", "Charlie");
        let edge_r1 = make_edge(100, "KNOWS", 1, 2);
        let edge_r2 = make_edge(101, "KNOWS", 2, 3);

        // Create initial path
        let initial_path = Path {
            nodes: vec![node_a.clone(), node_b.clone()],
            edges: vec![edge_r1.clone()],
        };

        // Extend it
        let mut builder = PathBuilder::from_path(initial_path);
        builder.add_hop(edge_r2.clone(), node_c.clone());

        let path = builder.build();

        assert_eq!(path.nodes.len(), 3);
        assert_eq!(path.edges.len(), 2);
    }

    #[test]
    fn test_current_node() {
        let node_a = make_node(1, "Person", "Alice");
        let node_b = make_node(2, "Person", "Bob");
        let edge_r = make_edge(100, "KNOWS", 1, 2);

        let mut builder = PathBuilder::new(node_a.clone());

        // Current node should be Alice
        assert_eq!(builder.current_node().vid, Vid::new(1));

        builder.add_hop(edge_r, node_b.clone());

        // After hop, current node should be Bob
        assert_eq!(builder.current_node().vid, Vid::new(2));
    }

    #[test]
    fn test_path_length() {
        let node_a = make_node(1, "Person", "Alice");
        let node_b = make_node(2, "Person", "Bob");
        let node_c = make_node(3, "Person", "Charlie");
        let edge_r1 = make_edge(100, "KNOWS", 1, 2);
        let edge_r2 = make_edge(101, "KNOWS", 2, 3);

        let mut builder = PathBuilder::new(node_a.clone());
        assert_eq!(builder.length(), 0);

        builder.add_hop(edge_r1, node_b.clone());
        assert_eq!(builder.length(), 1);

        builder.add_hop(edge_r2, node_c.clone());
        assert_eq!(builder.length(), 2);
    }

    #[test]
    fn test_build_value() {
        let node_a = make_node(1, "Person", "Alice");
        let node_b = make_node(2, "Person", "Bob");
        let edge_r = make_edge(100, "KNOWS", 1, 2);

        let mut builder = PathBuilder::new(node_a);
        builder.add_hop(edge_r, node_b);

        let value = builder.build_value();

        match value {
            Value::Path(path) => {
                assert_eq!(path.nodes.len(), 2);
                assert_eq!(path.edges.len(), 1);
            }
            _ => panic!("Expected Value::Path"),
        }
    }
}
