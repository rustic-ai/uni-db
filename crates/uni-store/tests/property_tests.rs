// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Property-based tests for graph operations and serialization.
//!
//! Uses proptest to verify invariants across randomly generated inputs.

use proptest::prelude::*;
use std::collections::HashSet;
use uni_common::core::id::{Eid, Vid};
use uni_common::graph::simple_graph::{Direction, SimpleGraph};
use uni_store::runtime::wal::{Mutation, WalSegment};

// -----------------------------------------------------------------------------
// VID/EID Encoding Round-Trip Tests
// -----------------------------------------------------------------------------

/// Strategy to generate valid VID values (pure u64).
fn vid_u64_strategy() -> impl Strategy<Value = u64> {
    0u64..=u64::MAX
}

proptest! {
    /// VID as_u64 and from must be inverses.
    #[test]
    fn vid_u64_roundtrip(id in vid_u64_strategy()) {
        let vid = Vid::new(id);
        let raw = vid.as_u64();
        let restored = Vid::from(raw);
        prop_assert_eq!(vid.as_u64(), restored.as_u64());
        prop_assert_eq!(raw, id);
    }

    /// EID as_u64 and from must be inverses.
    #[test]
    fn eid_u64_roundtrip(id in vid_u64_strategy()) {
        let eid = Eid::new(id);
        let raw = eid.as_u64();
        let restored = Eid::from(raw);
        prop_assert_eq!(eid.as_u64(), restored.as_u64());
        prop_assert_eq!(raw, id);
    }
}

// -----------------------------------------------------------------------------
// SimpleGraph Invariant Tests
// -----------------------------------------------------------------------------

/// Strategy to generate a small VID for graph operations.
fn small_vid_strategy() -> impl Strategy<Value = Vid> {
    (0u64..1000).prop_map(Vid::new)
}

/// Strategy to generate a small EID for graph operations.
fn small_eid_strategy() -> impl Strategy<Value = Eid> {
    (0u64..5000).prop_map(Eid::new)
}

/// Strategy to generate a sequence of graph operations.
#[derive(Debug, Clone)]
enum GraphOp {
    AddVertex(Vid),
    AddEdge(Vid, Vid, Eid, u32),
    RemoveVertex(Vid),
    RemoveEdge(Eid),
}

fn graph_op_strategy() -> impl Strategy<Value = GraphOp> {
    prop_oneof![
        small_vid_strategy().prop_map(GraphOp::AddVertex),
        (
            small_vid_strategy(),
            small_vid_strategy(),
            small_eid_strategy(),
            0u32..5
        )
            .prop_map(|(src, dst, eid, etype)| GraphOp::AddEdge(src, dst, eid, etype)),
        small_vid_strategy().prop_map(GraphOp::RemoveVertex),
        small_eid_strategy().prop_map(GraphOp::RemoveEdge),
    ]
}

proptest! {
    /// After any sequence of operations, vertex_count reflects contained vertices.
    #[test]
    fn graph_vertex_count_consistent(ops in prop::collection::vec(graph_op_strategy(), 0..50)) {
        let mut graph = SimpleGraph::new();
        let mut expected_vertices: HashSet<u64> = HashSet::new();

        for op in ops {
            match op {
                GraphOp::AddVertex(vid) => {
                    graph.add_vertex(vid);
                    expected_vertices.insert(vid.as_u64());
                }
                GraphOp::AddEdge(src, dst, eid, etype) => {
                    // Ensure vertices exist before adding edge
                    graph.add_vertex(src);
                    graph.add_vertex(dst);
                    expected_vertices.insert(src.as_u64());
                    expected_vertices.insert(dst.as_u64());
                    graph.add_edge(src, dst, eid, etype);
                }
                GraphOp::RemoveVertex(vid) => {
                    graph.remove_vertex(vid);
                    expected_vertices.remove(&vid.as_u64());
                }
                GraphOp::RemoveEdge(eid) => {
                    graph.remove_edge(eid);
                }
            }
        }

        prop_assert_eq!(graph.vertex_count(), expected_vertices.len());
    }

    /// Edge lookup via edge_map returns consistent data.
    #[test]
    fn graph_edge_lookup_consistent(
        src in small_vid_strategy(),
        dst in small_vid_strategy(),
        eid in small_eid_strategy(),
        etype in 0u32..5
    ) {
        let mut graph = SimpleGraph::new();
        graph.add_vertex(src);
        graph.add_vertex(dst);
        graph.add_edge(src, dst, eid, etype);

        // Edge should exist
        prop_assert!(graph.edge(eid).is_some());

        // Looking up edge should return correct endpoints
        if let Some(entry) = graph.edge(eid) {
            prop_assert_eq!(entry.src_vid.as_u64(), src.as_u64());
            prop_assert_eq!(entry.dst_vid.as_u64(), dst.as_u64());
            prop_assert_eq!(entry.edge_type, etype);
        } else {
            return Err(TestCaseError::fail("Edge should exist after insertion"));
        }
    }

    /// Removing an edge makes it absent from lookup.
    #[test]
    fn graph_edge_removal(
        src in small_vid_strategy(),
        dst in small_vid_strategy(),
        eid in small_eid_strategy(),
        etype in 0u32..5
    ) {
        let mut graph = SimpleGraph::new();
        graph.add_vertex(src);
        graph.add_vertex(dst);
        graph.add_edge(src, dst, eid, etype);

        prop_assert!(graph.edge(eid).is_some());

        graph.remove_edge(eid);

        prop_assert!(graph.edge(eid).is_none());
    }

    /// Outgoing and incoming neighbor counts are symmetric for added edges.
    #[test]
    fn graph_neighbor_symmetry(
        src in small_vid_strategy(),
        dst in small_vid_strategy(),
        eid in small_eid_strategy(),
        etype in 0u32..5
    ) {
        let mut graph = SimpleGraph::new();
        graph.add_vertex(src);
        graph.add_vertex(dst);
        graph.add_edge(src, dst, eid, etype);

        let outgoing = graph.neighbors(src, Direction::Outgoing);
        let incoming = graph.neighbors(dst, Direction::Incoming);

        // src should have dst in outgoing
        let has_outgoing = outgoing.iter().any(|e| e.dst_vid.as_u64() == dst.as_u64() && e.eid.as_u64() == eid.as_u64());
        prop_assert!(has_outgoing, "src should have outgoing edge to dst");

        // dst should have src in incoming
        let has_incoming = incoming.iter().any(|e| e.src_vid.as_u64() == src.as_u64() && e.eid.as_u64() == eid.as_u64());
        prop_assert!(has_incoming, "dst should have incoming edge from src");
    }
}

// -----------------------------------------------------------------------------
// WAL Serialization Round-Trip Tests
// -----------------------------------------------------------------------------

/// Strategy to generate a WAL Mutation.
fn mutation_strategy() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        // InsertVertex
        (
            small_vid_strategy(),
            prop::collection::hash_map(
                "[a-z]{1,8}",
                prop_oneof![
                    any::<i64>().prop_map(serde_json::Value::from),
                    any::<bool>().prop_map(serde_json::Value::from),
                    "[a-z]{0,20}".prop_map(serde_json::Value::String),
                ],
                0..5
            )
        )
            .prop_map(|(vid, props)| Mutation::InsertVertex {
                vid,
                properties: props
            }),
        // DeleteVertex
        small_vid_strategy().prop_map(|vid| Mutation::DeleteVertex { vid }),
        // InsertEdge
        (
            small_vid_strategy(),
            small_vid_strategy(),
            0u32..5,
            small_eid_strategy(),
            0u64..1000,
            prop::collection::hash_map(
                "[a-z]{1,8}",
                any::<i64>().prop_map(serde_json::Value::from),
                0..3
            )
        )
            .prop_map(|(src, dst, etype, eid, ver, props)| Mutation::InsertEdge {
                src_vid: src,
                dst_vid: dst,
                edge_type: etype,
                eid,
                version: ver,
                properties: props,
            }),
        // DeleteEdge
        (
            small_eid_strategy(),
            small_vid_strategy(),
            small_vid_strategy(),
            0u32..5,
            0u64..1000
        )
            .prop_map(|(eid, src, dst, etype, ver)| Mutation::DeleteEdge {
                eid,
                src_vid: src,
                dst_vid: dst,
                edge_type: etype,
                version: ver,
            }),
    ]
}

/// Helper to compare mutations for semantic equality.
fn mutations_equal(a: &Mutation, b: &Mutation) -> bool {
    match (a, b) {
        (
            Mutation::InsertVertex {
                vid: v1,
                properties: p1,
            },
            Mutation::InsertVertex {
                vid: v2,
                properties: p2,
            },
        ) => v1.as_u64() == v2.as_u64() && p1 == p2,
        (Mutation::DeleteVertex { vid: v1 }, Mutation::DeleteVertex { vid: v2 }) => {
            v1.as_u64() == v2.as_u64()
        }
        (
            Mutation::InsertEdge {
                src_vid: s1,
                dst_vid: d1,
                edge_type: t1,
                eid: e1,
                version: v1,
                properties: p1,
            },
            Mutation::InsertEdge {
                src_vid: s2,
                dst_vid: d2,
                edge_type: t2,
                eid: e2,
                version: v2,
                properties: p2,
            },
        ) => {
            s1.as_u64() == s2.as_u64()
                && d1.as_u64() == d2.as_u64()
                && t1 == t2
                && e1.as_u64() == e2.as_u64()
                && v1 == v2
                && p1 == p2
        }
        (
            Mutation::DeleteEdge {
                eid: e1,
                src_vid: s1,
                dst_vid: d1,
                edge_type: t1,
                version: v1,
            },
            Mutation::DeleteEdge {
                eid: e2,
                src_vid: s2,
                dst_vid: d2,
                edge_type: t2,
                version: v2,
            },
        ) => {
            e1.as_u64() == e2.as_u64()
                && s1.as_u64() == s2.as_u64()
                && d1.as_u64() == d2.as_u64()
                && t1 == t2
                && v1 == v2
        }
        _ => false,
    }
}

proptest! {
    /// Mutation serialization round-trip preserves data.
    #[test]
    fn mutation_serde_roundtrip(mutation in mutation_strategy()) {
        let json = serde_json::to_string(&mutation).expect("serialize");
        let restored: Mutation = serde_json::from_str(&json).expect("deserialize");

        // Compare semantically (HashMap order is not guaranteed)
        prop_assert!(mutations_equal(&mutation, &restored),
            "Mutation mismatch after round-trip: {:?} != {:?}", mutation, restored);
    }

    /// WalSegment serialization round-trip preserves data.
    #[test]
    fn wal_segment_serde_roundtrip(
        lsn in 1u64..10000,
        mutations in prop::collection::vec(mutation_strategy(), 0..10)
    ) {
        let segment = WalSegment { lsn, mutations };
        let json = serde_json::to_string(&segment).expect("serialize");
        let restored: WalSegment = serde_json::from_str(&json).expect("deserialize");

        prop_assert_eq!(segment.lsn, restored.lsn);
        prop_assert_eq!(segment.mutations.len(), restored.mutations.len());
    }
}
