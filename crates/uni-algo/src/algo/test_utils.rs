// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::algo::{GraphProjection, IdMap};
use uni_common::core::id::Vid;

pub fn build_test_graph(vids: Vec<Vid>, edges: Vec<(Vid, Vid)>) -> GraphProjection {
    let mut id_map = IdMap::with_capacity(vids.len());
    for vid in &vids {
        id_map.insert(*vid);
    }

    let vertex_count = id_map.len();

    // Build outbound edges
    let mut out_adj: Vec<Vec<u32>> = vec![Vec::new(); vertex_count];
    for (src, dst) in &edges {
        let u = id_map.to_slot(*src).expect("Source VID not in vids list");
        let v = id_map.to_slot(*dst).expect("Dest VID not in vids list");
        out_adj[u as usize].push(v);
    }

    let mut out_offsets = vec![0u32; vertex_count + 1];
    let mut out_neighbors = Vec::new();

    for i in 0..vertex_count {
        out_offsets[i + 1] = out_offsets[i] + out_adj[i].len() as u32;
        out_neighbors.extend_from_slice(&out_adj[i]);
    }

    GraphProjection {
        vertex_count,
        out_offsets,
        out_neighbors,
        in_offsets: vec![0; vertex_count + 1], // Empty inbound for these tests
        in_neighbors: Vec::new(),
        out_weights: None,
        id_map,
        _node_labels: Vec::new(),
        _edge_types: Vec::new(),
    }
}
