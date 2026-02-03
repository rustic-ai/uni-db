// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Compressed Sparse Row (CSR) representation of adjacency.
//!
//! In the new storage model, CSR uses DenseIdx for internal indexing.
//! VidRemapper handles the mapping between sparse VIDs and dense indices.

use crate::runtime::VidRemapper;
use uni_common::core::id::{DenseIdx, Eid, Vid};

/// Compressed Sparse Row (CSR) representation of adjacency
///
/// Optimized for:
/// 1. Low memory footprint (offsets are u32)
/// 2. O(1) neighbor lookup using DenseIdx
/// 3. CPU cache locality (contiguous memory)
///
/// Note: CSR uses DenseIdx internally. Use VidRemapper to convert Vid ↔ DenseIdx.
#[derive(Clone)]
pub struct CompressedSparseRow {
    /// Offset into neighbors for vertex i: offsets[i]..offsets[i+1]
    /// Index by DenseIdx
    offsets: Vec<u32>,

    /// Flattened neighbor list (stored as DenseIdx for internal operations)
    neighbors: Vec<DenseIdx>,

    /// Neighbor VIDs (for when caller needs actual VIDs)
    neighbor_vids: Vec<Vid>,

    /// Edge IDs parallel to neighbors
    edge_ids: Vec<Eid>,
}

impl CompressedSparseRow {
    /// Creates a new CSR from a list of edges.
    ///
    /// # Arguments
    /// * `entries` - List of (src_vid, dst_vid, eid) tuples
    /// * `remapper` - Mutable remapper that will be populated with all VIDs
    ///
    /// Returns the CSR with all vertices remapped to dense indices.
    pub fn from_edges(entries: Vec<(Vid, Vid, Eid)>, remapper: &mut VidRemapper) -> Self {
        if entries.is_empty() {
            return Self {
                offsets: vec![0],
                neighbors: Vec::new(),
                neighbor_vids: Vec::new(),
                edge_ids: Vec::new(),
            };
        }

        // First pass: insert all VIDs into remapper
        for (src, dst, _) in &entries {
            remapper.insert(*src);
            remapper.insert(*dst);
        }

        // Convert to (dense_src, dst_vid, eid) and sort by src
        let mut edges: Vec<(DenseIdx, Vid, Eid)> = entries
            .iter()
            .map(|(src, dst, eid)| (remapper.to_dense(*src).unwrap(), *dst, *eid))
            .collect();

        edges.sort_by_key(|(src, _, _)| *src);

        let max_dense = remapper.len();
        let mut offsets = vec![0u32; max_dense + 1];
        let mut neighbors = Vec::with_capacity(edges.len());
        let mut neighbor_vids = Vec::with_capacity(edges.len());
        let mut edge_ids = Vec::with_capacity(edges.len());

        let mut current_src = DenseIdx::new(0);
        let mut current_offset = 0u32;

        for (src_dense, dst_vid, eid) in edges {
            // Fill gaps in offsets
            while current_src < src_dense {
                offsets[current_src.as_usize() + 1] = current_offset;
                current_src = DenseIdx::new(current_src.as_u32() + 1);
            }

            let dst_dense = remapper.to_dense(dst_vid).unwrap();
            neighbors.push(dst_dense);
            neighbor_vids.push(dst_vid);
            edge_ids.push(eid);
            current_offset += 1;
        }

        // Fill remaining offsets
        while current_src.as_usize() < max_dense {
            offsets[current_src.as_usize() + 1] = current_offset;
            current_src = DenseIdx::new(current_src.as_u32() + 1);
        }

        Self {
            offsets,
            neighbors,
            neighbor_vids,
            edge_ids,
        }
    }

    /// Creates a CSR from pre-sorted edges with src as u64 offset.
    ///
    /// Entries are (src_offset, neighbor_vid, eid).
    pub fn new(max_vid_offset: usize, entries: Vec<(u64, Vid, Eid)>) -> Self {
        if entries.is_empty() {
            return Self {
                offsets: vec![0],
                neighbors: Vec::new(),
                neighbor_vids: Vec::new(),
                edge_ids: Vec::new(),
            };
        }

        // Sort by src_offset
        let mut sorted = entries;
        sorted.sort_by_key(|(src, _, _)| *src);

        let mut offsets = vec![0u32; max_vid_offset + 2];
        let mut neighbors = Vec::with_capacity(sorted.len());
        let mut neighbor_vids = Vec::with_capacity(sorted.len());
        let mut edge_ids = Vec::with_capacity(sorted.len());

        let mut current_offset = 0u32;
        let mut last_src = 0usize;

        for (src, neighbor, eid) in sorted {
            let src_idx = src as usize;

            // Fill gaps
            if src_idx > last_src {
                for offset in offsets.iter_mut().take(src_idx + 1).skip(last_src + 1) {
                    *offset = current_offset;
                }
            }
            last_src = src_idx;

            // Store neighbor as DenseIdx (using VID's raw value as dense index)
            neighbors.push(DenseIdx::new(neighbor.as_u64() as u32));
            neighbor_vids.push(neighbor);
            edge_ids.push(eid);
            current_offset += 1;
        }

        // Fill remaining offsets
        for offset in offsets.iter_mut().skip(last_src + 1) {
            *offset = current_offset;
        }

        Self {
            offsets,
            neighbors,
            neighbor_vids,
            edge_ids,
        }
    }

    /// O(1) neighbor lookup using DenseIdx.
    ///
    /// Returns slices of (neighbor dense indices, neighbor VIDs, edge IDs).
    pub fn get_neighbors_dense(&self, idx: DenseIdx) -> (&[DenseIdx], &[Vid], &[Eid]) {
        let i = idx.as_usize();
        if i + 1 >= self.offsets.len() {
            return (&[], &[], &[]);
        }

        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;

        if start >= self.neighbors.len() || end > self.neighbors.len() {
            return (&[], &[], &[]);
        }

        (
            &self.neighbors[start..end],
            &self.neighbor_vids[start..end],
            &self.edge_ids[start..end],
        )
    }

    /// O(1) neighbor lookup using Vid directly.
    ///
    /// Looks up using VID's raw value as offset.
    /// Returns slices of (neighbor VIDs, edge IDs).
    pub fn get_neighbors(&self, vid: Vid) -> (&[Vid], &[Eid]) {
        // Use the VID's raw value as the index
        let local = vid.as_u64() as usize;
        if local + 1 >= self.offsets.len() {
            return (&[], &[]);
        }

        let start = self.offsets[local] as usize;
        let end = self.offsets[local + 1] as usize;

        if start >= self.neighbor_vids.len() || end > self.neighbor_vids.len() {
            return (&[], &[]);
        }

        (&self.neighbor_vids[start..end], &self.edge_ids[start..end])
    }

    /// Approximate memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.offsets.len() * 4
            + self.neighbors.len() * 4
            + self.neighbor_vids.len() * 8
            + self.edge_ids.len() * 8
    }

    /// Returns the number of vertices (rows) in the CSR.
    pub fn num_vertices(&self) -> usize {
        if self.offsets.is_empty() {
            0
        } else {
            self.offsets.len() - 1
        }
    }

    /// Returns the number of edges in the CSR.
    pub fn num_edges(&self) -> usize {
        self.edge_ids.len()
    }

    /// Iterate over all edges in the CSR.
    /// Returns iterator over (src_offset, dst_vid, eid).
    pub fn iter_all(&self) -> impl Iterator<Item = (u64, Vid, Eid)> + '_ {
        (0..self.offsets.len().saturating_sub(1)).flat_map(move |i| {
            let start = self.offsets[i] as usize;
            let end = self.offsets[i + 1] as usize;
            (start..end).map(move |j| (i as u64, self.neighbor_vids[j], self.edge_ids[j]))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csr_from_edges() {
        let mut remapper = VidRemapper::new();

        let edges = vec![
            (Vid::new(100), Vid::new(200), Eid::new(1)),
            (Vid::new(100), Vid::new(300), Eid::new(2)),
            (Vid::new(200), Vid::new(300), Eid::new(3)),
        ];

        let csr = CompressedSparseRow::from_edges(edges, &mut remapper);

        // Check remapper has all VIDs
        assert_eq!(remapper.len(), 3);
        assert!(remapper.contains(Vid::new(100)));
        assert!(remapper.contains(Vid::new(200)));
        assert!(remapper.contains(Vid::new(300)));

        // Check neighbors for vid=100
        let idx100 = remapper.to_dense(Vid::new(100)).unwrap();
        let (_, vids, eids) = csr.get_neighbors_dense(idx100);
        assert_eq!(vids.len(), 2);
        assert_eq!(eids.len(), 2);
    }

    #[test]
    fn test_csr_empty() {
        let mut remapper = VidRemapper::new();
        let csr = CompressedSparseRow::from_edges(vec![], &mut remapper);
        assert_eq!(csr.num_edges(), 0);
    }
}

/// Entry in a versioned CSR that tracks when each edge was created.
#[derive(Debug, Clone, Copy)]
pub struct CsrEdgeEntry {
    /// Neighbor vertex ID.
    pub neighbor_vid: Vid,
    /// Edge ID.
    pub eid: Eid,
    /// Version at which this edge was created.
    pub created_version: u64,
}

/// Versioned CSR for the dual-CSR adjacency architecture.
///
/// Stores adjacency with per-edge version metadata, enabling
/// snapshot queries that filter by version without rebuilding.
/// Uses VID raw values as offsets (same as [`CompressedSparseRow`]).
#[derive(Clone)]
pub struct MainCsr {
    /// Offset into entries for vertex i: offsets[i]..offsets[i+1]
    offsets: Vec<u32>,
    /// Flattened entries with version metadata.
    entries: Vec<CsrEdgeEntry>,
}

impl MainCsr {
    /// Creates a MainCsr from versioned edge entries.
    ///
    /// # Arguments
    /// * `max_vid_offset` - Maximum VID offset value
    /// * `entries` - (src_offset, neighbor_vid, eid, created_version) tuples
    pub fn from_edge_entries(max_vid_offset: usize, mut raw: Vec<(u64, Vid, Eid, u64)>) -> Self {
        if raw.is_empty() {
            return Self {
                offsets: vec![0],
                entries: Vec::new(),
            };
        }

        raw.sort_by_key(|(src, _, _, _)| *src);

        let mut offsets = vec![0u32; max_vid_offset + 2];
        let mut entries = Vec::with_capacity(raw.len());

        let mut current_offset = 0u32;
        let mut last_src = 0usize;

        for (src, neighbor_vid, eid, created_version) in raw {
            let src_idx = src as usize;

            if src_idx > last_src {
                for offset in offsets.iter_mut().take(src_idx + 1).skip(last_src + 1) {
                    *offset = current_offset;
                }
            }
            last_src = src_idx;

            entries.push(CsrEdgeEntry {
                neighbor_vid,
                eid,
                created_version,
            });
            current_offset += 1;
        }

        for offset in offsets.iter_mut().skip(last_src + 1) {
            *offset = current_offset;
        }

        Self { offsets, entries }
    }

    /// O(1) versioned entry lookup by VID.
    pub fn get_entries(&self, vid: Vid) -> &[CsrEdgeEntry] {
        let local = vid.as_u64() as usize;
        if local + 1 >= self.offsets.len() {
            return &[];
        }
        let start = self.offsets[local] as usize;
        let end = self.offsets[local + 1] as usize;
        if start >= self.entries.len() || end > self.entries.len() {
            return &[];
        }
        &self.entries[start..end]
    }

    /// O(1) neighbor lookup (ignores version).
    pub fn get_neighbors_unversioned(&self, vid: Vid) -> (Vec<Vid>, Vec<Eid>) {
        let entries = self.get_entries(vid);
        let vids: Vec<Vid> = entries.iter().map(|e| e.neighbor_vid).collect();
        let eids: Vec<Eid> = entries.iter().map(|e| e.eid).collect();
        (vids, eids)
    }

    /// Approximate memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.offsets.len() * 4 + self.entries.len() * std::mem::size_of::<CsrEdgeEntry>()
    }

    /// Returns the number of edges.
    pub fn num_edges(&self) -> usize {
        self.entries.len()
    }

    /// Returns the number of vertices (rows) in the CSR.
    pub fn num_vertices(&self) -> usize {
        if self.offsets.is_empty() {
            0
        } else {
            self.offsets.len() - 1
        }
    }
}

#[cfg(test)]
mod main_csr_tests {
    use super::*;

    #[test]
    fn test_main_csr_basic() {
        let entries = vec![
            (0u64, Vid::new(1), Eid::new(100), 1u64),
            (0u64, Vid::new(2), Eid::new(101), 2u64),
            (1u64, Vid::new(2), Eid::new(102), 1u64),
        ];

        let csr = MainCsr::from_edge_entries(2, entries);

        let e0 = csr.get_entries(Vid::new(0));
        assert_eq!(e0.len(), 2);
        assert_eq!(e0[0].neighbor_vid, Vid::new(1));
        assert_eq!(e0[0].created_version, 1);
        assert_eq!(e0[1].neighbor_vid, Vid::new(2));

        let e1 = csr.get_entries(Vid::new(1));
        assert_eq!(e1.len(), 1);
        assert_eq!(e1[0].eid, Eid::new(102));
    }

    #[test]
    fn test_main_csr_empty() {
        let csr = MainCsr::from_edge_entries(0, vec![]);
        assert_eq!(csr.num_edges(), 0);
        assert_eq!(csr.get_entries(Vid::new(0)).len(), 0);
    }

    #[test]
    fn test_main_csr_get_neighbors() {
        let entries = vec![
            (0u64, Vid::new(10), Eid::new(100), 1u64),
            (0u64, Vid::new(20), Eid::new(101), 2u64),
        ];
        let csr = MainCsr::from_edge_entries(0, entries);
        let (vids, eids) = csr.get_neighbors_unversioned(Vid::new(0));
        assert_eq!(vids, vec![Vid::new(10), Vid::new(20)]);
        assert_eq!(eids, vec![Eid::new(100), Eid::new(101)]);
    }
}
