// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bidirectional mapping between sparse VIDs and dense array indices.
//!
//! During query execution, we load subgraphs into memory. VIDs are sparse
//! (auto-increment with gaps from deletions), but we want O(1) array access.
//! VidRemapper provides this mapping.

use std::collections::HashMap;
use uni_common::core::id::{DenseIdx, Vid};

/// Bidirectional mapping between sparse Vid and dense array indices.
///
/// Used during subgraph loading and query execution to enable O(1) array
/// access while preserving the ability to convert back to VIDs for results.
#[derive(Debug, Clone, Default)]
pub struct VidRemapper {
    /// Sparse VID → dense index
    vid_to_dense: HashMap<Vid, DenseIdx>,
    /// Dense index → sparse VID (O(1) reverse lookup)
    dense_to_vid: Vec<Vid>,
}

impl VidRemapper {
    /// Creates a new empty remapper.
    pub fn new() -> Self {
        Self {
            vid_to_dense: HashMap::new(),
            dense_to_vid: Vec::new(),
        }
    }

    /// Creates a remapper with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vid_to_dense: HashMap::with_capacity(capacity),
            dense_to_vid: Vec::with_capacity(capacity),
        }
    }

    /// Assigns a dense index to a VID (during subgraph load).
    ///
    /// If the VID is already mapped, returns the existing index.
    /// Otherwise, assigns the next available index.
    pub fn insert(&mut self, vid: Vid) -> DenseIdx {
        if let Some(&idx) = self.vid_to_dense.get(&vid) {
            return idx;
        }
        let idx = DenseIdx::new(self.dense_to_vid.len() as u32);
        self.dense_to_vid.push(vid);
        self.vid_to_dense.insert(vid, idx);
        idx
    }

    /// Inserts multiple VIDs at once, returning their dense indices.
    pub fn insert_many(&mut self, vids: &[Vid]) -> Vec<DenseIdx> {
        vids.iter().map(|&vid| self.insert(vid)).collect()
    }

    /// VID → DenseIdx (for array indexing)
    ///
    /// Returns None if the VID is not mapped.
    pub fn to_dense(&self, vid: Vid) -> Option<DenseIdx> {
        self.vid_to_dense.get(&vid).copied()
    }

    /// VID → DenseIdx, panicking if not found.
    ///
    /// Use when you're certain the VID was already inserted.
    pub fn to_dense_unchecked(&self, vid: Vid) -> DenseIdx {
        self.vid_to_dense[&vid]
    }

    /// DenseIdx → VID (for returning results)
    ///
    /// Panics if the index is out of bounds.
    pub fn to_vid(&self, idx: DenseIdx) -> Vid {
        self.dense_to_vid[idx.as_usize()]
    }

    /// DenseIdx → VID, returning None if out of bounds.
    pub fn to_vid_opt(&self, idx: DenseIdx) -> Option<Vid> {
        self.dense_to_vid.get(idx.as_usize()).copied()
    }

    /// Returns the number of mapped VIDs.
    pub fn len(&self) -> usize {
        self.dense_to_vid.len()
    }

    /// Returns true if no VIDs are mapped.
    pub fn is_empty(&self) -> bool {
        self.dense_to_vid.is_empty()
    }

    /// Checks if a VID is already mapped.
    pub fn contains(&self, vid: Vid) -> bool {
        self.vid_to_dense.contains_key(&vid)
    }

    /// Returns an iterator over all (DenseIdx, Vid) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (DenseIdx, Vid)> + '_ {
        self.dense_to_vid
            .iter()
            .enumerate()
            .map(|(i, &vid)| (DenseIdx::new(i as u32), vid))
    }

    /// Returns an iterator over all VIDs in dense order.
    pub fn vids(&self) -> impl Iterator<Item = Vid> + '_ {
        self.dense_to_vid.iter().copied()
    }

    /// Returns a slice of all VIDs in dense order.
    pub fn vids_slice(&self) -> &[Vid] {
        &self.dense_to_vid
    }

    /// Clears all mappings.
    pub fn clear(&mut self) {
        self.vid_to_dense.clear();
        self.dense_to_vid.clear();
    }

    /// Memory usage in bytes (approximate).
    pub fn memory_usage(&self) -> usize {
        // HashMap overhead + entries + Vec capacity
        self.vid_to_dense.capacity()
            * (std::mem::size_of::<Vid>() + std::mem::size_of::<DenseIdx>())
            + self.dense_to_vid.capacity() * std::mem::size_of::<Vid>()
    }
}

/// Bidirectional mapping between sparse EIDs and dense array indices.
///
/// Similar to VidRemapper but for edge IDs.
#[derive(Debug, Clone, Default)]
pub struct EidRemapper {
    eid_to_dense: HashMap<uni_common::core::id::Eid, DenseIdx>,
    dense_to_eid: Vec<uni_common::core::id::Eid>,
}

impl EidRemapper {
    pub fn new() -> Self {
        Self {
            eid_to_dense: HashMap::new(),
            dense_to_eid: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            eid_to_dense: HashMap::with_capacity(capacity),
            dense_to_eid: Vec::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, eid: uni_common::core::id::Eid) -> DenseIdx {
        if let Some(&idx) = self.eid_to_dense.get(&eid) {
            return idx;
        }
        let idx = DenseIdx::new(self.dense_to_eid.len() as u32);
        self.dense_to_eid.push(eid);
        self.eid_to_dense.insert(eid, idx);
        idx
    }

    pub fn to_dense(&self, eid: uni_common::core::id::Eid) -> Option<DenseIdx> {
        self.eid_to_dense.get(&eid).copied()
    }

    pub fn to_eid(&self, idx: DenseIdx) -> uni_common::core::id::Eid {
        self.dense_to_eid[idx.as_usize()]
    }

    pub fn len(&self) -> usize {
        self.dense_to_eid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.dense_to_eid.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vid_remapper_basic() {
        let mut remapper = VidRemapper::new();

        let vid1 = Vid::new(100);
        let vid2 = Vid::new(500);
        let vid3 = Vid::new(200);

        let idx1 = remapper.insert(vid1);
        let idx2 = remapper.insert(vid2);
        let idx3 = remapper.insert(vid3);

        assert_eq!(idx1.as_u32(), 0);
        assert_eq!(idx2.as_u32(), 1);
        assert_eq!(idx3.as_u32(), 2);

        assert_eq!(remapper.to_vid(idx1), vid1);
        assert_eq!(remapper.to_vid(idx2), vid2);
        assert_eq!(remapper.to_vid(idx3), vid3);

        assert_eq!(remapper.to_dense(vid1), Some(idx1));
        assert_eq!(remapper.to_dense(vid2), Some(idx2));
        assert_eq!(remapper.to_dense(Vid::new(999)), None);
    }

    #[test]
    fn test_vid_remapper_duplicate_insert() {
        let mut remapper = VidRemapper::new();
        let vid = Vid::new(42);

        let idx1 = remapper.insert(vid);
        let idx2 = remapper.insert(vid);

        assert_eq!(idx1, idx2);
        assert_eq!(remapper.len(), 1);
    }

    #[test]
    fn test_vid_remapper_insert_many() {
        let mut remapper = VidRemapper::new();
        let vids = vec![Vid::new(10), Vid::new(20), Vid::new(30)];

        let indices = remapper.insert_many(&vids);

        assert_eq!(indices.len(), 3);
        assert_eq!(remapper.len(), 3);
        for (idx, vid) in indices.iter().zip(vids.iter()) {
            assert_eq!(remapper.to_vid(*idx), *vid);
        }
    }

    #[test]
    fn test_vid_remapper_iter() {
        let mut remapper = VidRemapper::new();
        remapper.insert(Vid::new(100));
        remapper.insert(Vid::new(200));
        remapper.insert(Vid::new(300));

        let pairs: Vec<_> = remapper.iter().collect();
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (DenseIdx::new(0), Vid::new(100)));
        assert_eq!(pairs[1], (DenseIdx::new(1), Vid::new(200)));
        assert_eq!(pairs[2], (DenseIdx::new(2), Vid::new(300)));
    }
}
