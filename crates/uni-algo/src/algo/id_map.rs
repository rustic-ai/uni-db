// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Identity mapping between sparse VIDs and dense algorithm slots.
//!
//! Graph algorithms typically require dense integer indices (0..V) for efficient
//! array-based state storage. Uni uses sparse 64-bit VIDs. This module provides
//! bidirectional mapping between these representations.

use fxhash::FxHashMap;
use uni_common::core::id::Vid;

/// Bidirectional mapping between sparse VIDs and dense algorithm slots.
///
/// # Example
///
/// ```ignore
/// let mut id_map = IdMap::new();
/// id_map.insert(Vid::new(100));  // slot 0
/// id_map.insert(Vid::new(200));  // slot 1
///
/// assert_eq!(id_map.to_slot(Vid::new(100)), Some(0));
/// assert_eq!(id_map.to_vid(0), Some(Vid::new(100)));
/// ```
#[derive(Debug, Clone)]
pub struct IdMap {
    /// Dense slot -> Sparse VID
    slot_to_vid: Vec<Vid>,
    /// Sparse VID -> Dense slot (None if compacted)
    vid_to_slot: Option<FxHashMap<Vid, u32>>,
}

impl IdMap {
    /// Create an empty ID map.
    pub fn new() -> Self {
        Self {
            slot_to_vid: Vec::new(),
            vid_to_slot: Some(FxHashMap::default()),
        }
    }

    /// Create an ID map with preallocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slot_to_vid: Vec::with_capacity(capacity),
            vid_to_slot: Some(FxHashMap::with_capacity_and_hasher(
                capacity,
                Default::default(),
            )),
        }
    }

    /// Insert a VID and return its slot.
    ///
    /// If the VID already exists, returns the existing slot.
    /// Panics if the map has been compacted.
    pub fn insert(&mut self, vid: Vid) -> u32 {
        let map = self
            .vid_to_slot
            .as_mut()
            .expect("Cannot insert into compacted IdMap");

        if let Some(&slot) = map.get(&vid) {
            return slot;
        }

        let slot = self.slot_to_vid.len() as u32;
        self.slot_to_vid.push(vid);
        map.insert(vid, slot);
        slot
    }

    /// Get the slot for a VID.
    ///
    /// If compacted, uses binary search (assumes VIDs were inserted in sorted order).
    #[inline]
    pub fn to_slot(&self, vid: Vid) -> Option<u32> {
        if let Some(map) = &self.vid_to_slot {
            map.get(&vid).copied()
        } else {
            self.slot_to_vid.binary_search(&vid).ok().map(|i| i as u32)
        }
    }

    /// Get the VID for a slot.
    #[inline]
    pub fn to_vid(&self, slot: u32) -> Option<Vid> {
        self.slot_to_vid.get(slot as usize).copied()
    }

    /// Get the VID for a slot (panics if out of bounds).
    #[inline]
    pub fn to_vid_unchecked(&self, slot: u32) -> Vid {
        self.slot_to_vid[slot as usize]
    }

    /// Number of mapped vertices.
    #[inline]
    pub fn len(&self) -> usize {
        self.slot_to_vid.len()
    }

    /// Whether the map is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slot_to_vid.is_empty()
    }

    /// Check if a VID is in the map.
    #[inline]
    pub fn contains(&self, vid: Vid) -> bool {
        self.to_slot(vid).is_some()
    }

    /// Compact the map by dropping the hash map.
    ///
    /// **Warning:** This assumes that VIDs were inserted in sorted order.
    /// If not, `to_slot` will return incorrect results (binary search fails).
    pub fn compact(&mut self) {
        // Optional: verify sort?
        // if !self.slot_to_vid.windows(2).all(|w| w[0] <= w[1]) {
        //     log::warn!("Compacting unsorted IdMap - lookup will fail!");
        // }
        self.vid_to_slot = None;
    }

    /// Iterate over all (slot, vid) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u32, Vid)> + '_ {
        self.slot_to_vid
            .iter()
            .enumerate()
            .map(|(slot, &vid)| (slot as u32, vid))
    }

    /// Memory usage in bytes.
    pub fn memory_size(&self) -> usize {
        let map_size = self.vid_to_slot.as_ref().map_or(0, |m| {
            m.len() * (std::mem::size_of::<Vid>() + std::mem::size_of::<u32>() + 8)
        });
        self.slot_to_vid.len() * std::mem::size_of::<Vid>() + map_size
    }
}

impl Default for IdMap {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<Vid> for IdMap {
    fn from_iter<I: IntoIterator<Item = Vid>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, upper) = iter.size_hint();
        let mut map = Self::with_capacity(upper.unwrap_or(lower));

        for vid in iter {
            map.insert(vid);
        }

        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_lookup() {
        let mut map = IdMap::new();

        let vid1 = Vid::new(100);
        let vid2 = Vid::new(200);
        let vid3 = Vid::new(50);

        assert_eq!(map.insert(vid1), 0);
        assert_eq!(map.insert(vid2), 1);
        assert_eq!(map.insert(vid3), 2);

        // Duplicate insert returns same slot
        assert_eq!(map.insert(vid1), 0);

        assert_eq!(map.to_slot(vid1), Some(0));
        assert_eq!(map.to_slot(vid2), Some(1));
        assert_eq!(map.to_slot(vid3), Some(2));

        assert_eq!(map.to_vid(0), Some(vid1));
        assert_eq!(map.to_vid(1), Some(vid2));
        assert_eq!(map.to_vid(2), Some(vid3));
        assert_eq!(map.to_vid(3), None);
    }

    #[test]
    fn test_compact() {
        let mut map = IdMap::new();
        // Insert sorted
        let vid1 = Vid::new(100);
        let vid2 = Vid::new(200);

        map.insert(vid1);
        map.insert(vid2);

        assert!(map.vid_to_slot.is_some());

        map.compact();
        assert!(map.vid_to_slot.is_none());

        assert_eq!(map.to_slot(vid1), Some(0));
        assert_eq!(map.to_slot(vid2), Some(1));
        assert_eq!(map.to_slot(Vid::new(150)), None);
    }
}
