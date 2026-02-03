// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! VID-to-Labels index for the new storage model.
//!
//! In the new storage design, VIDs no longer embed label information.
//! This index provides efficient lookups from VID to labels and vice versa.
//! It's rebuilt from the main vertex table on startup.

use std::collections::{HashMap, HashSet};
use uni_common::core::id::Vid;

/// In-memory index mapping VIDs to their labels.
///
/// This index is rebuilt from the main vertex table on database open.
/// It provides O(1) lookups for:
/// - VID → labels (which labels does this vertex have?)
/// - label → VIDs (which vertices have this label?)
#[derive(Debug, Clone, Default)]
pub struct VidLabelsIndex {
    /// VID to labels mapping
    vid_to_labels: HashMap<Vid, Vec<String>>,
    /// Label to VIDs mapping (for label scans)
    label_to_vids: HashMap<String, HashSet<Vid>>,
}

impl VidLabelsIndex {
    /// Creates a new empty index.
    pub fn new() -> Self {
        Self {
            vid_to_labels: HashMap::new(),
            label_to_vids: HashMap::new(),
        }
    }

    /// Creates an index with pre-allocated capacity.
    pub fn with_capacity(num_vertices: usize, num_labels: usize) -> Self {
        Self {
            vid_to_labels: HashMap::with_capacity(num_vertices),
            label_to_vids: HashMap::with_capacity(num_labels),
        }
    }

    /// Inserts a VID with its labels.
    ///
    /// If the VID already exists, its labels are replaced.
    pub fn insert(&mut self, vid: Vid, labels: Vec<String>) {
        // Remove old label mappings if this VID exists
        if let Some(old_labels) = self.vid_to_labels.get(&vid) {
            for label in old_labels {
                if let Some(vids) = self.label_to_vids.get_mut(label) {
                    vids.remove(&vid);
                }
            }
        }

        // Add new label mappings
        for label in &labels {
            self.label_to_vids
                .entry(label.clone())
                .or_default()
                .insert(vid);
        }

        self.vid_to_labels.insert(vid, labels);
    }

    /// Adds a label to an existing VID.
    ///
    /// If the VID doesn't exist, creates it with just this label.
    pub fn add_label(&mut self, vid: Vid, label: String) {
        // Add to vid_to_labels
        let labels = self.vid_to_labels.entry(vid).or_default();
        if !labels.contains(&label) {
            labels.push(label.clone());
        }

        // Add to label_to_vids
        self.label_to_vids.entry(label).or_default().insert(vid);
    }

    /// Removes a label from a VID.
    ///
    /// Returns true if the label was removed, false if it wasn't present.
    pub fn remove_label(&mut self, vid: Vid, label: &str) -> bool {
        let removed = if let Some(labels) = self.vid_to_labels.get_mut(&vid) {
            if let Some(pos) = labels.iter().position(|l| l == label) {
                labels.remove(pos);
                true
            } else {
                false
            }
        } else {
            false
        };

        if removed && let Some(vids) = self.label_to_vids.get_mut(label) {
            vids.remove(&vid);
        }

        removed
    }

    /// Removes a VID entirely from the index.
    pub fn remove_vid(&mut self, vid: Vid) {
        if let Some(labels) = self.vid_to_labels.remove(&vid) {
            for label in labels {
                if let Some(vids) = self.label_to_vids.get_mut(&label) {
                    vids.remove(&vid);
                }
            }
        }
    }

    /// Gets the labels for a VID.
    pub fn get_labels(&self, vid: Vid) -> Option<&[String]> {
        self.vid_to_labels.get(&vid).map(|v| v.as_slice())
    }

    /// Checks if a VID has a specific label.
    pub fn has_label(&self, vid: Vid, label: &str) -> bool {
        self.vid_to_labels
            .get(&vid)
            .map(|labels| labels.iter().any(|l| l == label))
            .unwrap_or(false)
    }

    /// Checks if a VID has all the specified labels.
    pub fn has_all_labels(&self, vid: Vid, required_labels: &[&str]) -> bool {
        if let Some(labels) = self.vid_to_labels.get(&vid) {
            required_labels
                .iter()
                .all(|req| labels.iter().any(|l| l == *req))
        } else {
            false
        }
    }

    /// Gets all VIDs with a specific label.
    pub fn get_vids_with_label(&self, label: &str) -> Option<&HashSet<Vid>> {
        self.label_to_vids.get(label)
    }

    /// Gets all VIDs that have ALL the specified labels.
    pub fn get_vids_with_all_labels(&self, labels: &[&str]) -> HashSet<Vid> {
        if labels.is_empty() {
            return HashSet::new();
        }

        // Start with the smallest set for efficiency
        let mut sets: Vec<_> = labels
            .iter()
            .filter_map(|label| self.label_to_vids.get(*label))
            .collect();

        if sets.is_empty() {
            return HashSet::new();
        }

        // Sort by size (smallest first)
        sets.sort_by_key(|s| s.len());

        // Intersect all sets
        let mut result = sets[0].clone();
        for set in sets.iter().skip(1) {
            result.retain(|vid| set.contains(vid));
            if result.is_empty() {
                break;
            }
        }

        result
    }

    /// Returns the total number of vertices in the index.
    pub fn len(&self) -> usize {
        self.vid_to_labels.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.vid_to_labels.is_empty()
    }

    /// Returns the number of distinct labels.
    pub fn label_count(&self) -> usize {
        self.label_to_vids.len()
    }

    /// Returns an iterator over all known labels.
    pub fn labels(&self) -> impl Iterator<Item = &str> {
        self.label_to_vids.keys().map(|s| s.as_str())
    }

    /// Returns an iterator over all (VID, labels) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (Vid, &[String])> {
        self.vid_to_labels
            .iter()
            .map(|(&vid, labels)| (vid, labels.as_slice()))
    }

    /// Clears the index.
    pub fn clear(&mut self) {
        self.vid_to_labels.clear();
        self.label_to_vids.clear();
    }

    /// Approximate memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        let vid_to_labels_size = self.vid_to_labels.capacity()
            * (std::mem::size_of::<Vid>() + std::mem::size_of::<Vec<String>>());
        let label_to_vids_size = self.label_to_vids.capacity()
            * (std::mem::size_of::<String>() + std::mem::size_of::<HashSet<Vid>>());
        vid_to_labels_size + label_to_vids_size
    }
}

/// Index mapping edge types to their EIDs.
///
/// Similar to VidLabelsIndex but for edges. Since edges have a single type
/// (not multi-label like vertices), this is simpler.
#[derive(Debug, Clone, Default)]
pub struct EidTypeIndex {
    /// EID to type mapping
    eid_to_type: HashMap<uni_common::core::id::Eid, String>,
    /// Type to EIDs mapping
    type_to_eids: HashMap<String, HashSet<uni_common::core::id::Eid>>,
}

impl EidTypeIndex {
    pub fn new() -> Self {
        Self {
            eid_to_type: HashMap::new(),
            type_to_eids: HashMap::new(),
        }
    }

    pub fn insert(&mut self, eid: uni_common::core::id::Eid, edge_type: String) {
        // Remove old type mapping if this EID exists
        if let Some(old_type) = self.eid_to_type.get(&eid)
            && let Some(eids) = self.type_to_eids.get_mut(old_type)
        {
            eids.remove(&eid);
        }

        self.type_to_eids
            .entry(edge_type.clone())
            .or_default()
            .insert(eid);
        self.eid_to_type.insert(eid, edge_type);
    }

    pub fn get_type(&self, eid: uni_common::core::id::Eid) -> Option<&str> {
        self.eid_to_type.get(&eid).map(|s| s.as_str())
    }

    pub fn get_eids_with_type(
        &self,
        edge_type: &str,
    ) -> Option<&HashSet<uni_common::core::id::Eid>> {
        self.type_to_eids.get(edge_type)
    }

    pub fn len(&self) -> usize {
        self.eid_to_type.len()
    }

    pub fn is_empty(&self) -> bool {
        self.eid_to_type.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vid_labels_basic() {
        let mut index = VidLabelsIndex::new();

        let vid1 = Vid::new(1);
        let vid2 = Vid::new(2);
        let vid3 = Vid::new(3);

        index.insert(vid1, vec!["Person".to_string()]);
        index.insert(vid2, vec!["Person".to_string(), "Employee".to_string()]);
        index.insert(vid3, vec!["Company".to_string()]);

        assert_eq!(index.get_labels(vid1), Some(&["Person".to_string()][..]));
        assert_eq!(
            index.get_labels(vid2),
            Some(&["Person".to_string(), "Employee".to_string()][..])
        );
        assert!(index.has_label(vid1, "Person"));
        assert!(!index.has_label(vid1, "Employee"));
        assert!(index.has_all_labels(vid2, &["Person", "Employee"]));
    }

    #[test]
    fn test_get_vids_with_label() {
        let mut index = VidLabelsIndex::new();

        index.insert(Vid::new(1), vec!["Person".to_string()]);
        index.insert(
            Vid::new(2),
            vec!["Person".to_string(), "Employee".to_string()],
        );
        index.insert(Vid::new(3), vec!["Company".to_string()]);

        let persons = index.get_vids_with_label("Person").unwrap();
        assert_eq!(persons.len(), 2);
        assert!(persons.contains(&Vid::new(1)));
        assert!(persons.contains(&Vid::new(2)));
    }

    #[test]
    fn test_get_vids_with_all_labels() {
        let mut index = VidLabelsIndex::new();

        index.insert(Vid::new(1), vec!["Person".to_string()]);
        index.insert(
            Vid::new(2),
            vec!["Person".to_string(), "Employee".to_string()],
        );
        index.insert(
            Vid::new(3),
            vec![
                "Person".to_string(),
                "Employee".to_string(),
                "Manager".to_string(),
            ],
        );

        let result = index.get_vids_with_all_labels(&["Person", "Employee"]);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&Vid::new(2)));
        assert!(result.contains(&Vid::new(3)));
    }

    #[test]
    fn test_add_remove_label() {
        let mut index = VidLabelsIndex::new();
        let vid = Vid::new(1);

        index.insert(vid, vec!["Person".to_string()]);
        assert!(index.has_label(vid, "Person"));

        index.add_label(vid, "Employee".to_string());
        assert!(index.has_label(vid, "Person"));
        assert!(index.has_label(vid, "Employee"));

        index.remove_label(vid, "Person");
        assert!(!index.has_label(vid, "Person"));
        assert!(index.has_label(vid, "Employee"));
    }

    #[test]
    fn test_remove_vid() {
        let mut index = VidLabelsIndex::new();
        let vid = Vid::new(1);

        index.insert(vid, vec!["Person".to_string(), "Employee".to_string()]);
        assert_eq!(index.len(), 1);

        index.remove_vid(vid);
        assert_eq!(index.len(), 0);
        assert!(index.get_labels(vid).is_none());
        assert!(
            index.get_vids_with_label("Person").is_none()
                || index.get_vids_with_label("Person").unwrap().is_empty()
        );
    }
}
