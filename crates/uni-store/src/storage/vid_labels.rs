// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! VID-to-Labels index for the new storage model.
//!
//! In the new storage design, VIDs no longer embed label information.
//! This index provides efficient lookups from VID to labels and vice versa.
//! It's rebuilt from the main vertex table on startup.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uni_common::core::id::Vid;

/// VIDs per shard, as a power of two: shard `vid >> SHARD_BITS`.
///
/// VIDs are allocated in sequence, so a flush's new vertices land in a few
/// adjacent shards.
const SHARD_BITS: u32 = 12;

type Shard = HashMap<Vid, Vec<String>>;

/// In-memory index mapping VIDs to their labels.
///
/// This index is rebuilt from the main vertex table on database open.
/// It provides O(1) lookups of a VID's labels and the set of labels present.
///
/// Copy-on-write by shard: every transaction and fork takes its own copy of
/// the index when it begins, so a clone must not cost a pass over every
/// vertex. A clone shares each shard (one `Arc` per block of
/// `1 << SHARD_BITS` VIDs); a write copies only the shard it touches, and only
/// while some clone still shares it.
#[derive(Debug, Clone, Default)]
pub struct VidLabelsIndex {
    /// VID to labels, by shard.
    shards: HashMap<u64, Arc<Shard>>,
    /// How many indexed VIDs carry each label; a label with none is absent.
    label_counts: Arc<HashMap<String, usize>>,
    /// Number of indexed VIDs.
    len: usize,
}

impl VidLabelsIndex {
    /// Creates a new empty index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an index sized for `num_vertices` VIDs and `num_labels` labels.
    pub fn with_capacity(num_vertices: usize, num_labels: usize) -> Self {
        Self {
            shards: HashMap::with_capacity((num_vertices >> SHARD_BITS) + 1),
            label_counts: Arc::new(HashMap::with_capacity(num_labels)),
            len: 0,
        }
    }

    fn shard_of(vid: Vid) -> u64 {
        vid.as_u64() >> SHARD_BITS
    }

    fn entry(&self, vid: Vid) -> Option<&Vec<String>> {
        self.shards.get(&Self::shard_of(vid))?.get(&vid)
    }

    /// The shard holding `vid`, unshared from every clone.
    fn shard_mut(&mut self, vid: Vid) -> &mut Shard {
        Arc::make_mut(self.shards.entry(Self::shard_of(vid)).or_default())
    }

    fn count_labels(&mut self, labels: &[String], added: bool) {
        let counts = Arc::make_mut(&mut self.label_counts);
        for label in labels {
            if added {
                *counts.entry(label.clone()).or_insert(0) += 1;
            } else if let Some(n) = counts.get_mut(label) {
                *n -= 1;
                if *n == 0 {
                    counts.remove(label);
                }
            }
        }
    }

    /// Inserts a VID with its labels.
    ///
    /// If the VID already exists, its labels are replaced.
    pub fn insert(&mut self, vid: Vid, labels: Vec<String>) {
        self.count_labels(&labels, true);
        match self.shard_mut(vid).insert(vid, labels) {
            Some(old) => self.count_labels(&old, false),
            None => self.len += 1,
        }
    }

    /// Adds a label to an existing VID.
    ///
    /// If the VID doesn't exist, creates it with just this label.
    pub fn add_label(&mut self, vid: Vid, label: String) {
        if self.has_label(vid, &label) {
            return;
        }
        let shard = self.shard_mut(vid);
        let is_new = !shard.contains_key(&vid);
        shard.entry(vid).or_default().push(label.clone());
        if is_new {
            self.len += 1;
        }
        self.count_labels(&[label], true);
    }

    /// Removes a label from a VID.
    ///
    /// Returns true if the label was removed, false if it wasn't present.
    pub fn remove_label(&mut self, vid: Vid, label: &str) -> bool {
        if !self.has_label(vid, label) {
            return false;
        }
        if let Some(labels) = self.shard_mut(vid).get_mut(&vid)
            && let Some(pos) = labels.iter().position(|l| l == label)
        {
            let removed = labels.remove(pos);
            self.count_labels(&[removed], false);
        }
        true
    }

    /// Removes a VID entirely from the index.
    pub fn remove_vid(&mut self, vid: Vid) {
        if self.entry(vid).is_none() {
            return;
        }
        if let Some(labels) = self.shard_mut(vid).remove(&vid) {
            self.len -= 1;
            self.count_labels(&labels, false);
        }
    }

    /// Gets the labels for a VID.
    pub fn get_labels(&self, vid: Vid) -> Option<&[String]> {
        self.entry(vid).map(Vec::as_slice)
    }

    /// Checks if a VID has a specific label.
    pub fn has_label(&self, vid: Vid, label: &str) -> bool {
        self.entry(vid)
            .is_some_and(|labels| labels.iter().any(|l| l == label))
    }

    /// Checks if a VID has all the specified labels.
    pub fn has_all_labels(&self, vid: Vid, required_labels: &[&str]) -> bool {
        self.entry(vid).is_some_and(|labels| {
            required_labels
                .iter()
                .all(|req| labels.iter().any(|l| l == *req))
        })
    }

    /// Gets all VIDs with a specific label. A pass over the index.
    pub fn get_vids_with_label(&self, label: &str) -> HashSet<Vid> {
        self.get_vids_with_all_labels(&[label])
    }

    /// Gets all VIDs that have ALL the specified labels. A pass over the index.
    pub fn get_vids_with_all_labels(&self, labels: &[&str]) -> HashSet<Vid> {
        if labels.is_empty() || labels.iter().any(|l| !self.label_counts.contains_key(*l)) {
            return HashSet::new();
        }
        self.iter()
            .filter(|(_, have)| labels.iter().all(|l| have.iter().any(|h| h == l)))
            .map(|(vid, _)| vid)
            .collect()
    }

    /// Returns the total number of vertices in the index.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of distinct labels.
    pub fn label_count(&self) -> usize {
        self.label_counts.len()
    }

    /// Returns an iterator over all known labels.
    pub fn labels(&self) -> impl Iterator<Item = &str> {
        self.label_counts.keys().map(String::as_str)
    }

    /// Returns an iterator over all (VID, labels) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (Vid, &[String])> {
        self.shards
            .values()
            .flat_map(|shard| shard.iter().map(|(&vid, labels)| (vid, labels.as_slice())))
    }

    /// Clears the index.
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// Approximate memory usage in bytes (counting shared shards in full).
    pub fn memory_usage(&self) -> usize {
        let entries: usize = self.shards.values().map(|shard| shard.capacity()).sum();
        entries * (std::mem::size_of::<Vid>() + std::mem::size_of::<Vec<String>>())
            + self.label_counts.capacity()
                * (std::mem::size_of::<String>() + std::mem::size_of::<usize>())
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

        let persons = index.get_vids_with_label("Person");
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

    /// A clone is a snapshot: writes to either side never reach the other,
    /// and the clone shares every shard until one is written.
    #[test]
    fn test_clone_is_an_isolated_snapshot() {
        let mut live = VidLabelsIndex::new();
        for i in 0..10_000 {
            live.insert(Vid::new(i), vec!["A".to_string()]);
        }
        let pinned = live.clone();
        let shared = |a: &VidLabelsIndex, b: &VidLabelsIndex| {
            a.shards
                .iter()
                .filter(|(k, s)| b.shards.get(*k).is_some_and(|t| Arc::ptr_eq(s, t)))
                .count()
        };
        assert_eq!(shared(&live, &pinned), live.shards.len());

        live.insert(Vid::new(5), vec!["B".to_string()]);
        live.remove_vid(Vid::new(6));
        live.insert(Vid::new(20_000), vec!["C".to_string()]);

        assert_eq!(pinned.get_labels(Vid::new(5)), Some(&["A".to_string()][..]));
        assert!(pinned.has_label(Vid::new(6), "A"));
        assert!(pinned.get_labels(Vid::new(20_000)).is_none());
        assert_eq!(pinned.len(), 10_000);
        assert_eq!(pinned.labels().collect::<Vec<_>>(), ["A"]);

        assert_eq!(live.get_labels(Vid::new(5)), Some(&["B".to_string()][..]));
        assert!(live.get_labels(Vid::new(6)).is_none());
        assert_eq!(live.len(), 10_000);
        let mut labels: Vec<_> = live.labels().collect();
        labels.sort_unstable();
        assert_eq!(labels, ["A", "B", "C"]);
        // Only the written shards were copied.
        assert_eq!(shared(&live, &pinned), pinned.shards.len() - 1);
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
        assert!(index.get_vids_with_label("Person").is_empty());
        assert_eq!(index.label_count(), 0);
    }
}
