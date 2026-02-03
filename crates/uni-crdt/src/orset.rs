// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::CrdtMerge;
use fxhash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use uuid::Uuid;

/// An Observed-Remove Set (ORSet).
///
/// Supports both add and remove operations. Uses unique tags to track element provenance.
/// Conflict resolution: Add-wins (concurrent add + remove results in the element being present).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ORSet<T: Hash + Eq + Clone> {
    /// element -> {tags}
    elements: FxHashMap<T, FxHashSet<Uuid>>,
    /// removed tags
    tombstones: FxHashSet<Uuid>,
}

impl<T: Hash + Eq + Clone> Default for ORSet<T> {
    fn default() -> Self {
        Self {
            elements: FxHashMap::default(),
            tombstones: FxHashSet::default(),
        }
    }
}

impl<T: Hash + Eq + Clone> ORSet<T> {
    /// Create a new, empty ORSet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an element to the set. Returns the generated tag.
    pub fn add(&mut self, element: T) -> Uuid {
        let tag = Uuid::new_v4();
        self.elements.entry(element).or_default().insert(tag);
        tag
    }

    /// Remove an element from the set.
    /// All currently observed tags for this element are added to the tombstones.
    pub fn remove(&mut self, element: &T) {
        if let Some(tags) = self.elements.get(element) {
            for tag in tags {
                self.tombstones.insert(*tag);
            }
        }
    }

    /// Check if an element is in the set.
    /// An element is present if it has at least one tag that hasn't been tombstoned.
    pub fn contains(&self, element: &T) -> bool {
        if let Some(tags) = self.elements.get(element) {
            tags.iter().any(|tag| !self.tombstones.contains(tag))
        } else {
            false
        }
    }

    /// Returns a vector of all visible elements in the set.
    pub fn elements(&self) -> Vec<T> {
        self.elements
            .iter()
            .filter(|(_, tags)| tags.iter().any(|tag| !self.tombstones.contains(tag)))
            .map(|(elem, _)| elem.clone())
            .collect()
    }

    /// Returns the number of visible elements.
    pub fn len(&self) -> usize {
        self.elements
            .values()
            .filter(|tags| tags.iter().any(|tag| !self.tombstones.contains(tag)))
            .count()
    }

    /// Returns true if the set has no visible elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Hash + Eq + Clone> CrdtMerge for ORSet<T> {
    fn merge(&mut self, other: &Self) {
        // Merge elements and their tags
        for (element, other_tags) in &other.elements {
            let tags = self.elements.entry(element.clone()).or_default();
            for tag in other_tags {
                tags.insert(*tag);
            }
        }
        // Merge tombstones
        for tag in &other.tombstones {
            self.tombstones.insert(*tag);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove() {
        let mut os = ORSet::new();
        os.add("apple".to_string());
        assert!(os.contains(&"apple".to_string()));

        os.remove(&"apple".to_string());
        assert!(!os.contains(&"apple".to_string()));
    }

    #[test]
    fn test_add_wins() {
        let mut a = ORSet::new();
        a.add("apple".to_string());

        let mut b = a.clone();
        b.remove(&"apple".to_string());

        // Concurrent add on 'a'
        a.add("apple".to_string());

        a.merge(&b);

        // Add wins because the new tag in 'a' was not tombstoned in 'b'
        assert!(a.contains(&"apple".to_string()));
    }

    #[test]
    fn test_merge() {
        let mut a = ORSet::new();
        a.add(1);
        a.add(2);

        let mut b = ORSet::new();
        b.add(2);
        b.add(3);

        a.merge(&b);

        let elements = a.elements();
        assert!(elements.contains(&1));
        assert!(elements.contains(&2));
        assert!(elements.contains(&3));
        assert_eq!(elements.len(), 3);
    }
}
