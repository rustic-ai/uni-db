// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::CrdtMerge;
use fxhash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

/// A Grow-only Set (GSet).
///
/// Elements can only be added, never removed.
/// Merging takes the union of two sets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GSet<T: Hash + Eq + Clone> {
    elements: FxHashSet<T>,
}

impl<T: Hash + Eq + Clone> Default for GSet<T> {
    fn default() -> Self {
        Self {
            elements: FxHashSet::default(),
        }
    }
}

impl<T: Hash + Eq + Clone> GSet<T> {
    /// Create a new, empty GSet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an element to the set.
    pub fn add(&mut self, element: T) {
        self.elements.insert(element);
    }

    /// Check if an element is in the set.
    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains(element)
    }

    /// Returns an iterator over all elements in the set.
    pub fn elements(&self) -> impl Iterator<Item = &T> {
        self.elements.iter()
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns true if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }
}

impl<T: Hash + Eq + Clone> CrdtMerge for GSet<T> {
    fn merge(&mut self, other: &Self) {
        for element in &other.elements {
            self.elements.insert(element.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let mut gs = GSet::new();
        gs.add("apple".to_string());
        gs.add("banana".to_string());
        assert!(gs.contains(&"apple".to_string()));
        assert!(gs.contains(&"banana".to_string()));
        assert!(!gs.contains(&"cherry".to_string()));
        assert_eq!(gs.len(), 2);
    }

    #[test]
    fn test_merge() {
        let mut a = GSet::new();
        a.add(1);
        a.add(2);

        let mut b = GSet::new();
        b.add(2);
        b.add(3);

        a.merge(&b);

        assert!(a.contains(&1));
        assert!(a.contains(&2));
        assert!(a.contains(&3));
        assert_eq!(a.len(), 3);
    }
}
