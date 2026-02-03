// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::CrdtMerge;
use fxhash::FxHashMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A node in the Replicated Growable Array (RGA).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RgaNode<T: Clone> {
    pub id: Uuid,
    pub elem: T,
    /// The ID of the node to the left of this node when it was inserted.
    pub origin_left: Option<Uuid>,
    pub tombstone: bool,
    pub timestamp: i64,
}

/// A Replicated Growable Array (RGA).
///
/// An ordered sequence supporting insertion and deletion at any position.
/// Used for collaborative text editing and other ordered collections.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Rga<T: Clone> {
    nodes: FxHashMap<Uuid, RgaNode<T>>,
}

impl<T: Clone> Default for Rga<T> {
    fn default() -> Self {
        Self {
            nodes: FxHashMap::default(),
        }
    }
}

impl<T: Clone> Rga<T> {
    /// Create a new, empty RGA.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an element after the node with `prev_id`.
    /// If `prev_id` is None, insert at the beginning.
    pub fn insert(&mut self, prev_id: Option<Uuid>, elem: T, timestamp: i64) -> Uuid {
        let id = Uuid::new_v4();
        let node = RgaNode {
            id,
            elem,
            origin_left: prev_id,
            tombstone: false,
            timestamp,
        };
        self.nodes.insert(id, node);
        id
    }

    /// Delete the node with the given ID (marks as tombstone).
    pub fn delete(&mut self, id: Uuid) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.tombstone = true;
        }
    }

    /// Convert the RGA to a vector of elements in their logical order.
    pub fn to_vec(&self) -> Vec<T> {
        let mut result = Vec::with_capacity(self.nodes.len());
        let mut children: FxHashMap<Option<Uuid>, Vec<&RgaNode<T>>> = FxHashMap::default();

        for node in self.nodes.values() {
            children.entry(node.origin_left).or_default().push(node);
        }

        // Sort children by (timestamp DESC, id DESC) to ensure deterministic order
        for list in children.values_mut() {
            list.sort_by(|a, b| b.timestamp.cmp(&a.timestamp).then_with(|| b.id.cmp(&a.id)));
        }

        // Iterative traversal with explicit stack to prevent stack overflow
        // Stack holds: (node_id_option, child_index)
        let mut stack: Vec<(Option<Uuid>, usize)> = vec![(None, 0)];

        while let Some((current, idx)) = stack.pop() {
            if let Some(child_list) = children.get(&current).filter(|list| idx < list.len()) {
                // Re-push current with next index to process siblings later
                stack.push((current, idx + 1));

                let child = child_list[idx];
                if !child.tombstone {
                    result.push(child.elem.clone());
                }

                // Push child to process its subtree (start at index 0)
                stack.push((Some(child.id), 0));
            }
        }

        result
    }

    /// Returns the number of visible elements.
    pub fn len(&self) -> usize {
        self.nodes.values().filter(|n| !n.tombstone).count()
    }

    /// Returns true if the RGA has no visible elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T: Clone> CrdtMerge for Rga<T> {
    fn merge(&mut self, other: &Self) {
        for (id, other_node) in &other.nodes {
            match self.nodes.get_mut(id) {
                Some(node) => {
                    // Only tombstone status can change for an existing node in RGA
                    if other_node.tombstone {
                        node.tombstone = true;
                    }
                }
                None => {
                    self.nodes.insert(*id, other_node.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_order() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, 'H', 1);
        let id2 = rga.insert(Some(id1), 'e', 2);
        let id3 = rga.insert(Some(id2), 'l', 3);
        let id4 = rga.insert(Some(id3), 'l', 4);
        rga.insert(Some(id4), 'o', 5);

        let s: String = rga.to_vec().into_iter().collect();
        assert_eq!(s, "Hello");
    }

    #[test]
    fn test_delete() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, 'H', 1);
        let id2 = rga.insert(Some(id1), 'i', 2);
        assert_eq!(rga.to_vec(), vec!['H', 'i']);

        rga.delete(id2);
        assert_eq!(rga.to_vec(), vec!['H']);
    }

    #[test]
    fn test_merge_concurrent_insert() {
        let mut a = Rga::new();
        let id0 = a.insert(None, 'A', 1);

        let mut b = a.clone();

        // Concurrent inserts after id0
        a.insert(Some(id0), 'B', 2);
        b.insert(Some(id0), 'C', 3);

        a.merge(&b);
        let res: String = a.to_vec().into_iter().collect();
        // C should come before B because timestamp 3 > 2
        assert_eq!(res, "ACB");
    }

    #[test]
    fn test_stack_overflow_prevention() {
        let mut rga = Rga::new();
        let mut prev = None;
        // 20k elements would cause stack overflow with recursive implementation
        for i in 0..20000 {
            prev = Some(rga.insert(prev, i, i as i64));
        }

        let vec = rga.to_vec();
        assert_eq!(vec.len(), 20000);
        assert_eq!(vec[0], 0);
        assert_eq!(vec[19999], 19999);
    }
}
