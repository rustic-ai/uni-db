// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph edge traversal direction.

/// Edge traversal direction for adjacency lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Outgoing (forward) edges from a vertex.
    Outgoing,
    /// Incoming (backward) edges to a vertex.
    Incoming,
    /// Both outgoing and incoming edges.
    Both,
}

impl Direction {
    /// Returns the short string representation used in storage keys.
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Outgoing => "fwd",
            Direction::Incoming => "bwd",
            Direction::Both => "both",
        }
    }

    /// Expands `Both` into `[Outgoing, Incoming]`; otherwise returns a
    /// single-element slice containing the direction itself.
    pub fn expand(&self) -> &'static [Direction] {
        match self {
            Direction::Both => &[Direction::Outgoing, Direction::Incoming],
            Direction::Outgoing => &[Direction::Outgoing],
            Direction::Incoming => &[Direction::Incoming],
        }
    }
}
