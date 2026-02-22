use fxhash::{FxHashMap, FxHashSet};
use std::ops::ControlFlow;
use uni_common::core::id::{Eid, Vid};

use super::nfa::{NfaStateId, PathMode, PathSelector};

/// A single predecessor record in the DAG pool.
///
/// Records that to reach `(dst_vid, dst_state)` at a given depth,
/// one can come from `(src_vid, src_state)` via edge `eid`.
/// The `next` field implements a singly-linked list within the pool.
#[derive(Debug, Clone)]
pub struct PredRec {
    pub src_vid: Vid,
    pub src_state: NfaStateId,
    pub eid: Eid,
    /// Index of next PredRec in the chain, or -1 for end of chain.
    pub next: i32,
}

/// Predecessor DAG for path enumeration.
///
/// During BFS frontier expansion, predecessors are recorded into an append-only pool.
/// After BFS completes, backward DFS through the DAG enumerates all valid paths
/// while applying Trail/Acyclic/Simple filtering.
pub struct PredecessorDag {
    /// Append-only pool of predecessor records.
    pred_pool: Vec<PredRec>,

    /// Head of predecessor chain for each `(dst_vid, dst_state, depth)`.
    /// Value is an index into `pred_pool`, or absent if no predecessors.
    pred_head: FxHashMap<(Vid, NfaStateId, u32), i32>,

    /// First-visit depth for each `(vid, state)`.
    first_depth: FxHashMap<(Vid, NfaStateId), u32>,

    /// Path selector determines DAG construction mode.
    selector: PathSelector,
}

impl PredecessorDag {
    /// Create a new empty DAG with the given path selector.
    pub fn new(selector: PathSelector) -> Self {
        Self {
            pred_pool: Vec::new(),
            pred_head: FxHashMap::default(),
            first_depth: FxHashMap::default(),
            selector,
        }
    }

    /// Returns true for layered DAG modes (All/Any), false for shortest-only.
    pub fn is_layered(&self) -> bool {
        matches!(self.selector, PathSelector::All | PathSelector::Any)
    }

    /// Add a predecessor record to the DAG.
    ///
    /// Records that to reach `(dst, dst_state)` at `depth`, one can come from
    /// `(src, src_state)` via edge `eid`. In shortest-only mode, predecessors
    /// at depths greater than the first-visit depth are skipped.
    #[allow(clippy::too_many_arguments)]
    pub fn add_predecessor(
        &mut self,
        dst: Vid,
        dst_state: NfaStateId,
        src: Vid,
        src_state: NfaStateId,
        eid: Eid,
        depth: u32,
    ) {
        // Update first_depth to track minimum discovery depth.
        let first = self.first_depth.entry((dst, dst_state)).or_insert(depth);
        if depth < *first {
            *first = depth;
        }

        // For shortest-only mode, skip predecessors at depths > first visit.
        if !self.is_layered() && depth > *self.first_depth.get(&(dst, dst_state)).unwrap() {
            return;
        }

        // Get current head for this (dst, dst_state, depth) chain.
        let key = (dst, dst_state, depth);
        let current_head = self.pred_head.get(&key).copied().unwrap_or(-1);

        // Append new PredRec to pool, linking to current head.
        let new_idx = self.pred_pool.len() as i32;
        self.pred_pool.push(PredRec {
            src_vid: src,
            src_state,
            eid,
            next: current_head,
        });

        // Update head to point to new record.
        self.pred_head.insert(key, new_idx);
    }

    /// Enumerate all valid paths from `source` to `target` through `accepting_state`.
    ///
    /// Iterates over depths in `[min_depth, max_depth]`, performing backward DFS
    /// through predecessor chains. Applies mode-specific filtering (Trail/Acyclic/Simple).
    /// The callback can return `ControlFlow::Break(())` to stop enumeration early.
    #[allow(clippy::too_many_arguments)]
    pub fn enumerate_paths<F>(
        &self,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
        yield_path: &mut F,
    ) where
        F: FnMut(&[Vid], &[Eid]) -> ControlFlow<()>,
    {
        for depth in min_depth..=max_depth {
            // Special case: zero-length path.
            if depth == 0 {
                if source == target && yield_path(&[source], &[]).is_break() {
                    return;
                }
                continue;
            }

            if !self
                .pred_head
                .contains_key(&(target, accepting_state, depth))
            {
                continue;
            }

            let mut nodes = Vec::with_capacity(depth as usize + 1);
            let mut edges = Vec::with_capacity(depth as usize);
            let mut node_set = FxHashSet::default();
            let mut edge_set = FxHashSet::default();

            // Start backward DFS from target.
            nodes.push(target);
            if matches!(mode, PathMode::Acyclic | PathMode::Simple) {
                node_set.insert(target);
            }

            if self
                .dfs_backward(
                    source,
                    target,
                    accepting_state,
                    depth,
                    &mut nodes,
                    &mut edges,
                    &mut node_set,
                    &mut edge_set,
                    mode,
                    yield_path,
                )
                .is_break()
            {
                return;
            }
        }
    }

    /// Check if at least one Trail-valid path exists from source to target.
    ///
    /// Returns true on the first valid path found (early-stop).
    pub fn has_trail_valid_path(
        &self,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
    ) -> bool {
        let mut found = false;
        self.enumerate_paths(
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            &PathMode::Trail,
            &mut |_nodes, _edges| {
                found = true;
                ControlFlow::Break(())
            },
        );
        found
    }

    /// Internal backward DFS through predecessor chains.
    #[allow(clippy::too_many_arguments)]
    fn dfs_backward<F>(
        &self,
        source: Vid,
        current_vid: Vid,
        current_state: NfaStateId,
        remaining_depth: u32,
        nodes: &mut Vec<Vid>,
        edges: &mut Vec<Eid>,
        node_set: &mut FxHashSet<Vid>,
        edge_set: &mut FxHashSet<Eid>,
        mode: &PathMode,
        yield_path: &mut F,
    ) -> ControlFlow<()>
    where
        F: FnMut(&[Vid], &[Eid]) -> ControlFlow<()>,
    {
        if remaining_depth == 0 {
            if current_vid == source {
                // Reverse stacks to get forward path order.
                let fwd_nodes: Vec<Vid> = nodes.iter().rev().copied().collect();
                let fwd_edges: Vec<Eid> = edges.iter().rev().copied().collect();
                return yield_path(&fwd_nodes, &fwd_edges);
            }
            return ControlFlow::Continue(());
        }

        let key = (current_vid, current_state, remaining_depth);
        let Some(&head) = self.pred_head.get(&key) else {
            return ControlFlow::Continue(());
        };

        let mut idx = head;
        while idx >= 0 {
            let pred = &self.pred_pool[idx as usize];

            // Mode-specific filtering.
            let skip = match mode {
                PathMode::Walk => false,
                PathMode::Trail => edge_set.contains(&pred.eid),
                PathMode::Acyclic => node_set.contains(&pred.src_vid),
                PathMode::Simple => {
                    // No repeated nodes except source may equal target.
                    node_set.contains(&pred.src_vid)
                        && !(remaining_depth == 1 && pred.src_vid == source)
                }
            };

            if skip {
                idx = pred.next;
                continue;
            }

            // Push to stacks.
            nodes.push(pred.src_vid);
            edges.push(pred.eid);

            if matches!(mode, PathMode::Trail) {
                edge_set.insert(pred.eid);
            }
            if matches!(mode, PathMode::Acyclic | PathMode::Simple) {
                node_set.insert(pred.src_vid);
            }

            // Recurse.
            let result = self.dfs_backward(
                source,
                pred.src_vid,
                pred.src_state,
                remaining_depth - 1,
                nodes,
                edges,
                node_set,
                edge_set,
                mode,
                yield_path,
            );

            // Pop from stacks.
            nodes.pop();
            edges.pop();

            if matches!(mode, PathMode::Trail) {
                edge_set.remove(&pred.eid);
            }
            if matches!(mode, PathMode::Acyclic | PathMode::Simple) {
                node_set.remove(&pred.src_vid);
            }

            if result.is_break() {
                return ControlFlow::Break(());
            }

            idx = pred.next;
        }

        ControlFlow::Continue(())
    }

    /// Get the number of records in the predecessor pool.
    pub fn pool_len(&self) -> usize {
        self.pred_pool.len()
    }

    /// Get the first-visit depth for a (vid, state) pair, if any.
    pub fn first_depth_of(&self, vid: Vid, state: NfaStateId) -> Option<u32> {
        self.first_depth.get(&(vid, state)).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vid(n: u64) -> Vid {
        Vid::new(n)
    }
    fn eid(n: u64) -> Eid {
        Eid::new(n)
    }

    /// Collect all enumerated paths as (Vec<Vid>, Vec<Eid>) pairs.
    fn collect_paths(
        dag: &PredecessorDag,
        source: Vid,
        target: Vid,
        accepting_state: NfaStateId,
        min_depth: u32,
        max_depth: u32,
        mode: &PathMode,
    ) -> Vec<(Vec<Vid>, Vec<Eid>)> {
        let mut paths = Vec::new();
        dag.enumerate_paths(
            source,
            target,
            accepting_state,
            min_depth,
            max_depth,
            mode,
            &mut |nodes, edges| {
                paths.push((nodes.to_vec(), edges.to_vec()));
                ControlFlow::Continue(())
            },
        );
        paths
    }

    // ── Pool Storage Tests (23-26) ─────────────────────────────────────

    #[test]
    fn test_pred_dag_add_single() {
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(10), 1);
        assert_eq!(dag.pool_len(), 1);
        assert!(dag.pred_head.contains_key(&(vid(2), 1, 1)));
    }

    #[test]
    fn test_pred_dag_add_chain() {
        // A(0)→B(1)→C(2) chain
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(11), 2);
        assert_eq!(dag.pool_len(), 2);
        assert!(dag.pred_head.contains_key(&(vid(1), 1, 1)));
        assert!(dag.pred_head.contains_key(&(vid(2), 2, 2)));
    }

    #[test]
    fn test_pred_dag_multiple_preds() {
        // Both A(0) and B(1) reach C(2) at depth 1
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 1);
        assert_eq!(dag.pool_len(), 2);
        // Both are in the same chain for (vid(2), state=1, depth=1)
        let head = dag.pred_head[&(vid(2), 1, 1)];
        assert!(head >= 0);
        let first = &dag.pred_pool[head as usize];
        assert!(first.next >= 0); // Chain has a second element
    }

    #[test]
    fn test_pred_dag_first_depth() {
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 3);
        assert_eq!(dag.first_depth_of(vid(2), 1), Some(3));

        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 2);
        assert_eq!(dag.first_depth_of(vid(2), 1), Some(2));

        // Adding at higher depth doesn't change first_depth
        dag.add_predecessor(vid(2), 1, vid(3), 0, eid(12), 5);
        assert_eq!(dag.first_depth_of(vid(2), 1), Some(2));
    }

    // ── PathSelector / DAG Mode Tests (27-29) ─────────────────────────

    #[test]
    fn test_pred_dag_layered_stores_all() {
        let mut dag = PredecessorDag::new(PathSelector::All);
        assert!(dag.is_layered());

        // Add at depth 2 and depth 3 — both should be stored
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 2);
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 3);
        assert_eq!(dag.pool_len(), 2);
        assert!(dag.pred_head.contains_key(&(vid(2), 1, 2)));
        assert!(dag.pred_head.contains_key(&(vid(2), 1, 3)));
    }

    #[test]
    fn test_pred_dag_shortest_skips() {
        let mut dag = PredecessorDag::new(PathSelector::AnyShortest);
        assert!(!dag.is_layered());

        // First visit at depth 2
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 2);
        assert_eq!(dag.pool_len(), 1);

        // Second visit at depth 3 — should be skipped
        dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 3);
        assert_eq!(dag.pool_len(), 1); // Still 1 — depth 3 was skipped

        // Another visit at depth 2 — should be stored (same depth as first)
        dag.add_predecessor(vid(2), 1, vid(3), 0, eid(12), 2);
        assert_eq!(dag.pool_len(), 2);
    }

    #[test]
    fn test_pred_dag_selector_switch() {
        // Same graph, different selectors produce different pool sizes
        let build = |selector: PathSelector| -> usize {
            let mut dag = PredecessorDag::new(selector);
            dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 2);
            dag.add_predecessor(vid(2), 1, vid(1), 0, eid(11), 3);
            dag.add_predecessor(vid(2), 1, vid(3), 0, eid(12), 4);
            dag.pool_len()
        };

        assert_eq!(build(PathSelector::All), 3); // Layered: stores all
        assert_eq!(build(PathSelector::AnyShortest), 1); // Only depth 2
    }

    // ── Walk Enumeration Tests (30-33) ─────────────────────────────────

    #[test]
    fn test_pred_dag_linear_walk() {
        // A(0) → B(1) → C(2) linear chain
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(11), 2);

        let paths = collect_paths(&dag, vid(0), vid(2), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(2)]);
        assert_eq!(paths[0].1, vec![eid(10), eid(11)]);
    }

    #[test]
    fn test_pred_dag_diamond_walk() {
        // A(0) → {B(1), C(2)} → D(3) diamond
        let mut dag = PredecessorDag::new(PathSelector::All);
        // A→B and A→C at depth 1
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        // B→D and C→D at depth 2
        dag.add_predecessor(vid(3), 2, vid(1), 1, eid(12), 2);
        dag.add_predecessor(vid(3), 2, vid(2), 1, eid(13), 2);

        let paths = collect_paths(&dag, vid(0), vid(3), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths.len(), 2);

        let mut sorted: Vec<_> = paths.iter().map(|(n, _)| n.clone()).collect();
        sorted.sort();
        assert!(sorted.contains(&vec![vid(0), vid(1), vid(3)]));
        assert!(sorted.contains(&vec![vid(0), vid(2), vid(3)]));
    }

    #[test]
    fn test_pred_dag_multiple_depths() {
        // Target reachable at depth 1 (state q1) and depth 2 (state q2).
        // In a linear NFA, different depths use different NFA states.
        let mut dag = PredecessorDag::new(PathSelector::All);
        // Direct: A→C at depth 1, arriving at state 1
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(10), 1);
        // Via B: A→B at depth 1 (state 1), B→C at depth 2 (state 2)
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(12), 2);

        // Depth 1 via accepting state 1
        let paths1 = collect_paths(&dag, vid(0), vid(2), 1, 1, 1, &PathMode::Walk);
        assert_eq!(paths1.len(), 1);
        assert_eq!(paths1[0].0, vec![vid(0), vid(2)]); // [A, C]

        // Depth 2 via accepting state 2
        let paths2 = collect_paths(&dag, vid(0), vid(2), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths2.len(), 1);
        assert_eq!(paths2[0].0, vec![vid(0), vid(1), vid(2)]); // [A, B, C]

        // Total: 2 paths across both accepting states
        assert_eq!(paths1.len() + paths2.len(), 2);
    }

    #[test]
    fn test_pred_dag_fan_out() {
        // A(0) → {B1(1), B2(2), B3(3)} → C(4)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(3), 1, vid(0), 0, eid(12), 1);
        dag.add_predecessor(vid(4), 2, vid(1), 1, eid(13), 2);
        dag.add_predecessor(vid(4), 2, vid(2), 1, eid(14), 2);
        dag.add_predecessor(vid(4), 2, vid(3), 1, eid(15), 2);

        let paths = collect_paths(&dag, vid(0), vid(4), 2, 2, 2, &PathMode::Walk);
        assert_eq!(paths.len(), 3);
    }

    // ── Trail Mode Tests (34-37) ──────────────────────────────────────

    #[test]
    fn test_pred_dag_trail_no_repeat() {
        // A(0) → B(1) → A(0) → C(2) via edges e1, e2, e1
        // The path A→B→A→B uses edge e1 twice — rejected by Trail
        let mut dag = PredecessorDag::new(PathSelector::All);
        // depth 1: B reached from A via e1
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        // depth 2: A reached from B via e2
        dag.add_predecessor(vid(0), 2, vid(1), 1, eid(2), 2);
        // depth 3: B reached from A via e1 again (same edge!)
        dag.add_predecessor(vid(1), 3, vid(0), 2, eid(1), 3);

        // Walk mode: path exists (edge repeat OK)
        let walk_paths = collect_paths(&dag, vid(0), vid(1), 3, 3, 3, &PathMode::Walk);
        assert_eq!(walk_paths.len(), 1);
        assert_eq!(walk_paths[0].1, vec![eid(1), eid(2), eid(1)]);

        // Trail mode: path rejected (e1 appears twice)
        let trail_paths = collect_paths(&dag, vid(0), vid(1), 3, 3, 3, &PathMode::Trail);
        assert_eq!(trail_paths.len(), 0);
    }

    #[test]
    fn test_pred_dag_trail_allows_node_repeat() {
        // A(0) → B(1) → C(2) → B(1) with distinct edges e1, e2, e3
        // Trail allows this (node B repeated but all edges distinct)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(2), 2);
        dag.add_predecessor(vid(1), 3, vid(2), 2, eid(3), 3);

        let paths = collect_paths(&dag, vid(0), vid(1), 3, 3, 3, &PathMode::Trail);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(2), vid(1)]);
        assert_eq!(paths[0].1, vec![eid(1), eid(2), eid(3)]);
    }

    #[test]
    fn test_pred_dag_trail_diamond() {
        // Diamond A→{B,C}→D with distinct edges on each path
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(3), 2, vid(1), 1, eid(12), 2);
        dag.add_predecessor(vid(3), 2, vid(2), 1, eid(13), 2);

        // Trail: both paths kept (distinct edges on each)
        let paths = collect_paths(&dag, vid(0), vid(3), 2, 2, 2, &PathMode::Trail);
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_pred_dag_trail_cycle_2_hop() {
        // A→B→A on *2 pattern
        // depth 1: B from A via e1
        // depth 2: A from B via e2
        // This uses distinct edges so Trail allows it
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(0), 2, vid(1), 1, eid(2), 2);

        let paths = collect_paths(&dag, vid(0), vid(0), 2, 2, 2, &PathMode::Trail);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(0)]);
        assert_eq!(paths[0].1, vec![eid(1), eid(2)]);
    }

    // ── Acyclic Mode Tests (38-39) ────────────────────────────────────

    #[test]
    fn test_pred_dag_acyclic_filter() {
        // A(0) → B(1) → C(2) → A(0) — node A repeated
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(2), 2);
        dag.add_predecessor(vid(0), 3, vid(2), 2, eid(3), 3);

        // Walk: path exists
        let walk_paths = collect_paths(&dag, vid(0), vid(0), 3, 3, 3, &PathMode::Walk);
        assert_eq!(walk_paths.len(), 1);

        // Acyclic: path rejected (node A repeated)
        let acyclic_paths = collect_paths(&dag, vid(0), vid(0), 3, 3, 3, &PathMode::Acyclic);
        assert_eq!(acyclic_paths.len(), 0);
    }

    #[test]
    fn test_pred_dag_acyclic_diamond() {
        // Diamond A→{B,C}→D — no node repeats
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 1, vid(0), 0, eid(11), 1);
        dag.add_predecessor(vid(3), 2, vid(1), 1, eid(12), 2);
        dag.add_predecessor(vid(3), 2, vid(2), 1, eid(13), 2);

        let paths = collect_paths(&dag, vid(0), vid(3), 2, 2, 2, &PathMode::Acyclic);
        assert_eq!(paths.len(), 2);
    }

    // ── Trail Existence Tests (40-42) ─────────────────────────────────

    #[test]
    fn test_has_trail_valid_true() {
        // Simple A→B→C chain — Trail-valid
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(11), 2);

        assert!(dag.has_trail_valid_path(vid(0), vid(2), 2, 2, 2));
    }

    #[test]
    fn test_has_trail_valid_false() {
        // Only path to target reuses an edge
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(0), 2, vid(1), 1, eid(2), 2);
        dag.add_predecessor(vid(1), 3, vid(0), 2, eid(1), 3); // e1 reused

        assert!(!dag.has_trail_valid_path(vid(0), vid(1), 3, 3, 3));
    }

    #[test]
    fn test_has_trail_valid_one_of_many() {
        // Two paths to target: one valid, one not
        let mut dag = PredecessorDag::new(PathSelector::All);
        // Path 1: A→B→C (valid, distinct edges)
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(1), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(2), 2);
        // Path 2: A→D→C (also valid)
        dag.add_predecessor(vid(3), 1, vid(0), 0, eid(3), 1);
        dag.add_predecessor(vid(2), 2, vid(3), 1, eid(4), 2);

        // At least one valid → true
        assert!(dag.has_trail_valid_path(vid(0), vid(2), 2, 2, 2));
    }

    // ── Streaming & Early Termination Tests (43-45) ───────────────────

    #[test]
    fn test_pred_dag_early_stop() {
        // Build a DAG with many paths, verify callback can break early.
        // Fan-out: A → {B1..B10} → C (10 paths of length 2)
        let mut dag = PredecessorDag::new(PathSelector::All);
        for i in 1..=10u64 {
            dag.add_predecessor(Vid::new(i), 1, vid(0), 0, Eid::new(i), 1);
            dag.add_predecessor(vid(99), 2, Vid::new(i), 1, Eid::new(100 + i), 2);
        }

        let mut count = 0;
        dag.enumerate_paths(
            vid(0),
            vid(99),
            2,
            2,
            2,
            &PathMode::Walk,
            &mut |_nodes, _edges| {
                count += 1;
                if count >= 3 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            },
        );
        assert_eq!(count, 3); // Stopped after 3, not 10
    }

    #[test]
    fn test_pred_dag_empty_enumerate() {
        // Empty DAG — no paths
        let dag = PredecessorDag::new(PathSelector::All);
        let paths = collect_paths(&dag, vid(0), vid(1), 0, 1, 5, &PathMode::Walk);
        assert!(paths.is_empty());
    }

    #[test]
    fn test_pred_dag_zero_length() {
        // Zero-length path: source == target with min_depth=0
        let dag = PredecessorDag::new(PathSelector::All);
        let paths = collect_paths(&dag, vid(5), vid(5), 0, 0, 0, &PathMode::Walk);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, vec![vid(5)]);
        assert!(paths[0].1.is_empty());
    }

    // ── Correctness Tests (46-47) ─────────────────────────────────────

    #[test]
    fn test_pred_dag_path_order() {
        // Verify nodes and edges are in correct forward order (source → target)
        // Build: A(0)→B(1)→C(2)→D(3)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(10), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(20), 2);
        dag.add_predecessor(vid(3), 3, vid(2), 2, eid(30), 3);

        let paths = collect_paths(&dag, vid(0), vid(3), 3, 3, 3, &PathMode::Walk);
        assert_eq!(paths.len(), 1);
        // Forward order: source first, target last
        assert_eq!(paths[0].0, vec![vid(0), vid(1), vid(2), vid(3)]);
        assert_eq!(paths[0].1, vec![eid(10), eid(20), eid(30)]);
    }

    #[test]
    fn test_pred_dag_eid_in_path() {
        // Verify edges match traversal order exactly
        // A(0) →e1→ B(1) →e2→ C(2) →e3→ D(3)
        let mut dag = PredecessorDag::new(PathSelector::All);
        dag.add_predecessor(vid(1), 1, vid(0), 0, eid(100), 1);
        dag.add_predecessor(vid(2), 2, vid(1), 1, eid(200), 2);
        dag.add_predecessor(vid(3), 3, vid(2), 2, eid(300), 3);

        let paths = collect_paths(&dag, vid(0), vid(3), 3, 3, 3, &PathMode::Walk);
        assert_eq!(paths.len(), 1);

        // Edge e1 connects nodes[0]→nodes[1], e2 connects nodes[1]→nodes[2], etc.
        let (nodes, edges) = &paths[0];
        assert_eq!(nodes.len(), edges.len() + 1);
        assert_eq!(edges[0], eid(100)); // A→B
        assert_eq!(edges[1], eid(200)); // B→C
        assert_eq!(edges[2], eid(300)); // C→D
    }
}
