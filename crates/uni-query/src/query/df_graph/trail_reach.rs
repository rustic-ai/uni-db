// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Which vertices a trail of length `min..=max` reaches, without listing
//! trails.
//!
//! A reachability consumer (`EXISTS`, `DISTINCT`, a pattern predicate) asks
//! only *whether* a trail (no relationship twice) joins the source to an
//! endpoint. Deciding that by walking hop by hop is costly on a cyclic
//! graph: a walk can always step back along the edge it came by, so every
//! vertex stays on the frontier at every depth up to the hop ceiling (128 for
//! an unbounded pattern), and each depth asked the predecessor graph for a
//! trail of exactly that length, an exponential search when none exists.
//! An undirected pattern is cyclic as soon as it has one edge.
//!
//! Two facts decide most of it in breadth-first time instead:
//!
//! - A shortest walk is a path, and a path is a trail, so for a vertex
//!   `v != s` the shortest trail from `s` has length `dist(s, v)`, and every
//!   trail is at least that long. So a trail of length in `min..=max` exists
//!   when `min <= dist <= max`, and none does when `dist > max`.
//! - A closed trail from `s` of length at least 1 leaves by some edge `e` to
//!   `u` and returns along a trail from `u` that does not use `e`; the
//!   shortest returns along a shortest `u`-to-`s` path that avoids `e`. So the
//!   shortest closed trail has length `1 + min over e of dist_{G-e}(u, s)`
//!   (a self-loop gives 1, a parallel pair 2).
//!
//! When the shortest trail is shorter than `min` the answer depends on which
//! longer trails exist; [`trail_endpoints`] then returns `None` and the
//! caller keeps its exact search for that source.

use std::collections::VecDeque;

use rustc_hash::{FxHashMap, FxHashSet};
use uni_common::core::id::{Eid, Vid};

/// What [`trail_endpoints`] found for one source.
#[derive(Debug, Default)]
pub(crate) struct TrailEndpoints {
    /// Each accepted endpoint once, with the length of its shortest trail.
    pub endpoints: Vec<(Vid, u32)>,
    /// How many accepted endpoints are reachable only beyond `max`, which a
    /// larger hop ceiling would have answered.
    pub beyond_max: usize,
}

/// The endpoints reachable from `source` by a trail of length in
/// `min..=max`, each once, or `None` when that cannot be decided from
/// shortest distances (an accepted endpoint's shortest trail is shorter than
/// `min`).
///
/// `neighbors(v)` lists the relationships a hop may take from `v` (already
/// filtered by type, direction, properties and any relationship bound
/// elsewhere in the pattern), each once. `accept(v)` says whether `v` may be
/// an endpoint. A zero-length match (`min == 0`) is the caller's to emit:
/// it needs no relationship and is not decided here.
pub(crate) fn trail_endpoints(
    source: Vid,
    min: usize,
    max: usize,
    mut neighbors: impl FnMut(Vid) -> Vec<(Vid, Eid)>,
    mut accept: impl FnMut(Vid) -> bool,
) -> Option<TrailEndpoints> {
    let mut out = TrailEndpoints::default();
    let mut adjacency: FxHashMap<Vid, Vec<(Vid, Eid)>> = FxHashMap::default();
    let mut neighbors_of =
        |v: Vid| -> Vec<(Vid, Eid)> { adjacency.entry(v).or_insert_with(|| neighbors(v)).clone() };

    // Shortest distances from the source, over the whole reachable part.
    let mut dist: FxHashMap<Vid, usize> = FxHashMap::default();
    dist.insert(source, 0);
    let mut queue = VecDeque::from([source]);
    while let Some(v) = queue.pop_front() {
        let d = dist[&v];
        for (w, _) in neighbors_of(v) {
            if let std::collections::hash_map::Entry::Vacant(slot) = dist.entry(w) {
                slot.insert(d + 1);
                queue.push_back(w);
            }
        }
    }

    for (&v, &d) in &dist {
        if v == source || !accept(v) {
            continue;
        }
        if d > max {
            out.beyond_max += 1;
        } else if d >= min {
            out.endpoints.push((v, d as u32));
        } else {
            return None;
        }
    }

    // The source as its own endpoint, along a closed trail of length >= 1.
    if min >= 1 && accept(source) {
        let first_hops = neighbors_of(source);
        let mut best: Option<usize> = None;
        for &(u, e) in &first_hops {
            if u == source {
                best = Some(1);
                break;
            }
            // A closed trail other than a self-loop is at least 2 long.
            if best == Some(2) {
                break;
            }
            // Shortest `u`-to-`source` path that does not take `e`, cut off
            // where it could no longer improve on `best`.
            let limit = best.map_or(usize::MAX, |b| b - 1);
            let mut seen: FxHashSet<Vid> = FxHashSet::from_iter([u]);
            let mut frontier = vec![u];
            let mut depth = 0;
            'search: while !frontier.is_empty() && depth + 1 < limit {
                depth += 1;
                let mut next = Vec::new();
                for x in frontier {
                    for (y, f) in neighbors_of(x) {
                        if f == e {
                            continue;
                        }
                        if y == source {
                            best = Some(depth + 1);
                            break 'search;
                        }
                        if seen.insert(y) {
                            next.push(y);
                        }
                    }
                }
                frontier = next;
            }
        }
        match best {
            Some(c) if c > max => out.beyond_max += 1,
            Some(c) if c >= min => out.endpoints.push((source, c as u32)),
            Some(_) => return None,
            None => {}
        }
    }
    Some(out)
}

/// How many distinct relationships a hop sequence from `source` can take:
/// no trail from `source` is longer, since a trail uses each at most once.
pub(crate) fn reachable_relationships(
    source: Vid,
    mut neighbors: impl FnMut(Vid) -> Vec<(Vid, Eid)>,
) -> usize {
    let mut seen: FxHashSet<Vid> = FxHashSet::from_iter([source]);
    let mut edges: FxHashSet<Eid> = FxHashSet::default();
    let mut queue = VecDeque::from([source]);
    while let Some(v) = queue.pop_front() {
        for (w, e) in neighbors(v) {
            edges.insert(e);
            if seen.insert(w) {
                queue.push_back(w);
            }
        }
    }
    edges.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `edges` as `(src, dst)`; `undirected` lists each edge from both ends.
    fn graph(edges: &[(u64, u64)], undirected: bool) -> impl FnMut(Vid) -> Vec<(Vid, Eid)> {
        let edges = edges.to_vec();
        move |v: Vid| {
            let mut out = Vec::new();
            for (i, &(s, d)) in edges.iter().enumerate() {
                let e = Eid::new(i as u64);
                if s == v.as_u64() {
                    out.push((Vid::new(d), e));
                } else if undirected && d == v.as_u64() {
                    out.push((Vid::new(s), e));
                }
            }
            out
        }
    }

    /// Whether a trail of length in `min..=max` joins `a` to `b`.
    fn brute(
        edges: &[(u64, u64)],
        undirected: bool,
        min: usize,
        max: usize,
        a: u64,
        b: u64,
    ) -> bool {
        fn go(
            edges: &[(u64, u64)],
            undirected: bool,
            at: u64,
            len: usize,
            used: &mut Vec<bool>,
            (min, max, b): (usize, usize, u64),
        ) -> bool {
            if len >= min && len >= 1 && at == b {
                return true;
            }
            if len == max {
                return false;
            }
            for (i, &(s, d)) in edges.iter().enumerate() {
                if used[i] {
                    continue;
                }
                let mut nexts = Vec::new();
                if s == at {
                    nexts.push(d);
                }
                if undirected && d == at && s != d {
                    nexts.push(s);
                }
                for next in nexts {
                    used[i] = true;
                    let found = go(edges, undirected, next, len + 1, used, (min, max, b));
                    used[i] = false;
                    if found {
                        return true;
                    }
                }
            }
            false
        }
        go(
            edges,
            undirected,
            a,
            0,
            &mut vec![false; edges.len()],
            (min, max, b),
        )
    }

    #[test]
    fn matches_brute_force_where_decided() {
        let graphs: [&[(u64, u64)]; 5] = [
            &[(0, 1), (1, 2), (2, 0), (2, 3), (2, 3), (4, 4), (3, 4)],
            &[(0, 1), (1, 2), (2, 3)],
            &[(0, 1), (1, 0), (1, 2)],
            &[(0, 0), (1, 2), (2, 1), (3, 1)],
            &[(0, 1), (0, 1), (1, 2), (2, 3), (3, 1), (3, 4)],
        ];
        let ranges = [(1, 1), (1, 2), (1, 3), (1, 99), (2, 99), (3, 3), (2, 2)];
        let mut decided = 0;
        for edges in graphs {
            for undirected in [false, true] {
                for (min, max) in ranges {
                    for a in 0..5 {
                        let Some(found) = trail_endpoints(
                            Vid::new(a),
                            min,
                            max,
                            graph(edges, undirected),
                            |_| true,
                        ) else {
                            continue;
                        };
                        decided += 1;
                        for b in 0..5 {
                            let want = brute(edges, undirected, min, max, a, b);
                            let got = found.endpoints.iter().any(|(v, _)| v.as_u64() == b);
                            assert_eq!(
                                got, want,
                                "{edges:?} undirected={undirected} *{min}..{max} {a}->{b}"
                            );
                        }
                    }
                }
            }
        }
        assert!(decided > 100, "only {decided} cases decided");
    }

    #[test]
    fn a_shorter_trail_than_min_is_left_undecided() {
        // 0-1 directly, so `*2..` from 0 to 1 depends on longer trails.
        let edges = [(0, 1), (1, 2), (2, 0)];
        assert!(trail_endpoints(Vid::new(0), 2, 9, graph(&edges, false), |_| true).is_none());
        // Unless 1 is not an endpoint at all.
        assert!(
            trail_endpoints(Vid::new(0), 2, 9, graph(&edges, false), |v| v.as_u64() != 1).is_some()
        );
    }

    #[test]
    fn a_single_edge_is_not_a_closed_trail() {
        let found = trail_endpoints(Vid::new(0), 1, 99, graph(&[(0, 1)], true), |_| true).unwrap();
        assert_eq!(found.endpoints, vec![(Vid::new(1), 1)]);
        let found =
            trail_endpoints(Vid::new(0), 1, 99, graph(&[(0, 1), (1, 0)], true), |_| true).unwrap();
        let mut got = found.endpoints;
        got.sort();
        assert_eq!(got, vec![(Vid::new(0), 2), (Vid::new(1), 1)]);
    }

    #[test]
    fn beyond_max_is_reported() {
        let found = trail_endpoints(
            Vid::new(0),
            1,
            2,
            graph(&[(0, 1), (1, 2), (2, 3)], false),
            |_| true,
        )
        .unwrap();
        assert_eq!(found.beyond_max, 1);
        assert_eq!(found.endpoints.len(), 2);
    }
}
