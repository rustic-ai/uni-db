// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A reachability consumer of a variable-length pattern decides from
//! shortest distances, not by listing trails, and answers exactly as the
//! trail semantics say.
//!
//! `EXISTS { MATCH (a)-[:R*]-(b) }`, a pattern predicate and
//! `RETURN DISTINCT` over a variable-length pattern ask only whether a trail
//! (no relationship twice) joins two vertices. The schema'd traversal walked
//! hop by hop up to the hop ceiling, and on a cyclic graph (every undirected
//! one with an edge) each depth searched for a trail of exactly that length,
//! an exponential search where none exists; the schemaless traversal listed
//! every trail. Both now answer from breadth-first distances where those
//! decide (see `trail_reach` in uni-query), and keep the exact search where
//! they do not (a range whose lower bound exceeds the shortest trail).
//!
//! The oracle is a brute-force search over `(vertex, set of relationships
//! used)` states, which lists every trail length from every source. The
//! graphs carry cycles, a self-loop and a parallel pair, so `a = b` (a
//! closed trail, which may not reuse its first relationship) is exercised in
//! both directions and undirected.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(trail_reachability)'

// Rust guideline compliant

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use uni_db::Uni;

use super::exists_per_source_test::{Graph, bag, build, rel};

/// A graph with up to 15 relationships, so the oracle's state space
/// (`2^15` relationship sets per vertex) stays small, but well past the
/// size where listing trails hurt.
fn random_graph(seed: u64) -> Graph {
    let mut rng = StdRng::seed_from_u64(seed);
    let n = rng.random_range(5..=8);
    let mut r = Vec::new();
    for _ in 0..rng.random_range(8..=13) {
        r.push((rng.random_range(0..n), rng.random_range(0..n)));
    }
    let loop_at = rng.random_range(0..n);
    r.push((loop_at, loop_at));
    r.push(r[0]);
    Graph {
        n,
        r,
        s: Vec::new(),
    }
}

/// Every `(endpoint, length)` a trail from `a` reaches in direction `dir`.
fn trail_lengths(g: &Graph, dir: &str, a: i64) -> HashSet<(i64, usize)> {
    let mut out = HashSet::new();
    let mut seen: HashSet<(i64, u32)> = HashSet::new();
    let mut stack = vec![(a, 0_u32)];
    while let Some((at, used)) = stack.pop() {
        if !seen.insert((at, used)) {
            continue;
        }
        out.insert((at, used.count_ones() as usize));
        for (i, &(s, d)) in g.r.iter().enumerate() {
            if used & (1 << i) != 0 {
                continue;
            }
            let mut nexts = Vec::new();
            if dir != "<-" && s == at {
                nexts.push(d);
            }
            if dir != "->" && d == at && !(dir == "-" && s == d) {
                nexts.push(s);
            }
            for next in nexts {
                stack.push((next, used | (1 << i)));
            }
        }
    }
    out
}

const DIRS: [&str; 3] = ["->", "<-", "-"];
const RANGES: [(&str, usize, usize); 9] = [
    ("", 1, 1),
    ("*", 1, usize::MAX),
    ("*1..", 1, usize::MAX),
    ("*2..", 2, usize::MAX),
    ("*3..", 3, usize::MAX),
    ("*0..", 0, usize::MAX),
    ("*1..2", 1, 2),
    ("*2..3", 2, 3),
    ("*3", 3, 3),
];

/// `(schema, flush half the edges, execution batch size)`, one per graph.
const CONFIGS: [(bool, bool, Option<usize>); 3] = [
    (true, false, None),
    (false, true, Some(2)),
    (true, true, Some(3)),
];

async fn warnings(db: &Uni, q: &str) -> anyhow::Result<Vec<String>> {
    let rows = db.session().query(q).await?;
    Ok(rows.warnings().iter().map(|w| format!("{w:?}")).collect())
}

/// Which `(direction, min, max)` a run covers.
type Cover = fn(&str, usize, usize) -> bool;

/// An undirected unbounded range whose lower bound is above 1 is not decided
/// by distances (a trail at least `min` long to a nearer vertex is a longest-
/// trail question), so it keeps the exact search, which is exponential in
/// the relationship count. The random graphs leave it to the small one.
fn random_cover(dir: &str, min: usize, max: usize) -> bool {
    !(dir == "-" && min >= 2 && max == usize::MAX)
}

async fn check(g: &Graph, ctx: &str, db: &Uni, cover: Cover) -> anyhow::Result<()> {
    let lengths: Vec<HashSet<(i64, usize)>> = DIRS
        .iter()
        .flat_map(|dir| (0..g.n).map(move |a| (dir, a)))
        .map(|(dir, a)| trail_lengths(g, dir, a))
        .collect();
    for (di, dir) in DIRS.iter().enumerate() {
        for (range, min, max) in RANGES {
            if !cover(dir, min, max) {
                continue;
            }
            let p = rel(dir, range);
            let mut truth = BTreeMap::new();
            for a in 0..g.n {
                let reach = &lengths[di * g.n as usize + a as usize];
                for b in 0..g.n {
                    if reach.iter().any(|&(v, l)| v == b && l >= min && l <= max) {
                        truth.insert(vec![Some(a), Some(b)], 1);
                    }
                }
            }
            for q in [
                format!("MATCH (a:N), (b:N) WHERE EXISTS {{ MATCH (a){p}(b) }} RETURN a.id, b.id"),
                format!(
                    "MATCH (a:N), (b:N) WHERE EXISTS {{ MATCH (a){p}(b) \
                     WHERE a.id = a.id AND b.id = b.id }} RETURN a.id, b.id"
                ),
                format!("MATCH (a:N), (b:N) WHERE (a){p}(b) RETURN a.id, b.id"),
                format!("MATCH (a:N){p}(b:N) RETURN DISTINCT a.id, b.id"),
            ] {
                assert_eq!(bag(db, &q).await?, truth, "{ctx}: {q}");
            }
            let q = format!(
                "MATCH (a:N), (b:N) WHERE NOT EXISTS {{ MATCH (a){p}(b) }} RETURN a.id, b.id"
            );
            let not = bag(db, &q).await?;
            assert_eq!(not.len() + truth.len(), (g.n * g.n) as usize, "{ctx}: {q}");
            assert!(not.keys().all(|k| !truth.contains_key(k)), "{ctx}: {q}");
            // Every trail here is far shorter than the hop ceiling, so an
            // unbounded pattern has nothing to report as truncated.
            for q in [
                format!("MATCH (a:N), (b:N) WHERE EXISTS {{ MATCH (a){p}(b) }} RETURN a.id, b.id"),
                format!("MATCH (a:N){p}(b:N) RETURN DISTINCT a.id, b.id"),
            ] {
                let w = warnings(db, &q).await?;
                assert!(w.is_empty(), "{ctx}: {q} warned {w:?}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn trail_reachability_matches_brute_force() -> anyhow::Result<()> {
    for seed in 0_u64..4 {
        let g = random_graph(seed);
        let (schema, flush_half, batch) = CONFIGS[seed as usize % CONFIGS.len()];
        let db = build(&g, schema, flush_half, batch).await?;
        let ctx = format!(
            "seed={seed} edges={:?} schema={schema} flush_half={flush_half} batch={batch:?}",
            g.r
        );
        check(&g, &ctx, &db, random_cover).await?;
    }
    // Every range, including those that keep the exact search, on a graph
    // small enough for it: a triangle, a parallel pair, a self-loop and a
    // pendant vertex (4), whose only trail back to 2 is the way it came.
    let g = Graph {
        n: 5,
        r: vec![(0, 1), (1, 2), (2, 0), (2, 3), (2, 3), (3, 3), (2, 4)],
        s: Vec::new(),
    };
    for (schema, flush_half, batch) in CONFIGS {
        let db = build(&g, schema, flush_half, batch).await?;
        let ctx = format!("small schema={schema} flush_half={flush_half} batch={batch:?}");
        check(&g, &ctx, &db, |_, _, _| true).await?;
    }
    Ok(())
}

/// Four disjoint complete graphs on 5 vertices and one on 6, undirected:
/// every vertex lies on many cycles, and a walk never runs out of frontier.
/// In a debug build the schema'd traversal, searching for a trail of each
/// length up to the hop ceiling, took about 0.6 s per 5-clique source (over
/// 150 s for the 6-clique); the schemaless one, listing every trail, took
/// 7 s for the `EXISTS` over the 6-clique and 20 s for the `DISTINCT`.
/// Under a 5 s deadline the old searches time out, and the distance-based
/// answer takes milliseconds.
#[tokio::test]
async fn trail_reachability_does_not_list_trails() -> anyhow::Result<()> {
    let mut r = Vec::new();
    let mut n = 0;
    let mut pairs = 0;
    for size in [5, 5, 5, 5, 6] {
        for a in 0..size {
            for b in a + 1..size {
                r.push((n + a, n + b));
            }
        }
        n += size;
        pairs += size * size;
    }
    let g = Graph {
        n,
        r,
        s: Vec::new(),
    };
    for schema in [true, false] {
        let db = build(&g, schema, false, None).await?;
        for (q, want) in [
            (
                "MATCH (a:N), (b:N) WHERE EXISTS { MATCH (a)-[:R*]-(b) } RETURN count(*) AS c",
                pairs,
            ),
            // No parallel pair, so the shortest closed trail is a triangle.
            (
                "MATCH (a:N), (b:N) WHERE a = b AND NOT EXISTS { MATCH (a)-[:R*1..2]-(b) } \
                 RETURN count(*) AS c",
                n,
            ),
            (
                "MATCH (a:N)-[:R*]-(b:N) WITH DISTINCT a, b RETURN count(*) AS c",
                pairs,
            ),
        ] {
            let rows = db
                .session()
                .query_with(q)
                .timeout(Duration::from_secs(5))
                .fetch_all()
                .await
                .map_err(|e| anyhow::anyhow!("schema={schema}: {q}: {e}"))?;
            let got: i64 = rows.rows()[0].get("c")?;
            assert_eq!(got, want, "schema={schema}: {q}");
            assert!(
                rows.warnings().is_empty(),
                "schema={schema}: {q} warned {:?}",
                rows.warnings()
            );
        }
    }
    Ok(())
}
