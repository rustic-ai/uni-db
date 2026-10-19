// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Correlated `EXISTS` / `COUNT` subqueries answered once per source.
//!
//! `MATCH (a), (b) WHERE EXISTS { MATCH (a)-[:R*1..]->(b) }` used to execute
//! its body once per `(a, b)` pair. A body that leaves an outer node bare is
//! now executed once per distinct binding of its other imports, with that node
//! free, and each row reads its answer from the result.
//!
//! The oracle here is threefold:
//! - the same subquery with both endpoints also named in a `WHERE`, which
//!   keeps the per-row evaluation (a bare endpoint is the trigger);
//! - for `EXISTS`, a brute-force trail search over the generated graph;
//! - for `COUNT`, the top-level enumeration of the same pattern.

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use uni_db::{DataType, Uni, UniConfig, Value};

/// A generated graph: node ids `0..n`, `R` edges, and sparse `S` edges.
pub(crate) struct Graph {
    pub(crate) n: i64,
    pub(crate) r: Vec<(i64, i64)>,
    pub(crate) s: Vec<(i64, i64)>,
}

fn random_graph(seed: u64) -> Graph {
    let mut rng = StdRng::seed_from_u64(seed);
    let n = rng.random_range(4..=7);
    let mut r = Vec::new();
    for _ in 0..rng.random_range(4..=8) {
        r.push((rng.random_range(0..n), rng.random_range(0..n)));
    }
    // A self-loop and a parallel edge in every graph.
    let loop_at = rng.random_range(0..n);
    r.push((loop_at, loop_at));
    r.push(r[0]);
    let mut s = Vec::new();
    for i in 0..n {
        if rng.random_bool(0.4) {
            s.push((i, rng.random_range(0..n)));
        }
    }
    Graph { n, r, s }
}

/// Whether a trail (no relationship twice) from `a` to `b` exists with a
/// length in `min..=max`.
fn trail_exists(g: &Graph, dir: &str, min: usize, max: usize, a: i64, b: i64) -> bool {
    fn go(
        g: &Graph,
        dir: &str,
        at: i64,
        len: usize,
        used: &mut Vec<bool>,
        min: usize,
        max: usize,
        b: i64,
    ) -> bool {
        if len >= min && at == b {
            return true;
        }
        if len == max {
            return false;
        }
        for (i, &(s, d)) in g.r.iter().enumerate() {
            if used[i] {
                continue;
            }
            let mut nexts = Vec::new();
            if dir != "<-" && s == at {
                nexts.push(d);
            }
            if dir != "->" && d == at {
                nexts.push(s);
            }
            for next in nexts {
                used[i] = true;
                let found = go(g, dir, next, len + 1, used, min, max, b);
                used[i] = false;
                if found {
                    return true;
                }
            }
        }
        false
    }
    let mut used = vec![false; g.r.len()];
    go(g, dir, a, 0, &mut used, min, max, b)
}

pub(crate) async fn build(
    g: &Graph,
    schema: bool,
    flush_half: bool,
    batch: Option<usize>,
) -> anyhow::Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            execution_batch_size: batch,
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            ..Default::default()
        })
        .build()
        .await?;
    if schema {
        db.schema()
            .label("N")
            .property("id", DataType::Int)
            .edge_type("R", &["N"], &["N"])
            .edge_type("S", &["N"], &["N"])
            .done()
            .apply()
            .await?;
    }
    let session = db.session();
    let nodes: Vec<String> = (0..g.n).map(|i| format!("(n{i}:N {{id: {i}}})")).collect();
    let tx = session.tx().await?;
    tx.execute(&format!("CREATE {}", nodes.join(", "))).await?;
    tx.commit().await?;
    let edges: Vec<String> = g
        .r
        .iter()
        .map(|(s, d)| format!("MATCH (x:N {{id: {s}}}), (y:N {{id: {d}}}) CREATE (x)-[:R]->(y)"))
        .chain(g.s.iter().map(|(s, d)| {
            format!("MATCH (x:N {{id: {s}}}), (y:N {{id: {d}}}) CREATE (x)-[:S]->(y)")
        }))
        .collect();
    let split = if flush_half { edges.len() / 2 } else { 0 };
    for (i, e) in edges.iter().enumerate() {
        if i == split && flush_half {
            db.flush().await?;
        }
        let tx = session.tx().await?;
        tx.execute(e).await?;
        tx.commit().await?;
    }
    Ok(db)
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Null => None,
        other => panic!("not an int: {other:?}"),
    }
}

/// Rows as `(c0, c1, c2)` with NULL as `None`, in a bag.
pub(crate) async fn bag(db: &Uni, q: &str) -> anyhow::Result<BTreeMap<Vec<Option<i64>>, usize>> {
    let rows = db
        .session()
        .query(q)
        .await
        .map_err(|e| anyhow::anyhow!("{q}: {e}"))?;
    let mut out = BTreeMap::new();
    for row in rows.rows() {
        let key: Vec<Option<i64>> = row.values().iter().map(int).collect();
        *out.entry(key).or_default() += 1;
    }
    Ok(out)
}

const DIRS: [&str; 3] = ["->", "<-", "-"];
const RANGES: [(&str, usize, usize); 9] = [
    ("", 1, 1),
    ("*", 1, usize::MAX),
    ("*1..", 1, usize::MAX),
    ("*1..2", 1, 2),
    ("*2..3", 2, 3),
    ("*2..", 2, usize::MAX),
    ("*0..", 0, usize::MAX),
    ("*0..1", 0, 1),
    ("*3", 3, 3),
];

pub(crate) fn rel(dir: &str, range: &str) -> String {
    match dir {
        "->" => format!("-[:R{range}]->"),
        "<-" => format!("<-[:R{range}]-"),
        _ => format!("-[:R{range}]-"),
    }
}

/// Which `(direction, upper bound)` combinations a run covers.
type Cover = fn(&str, usize) -> bool;

/// Undirected unbounded trails are exponential in the edge count, so the
/// random graphs leave them to the small fixed graph.
fn random_cover(dir: &str, max: usize) -> bool {
    dir != "-" || max != usize::MAX
}

async fn check_graph(
    g: &Graph,
    name: &str,
    cover: Cover,
    schema: bool,
    flush_half: bool,
    batch: Option<usize>,
) -> anyhow::Result<()> {
    let db = build(g, schema, flush_half, batch).await?;
    let ctx = format!("{name} schema={schema} flush_half={flush_half} batch={batch:?}");
    for dir in DIRS {
        for (range, min, max) in RANGES {
            if !cover(dir, max) {
                continue;
            }
            let p = rel(dir, range);
            // Long trails cannot exceed the edge count, so the search ends.
            let max = max.min(g.r.len());

            // EXISTS: per-source vs per-row vs brute force.
            let fast = bag(
                &db,
                &format!("MATCH (a:N), (b:N) WHERE EXISTS {{ MATCH (a){p}(b) }} RETURN a.id, b.id"),
            )
            .await?;
            let per_row = bag(
                &db,
                &format!(
                    "MATCH (a:N), (b:N) WHERE EXISTS {{ MATCH (a){p}(b) WHERE a.id = a.id AND b.id = b.id }} \
                     RETURN a.id, b.id"
                ),
            )
            .await?;
            let mut truth = BTreeMap::new();
            for a in 0..g.n {
                for b in 0..g.n {
                    if trail_exists(g, dir, min, max, a, b) {
                        truth.insert(vec![Some(a), Some(b)], 1);
                    }
                }
            }
            assert_eq!(fast, per_row, "EXISTS per-source vs per-row, {ctx} {p}");
            assert_eq!(fast, truth, "EXISTS vs brute force, {ctx} {p}");

            // NOT EXISTS is the complement.
            let not_fast = bag(
                &db,
                &format!(
                    "MATCH (a:N), (b:N) WHERE NOT EXISTS {{ MATCH (a){p}(b) }} RETURN a.id, b.id"
                ),
            )
            .await?;
            assert_eq!(
                not_fast.len() + fast.len(),
                (g.n * g.n) as usize,
                "NOT EXISTS complement, {ctx} {p}"
            );
            assert!(not_fast.keys().all(|k| !fast.contains_key(k)), "{ctx} {p}");

            // The pattern-predicate spelling.
            let pred = bag(
                &db,
                &format!("MATCH (a:N), (b:N) WHERE (a){p}(b) RETURN a.id, b.id"),
            )
            .await?;
            assert_eq!(pred, fast, "pattern predicate, {ctx} {p}");

            // COUNT: per-source vs per-row vs enumeration.
            let count_fast = bag(
                &db,
                &format!("MATCH (a:N), (b:N) RETURN a.id, b.id, COUNT {{ MATCH (a){p}(b) }}"),
            )
            .await?;
            let count_per_row = bag(
                &db,
                &format!(
                    "MATCH (a:N), (b:N) RETURN a.id, b.id, \
                     COUNT {{ MATCH (a){p}(b) WHERE a.id = a.id AND b.id = b.id }}"
                ),
            )
            .await?;
            assert_eq!(
                count_fast, count_per_row,
                "COUNT per-source vs per-row, {ctx} {p}"
            );
            let enumerated = bag(
                &db,
                &format!("MATCH (a:N){p}(b:N) RETURN a.id, b.id, count(*)"),
            )
            .await?;
            let nonzero: BTreeMap<_, _> = count_fast
                .iter()
                .filter(|(k, _)| k[2] != Some(0))
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            assert_eq!(nonzero, enumerated, "COUNT vs enumeration, {ctx} {p}");
        }
    }

    // Shapes beyond a single relationship.
    for q in [
        // A fresh intermediate node: `b` is still bare.
        "MATCH (a:N), (b:N) WHERE EXISTS { MATCH (a)-[:R*1..]->(x)-[:R]->(b) } RETURN a.id, b.id",
        // `a` is read by the WHERE, so `b` is the free one and `a.id` is in the key.
        "MATCH (a:N), (b:N) WHERE EXISTS { MATCH (a)-[:R*1..2]->(b) WHERE a.id > 1 } RETURN a.id, b.id",
        // A third import keeps its own binding.
        "MATCH (a:N), (b:N), (c:N) WHERE c.id < 3 AND EXISTS { MATCH (a)-[:R]->(c)-[:R*0..]->(b) } \
         RETURN a.id, b.id, c.id",
        // NULL endpoints from OPTIONAL MATCH.
        "MATCH (a:N) OPTIONAL MATCH (a)-[:S]->(b:N) WITH a, b \
         WHERE NOT EXISTS { MATCH (a)-[:R*1..]->(b) } RETURN a.id, b.id",
        "MATCH (b:N) OPTIONAL MATCH (a:N)-[:S]->(b) WITH a, b \
         RETURN a.id, b.id, COUNT { MATCH (a)-[:R*1..]->(b) }",
        // An outer relationship variable read by the body.
        "MATCH (a:N)-[s:S]->(c:N), (b:N) WHERE EXISTS { MATCH (a)-[s]->(c)-[:R*0..]->(b) } \
         RETURN a.id, b.id, c.id",
    ] {
        let fast = bag(&db, q).await?;
        let per_row = bag(&db, &disable_per_source(q)).await?;
        assert_eq!(fast, per_row, "{ctx}: {q}");
    }
    Ok(())
}

/// The same query with every endpoint the body names also read by its
/// `WHERE`, which keeps every row on the per-row path.
fn disable_per_source(q: &str) -> String {
    let (head, rest) = q.split_once("{ MATCH ").expect("has a body");
    let (body, tail) = rest.split_once(" }").expect("body ends");
    let guard = ["a", "b", "c"]
        .iter()
        .filter(|v| body.contains(&format!("({v}")))
        .map(|v| format!("coalesce({v}.id, 0) = coalesce({v}.id, 0)"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let body = if body.contains(" WHERE ") {
        format!("{body} AND {guard}")
    } else {
        format!("{body} WHERE {guard}")
    };
    format!("{head}{{ MATCH {body} }}{tail}")
}

#[tokio::test]
async fn exists_per_source_matches_per_row_and_brute_force() -> anyhow::Result<()> {
    // One configuration per graph keeps the debug-build run near 35 s; the
    // four graphs together cover every configuration.
    for (seed, (schema, flush_half, batch)) in (0_u64..).zip(CONFIGS) {
        let g = random_graph(seed);
        let name = format!("seed={seed}");
        check_graph(&g, &name, random_cover, schema, flush_half, batch).await?;
    }
    Ok(())
}

/// Undirected unbounded ranges over a small graph with a cycle, a parallel
/// pair and a self-loop: a walk may return to its start along the edge it
/// left by, a trail may not, so `a = b` separates the two.
#[tokio::test]
async fn exists_per_source_undirected_unbounded() -> anyhow::Result<()> {
    let g = Graph {
        n: 5,
        r: vec![(0, 1), (1, 2), (2, 0), (2, 3), (2, 3), (4, 4), (3, 4)],
        s: vec![(0, 3), (1, 1)],
    };
    for (schema, flush_half, batch) in [CONFIGS[0], CONFIGS[3]] {
        check_graph(&g, "fixed", |_, _| true, schema, flush_half, batch).await?;
    }
    Ok(())
}

/// `(schema, flush half the edges, execution batch size)`.
const CONFIGS: [(bool, bool, Option<usize>); 4] = [
    (true, true, None),
    (false, false, None),
    (true, false, Some(2)),
    (false, true, Some(3)),
];

/// The body runs once per source, not once per pair.
///
/// `physical_plans` counts the query's own plan plus one per sub-plan
/// execution, since the body is physically planned each time it runs. Over
/// `|a| = |b| = 30` the per-row path pays 900 executions; per source it pays
/// at most one bound execution and one free one per distinct `a`.
#[tokio::test]
async fn exists_per_source_cost_does_not_scale_with_targets() -> anyhow::Result<()> {
    let n = 30_i64;
    let g = Graph {
        n,
        r: (0..n).map(|i| (i, (i * 7 + 3) % n)).collect(),
        s: Vec::new(),
    };
    for batch in [None, Some(2)] {
        let db = build(&g, true, true, batch).await?;
        let q = "MATCH (a:N), (b:N) WHERE NOT EXISTS { MATCH (a)-[:R*1..]->(b) } RETURN count(*)";
        let fast = db.session().query(q).await?;
        let slow = db.session().query(&disable_per_source(q)).await?;
        assert_eq!(
            fast.rows()[0].values(),
            slow.rows()[0].values(),
            "batch={batch:?}"
        );
        let pairs = (n * n) as u64;
        assert_eq!(
            slow.metrics().physical_plans,
            1 + pairs,
            "per-row path executes once per pair, batch={batch:?}"
        );
        let fast_runs = fast.metrics().physical_plans - 1;
        assert!(
            fast_runs <= 2 * n as u64,
            "per-source path ran {fast_runs} sub-plans for {n} sources, batch={batch:?}"
        );
    }
    Ok(())
}

#[test]
fn trail_search_controls() {
    // 0 -> 1 -> 2, and an undirected back-and-forth is not a trail.
    let g = Graph {
        n: 3,
        r: vec![(0, 1), (1, 2)],
        s: Vec::new(),
    };
    assert!(trail_exists(&g, "->", 1, usize::MAX, 0, 2));
    assert!(!trail_exists(&g, "->", 1, usize::MAX, 2, 0));
    assert!(trail_exists(&g, "<-", 1, usize::MAX, 2, 0));
    assert!(!trail_exists(&g, "-", 1, 2, 0, 0));
    assert!(trail_exists(&g, "-", 0, 0, 0, 0));
    assert!(!trail_exists(&g, "->", 3, 3, 0, 2));
}
