// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Anchoring beyond #301: an IS reference on the walk's *target*, a reference
//! to a lower stratum, and the node a reference binds.
//!
//! Issue #301 walks a recursive clause's MATCH pattern from its same-stratum
//! reference when that reference tests the pattern's *source*. The 2026-10-17
//! perf-class audit measured three shapes it does not reach:
//!
//! * **A1b** — `(a)-[:T]->(m) WHERE m IS r TO b`: the reference tests the walk's
//!   target, and every iteration re-scanned the label.
//! * **A2** — a non-recursive clause referencing a lower-stratum rule scanned
//!   and traversed the whole label, then hash-joined the (complete) facts.
//! * **BindNode** — a node a reference *binds* was materialised by a full
//!   `ScanAll` joined on its vid, every iteration.
//!
//! The guards count operator rows under `profile()` at two graph sizes, so they
//! are not timing sensitive: the noise part of the graph can never reach the
//! answer, so an evaluation that costs the frontier costs the same at both
//! sizes. The differentials check answers against brute-force evaluations
//! computed here in Rust, independently of the engine, across directions,
//! multi-hop patterns, several references, ALONG / FOLD / BEST BY, recursive
//! and non-recursive clauses, and flushed and L0-only storage.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::Duration;

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, UniConfig, Value};

const SCHEMA: [&str; 4] = [
    "CREATE LABEL E (uid STRING, seed BOOL)",
    "CREATE LABEL S (uid STRING)",
    "CREATE EDGE TYPE T (w FLOAT) FROM E TO E",
    "CREATE INDEX idx_e_uid FOR (e:E) ON (e.uid)",
];

/// A graph by uid: `E` vertices and weighted `T` edges.
#[derive(Clone)]
struct Graph {
    /// (uid, seed)
    vertices: Vec<(String, bool)>,
    /// (src index, dst index, w)
    edges: Vec<(usize, usize, f64)>,
}

impl Graph {
    fn index(&self, uid: &str) -> usize {
        self.vertices
            .iter()
            .position(|(u, _)| u == uid)
            .unwrap_or_else(|| panic!("no vertex {uid}"))
    }

    fn uid(&self, i: usize) -> String {
        self.vertices[i].0.clone()
    }
}

/// The chain `c0 -> c1 -> ... -> c8` (`c0` the seed, weights 1..8) plus
/// `n_noise` edges among noise nodes no chain node reaches or is reached from.
fn chain_noise(n_noise: usize) -> Graph {
    let mut vertices: Vec<(String, bool)> = (0..9).map(|i| (format!("c{i}"), i == 0)).collect();
    let len = n_noise / 2 + 2;
    vertices.extend((0..len).map(|i| (format!("n{i}"), false)));
    let mut edges: Vec<(usize, usize, f64)> = (0..8).map(|i| (i, i + 1, (i + 1) as f64)).collect();
    for i in 0..n_noise {
        let (a, b) = (i % len, (i * 7 + 3) % len);
        if a != b {
            edges.push((9 + a, 9 + b, 1.0));
        }
    }
    Graph { vertices, edges }
}

fn no_auto_flush() -> UniConfig {
    UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: usize::MAX / 2,
        query_timeout: Duration::from_secs(900),
        ..Default::default()
    }
}

async fn apply_schema(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    for ddl in SCHEMA {
        tx.execute(ddl).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Bulk-load, flush, close, reopen: every row is in L1.
async fn build_flushed(g: &Graph, dir: &std::path::Path) -> Result<Uni> {
    let path = dir.to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).build().await?;
        apply_schema(&db).await?;
        let tx = db.session().tx().await?;
        let mut bulk = tx.bulk_writer().build()?;
        let vs: Vec<HashMap<String, Value>> = g
            .vertices
            .iter()
            .map(|(uid, seed)| {
                HashMap::from([
                    ("uid".to_string(), Value::String(uid.clone())),
                    ("seed".to_string(), Value::Bool(*seed)),
                ])
            })
            .collect();
        let vids = bulk.insert_vertices("E", vs).await?;
        let es = g
            .edges
            .iter()
            .map(|&(a, b, w)| {
                uni_bulk::EdgeData::new(
                    vids[a],
                    vids[b],
                    HashMap::from([("w".to_string(), Value::Float(w))]),
                )
            })
            .collect();
        bulk.insert_edges("T", es).await?;
        bulk.commit().await?;
        tx.commit().await?;
        db.flush().await?;
        db.shutdown().await?;
    }
    Ok(Uni::open(&path).build().await?)
}

/// Cypher writes only, auto-flush disabled: every row stays in L0.
async fn build_l0(g: &Graph) -> Result<Uni> {
    let db = Uni::in_memory().config(no_auto_flush()).build().await?;
    apply_schema(&db).await?;
    let vs: Vec<Value> = g
        .vertices
        .iter()
        .map(|(uid, seed)| {
            Value::Map(HashMap::from([
                ("uid".to_string(), Value::String(uid.clone())),
                ("seed".to_string(), Value::Bool(*seed)),
            ]))
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $rows AS r CREATE (:E {uid: r.uid, seed: r.seed})")
        .param("rows", Value::List(vs))
        .run()
        .await?;
    tx.commit().await?;
    let es: Vec<Value> = g
        .edges
        .iter()
        .map(|&(a, b, w)| {
            Value::Map(HashMap::from([
                ("a".to_string(), Value::String(g.uid(a))),
                ("b".to_string(), Value::String(g.uid(b))),
                ("w".to_string(), Value::Float(w)),
            ]))
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.execute_with(
        "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) \
         CREATE (a)-[:T {w: r.w}]->(b)",
    )
    .param("rows", Value::List(es))
    .run()
    .await?;
    tx.commit().await?;
    Ok(db)
}

fn short_op(name: &str) -> String {
    name.split(['(', ':', ' '])
        .next()
        .unwrap_or(name)
        .to_string()
}

/// Operator totals of one evaluation unit (an iteration or a stratum),
/// keyed by short operator name: (summed `actual_rows`, operator count).
#[derive(Debug, Default)]
struct Ops(BTreeMap<String, (usize, usize)>);

impl Ops {
    fn rows(&self, op: &str) -> usize {
        self.0.get(op).map_or(0, |(rows, _)| *rows)
    }

    fn render(&self) -> String {
        self.0
            .iter()
            .map(|(k, (r, n))| format!("{k}x{n}={r}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn ops_of(operators: &[uni_db::query_crate::OperatorStats]) -> Ops {
    let mut ops = Ops::default();
    for o in operators {
        let e = ops.0.entry(short_op(&o.operator)).or_default();
        e.0 += o.actual_rows;
        e.1 += 1;
    }
    ops
}

/// What one profiled run reports for the rule under test.
struct Profiled {
    /// The QUERY's rows, each rendered `col=value` in column order.
    rows: Vec<String>,
    /// Per-iteration operator totals of the rule (recursive strata).
    iterations: Vec<(usize, Ops)>,
    /// Operator totals of the stratum holding the rule (non-recursive strata
    /// report their operators per stratum).
    stratum: Ops,
}

fn render_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Float(f) => format!("{f:.3}"),
        Value::Int(i) => format!("{i}"),
        other => format!("{other:?}"),
    }
}

fn locy_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 200,
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    }
}

/// Runs `program` under `profile()`; `rule` names the rule whose cost is read.
async fn profile_rule(db: &Uni, program: &str, rule: &str) -> Result<Profiled> {
    let (result, profile) = db
        .session()
        .locy_with(program)
        .with_config(locy_config())
        .profile()
        .await?;
    let mut rows: Vec<String> = result
        .rows()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    let sorted: BTreeMap<&String, &Value> = r.iter().collect();
                    sorted
                        .iter()
                        .map(|(c, v)| format!("{c}={}", render_value(v)))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect()
        })
        .unwrap_or_default();
    rows.sort();
    let mut iterations = Vec::new();
    let mut stratum = Ops::default();
    for s in &profile.profile.strata {
        let holds = s.rules.iter().any(|r| r.name == rule);
        if !holds {
            continue;
        }
        stratum = ops_of(&s.operators);
        for r in s.rules.iter().filter(|r| r.name == rule) {
            for it in &r.iterations {
                iterations.push((it.delta_facts, ops_of(&it.operators)));
            }
        }
    }
    Ok(Profiled {
        rows,
        iterations,
        stratum,
    })
}

/// Measurement: prints what each shape costs per iteration, at two sizes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement probe; run with --run-ignored all --no-capture"]
async fn anchor_p4_probe() -> Result<()> {
    for n in [2_000usize, 8_000] {
        let dir = tempfile::tempdir()?;
        let db = build_flushed(&chain_noise(n), dir.path()).await?;
        for (name, program, rule) in PROBED {
            let started = std::time::Instant::now();
            let p = profile_rule(&db, program, rule).await?;
            eprintln!(
                "\n=== N={n} {name}: rows={} {:.1}ms stratum: {}",
                p.rows.len(),
                started.elapsed().as_secs_f64() * 1000.0,
                p.stratum.render()
            );
            for (i, (delta, ops)) in p.iterations.iter().enumerate() {
                eprintln!("    it{i} delta={delta} | {}", ops.render());
            }
        }
    }
    Ok(())
}

/// (name, program, rule under test)
const PROBED: [(&str, &str, &str); 8] = [
    ("A1b target-side, binds b", A1B, "back"),
    ("A1b target-side, one KEY", A1B_KEY1, "back"),
    ("BindNode forward", BIND_FWD, "fwd"),
    ("A2 cross-stratum", A2, "nb"),
    ("A2 target-side", A2_TARGET, "nb"),
    ("A2 nine facts", A2_MULTI, "nb"),
    ("A2 every vertex a fact", A2_ALL, "nb"),
    ("non-linear TC", NONLINEAR, "tc"),
];

/// A1b: the reference tests the walk's target `m`; `b` is bound by it.
const A1B: &str = "\
CREATE RULE back AS MATCH (a:E)-[:T]->(b:E) WHERE b.uid = 'c8' YIELD KEY a, KEY b \n\
CREATE RULE back AS MATCH (a:E)-[:T]->(m:E) WHERE m IS back TO b YIELD KEY a, KEY b \n\
QUERY back RETURN a.uid AS uid";

/// A1b with no bound node: only the target-side anchor is exercised.
const A1B_KEY1: &str = "\
CREATE RULE back AS MATCH (a:E)-[:T]->(b:E) WHERE b.uid = 'c8' YIELD KEY a \n\
CREATE RULE back AS MATCH (a:E)-[:T]->(m:E) WHERE m IS back YIELD KEY a \n\
QUERY back RETURN a.uid AS uid";

/// The source-side anchor of #301, with `a` bound by the reference.
const BIND_FWD: &str = "\
CREATE RULE fwd AS MATCH (a:E)-[:T]->(b:E) WHERE a.uid = 'c0' YIELD KEY a, KEY b \n\
CREATE RULE fwd AS MATCH (m:E)-[:T]->(b:E) WHERE a IS fwd TO m YIELD KEY a, KEY b \n\
QUERY fwd RETURN b.uid AS uid";

/// A2: a non-recursive clause referencing a one-fact lower stratum.
const A2: &str = "\
CREATE RULE sd AS MATCH (e:E) WHERE e.uid = 'c3' YIELD KEY e \n\
CREATE RULE nb AS MATCH (e:E)-[:T]->(f:E) WHERE e IS sd YIELD KEY f \n\
QUERY nb RETURN f.uid AS uid";

/// A2 with the reference on the walk's target.
const A2_TARGET: &str = "\
CREATE RULE sd AS MATCH (e:E) WHERE e.uid = 'c3' YIELD KEY e \n\
CREATE RULE nb AS MATCH (f:E)-[:T]->(e:E) WHERE e IS sd YIELD KEY f \n\
QUERY nb RETURN f.uid AS uid";

/// A2 whose fact was reached by a traversal, and whose clause reads a
/// property of the anchored vertex, which only its vid lookup scans.
const A2_HOP: &str = "\
CREATE RULE sd AS MATCH (x:E)-[:T]->(e:E) WHERE x.uid = 'c2' YIELD KEY e \n\
CREATE RULE nb AS MATCH (e:E)-[:T]->(f:E) WHERE e IS sd AND e.seed = false YIELD KEY f \n\
QUERY nb RETURN f.uid AS uid";

/// A2 over a recursive lower stratum: the nine chain nodes.
const A2_MULTI: &str = "\
CREATE RULE reach AS MATCH (e:E) WHERE e.seed = true YIELD KEY e \n\
CREATE RULE reach AS MATCH (o:E)-[:T]->(e:E) WHERE o IS reach YIELD KEY e \n\
CREATE RULE nb AS MATCH (e:E)-[:T]->(f:E) WHERE e IS reach YIELD KEY e, KEY f \n\
QUERY nb RETURN e.uid AS e, f.uid AS f";

/// A2 where every vertex is a fact: the seed covers the whole label, so the
/// lookup reads it in one pass. Measures what anchoring costs when it cannot
/// help.
const A2_ALL: &str = "\
CREATE RULE sd AS MATCH (e:E) YIELD KEY e \n\
CREATE RULE nb AS MATCH (e:E)-[:T]->(f:E) WHERE e IS sd YIELD KEY e, KEY f \n\
QUERY nb RETURN count(*) AS n";

/// Non-linear transitive closure: both references bind a node.
const NONLINEAR: &str = "\
CREATE RULE tc AS MATCH (a:E)-[:T]->(b:E) WHERE a.uid = 'c0' YIELD KEY a, KEY b \n\
CREATE RULE tc AS MATCH (a:E)-[:T]->(b:E) WHERE a.uid STARTS WITH 'c' YIELD KEY a, KEY b \n\
CREATE RULE tc AS MATCH (a:E) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b \n\
QUERY tc WHERE a.uid = 'c0' RETURN b.uid AS uid";

// ---------------------------------------------------------------------------
// Guards: cost tracks the frontier / the facts, not the label.
// ---------------------------------------------------------------------------

/// `GraphScanExec` and `GraphTraverseExec` rows per iteration after the first.
///
/// The first iteration also runs the seed clause, whose scan reads the label
/// through the `uid` index; every later one runs the recursive clause alone.
fn per_iteration_reads(p: &Profiled) -> Vec<(usize, usize)> {
    p.iterations
        .iter()
        .skip(1)
        .map(|(_, ops)| (ops.rows("GraphScanExec"), ops.rows("GraphTraverseExec")))
        .collect()
}

fn chain_answer(range: std::ops::Range<usize>) -> Vec<String> {
    let mut v: Vec<String> = range.map(|i| format!("uid=c{i}")).collect();
    v.sort();
    v
}

/// A1b and the #301 leftover: a recursive clause anchored on the walk's
/// target, and a node the reference binds, cost the frontier per iteration.
///
/// Before: the bound node was materialised by a `ScanAll` hash-joined on
/// `b._vid` — `GraphScanExec` read every vertex (1,012 / 4,012 rows at the two
/// sizes) on every iteration. After: the bound node is looked up by vid, so an
/// iteration reads one vertex for the anchor and one for the bound node.
#[tokio::test(flavor = "multi_thread")]
async fn p4_target_side_and_bound_node_cost_tracks_frontier() -> Result<()> {
    let mut per_size: BTreeMap<&str, Vec<Vec<(usize, usize)>>> = BTreeMap::new();
    for n in [2_000usize, 8_000] {
        let dir = tempfile::tempdir()?;
        let db = build_flushed(&chain_noise(n), dir.path()).await?;
        for (name, program, rule, answer) in [
            ("A1b binds b", A1B, "back", chain_answer(0..8)),
            ("A1b one KEY", A1B_KEY1, "back", chain_answer(0..8)),
            ("bind forward", BIND_FWD, "fwd", chain_answer(1..9)),
        ] {
            let p = profile_rule(&db, program, rule).await?;
            assert_eq!(p.rows, answer, "N={n} {name}: wrong answer");
            let deltas: Vec<usize> = p.iterations.iter().map(|(d, _)| *d).collect();
            assert_eq!(
                deltas,
                [1, 1, 1, 1, 1, 1, 1, 1, 0],
                "N={n} {name}: one chain node per iteration, then convergence"
            );
            let reads = per_iteration_reads(&p);
            eprintln!("N={n} {name}: {reads:?}");
            per_size.entry(name).or_default().push(reads);
        }
    }
    for (name, sizes) in &per_size {
        assert_eq!(
            sizes[0], sizes[1],
            "{name}: per-iteration reads grew with the label (N=2000 vs N=8000)"
        );
    }
    // Iterations 1..=7 each look up one frontier vertex (the anchor), walk its
    // one chain edge and, where the reference binds a node, look that node up
    // by vid. The converging iteration looks up the last frontier vertex,
    // finds no edge, and so has nothing to look the bound node up for.
    let binds = [vec![(2, 1); 7], vec![(1, 0)]].concat();
    let plain = [vec![(1, 1); 7], vec![(1, 0)]].concat();
    assert_eq!(per_size["A1b binds b"][0], binds, "A1b binds b");
    assert_eq!(per_size["bind forward"][0], binds, "bind forward");
    assert_eq!(per_size["A1b one KEY"][0], plain, "A1b one KEY");
    Ok(())
}

/// A2: a clause over a lower stratum's facts reads the facts' vertices, not
/// the label.
///
/// Before: `GraphScanExec` read the whole label (1,011 / 4,011 rows) and
/// `GraphTraverseExec` walked every edge (2,008 / 8,008) before the hash join
/// kept the one fact's neighbours.
#[tokio::test(flavor = "multi_thread")]
async fn p4_lower_stratum_cost_tracks_facts_not_label() -> Result<()> {
    let mut per_size: BTreeMap<&str, Vec<(usize, usize)>> = BTreeMap::new();
    for n in [2_000usize, 8_000] {
        let dir = tempfile::tempdir()?;
        let db = build_flushed(&chain_noise(n), dir.path()).await?;
        let multi: Vec<String> = (0..8).map(|i| format!("e=c{i},f=c{}", i + 1)).collect();
        for (name, program, answer) in [
            ("source side", A2, vec!["uid=c4".to_string()]),
            ("target side", A2_TARGET, vec!["uid=c2".to_string()]),
            ("nine facts", A2_MULTI, multi),
        ] {
            let p = profile_rule(&db, program, "nb").await?;
            assert_eq!(p.rows, answer, "N={n} {name}: wrong answer");
            let [(_, ops)] = p.iterations.as_slice() else {
                panic!("N={n} {name}: a non-recursive rule runs once");
            };
            let reads = (ops.rows("GraphScanExec"), ops.rows("GraphTraverseExec"));
            eprintln!("N={n} {name}: {reads:?}");
            per_size.entry(name).or_default().push(reads);
        }
    }
    for (name, sizes) in &per_size {
        assert_eq!(
            sizes[0], sizes[1],
            "{name}: reads grew with the label (N=2000 vs N=8000)"
        );
    }
    // One fact: look it up, walk its one edge. Nine facts (c0..c8): look each
    // up, walk the eight chain edges.
    assert_eq!(per_size["source side"][0], (1, 1));
    assert_eq!(per_size["target side"][0], (1, 1));
    assert_eq!(per_size["nine facts"][0], (9, 8));
    Ok(())
}

// ---------------------------------------------------------------------------
// Differentials: answers against brute-force evaluations computed here.
// ---------------------------------------------------------------------------

/// The chain `c0..c8` plus 20 nodes `r0..r19` and pseudo-random edges among
/// all of them (cycles, parallel edges, no self-loops), weights 1..=5.
fn mixed_graph() -> Graph {
    let mut vertices: Vec<(String, bool)> = (0..9).map(|i| (format!("c{i}"), i == 0)).collect();
    vertices.extend((0..20).map(|i| (format!("r{i}"), false)));
    let n = vertices.len();
    let mut edges: Vec<(usize, usize, f64)> = (0..8).map(|i| (i, i + 1, 1.0)).collect();
    let mut x: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    while edges.len() < 60 {
        let a = (next() % n as u64) as usize;
        let b = (next() % n as u64) as usize;
        if a != b {
            edges.push((a, b, (next() % 5 + 1) as f64));
        }
    }
    Graph { vertices, edges }
}

/// Pairs `(a, b)` with a walk of length >= 1 from `a` to `b` whose length is
/// congruent to `1` modulo `step`, following `next(node)`.
fn walks(g: &Graph, step: usize, next: &dyn Fn(usize) -> Vec<usize>) -> BTreeSet<(usize, usize)> {
    let mut out = BTreeSet::new();
    for a in 0..g.vertices.len() {
        // (node, walk length mod step)
        let mut seen = BTreeSet::new();
        let mut queue = VecDeque::new();
        for m in next(a) {
            if seen.insert((m, 1 % step)) {
                queue.push_back((m, 1 % step));
            }
        }
        while let Some((u, k)) = queue.pop_front() {
            if k == 1 % step {
                out.insert((a, u));
            }
            for v in next(u) {
                let state = (v, (k + 1) % step);
                if seen.insert(state) {
                    queue.push_back(state);
                }
            }
        }
    }
    out
}

fn out_neighbours(g: &Graph) -> impl Fn(usize) -> Vec<usize> + '_ {
    move |u| g.edges.iter().filter(|e| e.0 == u).map(|e| e.1).collect()
}

fn in_neighbours(g: &Graph) -> impl Fn(usize) -> Vec<usize> + '_ {
    move |u| g.edges.iter().filter(|e| e.1 == u).map(|e| e.0).collect()
}

fn any_neighbours(g: &Graph) -> impl Fn(usize) -> Vec<usize> + '_ {
    move |u| {
        g.edges
            .iter()
            .filter_map(|e| match (e.0 == u, e.1 == u) {
                (true, _) => Some(e.1),
                (_, true) => Some(e.0),
                _ => None,
            })
            .collect()
    }
}

fn pair_rows(g: &Graph, pairs: &BTreeSet<(usize, usize)>) -> Vec<String> {
    let mut v: Vec<String> = pairs
        .iter()
        .map(|&(a, b)| format!("a={},b={}", g.uid(a), g.uid(b)))
        .collect();
    v.sort();
    v
}

fn uid_set(g: &Graph, uids: &[&str]) -> BTreeSet<usize> {
    uids.iter().map(|u| g.index(u)).collect()
}

const SRC: &str =
    "CREATE RULE src AS MATCH (e:E) WHERE e.uid IN ['c0', 'c4', 'r3', 'r7'] YIELD KEY e \n";
const DST: &str =
    "CREATE RULE dst AS MATCH (e:E) WHERE e.uid IN ['c5', 'c8', 'r1', 'r9'] YIELD KEY e \n";
const SRC_UIDS: [&str; 4] = ["c0", "c4", "r3", "r7"];
const DST_UIDS: [&str; 4] = ["c5", "c8", "r1", "r9"];

/// Every differential case: (name, program, expected sorted rows).
fn differential_cases(g: &Graph) -> Vec<(&'static str, String, Vec<String>)> {
    let out = out_neighbours(g);
    let inn = in_neighbours(g);
    let any = any_neighbours(g);
    let tc = walks(g, 1, &out);
    let reverse_tc = walks(g, 1, &inn);
    let undirected = walks(g, 1, &any);
    let odd = walks(g, 2, &out);
    let src = uid_set(g, &SRC_UIDS);
    let dst = uid_set(g, &DST_UIDS);
    let mut cases = Vec::new();

    // Recursive, anchored on the walk's target, in each direction; `b` is
    // bound by the reference and read back by property.
    cases.push((
        "target-side ->",
        "CREATE RULE r AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
         CREATE RULE r AS MATCH (a:E)-[:T]->(m:E) WHERE m IS r TO b YIELD KEY a, KEY b \n\
         QUERY r RETURN a.uid AS a, b.uid AS b"
            .to_string(),
        pair_rows(g, &tc),
    ));
    cases.push((
        "target-side <-",
        "CREATE RULE r AS MATCH (a:E)<-[:T]-(b:E) YIELD KEY a, KEY b \n\
         CREATE RULE r AS MATCH (a:E)<-[:T]-(m:E) WHERE m IS r TO b YIELD KEY a, KEY b \n\
         QUERY r RETURN a.uid AS a, b.uid AS b"
            .to_string(),
        pair_rows(g, &reverse_tc),
    ));
    cases.push((
        "target-side undirected",
        "CREATE RULE r AS MATCH (a:E)-[:T]-(b:E) YIELD KEY a, KEY b \n\
         CREATE RULE r AS MATCH (a:E)-[:T]-(m:E) WHERE m IS r TO b YIELD KEY a, KEY b \n\
         QUERY r RETURN a.uid AS a, b.uid AS b"
            .to_string(),
        pair_rows(g, &undirected),
    ));
    // Source side with the subject bound by the reference (BindNode).
    cases.push((
        "source-side binds a",
        "CREATE RULE r AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
         CREATE RULE r AS MATCH (m:E)-[:T]->(b:E) WHERE a IS r TO m YIELD KEY a, KEY b \n\
         QUERY r RETURN a.uid AS a, b.uid AS b"
            .to_string(),
        pair_rows(g, &tc),
    ));
    // Two hops, anchored on the far end of a five-element path.
    cases.push((
        "two hops",
        "CREATE RULE r AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
         CREATE RULE r AS MATCH (a:E)-[:T]->(x:E)-[:T]->(m:E) WHERE m IS r TO b \
           YIELD KEY a, KEY b \n\
         QUERY r RETURN a.uid AS a, b.uid AS b"
            .to_string(),
        pair_rows(g, &odd),
    ));
    // Non-linear: two references, both binding a node.
    cases.push((
        "non-linear",
        "CREATE RULE r AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
         CREATE RULE r AS MATCH (a:E) WHERE a IS r TO m, m IS r TO b YIELD KEY a, KEY b \n\
         QUERY r RETURN a.uid AS a, b.uid AS b"
            .to_string(),
        pair_rows(g, &tc),
    ));
    // A recursive reference beside a lower-stratum one that tests the bound
    // node.
    let to_dst: BTreeSet<(usize, usize)> = tc
        .iter()
        .copied()
        .filter(|(_, b)| dst.contains(b))
        .collect();
    cases.push((
        "recursive + lower refs",
        format!(
            "{DST}CREATE RULE r AS MATCH (a:E)-[:T]->(b:E) WHERE b IS dst YIELD KEY a, KEY b \n\
             CREATE RULE r AS MATCH (a:E)-[:T]->(m:E) WHERE m IS r TO b AND b IS dst \
               YIELD KEY a, KEY b \n\
             QUERY r RETURN a.uid AS a, b.uid AS b"
        ),
        pair_rows(g, &to_dst),
    ));
    // Non-recursive, two lower-stratum references on both ends; the
    // smaller rule name (`dst`) anchors, on the walk's target.
    let both: BTreeSet<(usize, usize)> = g
        .edges
        .iter()
        .filter(|e| src.contains(&e.0) && dst.contains(&e.1))
        .map(|e| (e.0, e.1))
        .collect();
    cases.push((
        "two lower refs",
        format!(
            "{SRC}{DST}CREATE RULE r AS MATCH (a:E)-[:T]->(b:E) WHERE a IS src AND b IS dst \
               YIELD KEY a, KEY b \n\
             QUERY r RETURN a.uid AS a, b.uid AS b"
        ),
        pair_rows(g, &both),
    ));
    // Lower stratum, each direction.
    for (name, arrow, nb) in [
        ("lower ->", "-[:T]->", &out as &dyn Fn(usize) -> Vec<usize>),
        ("lower <-", "<-[:T]-", &inn),
        ("lower undirected", "-[:T]-", &any),
    ] {
        let pairs: BTreeSet<(usize, usize)> = src
            .iter()
            .flat_map(|&a| nb(a).into_iter().map(move |b| (a, b)))
            .collect();
        cases.push((
            name,
            format!(
                "{SRC}CREATE RULE r AS MATCH (a:E){arrow}(b:E) WHERE a IS src YIELD KEY a, KEY b \n\
                 QUERY r RETURN a.uid AS a, b.uid AS b"
            ),
            pair_rows(g, &pairs),
        ));
    }
    // FOLD over a lower stratum: per target, the summed weight of the edges
    // from a source fact. A repeated walk would double a sum.
    let mut sums: BTreeMap<usize, f64> = BTreeMap::new();
    for e in g.edges.iter().filter(|e| src.contains(&e.0)) {
        *sums.entry(e.1).or_default() += e.2;
    }
    cases.push((
        "lower FOLD SUM",
        format!(
            "{SRC}CREATE RULE r AS MATCH (e:E)-[w:T]->(f:E) WHERE e IS src \
               FOLD s = SUM(w.w) YIELD KEY f, s \n\
             QUERY r RETURN f.uid AS f, s"
        ),
        sums.iter()
            .map(|(f, s)| format!("f={},s={s:.3}", g.uid(*f)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    ));
    // FOLD over a lower stratum with several facts per anchored node: each
    // `(a, b)` fact — one per distinct successor `b`, parallel edges
    // collapsing on the KEY — is a separate derivation of every edge out of
    // `a`.
    let mut fan: BTreeMap<usize, f64> = BTreeMap::new();
    for a in 0..g.vertices.len() {
        let degree = out(a).into_iter().collect::<BTreeSet<_>>().len();
        let weight: f64 = g.edges.iter().filter(|e| e.0 == a).map(|e| e.2).sum();
        if degree > 0 {
            fan.insert(a, degree as f64 * weight);
        }
    }
    cases.push((
        "lower FOLD, facts share the anchor",
        "CREATE RULE pairs AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
         CREATE RULE r AS MATCH (a:E)-[w:T]->(c:E) WHERE (a, b) IS pairs \
           FOLD s = SUM(w.w) YIELD KEY a, s \n\
         QUERY r RETURN a.uid AS a, s"
            .to_string(),
        fan.iter()
            .map(|(a, s)| format!("a={},s={s:.3}", g.uid(*a)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    ));
    // ALONG + BEST BY, anchored on the target: the cheapest walk to `c8`.
    let target = g.index("c8");
    let mut best: BTreeMap<usize, f64> = BTreeMap::new();
    loop {
        let mut changed = false;
        for &(a, m, w) in &g.edges {
            let via = if m == target { Some(w) } else { None };
            let through = best.get(&m).map(|d| d + w);
            for candidate in via.into_iter().chain(through) {
                if best.get(&a).is_none_or(|d| candidate < *d) {
                    best.insert(a, candidate);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    cases.push((
        "target-side ALONG BEST BY",
        "CREATE RULE d AS MATCH (a:E)-[w:T]->(b:E) WHERE b.uid = 'c8' \
           ALONG c = w.w BEST BY c ASC YIELD KEY a, c \n\
         CREATE RULE d AS MATCH (a:E)-[w:T]->(m:E) WHERE m IS d \
           ALONG c = prev.c + w.w BEST BY c ASC YIELD KEY a, c \n\
         QUERY d RETURN a.uid AS a, c"
            .to_string(),
        best.iter()
            .map(|(a, c)| format!("a={},c={c:.3}", g.uid(*a)))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    ));
    cases
}

/// Every differential case agrees with its brute-force answer, flushed and
/// L0-only.
#[tokio::test(flavor = "multi_thread")]
async fn p4_anchored_answers_match_brute_force() -> Result<()> {
    let g = mixed_graph();
    let dir = tempfile::tempdir()?;
    let stores = [
        ("flushed", build_flushed(&g, dir.path()).await?),
        ("l0", build_l0(&g).await?),
    ];
    let mut failures = Vec::new();
    for (store, db) in &stores {
        for (name, program, expected) in differential_cases(&g) {
            let got = match db
                .session()
                .locy_with(&program)
                .with_config(locy_config())
                .run()
                .await
            {
                Ok(result) => {
                    let mut rows: Vec<String> = result
                        .rows()
                        .expect("every case has a QUERY")
                        .iter()
                        .map(|r| {
                            let sorted: BTreeMap<&String, &Value> = r.iter().collect();
                            sorted
                                .iter()
                                .map(|(c, v)| format!("{c}={}", render_value(v)))
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .collect();
                    rows.sort();
                    rows.dedup();
                    rows
                }
                Err(e) => vec![format!("ERROR {e}")],
            };
            assert!(!expected.is_empty(), "{name}: the case must have an answer");
            if got != expected {
                failures.push(format!(
                    "{store} {name}: got {} rows, want {}\n  got:  {got:?}\n  want: {expected:?}",
                    got.len(),
                    expected.len()
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// ---------------------------------------------------------------------------
// SSI: a looked-up vertex is still in the transaction's read set.
// ---------------------------------------------------------------------------

/// A Locy read whose anchored clause looked a vertex up must conflict with a
/// concurrent write to that vertex, to the edge it walked, or to the vertex it
/// reached — exactly as the unanchored full scan did.
///
/// `VidLookupJoinExec` looks *through* the SSI `ReadSetRecordingExec` over its
/// probe scan and keeps it as the probe child, so a looked-up vertex is
/// recorded. These cases do not isolate that wrapper: a traversal records its
/// source and target vertices and its edges too (`record_edge_adjacency`), and
/// they still pass with the wrapper dropped. They guard the end-to-end
/// property — anchoring loses no conflict the full scan had — before and
/// after the change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p4_anchored_reads_stay_in_the_ssi_read_set() -> Result<()> {
    // (program, the anchored vertex, a vertex the walk reached from it, the
    // walked edge as (src, dst), the vertex a reference binds if any)
    let cases = [
        (A2, "c3", "c4", ("c3", "c4"), None),
        (A2_HOP, "c3", "c4", ("c3", "c4"), None),
        (A2_TARGET, "c3", "c2", ("c2", "c3"), None),
        (A1B, "c3", "c2", ("c2", "c3"), Some("c8")),
        (BIND_FWD, "c3", "c4", ("c3", "c4"), Some("c0")),
    ];
    for (program, anchored, reached, (src, dst), bound) in cases {
        let mut writes = vec![
            (
                "SET on the anchored vertex",
                format!("MATCH (e:E {{uid: '{anchored}'}}) SET e.seed = true"),
            ),
            (
                "SET on the reached vertex",
                format!("MATCH (e:E {{uid: '{reached}'}}) SET e.seed = true"),
            ),
            (
                "DELETE of the walked edge",
                format!("MATCH (:E {{uid: '{src}'}})-[t:T]->(:E {{uid: '{dst}'}}) DELETE t"),
            ),
            (
                "SET on the anchored vertex with a new neighbour",
                format!(
                    "MATCH (e:E {{uid: '{anchored}'}}) SET e.seed = true \
                     CREATE (e)-[:T {{w: 1.0}}]->(:E {{uid: 'new', seed: false}})"
                ),
            ),
        ];
        if let Some(bound) = bound {
            writes.push((
                "SET on the bound vertex",
                format!("MATCH (e:E {{uid: '{bound}'}}) SET e.seed = true"),
            ));
        }
        for (what, write) in writes {
            let db = build_l0(&chain_noise(200)).await?;
            let session = db.session();
            let tx = session.tx().await?;
            let result = tx.locy(program).await?;
            assert!(
                result.rows().is_some_and(|r| !r.is_empty()),
                "{program}: the read must see the chain"
            );
            {
                let other = db.session();
                let tx_b = other.tx().await?;
                tx_b.execute(&write).await?;
                tx_b.commit().await?;
            }
            tx.execute("CREATE (:S {uid: 'summary'})").await?;
            match tx.commit().await {
                Err(uni_db::UniError::SerializationConflict { .. }) => {}
                other => panic!(
                    "{what} after\n{program}\nmust conflict with the Locy read, got {other:?}"
                ),
            }
        }
    }
    Ok(())
}

/// Observation, not a guard: does a pure edge insert from the anchored vertex
/// conflict? The read set is item-level and phantoms are out of its contract
/// (`OccReadSet`), so the answer is whatever the unanchored plan gave too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "observation; run with --run-ignored all --no-capture"]
async fn p4_edge_insert_from_anchor_observation() -> Result<()> {
    for program in [A2, A2_TARGET] {
        let db = build_l0(&chain_noise(200)).await?;
        let session = db.session();
        let tx = session.tx().await?;
        tx.locy(program).await?;
        {
            let other = db.session();
            let tx_b = other.tx().await?;
            tx_b.execute(
                "MATCH (a:E {uid: 'c3'}), (b:E {uid: 'c6'}) CREATE (a)-[:T {w: 1.0}]->(b), \
                 (b)-[:T {w: 1.0}]->(a)",
            )
            .await?;
            tx_b.commit().await?;
        }
        tx.execute("CREATE (:S {uid: 'summary'})").await?;
        eprintln!("edge insert vs {program:?}: {:?}", tx.commit().await.err());
    }
    Ok(())
}
