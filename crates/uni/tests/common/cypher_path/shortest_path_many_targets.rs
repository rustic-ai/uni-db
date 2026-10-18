// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `shortestPath` / `allShortestPaths` with an unbound or filtered target (A8).
//!
//! The pattern `shortestPath((a {id})-[:R*]->(b:N)) WHERE b.x < 20` used to
//! cross-join the source with *every* `N` node, run one breadth-first search per
//! row, and only then apply `b.x < 20`: `|B|` searches where one suffices, and
//! `N` rows where the filter leaves 20. Two families of tests live here.
//!
//! - **Differential.** Random small graphs, part flushed and part still in L0,
//!   queried in many shapes (direction, edge types, hop bounds, an edge property
//!   map, one or several sources, target filters, `OPTIONAL MATCH`, a fork) and
//!   compared against a brute-force BFS oracle written here, which shares no
//!   code with the engine: every pair, every length, every `allShortestPaths`
//!   path with its multiplicity, and for `shortestPath` a path that is one of
//!   the pair's shortest (see [`check_shortest`] for why not *which* one).
//! - **Cost guards.** The same queries at two scales under `profile()`: the
//!   filtered shape must hand the path operator only the filtered targets, and
//!   the unbound shape must not grow quadratically.

// Rust guideline compliant

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, Session, Uni, UniConfig, Value};

// ---------------------------------------------------------------------------
// Random graphs and the oracle
// ---------------------------------------------------------------------------

/// SplitMix64: a dependency-free deterministic generator, so a failing seed
/// reproduces exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Edge type, as stored.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ty {
    R,
    S,
}

impl Ty {
    fn name(self) -> &'static str {
        match self {
            Ty::R => "R",
            Ty::S => "S",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dir {
    Out,
    In,
    Both,
}

#[derive(Clone, Debug)]
struct Edge {
    ty: Ty,
    src: usize,
    dst: usize,
    w: i64,
}

/// `n` nodes `:N {id: i, x}`; edges include self loops and parallel edges.
#[derive(Clone, Debug)]
struct Graph {
    n: usize,
    x: Vec<i64>,
    edges: Vec<Edge>,
}

fn random_graph(seed: u64) -> Graph {
    let mut rng = Rng(seed);
    let n = 6 + rng.below(13) as usize;
    let x = (0..n).map(|_| rng.below(10) as i64).collect();
    let m = n + rng.below(2 * n as u64) as usize;
    let edges = (0..m)
        .map(|_| Edge {
            ty: if rng.below(3) == 0 { Ty::S } else { Ty::R },
            src: rng.below(n as u64) as usize,
            dst: rng.below(n as u64) as usize,
            w: rng.below(2) as i64,
        })
        .collect();
    Graph { n, x, edges }
}

/// The relationship part of a pattern and what it admits.
#[derive(Clone, Debug)]
struct Rel {
    text: &'static str,
    types: &'static [Ty],
    min0: bool,
    max: Option<usize>,
    w_eq_1: bool,
}

const RELS: &[Rel] = &[
    Rel {
        text: ":R*",
        types: &[Ty::R],
        min0: false,
        max: None,
        w_eq_1: false,
    },
    Rel {
        text: ":R|S*",
        types: &[Ty::R, Ty::S],
        min0: false,
        max: None,
        w_eq_1: false,
    },
    Rel {
        text: "*",
        types: &[Ty::R, Ty::S],
        min0: false,
        max: None,
        w_eq_1: false,
    },
    Rel {
        text: ":R*1..2",
        types: &[Ty::R],
        min0: false,
        max: Some(2),
        w_eq_1: false,
    },
    Rel {
        text: ":R*0..",
        types: &[Ty::R],
        min0: true,
        max: None,
        w_eq_1: false,
    },
    Rel {
        text: ":R* {w: 1}",
        types: &[Ty::R],
        min0: false,
        max: None,
        w_eq_1: true,
    },
];

/// Neighbours of `v` with multiplicity: one entry per admitted edge.
fn neighbours(g: &Graph, rel: &Rel, dir: Dir, v: usize) -> Vec<usize> {
    let mut out = Vec::new();
    for e in &g.edges {
        if !rel.types.contains(&e.ty) || (rel.w_eq_1 && e.w != 1) {
            continue;
        }
        if matches!(dir, Dir::Out | Dir::Both) && e.src == v {
            out.push(e.dst);
        }
        if matches!(dir, Dir::In | Dir::Both) && e.dst == v {
            out.push(e.src);
        }
    }
    out
}

/// Hop distance from `s` to every vertex within the hop bound.
fn distances(g: &Graph, rel: &Rel, dir: Dir, s: usize) -> Vec<Option<usize>> {
    let mut dist = vec![None; g.n];
    dist[s] = Some(0);
    let mut q = VecDeque::from([s]);
    while let Some(v) = q.pop_front() {
        let d = dist[v].unwrap();
        if rel.max.is_some_and(|m| d >= m) {
            continue;
        }
        for u in neighbours(g, rel, dir, v) {
            if dist[u].is_none() {
                dist[u] = Some(d + 1);
                q.push_back(u);
            }
        }
    }
    dist
}

/// Length of the shortest match from `s` to `t`, as the engine defines it: a
/// vertex reaches itself only through the zero-length path, and only under
/// `*0..`.
fn shortest_len(g: &Graph, rel: &Rel, dir: Dir, s: usize, t: usize) -> Option<usize> {
    if s == t {
        return rel.min0.then_some(0);
    }
    distances(g, rel, dir, s)[t]
}

/// Every shortest path from `s` to `t` as a node list, once per distinct
/// sequence of edges (so parallel edges multiply).
fn all_shortest(g: &Graph, rel: &Rel, dir: Dir, s: usize, t: usize) -> Vec<Vec<usize>> {
    let Some(len) = shortest_len(g, rel, dir, s, t) else {
        return Vec::new();
    };
    if len == 0 {
        return vec![vec![s]];
    }
    let dist = distances(g, rel, dir, s);
    // Walk backwards from `t` through predecessors one layer closer.
    fn walk(
        g: &Graph,
        rel: &Rel,
        dir: Dir,
        dist: &[Option<usize>],
        s: usize,
        path: &mut Vec<usize>,
        out: &mut Vec<Vec<usize>>,
    ) {
        let v = *path.last().unwrap();
        if v == s {
            let mut p = path.clone();
            p.reverse();
            out.push(p);
            return;
        }
        let dv = dist[v].unwrap();
        for u in 0..g.n {
            if dist[u] != Some(dv - 1) {
                continue;
            }
            let mult = neighbours(g, rel, dir, u)
                .into_iter()
                .filter(|&w| w == v)
                .count();
            for _ in 0..mult {
                path.push(u);
                walk(g, rel, dir, dist, s, path, out);
                path.pop();
            }
        }
    }
    let mut out = Vec::new();
    walk(g, rel, dir, &dist, s, &mut vec![t], &mut out);
    out
}

/// Whether `ids` is a walk of admitted edges from `s` to `t` of exactly `len`.
fn is_valid_path(
    g: &Graph,
    rel: &Rel,
    dir: Dir,
    s: usize,
    t: usize,
    len: usize,
    ids: &[usize],
) -> bool {
    ids.len() == len + 1
        && ids.first() == Some(&s)
        && ids.last() == Some(&t)
        && ids
            .windows(2)
            .all(|w| neighbours(g, rel, dir, w[0]).contains(&w[1]))
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

async fn schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property("x", DataType::Int64)
        .edge_type("R", &["N"], &["N"])
        .property("w", DataType::Int64)
        .edge_type("S", &["N"], &["N"])
        .property("w", DataType::Int64)
        .apply()
        .await?;
    Ok(())
}

async fn create_nodes(session: &Session, g: &Graph, ids: std::ops::Range<usize>) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let rows: Vec<String> = ids.map(|i| format!("[{i}, {}]", g.x[i])).collect();
    let tx = session.tx().await?;
    tx.execute(&format!(
        "UNWIND [{}] AS r CREATE (:N {{id: r[0], x: r[1]}})",
        rows.join(", ")
    ))
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn create_edges(session: &Session, edges: &[Edge]) -> Result<()> {
    let tx = session.tx().await?;
    for ty in [Ty::R, Ty::S] {
        let rows: Vec<String> = edges
            .iter()
            .filter(|e| e.ty == ty)
            .map(|e| format!("[{}, {}, {}]", e.src, e.dst, e.w))
            .collect();
        if rows.is_empty() {
            continue;
        }
        tx.execute(&format!(
            "UNWIND [{}] AS e MATCH (a:N {{id: e[0]}}), (b:N {{id: e[1]}}) \
             CREATE (a)-[:{} {{w: e[2]}}]->(b)",
            rows.join(", "),
            ty.name()
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Edges touching the last two nodes, plus every third other edge, stay in L0.
fn split_l0(g: &Graph) -> (Vec<Edge>, Vec<Edge>) {
    let late = g.n - 2;
    let (mut flushed, mut l0) = (Vec::new(), Vec::new());
    for (i, e) in g.edges.iter().enumerate() {
        if e.src >= late || e.dst >= late || i % 3 == 0 {
            l0.push(e.clone());
        } else {
            flushed.push(e.clone());
        }
    }
    (flushed, l0)
}

/// A store holding `g` with part of it flushed to L1 and the rest in L0.
async fn load_mixed(g: &Graph) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    let (flushed, l0) = split_l0(g);
    create_nodes(&session, g, 0..g.n - 2).await?;
    create_edges(&session, &flushed).await?;
    db.flush().await?;
    create_nodes(&session, g, g.n - 2..g.n).await?;
    create_edges(&session, &l0).await?;
    Ok(db)
}

// ---------------------------------------------------------------------------
// Querying
// ---------------------------------------------------------------------------

fn as_int(v: Option<&Value>) -> Option<usize> {
    match v {
        Some(Value::Int(i)) => Some(*i as usize),
        _ => None,
    }
}

fn as_ids(v: Option<&Value>) -> Option<Vec<usize>> {
    match v {
        Some(Value::List(items)) => Some(items.iter().map(|i| as_int(Some(i)).unwrap()).collect()),
        _ => None,
    }
}

/// One output row: source id, target id (None for an OPTIONAL MATCH miss) and
/// the path's node ids (None for a NULL path).
type Row = (usize, Option<usize>, Option<Vec<usize>>);

const RET: &str = "RETURN a.id AS s, b.id AS t, \
                   CASE WHEN p IS NULL THEN NULL ELSE [n IN nodes(p) | n.id] END AS ids";

async fn rows(session: &Session, q: &str) -> Result<Vec<Row>> {
    let res = session
        .query(q)
        .await
        .map_err(|e| anyhow::anyhow!("{q}: {e}"))?;
    let mut out: Vec<Row> = res
        .rows()
        .iter()
        .map(|r| {
            (
                as_int(r.value("s")).expect("source id"),
                as_int(r.value("t")),
                as_ids(r.value("ids")),
            )
        })
        .collect();
    out.sort();
    Ok(out)
}

fn arrow(dir: Dir, rel: &str) -> String {
    match dir {
        Dir::Out => format!("-[{rel}]->"),
        Dir::In => format!("<-[{rel}]-"),
        Dir::Both => format!("-[{rel}]-"),
    }
}

/// Target filters: Cypher text and the same predicate on `(id, x)`.
type TargetFilter = (&'static str, fn(usize, i64) -> bool);

const FILTERS: &[TargetFilter] = &[
    ("", |_, _| true),
    ("WHERE b.x < 5", |_, x| x < 5),
    ("WHERE b.x < 5 AND b.id <> 2", |id, x| x < 5 && id != 2),
    ("WHERE b.x >= 3 AND b.x <= 6", |_, x| (3..=6).contains(&x)),
];

/// Sources: the single node `id = 0`, or every node with `x < 4`.
#[derive(Clone, Copy, Debug)]
enum Sources {
    One,
    XBelow4,
}

impl Sources {
    fn ids(self, g: &Graph) -> Vec<usize> {
        match self {
            Sources::One => vec![0],
            Sources::XBelow4 => (0..g.n).filter(|&i| g.x[i] < 4).collect(),
        }
    }
}

/// The query for one shape. `fname` is `shortestPath` or `allShortestPaths`.
fn query(fname: &str, src: Sources, dir: Dir, rel: &Rel, filter: &str) -> String {
    let pat = arrow(dir, rel.text);
    match src {
        Sources::One => {
            format!("MATCH p = {fname}((a:N {{id: 0}}){pat}(b:N)) {filter} {RET}")
        }
        Sources::XBelow4 => {
            format!("MATCH (a:N) WHERE a.x < 4 MATCH p = {fname}((a){pat}(b:N)) {filter} {RET}")
        }
    }
}

/// Expected `shortestPath` rows, with the path reduced to its length since the
/// oracle cannot predict which of several equal-length paths is chosen.
///
/// A mandatory MATCH drops a pair with no path, as openCypher does. Intended
/// semantics change (user decision, 2026-10-17): the oracle used to expect such
/// a pair as a row with a NULL path.
fn expected_shortest(
    g: &Graph,
    rel: &Rel,
    dir: Dir,
    sources: &[usize],
    keep: fn(usize, i64) -> bool,
) -> Vec<(usize, usize, Option<usize>)> {
    let mut out = Vec::new();
    for &s in sources {
        for t in (0..g.n).filter(|&t| keep(t, g.x[t])) {
            if let Some(len) = shortest_len(g, rel, dir, s, t) {
                out.push((s, t, Some(len)));
            }
        }
    }
    out.sort();
    out
}

/// Expected `allShortestPaths` rows: one per shortest path, none for a pair
/// with no path (intended semantics change, user decision 2026-10-17; the
/// oracle used to expect one NULL-path row for such a pair).
fn expected_all(
    g: &Graph,
    rel: &Rel,
    dir: Dir,
    sources: &[usize],
    keep: fn(usize, i64) -> bool,
) -> Vec<Row> {
    let mut out = Vec::new();
    for &s in sources {
        for t in (0..g.n).filter(|&t| keep(t, g.x[t])) {
            let paths = all_shortest(g, rel, dir, s, t);
            out.extend(paths.into_iter().map(|p| (s, Some(t), Some(p))));
        }
    }
    out.sort();
    out
}

/// Check every `shortestPath` row against the oracle: the right pairs, the
/// right lengths, and a path that is one of the pair's shortest paths.
///
/// Which shortest path is returned when there are several is not compared with
/// anything: it is not deterministic, before or after A8.
/// `AdjacencyManager::get_neighbors` gathers a vertex's neighbours into a
/// fresh `std::collections::HashMap` per call and returns them in its
/// iteration order, so the breadth-first expansion order — which is what picks
/// the parent of a vertex reachable two ways — changes from call to call. The
/// same bound-pair query repeated 30 times returned two different paths.
async fn check_shortest(
    session: &Session,
    g: &Graph,
    ctx: &str,
    src: Sources,
    dir: Dir,
    rel: &Rel,
    filter: &TargetFilter,
) -> Result<()> {
    let q = query("shortestPath", src, dir, rel, filter.0);
    let got = rows(session, &q).await?;
    let want = expected_shortest(g, rel, dir, &src.ids(g), filter.1);
    let got_lens: Vec<(usize, usize, Option<usize>)> = got
        .iter()
        .map(|(s, t, ids)| (*s, t.expect("target"), ids.as_ref().map(|p| p.len() - 1)))
        .collect();
    assert_eq!(got_lens, want, "{ctx}\nquery: {q}\ngraph: {g:?}");
    for (s, t, ids) in &got {
        let t = t.unwrap();
        if let Some(ids) = ids {
            assert!(
                is_valid_path(g, rel, dir, *s, t, ids.len() - 1, ids)
                    && all_shortest(g, rel, dir, *s, t).contains(ids),
                "{ctx}\nquery: {q}\nrow ({s},{t}) path {ids:?} is not a shortest \
                 walk of admitted edges\ngraph: {g:?}"
            );
        }
    }
    Ok(())
}

async fn check_all(
    session: &Session,
    g: &Graph,
    ctx: &str,
    src: Sources,
    dir: Dir,
    rel: &Rel,
    filter: &TargetFilter,
) -> Result<()> {
    let q = query("allShortestPaths", src, dir, rel, filter.0);
    let got = rows(session, &q).await?;
    let want = expected_all(g, rel, dir, &src.ids(g), filter.1);
    assert_eq!(got, want, "{ctx}\nquery: {q}\ngraph: {g:?}");
    Ok(())
}

const DIRS: [Dir; 3] = [Dir::Out, Dir::In, Dir::Both];

/// Every shape against the oracle, over random graphs split across L1 and L0.
///
/// Shapes rotate the source set and target filter across seeds so each seed
/// stays cheap while the whole run covers every combination.
#[tokio::test(flavor = "multi_thread")]
async fn many_target_shortest_paths_match_a_bfs_oracle() -> Result<()> {
    for seed in 0..12u64 {
        let g = random_graph(0xA8_0000 + seed);
        let db = load_mixed(&g).await?;
        let session = db.session();
        let mut k = seed as usize;
        for rel in RELS {
            for dir in DIRS {
                k += 1;
                let src = if k.is_multiple_of(2) {
                    Sources::One
                } else {
                    Sources::XBelow4
                };
                let filter = &FILTERS[k % FILTERS.len()];
                let ctx = format!("seed {seed}, {src:?}, {dir:?}, rel {}", rel.text);
                check_shortest(&session, &g, &ctx, src, dir, rel, filter).await?;
                check_all(&session, &g, &ctx, src, dir, rel, filter).await?;
            }
        }
        db.shutdown().await?;
    }
    Ok(())
}

/// `OPTIONAL MATCH` against the oracle, with the target unbound (filtered or
/// not) and bound.
///
/// openCypher null-extends an entering row only when the optional pattern as a
/// whole matches nothing. With an unbound target the pattern's matches are the
/// targets the source has a path to, so a source reaching none of the
/// qualifying targets yields exactly one row, with NULL target and path, and a
/// source reaching some yields only those. Intended semantics change (user
/// decision, 2026-10-17): the oracle used to expect a NULL-path row for every
/// unreachable target, beside the reachable ones. With a bound target the
/// pattern is one pair, so a pair with no path keeps its row with a NULL path,
/// as before.
///
/// The `a.x < 3` arm is what keeps the shortest-path pushdown out of OPTIONAL
/// MATCH. `a` is bound by the mandatory clause before it, so pushing the
/// conjunct into `a`'s scan would drop the sources with `x = 3` instead of
/// null-extending them. The arm with no `WHERE` is the one with no
/// `OptionalFilterExec` of its own: the clause has to be closed regardless.
#[tokio::test(flavor = "multi_thread")]
async fn optional_many_target_shortest_path_matches_the_oracle() -> Result<()> {
    for seed in 0..4u64 {
        let g = random_graph(0xA8_1000 + seed);
        let db = load_mixed(&g).await?;
        let session = db.session();
        let rel = &RELS[0];
        for fname in ["shortestPath", "allShortestPaths"] {
            // (target cut on `b.x`, source predicate `a.x < 3`, target bound).
            // A `None` cut writes no WHERE at all.
            for (cut, on_source, bound) in [
                (Some(5), false, false),
                (Some(-1), false, false),
                (Some(5), true, false),
                (None, false, false),
                (None, false, true),
                (Some(5), false, true),
            ] {
                let mut preds = Vec::new();
                if on_source {
                    preds.push("a.x < 3".to_string());
                }
                if let Some(cut) = cut {
                    preds.push(format!("b.x < {cut}"));
                }
                let where_clause = if preds.is_empty() {
                    String::new()
                } else {
                    format!("WHERE {}", preds.join(" AND "))
                };
                let q = if bound {
                    format!(
                        "MATCH (a:N), (b:N) WHERE a.x < 4 \
                         OPTIONAL MATCH p = {fname}((a)-[:R*]->(b)) {where_clause} {RET}"
                    )
                } else {
                    format!(
                        "MATCH (a:N) WHERE a.x < 4 \
                         OPTIONAL MATCH p = {fname}((a)-[:R*]->(b:N)) {where_clause} {RET}"
                    )
                };
                let got = rows(&session, &q).await?;
                // The matches of the optional pattern for one entering row, as
                // rows; the path of a `shortestPath` match is compared by
                // length only.
                let matches = |s: usize, t: usize| -> Vec<Row> {
                    let qualifies = cut.is_none_or(|c| g.x[t] < c) && (!on_source || g.x[s] < 3);
                    if !qualifies {
                        return Vec::new();
                    }
                    if fname == "shortestPath" {
                        shortest_len(&g, rel, Dir::Out, s, t)
                            .map(|l| (s, Some(t), Some(vec![usize::MAX; l + 1])))
                            .into_iter()
                            .collect()
                    } else {
                        all_shortest(&g, rel, Dir::Out, s, t)
                            .into_iter()
                            .map(|p| (s, Some(t), Some(p)))
                            .collect()
                    }
                };
                let mut want: Vec<Row> = Vec::new();
                for s in Sources::XBelow4.ids(&g) {
                    if bound {
                        // One entering row per pair; `b` stays bound.
                        for t in 0..g.n {
                            let found = matches(s, t);
                            if found.is_empty() {
                                want.push((s, Some(t), None));
                            }
                            want.extend(found);
                        }
                    } else {
                        let found: Vec<Row> = (0..g.n).flat_map(|t| matches(s, t)).collect();
                        if found.is_empty() {
                            want.push((s, None, None));
                        }
                        want.extend(found);
                    }
                }
                want.sort();
                let mut got_cmp: Vec<Row> = if fname == "shortestPath" {
                    got.iter()
                        .map(|(s, t, ids)| {
                            (*s, *t, ids.as_ref().map(|p| vec![usize::MAX; p.len()]))
                        })
                        .collect()
                } else {
                    got.clone()
                };
                got_cmp.sort();
                assert_eq!(got_cmp, want, "seed {seed}\nquery: {q}\ngraph: {g:?}");
            }
        }
        db.shutdown().await?;
    }
    Ok(())
}

/// A fork sees its own edges in the many-target search, and primary does not.
#[tokio::test(flavor = "multi_thread")]
async fn many_target_shortest_paths_on_a_fork_match_the_oracle() -> Result<()> {
    let g = random_graph(0xA8_2000);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    let (base, extra) = split_l0(&g);
    create_nodes(&session, &g, 0..g.n).await?;
    create_edges(&session, &base).await?;
    db.flush().await?;
    let fork = session.fork("a8_fork").await?;
    create_edges(&fork, &extra).await?;

    let base_graph = Graph {
        edges: base.clone(),
        ..g.clone()
    };
    for (who, sess, graph) in [("fork", &fork, &g), ("primary", &session, &base_graph)] {
        for (i, rel) in RELS.iter().enumerate() {
            let filter = &FILTERS[i % FILTERS.len()];
            let ctx = format!("{who}, rel {}", rel.text);
            check_shortest(sess, graph, &ctx, Sources::One, Dir::Out, rel, filter).await?;
            check_all(sess, graph, &ctx, Sources::XBelow4, Dir::Both, rel, filter).await?;
        }
    }
    db.shutdown().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Cost guards
// ---------------------------------------------------------------------------

fn guard_cfg() -> UniConfig {
    UniConfig {
        query_timeout: Duration::from_secs(600),
        ..Default::default()
    }
}

/// The perf-class probe's graph — `n` nodes `:N {id: i, x: i}` and `2n` `:R`
/// edges `i -> i+1`, `i -> 7i+3` (mod `n`), all reachable from node 0 — plus
/// `n` *island* nodes `:N {id: n+j, x: n+j}` chained `n+j -> n+j+1` that
/// nothing on the main graph reaches. Bulk-loaded and flushed.
async fn ladder(n: usize, dir: &std::path::Path) -> Result<Uni> {
    let path = dir.to_str().unwrap().to_string();
    let db = Uni::open(&path).config(guard_cfg()).build().await?;
    let tx = db.session().tx().await?;
    for d in [
        "CREATE LABEL N (id INT, x INT)",
        "CREATE EDGE TYPE R (w INT) FROM N TO N",
        "CREATE INDEX idx_n_id FOR (n:N) ON (n.id)",
        "CREATE INDEX idx_n_x FOR (n:N) ON (n.x)",
    ] {
        tx.execute(d).await?;
    }
    tx.commit().await?;
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let props: Vec<HashMap<String, Value>> = (0..2 * n)
        .map(|i| {
            HashMap::from([
                ("id".to_string(), Value::Int(i as i64)),
                ("x".to_string(), Value::Int(i as i64)),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("N", props).await?;
    let mut edges = Vec::with_capacity(3 * n);
    let pairs = (0..n)
        .flat_map(|i| [(i, (i + 1) % n), (i, (i * 7 + 3) % n)])
        .chain((n..2 * n - 1).map(|i| (i, i + 1)));
    for (i, j) in pairs {
        edges.push(uni_bulk::EdgeData::new(
            vids[i],
            vids[j],
            HashMap::from([("w".to_string(), Value::Int(1))]),
        ));
    }
    bulk.insert_edges("R", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Profile `q`, returning the result rows' first column as an int and the
/// path operator's `(output rows, compute ms)`.
async fn profile_path_op(db: &Uni, q: &str, op: &str) -> Result<(i64, usize, f64)> {
    let (res, prof) = db.session().query_with(q).profile().await?;
    let first = match res.rows().first().and_then(|r| r.values().first()) {
        Some(Value::Int(i)) => *i,
        other => anyhow::bail!("{q}: unexpected first value {other:?}"),
    };
    let stats = prof
        .runtime_stats
        .iter()
        .find(|o| o.operator == op)
        .ok_or_else(|| anyhow::anyhow!("{q}: no {op} in {:?}", prof.runtime_stats))?;
    Ok((first, stats.actual_rows, stats.time_ms))
}

/// The target filter reaches the target scan, so the path operator sees only
/// the qualifying targets — at both scales — instead of every `N` node.
///
/// `actual_rows` of `GraphShortestPathExec` is its output: one row per input
/// pair that has a path, so this is a counter, not a timing. Of the 20
/// qualifying targets, 19 have a path: the 20th is the source itself, which
/// reaches itself only through the zero-length path `*1..` excludes, and a
/// mandatory MATCH drops it. Intended semantics change (user decision,
/// 2026-10-17): that pair used to be emitted with a NULL path, and the counts
/// below were 20.
#[tokio::test(flavor = "multi_thread")]
async fn filtered_target_reaches_the_target_scan() -> Result<()> {
    for n in [1000usize, 4000] {
        let dir = tempfile::tempdir()?;
        let db = ladder(n, dir.path()).await?;
        for (fname, op) in [
            ("shortestPath", "GraphShortestPathExec"),
            ("allShortestPaths", "GraphShortestPathExec"),
        ] {
            let q = format!(
                "MATCH p = {fname}((a:N {{id: 0}})-[:R*]->(b:N)) WHERE b.x < 20 RETURN count(DISTINCT b)"
            );
            let (count, op_rows, _) = profile_path_op(&db, &q, op).await?;
            assert_eq!(count, 19, "N={n}: {q}");
            // allShortestPaths emits one row per path, so it may exceed the
            // 19 reachable pairs; it must not approach N.
            if fname == "shortestPath" {
                assert_eq!(
                    op_rows, 19,
                    "N={n}: the path operator emitted {op_rows} rows for 20 \
                     qualifying targets, 19 reachable; `b.x < 20` was applied \
                     after the search"
                );
            } else {
                assert!(
                    op_rows < 200,
                    "N={n}: allShortestPaths emitted {op_rows} rows for 20 \
                     qualifying targets; `b.x < 20` was applied after the search"
                );
            }
        }
        db.shutdown().await?;
    }
    Ok(())
}

/// Many targets of one source share one search, not one search each.
///
/// Counted, not timed: `QueryMetrics::shortest_path_searches` is the per-query
/// count of searches the operator began, so the guard is deterministic and
/// load-independent. At `n = 250`
/// and `n = 2000`, one source and `n` island targets must cost exactly one
/// search per execution, and five sources exactly five; answering each row with
/// its own search (the code before A8, and the revert check) starts one per
/// row, `n` and `5n`.
///
/// The island targets are all unreachable from the sources, so every pair is
/// dropped and the query counts nothing. That is the worst case for a per-row
/// search — each one exhausts the source's `n`-node component before giving up
/// — and keeps path materialisation out of the printed timings, which are
/// reported for the record only. (A wall-time ratio was the first form of this guard; it failed
/// once under heavy machine load at 6.9x against a 2x threshold.)
#[tokio::test(flavor = "multi_thread")]
async fn many_targets_share_one_search() -> Result<()> {
    for n in [250usize, 2000] {
        let dir = tempfile::tempdir()?;
        let db = ladder(n, dir.path()).await?;
        for fname in ["shortestPath", "allShortestPaths"] {
            for (sources, source_pred) in [(1usize, "a.id = 0"), (5, "a.id < 5")] {
                let q = format!(
                    "MATCH (a:N) WHERE {source_pred} \
                     MATCH p = {fname}((a)-[:R*]->(b:N)) WHERE b.x >= {n} \
                     RETURN count(b) AS targets, count(p) AS paths"
                );
                let (res, prof) = db.session().query_with(&q).profile().await?;
                let searches = res.metrics().shortest_path_searches;
                let row = &res.rows()[0];
                // No island is reachable, so a mandatory MATCH keeps no pair.
                // Intended semantics change (user decision, 2026-10-17): this
                // counted `sources * n` NULL-path rows under the old contract.
                assert_eq!(row.value("targets"), Some(&Value::Int(0)), "{q}");
                assert_eq!(row.value("paths"), Some(&Value::Int(0)), "{q}");
                let ms = prof
                    .runtime_stats
                    .iter()
                    .find(|o| o.operator == "GraphShortestPathExec")
                    .map_or(f64::NAN, |o| o.time_ms);
                eprintln!(
                    "A8 guard n={n} {fname} sources={sources}: {searches} searches, \
                     path operator {ms:.1} ms"
                );
                assert_eq!(
                    searches,
                    sources as u64,
                    "n={n}: {q}\nstarted {searches} searches for {sources} source(s) \
                     and {} targets; each target is running its own search",
                    sources * n
                );
            }
        }
        db.shutdown().await?;
    }
    Ok(())
}
