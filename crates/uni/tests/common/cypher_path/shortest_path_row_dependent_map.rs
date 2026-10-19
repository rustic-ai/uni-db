// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `shortestPath` / `allShortestPaths` with a relationship map whose value
//! depends on the row: `shortestPath((a)-[:R*1..4 {w: a.x}]->(b))`.
//!
//! The map gates the search, so the answer is the shortest path among those
//! whose every relationship satisfies `r.w = a.x` for that row's `a`. The
//! reference is an ordinary variable-length match over the same pairs with
//! the predicate written as `all(r IN rs WHERE ...)`, reduced in Rust to the
//! minimum length per pair (and, for `allShortestPaths`, to the number of
//! trails of that length — a shortest path is always a trail, and parallel
//! edges make distinct ones). The reference walks without the map, so it
//! reaches the answer by a different mechanism.
//!
//! The graphs are random and small, with parallel edges, self-loops, edges
//! without `w`, and nodes without `x` (a `NULL` map value, which admits no
//! edge). Each runs with and without a declared schema, unflushed and
//! flushed.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(shortest_path_row_dependent_map)'

// Rust guideline compliant

use std::collections::BTreeMap;

use anyhow::{Result, anyhow};
use uni_db::{DataType, Session, Uni, Value};

/// SplitMix64, so a failing seed reproduces exactly.
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

#[derive(Clone, Debug)]
struct Edge {
    src: usize,
    dst: usize,
    /// `None`: the edge has no `w`.
    w: Option<i64>,
    k: i64,
}

#[derive(Clone, Debug)]
struct Graph {
    /// `x` per node; `None`: the node has no `x`.
    x: Vec<Option<i64>>,
    edges: Vec<Edge>,
}

fn random_graph(seed: u64) -> Graph {
    let mut rng = Rng(seed);
    let n = 5 + rng.below(4) as usize;
    let x = (0..n)
        .map(|_| match rng.below(5) {
            0 => None,
            v => Some((v % 2) as i64),
        })
        .collect();
    let m = n + rng.below(n as u64) as usize;
    let mut edges: Vec<Edge> = (0..m)
        .map(|_| Edge {
            src: rng.below(n as u64) as usize,
            dst: rng.below(n as u64) as usize,
            w: match rng.below(5) {
                0 => None,
                v => Some((v % 2) as i64),
            },
            k: rng.below(3) as i64,
        })
        .collect();
    // A parallel pair and a self-loop, always.
    let first = edges[0].clone();
    edges.push(Edge {
        w: first.w,
        k: (first.k + 1) % 3,
        ..first
    });
    edges.push(Edge {
        src: 0,
        dst: 0,
        w: Some(1),
        k: 0,
    });
    Graph { x, edges }
}

async fn declare_schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property_nullable("x", DataType::Int64)
        .edge_type("R", &["N"], &["N"])
        .property_nullable("w", DataType::Int64)
        .property("k", DataType::Int64)
        .apply()
        .await?;
    Ok(())
}

async fn load(session: &Session, g: &Graph) -> Result<()> {
    let nodes: Vec<String> =
        g.x.iter()
            .enumerate()
            .map(|(i, x)| format!("[{i}, {}]", x.map_or("null".into(), |x| x.to_string())))
            .collect();
    let edges: Vec<String> = g
        .edges
        .iter()
        .map(|e| {
            let w = e.w.map_or("null".to_string(), |w| w.to_string());
            format!("[{}, {}, {w}, {}]", e.src, e.dst, e.k)
        })
        .collect();
    let tx = session.tx().await?;
    tx.execute(&format!(
        "UNWIND [{}] AS r CREATE (:N {{id: r[0], x: r[1]}})",
        nodes.join(", ")
    ))
    .await?;
    tx.execute(&format!(
        "UNWIND [{}] AS e MATCH (a:N {{id: e[0]}}), (b:N {{id: e[1]}}) \
         CREATE (a)-[:R {{w: e[2], k: e[3]}}]->(b)",
        edges.join(", ")
    ))
    .await?;
    tx.commit().await?;
    Ok(())
}

fn int(v: Option<&Value>) -> Result<i64> {
    match v {
        Some(Value::Int(i)) => Ok(*i),
        other => Err(anyhow!("expected an integer, got {other:?}")),
    }
}

/// `(a.id, b.id) -> lengths`, one entry per row of `q` (columns `a`, `b`, `len`).
async fn lengths(session: &Session, q: &str) -> Result<BTreeMap<(i64, i64), Vec<i64>>> {
    let res = session
        .query_with(q)
        .param("one", 1)
        .fetch_all()
        .await
        .map_err(|e| anyhow!("{q}: {e}"))?;
    let mut out: BTreeMap<(i64, i64), Vec<i64>> = BTreeMap::new();
    for row in res.rows() {
        let key = (int(row.value("a"))?, int(row.value("b"))?);
        out.entry(key).or_default().push(int(row.value("len"))?);
    }
    for v in out.values_mut() {
        v.sort_unstable();
    }
    Ok(out)
}

/// A case: the relationship map, its per-relationship test, the arrow, the
/// hop range, and how the endpoints are bound before the search.
struct Case {
    map: &'static str,
    test: &'static str,
    open: &'static str,
    close: &'static str,
    hops: &'static str,
}

const MAPS: &[(&str, &str)] = &[
    ("{w: a.x}", "r.w = a.x"),
    ("{w: b.x}", "r.w = b.x"),
    ("{w: a.x - 1 + $one}", "r.w = a.x - 1 + $one"),
    ("{w: toFloat(a.x)}", "r.w = toFloat(a.x)"),
    ("{k: a.x, w: 1}", "r.k = a.x AND r.w = 1"),
    ("{w: 1, k: b.x}", "r.w = 1 AND r.k = b.x"),
    // A control with a walk-constant map: checks the reference itself.
    ("{w: 1}", "r.w = 1"),
];

const ARROWS: &[(&str, &str)] = &[("-[", "]->"), ("<-[", "]-"), ("-[", "]-")];

const HOPS: &[&str] = &["*1..4", "*..3", "*"];

fn cases() -> Vec<Case> {
    let mut out = Vec::new();
    for &(map, test) in MAPS {
        for &(open, close) in ARROWS {
            for &hops in HOPS {
                out.push(Case {
                    map,
                    test,
                    open,
                    close,
                    hops,
                });
            }
        }
    }
    out
}

/// Every case against its reference on whatever `session` sees.
async fn check(session: &Session, n: usize, ctx: &str) -> Result<()> {
    let mut failures = Vec::new();
    // The bound forms: `a` and `b` from an earlier clause, and the target
    // scanned by the shortest-path clause itself.
    let pairs = "MATCH (a:N), (b:N) WHERE a.id <> b.id";
    for c in cases() {
        // A shortest path is simple, so no longer than `n - 1`: the
        // reference's unbounded `*` stops there instead of enumerating every
        // trail.
        let ref_hops = match c.hops {
            "*" => format!("*1..{}", n - 1),
            "*..3" => "*1..3".to_string(),
            h => h.to_string(),
        };
        let reference = format!(
            "{pairs} MATCH q = (a){open}rs:R{ref_hops}{close}(b) \
             WHERE all(r IN rs WHERE {test}) RETURN a.id AS a, b.id AS b, length(q) AS len",
            open = c.open,
            close = c.close,
            test = c.test,
        );
        let trails = lengths(session, &reference).await?;
        let want_short: BTreeMap<(i64, i64), Vec<i64>> =
            trails.iter().map(|(k, v)| (*k, vec![v[0]])).collect();
        let want_all: BTreeMap<(i64, i64), Vec<i64>> = trails
            .iter()
            .map(|(k, v)| (*k, v.iter().copied().filter(|l| *l == v[0]).collect()))
            .collect();
        for (func, want) in [
            ("shortestPath", &want_short),
            ("allShortestPaths", &want_all),
        ] {
            let qs = [
                format!(
                    "{pairs} MATCH p = {func}((a){open}:R{hops} {map}{close}(b)) \
                     RETURN a.id AS a, b.id AS b, length(p) AS len",
                    open = c.open,
                    close = c.close,
                    hops = c.hops,
                    map = c.map,
                ),
                // Target scanned inside the clause.
                format!(
                    "MATCH (a:N) MATCH p = {func}((a){open}:R{hops} {map}{close}(b:N)) \
                     WHERE a.id <> b.id RETURN a.id AS a, b.id AS b, length(p) AS len",
                    open = c.open,
                    close = c.close,
                    hops = c.hops,
                    map = c.map,
                ),
            ];
            for q in qs {
                match lengths(session, &q).await {
                    Ok(got) if &got == want => {}
                    Ok(got) => failures.push(format!(
                        "query:     {q}\nreference: {reference}\n  got:  {got:?}\n  want: {want:?}"
                    )),
                    Err(e) => failures.push(format!("failed: {e}")),
                }
            }
        }
        // Every relationship the search returned satisfies the map.
        let direct = format!(
            "{pairs} MATCH p = shortestPath((a){open}rs:R{hops} {map}{close}(b)) \
             RETURN count(*) AS n, sum(size([r IN rs WHERE NOT coalesce({test}, false)])) AS bad",
            open = c.open,
            close = c.close,
            hops = c.hops,
            map = c.map,
            test = c.test,
        );
        let res = session
            .query_with(&direct)
            .param("one", 1)
            .fetch_all()
            .await
            .map_err(|e| anyhow!("{direct}: {e}"));
        match res {
            Ok(res) => {
                let row = &res.rows()[0];
                let bad = match row.value("bad") {
                    Some(Value::Int(i)) => *i,
                    _ => 0,
                };
                if bad != 0 {
                    failures.push(format!("{direct}: {bad} relationships fail the map"));
                }
            }
            Err(e) => failures.push(format!("failed: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{ctx}\n{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

async fn run(seed: u64, with_schema: bool, flush: bool) -> Result<()> {
    let g = random_graph(seed);
    let db = Uni::in_memory().build().await?;
    if with_schema {
        declare_schema(&db).await?;
    }
    let session = db.session();
    load(&session, &g).await?;
    if flush {
        db.flush().await?;
    }
    check(
        &session,
        g.x.len(),
        &format!("seed {seed:#x}, schema {with_schema}, flushed {flush}\n{g:?}"),
    )
    .await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn row_dependent_map_with_schema_unflushed() -> Result<()> {
    for seed in 0..2 {
        run(0x5A_0000 + seed, true, false).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn row_dependent_map_with_schema_flushed() -> Result<()> {
    // One seed: a flushed graph costs several times an unflushed one here.
    for seed in 2..3 {
        run(0x5A_0000 + seed, true, true).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn row_dependent_map_schemaless_unflushed() -> Result<()> {
    for seed in 4..6 {
        run(0x5A_0000 + seed, false, false).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn row_dependent_map_schemaless_flushed() -> Result<()> {
    // One seed: a flushed graph costs several times an unflushed one here.
    for seed in 6..7 {
        run(0x5A_0000 + seed, false, true).await?;
    }
    Ok(())
}

/// One source, many targets, many distinct map values in one batch: rows are
/// grouped by value and each group searches with its own allow-set. Sources
/// with the same `x` share a filter; a `NULL` `x` admits no edge.
#[tokio::test(flavor = "multi_thread")]
async fn row_dependent_map_groups_rows_by_value() -> Result<()> {
    // A ring 0 -> 1 -> ... -> 9 -> 0 whose edges alternate w = 0 / 1, plus a
    // chord 0 -> 5 with w = 1.
    let db = Uni::in_memory().build().await?;
    declare_schema(&db).await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(
        "UNWIND range(0, 9) AS i CREATE (:N {id: i, x: CASE WHEN i = 9 THEN null ELSE i % 2 END})",
    )
    .await?;
    tx.execute(
        "UNWIND range(0, 9) AS i MATCH (a:N {id: i}), (b:N {id: (i + 1) % 10}) \
         CREATE (a)-[:R {w: i % 2, k: 0}]->(b)",
    )
    .await?;
    tx.execute("MATCH (a:N {id: 0}), (b:N {id: 5}) CREATE (a)-[:R {w: 0, k: 1}]->(b)")
        .await?;
    tx.commit().await?;
    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        // From every source with `x = 0` only `w = 0` edges, with `x = 1`
        // only `w = 1` edges; from 9 (x NULL) nothing.
        let got = lengths(
            &session,
            "MATCH (a:N), (b:N) WHERE a.id <> b.id \
             MATCH p = shortestPath((a)-[:R* {w: a.x}]->(b)) \
             RETURN a.id AS a, b.id AS b, length(p) AS len",
        )
        .await?;
        let mut want = BTreeMap::new();
        // Even sources walk their own edge (w = 0) and nothing past it,
        // since the next edge out has w = 1; 0 also takes the chord.
        for s in (0..9).step_by(2) {
            want.insert((s, (s + 1) % 10), vec![1]);
        }
        want.insert((0, 5), vec![1]);
        // 5 is reached by 0 over the chord, and 5 -> 6 has w = 1: blocked.
        // Odd sources take w = 1 edges: i -> i + 1 for odd i.
        for s in (1..9).step_by(2) {
            want.insert((s, s + 1), vec![1]);
        }
        assert_eq!(got, want, "flushed {flush}");
        // OPTIONAL: every source survives; only 2 reaches 3 under its map.
        let got = lengths(
            &session,
            "MATCH (a:N) OPTIONAL MATCH p = shortestPath((a)-[:R* {w: a.x}]->(b:N {id: 3})) \
             RETURN a.id AS a, 3 AS b, coalesce(length(p), -1) AS len",
        )
        .await?;
        let want: BTreeMap<(i64, i64), Vec<i64>> = (0..10)
            .map(|a| ((a, 3), vec![if a == 2 { 1 } else { -1 }]))
            .collect();
        assert_eq!(got, want, "flushed {flush}: OPTIONAL");
    }
    // A value naming what the search itself binds is refused.
    for q in [
        "MATCH (a:N {id: 0}), (b:N) MATCH p = shortestPath((a)-[rs:R* {w: size(rs)}]->(b)) \
         RETURN b.id",
        "MATCH (a:N {id: 0}), (b:N) MATCH p = shortestPath((a)-[:R* {w: length(p)}]->(b)) \
         RETURN b.id",
    ] {
        let err = match session.query(q).await {
            Ok(res) => panic!("{q}: expected a refusal, got {} rows", res.rows().len()),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("not bound before the search"), "{q}: {err}");
    }
    db.shutdown().await?;
    Ok(())
}
