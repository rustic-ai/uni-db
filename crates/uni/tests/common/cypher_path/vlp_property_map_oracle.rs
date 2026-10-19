// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Variable-length patterns with an inline property map, against a reference.
//!
//! `(a)-[rs:R*1..3 {w: 1}]->(b)` and `(a)-[rs:R*1..3]->(b:N {x: 2})` gate the
//! walk on the map. The reference for each is the same pattern with the map
//! removed and the predicate written in `WHERE` instead —
//! `all(r IN rs WHERE r.w = 1)` and `b.x = 2` — which reaches the answer
//! through a different mechanism: the walk is unfiltered, and the predicate
//! is an ordinary expression with Cypher's comparison semantics. Every shape
//! is compared as a bag of `(source, target, relationship ids)` rows.
//!
//! The graphs are random and small, with parallel edges, self-loops, edges
//! with no `w` at all, and are queried split across L1 and L0, from inside a
//! transaction whose edges are not committed, through a fork, and after a
//! `SET` that touched a flushed edge in L0. The maps include a float literal
//! against an integer property, a string literal, `null` and a parameter.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, QueryResult, Session, Transaction, Uni, Value};

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

/// One stored edge. `w` is `None` for an edge with no `w` property.
#[derive(Clone, Debug)]
struct Edge {
    ty: &'static str,
    src: usize,
    dst: usize,
    w: Option<i64>,
    /// Stored as `k`: the edge's index, so rows name exact relationships.
    k: usize,
}

#[derive(Clone, Debug)]
struct Graph {
    n: usize,
    x: Vec<i64>,
    edges: Vec<Edge>,
}

fn random_graph(seed: u64) -> Graph {
    let mut rng = Rng(seed);
    let n = 6 + rng.below(8) as usize;
    let x = (0..n).map(|_| rng.below(4) as i64).collect();
    let m = n + rng.below(2 * n as u64) as usize;
    let mut edges: Vec<Edge> = (0..m)
        .map(|k| Edge {
            ty: if rng.below(4) == 0 { "S" } else { "R" },
            src: rng.below(n as u64) as usize,
            dst: rng.below(n as u64) as usize,
            w: match rng.below(5) {
                0 => None,
                v => Some((v % 2) as i64),
            },
            k,
        })
        .collect();
    // Always at least one parallel pair and one self-loop.
    let first = edges[0].clone();
    edges.push(Edge {
        k: edges.len(),
        w: Some(1 - first.w.unwrap_or(0)),
        ..first
    });
    edges.push(Edge {
        ty: "R",
        src: 0,
        dst: 0,
        w: Some(1),
        k: edges.len(),
    });
    Graph { n, x, edges }
}

async fn schema(db: &Uni) -> Result<()> {
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property("x", DataType::Int64)
        .edge_type("R", &["N"], &["N"])
        .property_nullable("w", DataType::Int64)
        .property("k", DataType::Int64)
        .edge_type("S", &["N"], &["N"])
        .property_nullable("w", DataType::Int64)
        .property("k", DataType::Int64)
        .apply()
        .await?;
    Ok(())
}

fn node_rows(g: &Graph) -> String {
    let rows: Vec<String> = (0..g.n).map(|i| format!("[{i}, {}]", g.x[i])).collect();
    format!(
        "UNWIND [{}] AS r CREATE (:N {{id: r[0], x: r[1]}})",
        rows.join(", ")
    )
}

/// One `CREATE` statement per edge type for `edges`.
fn edge_statements(edges: &[Edge]) -> Vec<String> {
    ["R", "S"]
        .iter()
        .filter_map(|ty| {
            let rows: Vec<String> = edges
                .iter()
                .filter(|e| e.ty == *ty)
                .map(|e| {
                    let w = e.w.map_or("null".to_string(), |w| w.to_string());
                    format!("[{}, {}, {w}, {}]", e.src, e.dst, e.k)
                })
                .collect();
            (!rows.is_empty()).then(|| {
                format!(
                    "UNWIND [{}] AS e MATCH (a:N {{id: e[0]}}), (b:N {{id: e[1]}}) \
                     CREATE (a)-[:{ty} {{w: e[2], k: e[3]}}]->(b)",
                    rows.join(", ")
                )
            })
        })
        .collect()
}

async fn commit_all(session: &Session, statements: &[String]) -> Result<()> {
    let tx = session.tx().await?;
    for s in statements {
        tx.execute(s).await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Where the queries run.
enum Reader<'a> {
    Session(&'a Session),
    Tx(&'a Transaction),
}

impl Reader<'_> {
    async fn run(&self, q: &str) -> Result<QueryResult> {
        let res = match self {
            Reader::Session(s) => s.query_with(q).param("p", 1).fetch_all().await,
            Reader::Tx(t) => t.query_with(q).param("p", 1).fetch_all().await,
        };
        res.map_err(|e| anyhow::anyhow!("{q}: {e}"))
    }
}

fn cell(v: Option<&Value>) -> String {
    match v {
        Some(Value::Int(i)) => i.to_string(),
        Some(Value::List(items)) => {
            let parts: Vec<String> = items.iter().map(|i| cell(Some(i))).collect();
            format!("[{}]", parts.join(","))
        }
        Some(Value::Null) | None => "null".to_string(),
        Some(other) => format!("{other:?}"),
    }
}

/// The result as a sorted bag of rendered rows.
async fn bag(reader: &Reader<'_>, q: &str) -> Result<Vec<String>> {
    let res = reader.run(q).await?;
    let mut rows: Vec<String> = res
        .rows()
        .iter()
        .map(|r| {
            let cols: Vec<String> = r.values().iter().map(|v| cell(Some(v))).collect();
            cols.join("|")
        })
        .collect();
    rows.sort();
    Ok(rows)
}

/// An edge-map case: the map text and the equivalent per-relationship test.
const EDGE_MAPS: &[(&str, &str)] = &[
    ("{w: 1}", "r.w = 1"),
    ("{w: 0}", "r.w = 0"),
    ("{w: 1.0}", "r.w = 1.0"),
    ("{w: '1'}", "r.w = '1'"),
    ("{w: null}", "r.w = null"),
    ("{w: $p}", "r.w = $p"),
    ("{w: 1, k: 3}", "r.w = 1 AND r.k = 3"),
];

/// A target-map case: the node pattern and the equivalent `WHERE` test.
const TARGET_MAPS: &[(&str, &str)] = &[
    ("(b:N {x: 2})", "b.x = 2"),
    ("(b:N {x: 2.0})", "b.x = 2.0"),
    ("(b:N {x: $p})", "b.x = $p"),
];

/// Hop ranges. Unbounded `*` is exercised separately, on `:S` alone and
/// directed: the unmapped reference enumerates every trail, which on these
/// cyclic graphs over both types and directions runs past the timeout. Four
/// hops, not five: see `list_expression_over_a_long_vlp_result_does_not_fail`.
const HOPS: &[&str] = &["*1..3", "*2..2", "*0..2", "*1..4"];

const ARROWS: &[(&str, &str)] = &[("-[", "]->"), ("<-[", "]-"), ("-[", "]-")];

/// Every shape against its reference, on whatever `reader` sees.
async fn check(reader: &Reader<'_>, ctx: &str) -> Result<()> {
    let sources = ["MATCH (a:N {id: 0})", "MATCH (a:N) WHERE a.x < 2 MATCH (a)"];
    let mut k = 0usize;
    for (open, close) in ARROWS {
        for hops in HOPS {
            for (map, test) in EDGE_MAPS {
                k += 1;
                let src = sources[k % sources.len()];
                // Full paths: one row per trail.
                let q = format!(
                    "{src}{open}rs:R|S{hops} {map}{close}(b:N) \
                     RETURN a.id, b.id, [r IN rs | r.k]"
                );
                let reference = format!(
                    "{src}{open}rs:R|S{hops}{close}(b:N) WHERE all(r IN rs WHERE {test}) \
                     RETURN a.id, b.id, [r IN rs | r.k]"
                );
                compare(reader, ctx, &q, &reference).await?;
                // Endpoints only, with no relationship variable: the
                // reachability evaluation, not the path enumeration.
                let q = format!("{src}{open}:R{hops} {map}{close}(b:N) RETURN DISTINCT a.id, b.id");
                let reference = format!(
                    "{src}{open}rs:R{hops}{close}(b:N) WHERE all(r IN rs WHERE {test}) \
                     RETURN DISTINCT a.id, b.id"
                );
                compare(reader, ctx, &q, &reference).await?;
            }
            if *open == "-[" && *close == "]->" && *hops == "*1..3" {
                for (map, test) in [EDGE_MAPS[0], TARGET_MAPS[0]] {
                    let (q, reference) = if map.starts_with('{') {
                        (
                            format!(
                                "MATCH (a:N {{id: 0}})-[rs:S* {map}]->(b:N) \
                                 RETURN a.id, b.id, [r IN rs | r.k]"
                            ),
                            format!(
                                "MATCH (a:N {{id: 0}})-[rs:S*]->(b:N) \
                                 WHERE all(r IN rs WHERE {test}) \
                                 RETURN a.id, b.id, [r IN rs | r.k]"
                            ),
                        )
                    } else {
                        (
                            format!(
                                "MATCH (a:N {{id: 0}})-[rs:S*]->{map} \
                                 RETURN a.id, b.id, [r IN rs | r.k]"
                            ),
                            format!(
                                "MATCH (a:N {{id: 0}})-[rs:S*]->(b:N) WHERE {test} \
                                 RETURN a.id, b.id, [r IN rs | r.k]"
                            ),
                        )
                    };
                    compare(reader, ctx, &q, &reference).await?;
                }
            }
            for (target, test) in TARGET_MAPS {
                k += 1;
                let src = sources[k % sources.len()];
                let q = format!(
                    "{src}{open}rs:R{hops}{close}{target} RETURN a.id, b.id, [r IN rs | r.k]"
                );
                let reference = format!(
                    "{src}{open}rs:R{hops}{close}(b:N) WHERE {test} \
                     RETURN a.id, b.id, [r IN rs | r.k]"
                );
                compare(reader, ctx, &q, &reference).await?;
                // Both maps at once.
                let q = format!(
                    "{src}{open}rs:R{hops} {{w: 1}}{close}{target} \
                     RETURN a.id, b.id, [r IN rs | r.k]"
                );
                let reference = format!(
                    "{src}{open}rs:R{hops}{close}(b:N) \
                     WHERE {test} AND all(r IN rs WHERE r.w = 1) \
                     RETURN a.id, b.id, [r IN rs | r.k]"
                );
                compare(reader, ctx, &q, &reference).await?;
            }
        }
    }
    Ok(())
}

async fn compare(reader: &Reader<'_>, ctx: &str, q: &str, reference: &str) -> Result<()> {
    let got = bag(reader, q).await?;
    let want = bag(reader, reference).await?;
    assert_eq!(got, want, "{ctx}\nquery:     {q}\nreference: {reference}");
    Ok(())
}

/// Edges touching the last two nodes, and every third other edge, stay in L0.
fn split(g: &Graph) -> (Vec<Edge>, Vec<Edge>) {
    let late = g.n - 2;
    g.edges
        .iter()
        .cloned()
        .partition(|e| !(e.src >= late || e.dst >= late || e.k % 3 == 0))
}

/// Part flushed, part in L0, over several random graphs.
#[tokio::test(flavor = "multi_thread")]
async fn vlp_property_maps_match_the_reference_flushed_and_l0() -> Result<()> {
    for seed in 5..6u64 {
        let g = random_graph(0x97_0000 + seed);
        let db = Uni::in_memory().build().await?;
        schema(&db).await?;
        let session = db.session();
        let (flushed, l0) = split(&g);
        commit_all(&session, &[node_rows(&g)]).await?;
        commit_all(&session, &edge_statements(&flushed)).await?;
        db.flush().await?;
        commit_all(&session, &edge_statements(&l0)).await?;
        check(
            &Reader::Session(&session),
            &format!("seed {seed}, mixed\n{g:?}"),
        )
        .await?;
        db.shutdown().await?;
    }
    Ok(())
}

/// Edges that exist only in an uncommitted transaction, and a flushed edge
/// whose L0 entry holds only a later `SET` — the map must still see the `w`
/// it was flushed with.
#[tokio::test(flavor = "multi_thread")]
async fn vlp_property_maps_match_the_reference_in_a_transaction() -> Result<()> {
    for seed in 0..3u64 {
        let g = random_graph(0x97_1000 + seed);
        let db = Uni::in_memory().build().await?;
        schema(&db).await?;
        let session = db.session();
        let (flushed, l0) = split(&g);
        commit_all(&session, &[node_rows(&g)]).await?;
        commit_all(&session, &edge_statements(&flushed)).await?;
        db.flush().await?;
        // Rewrite every flushed edge's `k` in L0, leaving its `w` in L1.
        commit_all(&session, &["MATCH ()-[r]->() SET r.k = r.k".to_string()]).await?;
        let tx = session.tx().await?;
        for s in edge_statements(&l0) {
            tx.execute(&s).await?;
        }
        check(
            &Reader::Tx(&tx),
            &format!("seed {seed}, transaction\n{g:?}"),
        )
        .await?;
        tx.rollback();
        db.shutdown().await?;
    }
    Ok(())
}

/// A fork's own edges are gated by the map, and primary does not see them.
#[tokio::test(flavor = "multi_thread")]
async fn vlp_property_maps_match_the_reference_on_a_fork() -> Result<()> {
    let g = random_graph(0x97_2000);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    let (base, extra) = split(&g);
    commit_all(&session, &[node_rows(&g)]).await?;
    commit_all(&session, &edge_statements(&base)).await?;
    db.flush().await?;
    let fork = session.fork("vlp_map_fork").await?;
    commit_all(&fork, &edge_statements(&extra)).await?;
    check(&Reader::Session(&fork), &format!("fork\n{g:?}")).await?;
    check(&Reader::Session(&session), &format!("primary\n{g:?}")).await?;
    db.shutdown().await?;
    Ok(())
}

/// A map value that is not a constant — `{w: a.x}` — used to be dropped, and
/// the pattern matched as if it had no map.
///
/// The physical planner extracts only `prop = literal` and `prop = $param`
/// conditions from a variable-length relationship's map
/// (`extract_edge_property_conditions`); anything else was skipped, and with
/// no conditions left the walk admitted every edge. The logical planner now
/// adds a residual per-relationship filter, `all(r IN rs WHERE r.w = a.x)`,
/// for each non-constant entry. The wider sweep is `NON_CONSTANT_MAPS` below.
#[tokio::test(flavor = "multi_thread")]
async fn vlp_property_map_with_a_variable_value_is_not_dropped() -> Result<()> {
    let g = random_graph(0x97_0000);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    commit_all(&session, &[node_rows(&g)]).await?;
    commit_all(&session, &edge_statements(&g.edges)).await?;
    compare(
        &Reader::Session(&session),
        "variable-valued map",
        "MATCH (a:N {id: 0})-[rs:R|S*1..3 {w: a.x}]->(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N {id: 0})-[rs:R|S*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    )
    .await?;
    db.shutdown().await?;
    Ok(())
}

/// A list expression over a variable-length relationship list answers once
/// the traversal emits more than one batch's worth of rows for its input.
///
/// `MATCH (a:N) WHERE a.x < 2 MATCH (a)-[rs:R*1..5]-(b:N)` over seed 5's graph
/// yields 8,198 trails. `RETURN count(rs)` answered; any expression that walked
/// the list — `all(r IN rs WHERE true)`, `[r IN rs | r.k]` — failed with
/// "all columns in a record batch must have the same length". The batch
/// reaching the expression was a slice, and a sliced list's `values()` is its
/// parent's whole buffer, so the loop column of the inner batch was longer
/// than the columns taken by offset (`compact_large_list`).
#[tokio::test(flavor = "multi_thread")]
async fn list_expression_over_a_long_vlp_result_does_not_fail() -> Result<()> {
    let g = random_graph(0x97_0000 + 5);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    let (flushed, l0) = split(&g);
    commit_all(&session, &[node_rows(&g)]).await?;
    commit_all(&session, &edge_statements(&flushed)).await?;
    db.flush().await?;
    commit_all(&session, &edge_statements(&l0)).await?;
    let q = "MATCH (a:N) WHERE a.x < 2 MATCH (a)-[rs:R*1..5]-(b:N) \
             WHERE all(r IN rs WHERE true) RETURN count(*) AS c";
    let res = session.query(q).await?;
    let control = session
        .query("MATCH (a:N) WHERE a.x < 2 MATCH (a)-[rs:R*1..5]-(b:N) RETURN count(rs) AS c")
        .await?;
    let control = control.rows()[0].get::<i64>("c")?;
    // The failure needs more than one batch (8,192 rows) of trails.
    assert!(
        control > 8_192,
        "only {control} trails: the repro no longer spans a batch"
    );
    assert_eq!(res.rows()[0].get::<i64>("c")?, control);
    // A comprehension over the same list: every trail keeps its length.
    let lens = session
        .query(
            "MATCH (a:N) WHERE a.x < 2 MATCH (a)-[rs:R*1..5]-(b:N) \
             RETURN sum(size([r IN rs | r.k])) AS s, sum(size(rs)) AS t",
        )
        .await?;
    assert_eq!(
        lens.rows()[0].get::<i64>("s")?,
        lens.rows()[0].get::<i64>("t")?
    );
    db.shutdown().await?;
    Ok(())
}

/// A walk deeper than the region read's round budget falls back to the
/// whole-type filters part-way through a batch, and the answer is unchanged.
///
/// A 60-edge chain, one edge failing `{w: 1}`, read from several
/// sources in one batch: covering a source costs one property-read round per
/// hop, so the 16-round minimum budget runs out part-way and the rest of the
/// query uses the whole-type allow-set.
#[tokio::test(flavor = "multi_thread")]
async fn vlp_property_map_falls_back_to_the_whole_type_on_a_deep_walk() -> Result<()> {
    let n = 61;
    let g = Graph {
        n,
        x: (0..n).map(|i| (i % 4) as i64).collect(),
        edges: (0..n - 1)
            .map(|k| Edge {
                ty: "R",
                src: k,
                dst: k + 1,
                w: Some(if k == 45 { 0 } else { 1 }),
                k,
            })
            .collect(),
    };
    for flush in [false, true] {
        let db = Uni::in_memory().build().await?;
        schema(&db).await?;
        let session = db.session();
        commit_all(&session, &[node_rows(&g)]).await?;
        commit_all(&session, &edge_statements(&g.edges)).await?;
        if flush {
            db.flush().await?;
        }
        let reader = Reader::Session(&session);
        // The fall-back is what this test is for: its whole-type scan reads
        // every one of the 60 edges, where the region read alone would not.
        let res = session
            .query(
                "MATCH (a:N) WHERE a.id % 7 = 0 MATCH (a)-[:R*1..30 {w: 1}]->(b:N) RETURN count(*)",
            )
            .await?;
        assert!(
            res.metrics().rows_scanned >= 60,
            "flushed {flush}: expected the whole-type fall-back to read all 60 edges, \
             rows_scanned = {}",
            res.metrics().rows_scanned
        );
        for (map, test) in [("{w: 1}", "r.w = 1"), ("{w: 0}", "r.w = 0")] {
            compare(
                &reader,
                &format!("deep chain, flushed {flush}"),
                &format!(
                    "MATCH (a:N) WHERE a.id % 7 = 0 MATCH (a)-[:R*1..30 {map}]->(b:N) \
                     RETURN DISTINCT a.id, b.id"
                ),
                &format!(
                    "MATCH (a:N) WHERE a.id % 7 = 0 MATCH (a)-[rs:R*1..30]->(b:N) \
                     WHERE all(r IN rs WHERE {test}) RETURN DISTINCT a.id, b.id"
                ),
            )
            .await?;
        }
        db.shutdown().await?;
    }
    Ok(())
}

/// A pattern map whose value is not a constant, against its `WHERE` form.
///
/// `{w: a.x}` is the predicate `r.w = a.x` written inline: for a
/// variable-length relationship it holds for every relationship on the path,
/// for a node it is `b.x = a.x`. The value may name a variable from an
/// earlier clause or from the same pattern — the source, the far endpoint,
/// the element itself — as the predicate in `WHERE` may. The constant rows
/// (`{w: 1}`) are controls: they check the reference formulation itself.
const NON_CONSTANT_MAPS: &[(&str, &str)] = &[
    // Variable-length, the source's property.
    (
        "MATCH (a:N)-[rs:R|S*1..3 {w: a.x}]->(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N)-[rs:R|S*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    // The far endpoint, bound by the same pattern.
    (
        "MATCH (a:N)-[rs:R*1..3 {w: b.x}]->(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = b.x) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    // An arithmetic expression, a function of a parameter, and a constant
    // entry beside a non-constant one, in both orders.
    (
        "MATCH (a:N)<-[rs:R*0..2 {w: a.x - 1}]-(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N)<-[rs:R*0..2]-(b:N) WHERE all(r IN rs WHERE r.w = a.x - 1) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N {id: 0})-[rs:R*1..3 {w: toInteger($p)}]-(b:N) \
         RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N {id: 0})-[rs:R*1..3]-(b:N) WHERE all(r IN rs WHERE r.w = toInteger($p)) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N)-[rs:R*1..3 {w: a.x, k: 3}]->(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x AND r.k = 3) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N)-[rs:R*1..3 {k: 3, w: a.x}]->(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x AND r.k = 3) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    // A variable from an earlier clause, and from `WITH`.
    (
        "MATCH (c:N {id: 1}) MATCH (a:N)-[rs:R*1..2 {w: c.x}]->(b:N) \
         RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (c:N {id: 1}) MATCH (a:N)-[rs:R*1..2]->(b:N) WHERE all(r IN rs WHERE r.w = c.x) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "WITH 0 AS v MATCH (a:N)-[rs:R*1..2 {w: v}]->(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "WITH 0 AS v MATCH (a:N)-[rs:R*1..2]->(b:N) WHERE all(r IN rs WHERE r.w = v) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    // No relationship variable: endpoints only.
    (
        "MATCH (a:N)-[:R*1..3 {w: a.x}]->(b:N) RETURN DISTINCT a.id, b.id",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) \
         RETURN DISTINCT a.id, b.id",
    ),
    (
        "MATCH (a:N)-[:R*1..3 {w: a.x}]->(b:N) RETURN a.id, b.id",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) RETURN a.id, b.id",
    ),
    // Unknown (schemaless) edge type, variable-length and single-hop.
    (
        "MATCH (a:N)-[rs:NOPE*1..2 {w: a.x}]->(b:N) RETURN a.id, b.id",
        "MATCH (a:N)-[rs:NOPE*1..2]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) RETURN a.id, b.id",
    ),
    // Unknown-to-the-schema edge type `U` (a copy of every `R`), variable-length.
    (
        "MATCH (a:N)-[rs:U*1..3 {w: a.x}]->(b:N) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N)-[rs:U*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N)-[:U*1..3 {w: a.x}]->(b:N) RETURN a.id, b.id",
        "MATCH (a:N)-[rs:U*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) RETURN a.id, b.id",
    ),
    // A variable bound later in the same pattern than the element whose map
    // names it.
    (
        "MATCH (a:N)-[rs:R*1..2 {w: c.x}]->(b:N)-[:R]->(c:N) \
         RETURN a.id, b.id, c.id, [r IN rs | r.k]",
        "MATCH (a:N)-[rs:R*1..2]->(b:N)-[:R]->(c:N) WHERE all(r IN rs WHERE r.w = c.x) \
         RETURN a.id, b.id, c.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N)-[:R {w: c.x}]->(b:N)-[:R]->(c:N) RETURN a.id, b.id, c.id",
        "MATCH (a:N)-[r:R]->(b:N)-[:R]->(c:N) WHERE r.w = c.x RETURN a.id, b.id, c.id",
    ),
    (
        "MATCH (:N {x: b.x})-[:R]->(b:N) RETURN b.id",
        "MATCH (a:N)-[:R]->(b:N) WHERE a.x = b.x RETURN b.id",
    ),
    // Single-hop.
    (
        "MATCH (a:N)-[r:R|S {w: a.x}]->(b:N) RETURN a.id, b.id, r.k",
        "MATCH (a:N)-[r:R|S]->(b:N) WHERE r.w = a.x RETURN a.id, b.id, r.k",
    ),
    (
        "MATCH (a:N)-[:R {w: b.x}]-(b:N) RETURN a.id, b.id",
        "MATCH (a:N)-[r:R]-(b:N) WHERE r.w = b.x RETURN a.id, b.id",
    ),
    // OPTIONAL: the map gates the optional part only; every `a` survives.
    (
        "MATCH (a:N) OPTIONAL MATCH (a)-[rs:R*1..2 {w: a.x}]->(b:N) \
         RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N) OPTIONAL MATCH (a)-[rs:R*1..2]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N) OPTIONAL MATCH (a)-[r:R {w: a.x}]->(b:N) RETURN a.id, b.id, r.k",
        "MATCH (a:N) OPTIONAL MATCH (a)-[r:R]->(b:N) WHERE r.w = a.x RETURN a.id, b.id, r.k",
    ),
    (
        "MATCH (a:N) OPTIONAL MATCH (a)-[:R]->(b:N {x: a.x}) RETURN a.id, b.id",
        "MATCH (a:N) OPTIONAL MATCH (a)-[:R]->(b:N) WHERE b.x = a.x RETURN a.id, b.id",
    ),
    (
        "MATCH (a:N) OPTIONAL MATCH (a)-[r:R {w: b.x}]->(b:N) RETURN a.id, b.id, r.k",
        "MATCH (a:N) OPTIONAL MATCH (a)-[r:R]->(b:N) WHERE r.w = b.x RETURN a.id, b.id, r.k",
    ),
    (
        "MATCH (a:N) OPTIONAL MATCH (a)-[rs:R*1..2 {w: b.x}]->(b:N) \
         RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N) OPTIONAL MATCH (a)-[rs:R*1..2]->(b:N) WHERE all(r IN rs WHERE r.w = b.x) \
         RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N) OPTIONAL MATCH (b:N {x: a.x + 10}) RETURN a.id, b.id",
        "MATCH (a:N) OPTIONAL MATCH (b:N) WHERE b.x = a.x + 10 RETURN a.id, b.id",
    ),
    (
        "MATCH (a:N) OPTIONAL MATCH (b:N {x: a.x}) RETURN a.id, b.id",
        "MATCH (a:N) OPTIONAL MATCH (b:N) WHERE b.x = a.x RETURN a.id, b.id",
    ),
    // Quantified path pattern.
    (
        "MATCH (a:N) MATCH (a)(()-[:R {w: a.x}]->()){1,3}(b:N) RETURN a.id, b.id",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = a.x) RETURN a.id, b.id",
    ),
    (
        "MATCH (a:N) MATCH (a)((x)-[r:R {w: 1}]->(y)){1,3}(b:N) RETURN a.id, b.id",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE all(r IN rs WHERE r.w = 1) RETURN a.id, b.id",
    ),
    // shortestPath: the map gates the search, so the length is the shortest
    // over the admitted paths.
    (
        "MATCH (a:N {id: 0}), (b:N) WHERE b.id <> 0 \
         MATCH p = shortestPath((a)-[:R*1..4 {w: 1}]->(b)) RETURN b.id, length(p)",
        "MATCH (a:N {id: 0}), (b:N) WHERE b.id <> 0 MATCH q = (a)-[rs:R*1..4]->(b) \
         WHERE all(r IN rs WHERE r.w = 1) RETURN b.id, min(length(q))",
    ),
    (
        "MATCH (a:N {id: 0}) MATCH p = shortestPath((a)-[:R*1..4]->(b:N {x: a.x})) \
         WHERE b.id <> 0 RETURN b.id, length(p)",
        "MATCH (a:N {id: 0}), (b:N) WHERE b.id <> 0 AND b.x = a.x \
         MATCH q = (a)-[:R*1..4]->(b) RETURN b.id, min(length(q))",
    ),
    // Node maps.
    (
        "MATCH (a:N)-[:R]->(b:N {x: a.x}) RETURN a.id, b.id",
        "MATCH (a:N)-[:R]->(b:N) WHERE b.x = a.x RETURN a.id, b.id",
    ),
    (
        "MATCH (a:N {x: b.x})-[:R]->(b:N) RETURN a.id, b.id",
        "MATCH (a:N)-[:R]->(b:N) WHERE a.x = b.x RETURN a.id, b.id",
    ),
    (
        "MATCH (a:N)-[rs:R*1..3]->(b:N {x: a.x}) RETURN a.id, b.id, [r IN rs | r.k]",
        "MATCH (a:N)-[rs:R*1..3]->(b:N) WHERE b.x = a.x RETURN a.id, b.id, [r IN rs | r.k]",
    ),
    (
        "MATCH (a:N), (b:N {x: a.x}) RETURN a.id, b.id",
        "MATCH (a:N), (b:N) WHERE b.x = a.x RETURN a.id, b.id",
    ),
    (
        "MATCH (a:N {id: 0}) MATCH (b:N {x: a.x}) RETURN a.id, b.id",
        "MATCH (a:N {id: 0}) MATCH (b:N) WHERE b.x = a.x RETURN a.id, b.id",
    ),
    (
        "MATCH (b:N {x: b.id}) RETURN b.id",
        "MATCH (b:N) WHERE b.x = b.id RETURN b.id",
    ),
    (
        "MATCH (b:N {x: $p + 1}) RETURN b.id",
        "MATCH (b:N) WHERE b.x = $p + 1 RETURN b.id",
    ),
    // Existential subpatterns and pattern comprehension.
    (
        "MATCH (a:N) WHERE EXISTS { (a)-[:R {w: a.x}]->() } RETURN a.id",
        "MATCH (a:N) WHERE EXISTS { MATCH (a)-[r:R]->() WHERE r.w = a.x } RETURN a.id",
    ),
    (
        "MATCH (a:N) WHERE EXISTS { (a)-[:R*1..2 {w: a.x}]->() } RETURN a.id",
        "MATCH (a:N) WHERE EXISTS { MATCH (a)-[rs:R*1..2]->() WHERE all(r IN rs WHERE r.w = a.x) } \
         RETURN a.id",
    ),
    (
        "MATCH (a:N) WHERE EXISTS { (a)-[:R]->(:N {x: a.x}) } RETURN a.id",
        "MATCH (a:N) WHERE EXISTS { MATCH (a)-[:R]->(b:N) WHERE b.x = a.x } RETURN a.id",
    ),
    (
        "MATCH (a:N) WHERE (a)-[:R {w: a.x}]->() RETURN a.id",
        "MATCH (a:N) WHERE EXISTS { MATCH (a)-[r:R]->() WHERE r.w = a.x } RETURN a.id",
    ),
    (
        "MATCH (a:N) RETURN a.id, size([(a)-[r:R {w: a.x}]->(b) | b.id]) AS n, \
         reduce(s = 0, i IN [(a)-[r:R {w: a.x}]->(b) | b.id] | s + i) AS t",
        "MATCH (a:N) RETURN a.id, size([(a)-[r:R]->(b) WHERE r.w = a.x | b.id]) AS n, \
         reduce(s = 0, i IN [(a)-[r:R]->(b) WHERE r.w = a.x | b.id] | s + i) AS t",
    ),
    (
        "MATCH (a:N) RETURN a.id, size([(a)-[:R]->(b:N {x: a.x}) | b.id]) AS n",
        "MATCH (a:N) RETURN a.id, size([(a)-[:R]->(b:N) WHERE b.x = a.x | b.id]) AS n",
    ),
    (
        "MATCH (a:N) RETURN a.id, size([(a)-[:R*1..2 {w: a.x}]->(b) | b.id]) AS n",
        "MATCH (a:N) RETURN a.id, size([(a)-[rs:R*1..2]->(b) WHERE all(r IN rs WHERE r.w = a.x) \
         | b.id]) AS n",
    ),
];

/// Copies every `R` edge as a `U` edge, a type the schema does not declare.
const COPY_R_AS_U: &str = "MATCH (a:N)-[r:R]->(b:N) CREATE (a)-[:U {w: r.w, k: r.k}]->(b)";

/// Every non-constant map against its reference, on whatever `reader` sees.
/// Every mismatch is reported, not only the first.
async fn check_non_constant(reader: &Reader<'_>, ctx: &str) -> Result<()> {
    let mut failures = Vec::new();
    for (q, reference) in NON_CONSTANT_MAPS {
        let want = bag(reader, reference).await?;
        match bag(reader, q).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!(
                "query:     {q}\nreference: {reference}\n  got:  {got:?}\n  want: {want:?}"
            )),
            Err(e) => failures.push(format!("query failed: {e}")),
        }
    }
    assert!(
        failures.is_empty(),
        "{ctx}\n{} of {} cases differ:\n{}",
        failures.len(),
        NON_CONSTANT_MAPS.len(),
        failures.join("\n")
    );
    Ok(())
}

/// The map `{w: a.x}` must not admit an edge whose `w` differs from `a.x`.
/// A direct check, independent of the reference formulation: every returned
/// relationship satisfies the map.
async fn check_non_constant_direct(reader: &Reader<'_>, ctx: &str) -> Result<()> {
    let q = "MATCH (a:N)-[rs:R|S*1..3 {w: a.x}]->(b:N) \
             RETURN count(*) AS paths, \
                    sum(size([r IN rs WHERE r.w IS NULL OR r.w <> a.x])) AS bad";
    let res = reader.run(q).await?;
    let row = &res.rows()[0];
    assert_eq!(row.get::<i64>("bad")?, 0, "{ctx}\n{q}");
    Ok(())
}

/// Part flushed, part in L0, over several random graphs.
#[tokio::test(flavor = "multi_thread")]
async fn non_constant_pattern_maps_match_the_reference_flushed_and_l0() -> Result<()> {
    for seed in 0..3u64 {
        let g = random_graph(0x97_3000 + seed);
        let db = Uni::in_memory().build().await?;
        schema(&db).await?;
        let session = db.session();
        let (flushed, l0) = split(&g);
        commit_all(&session, &[node_rows(&g)]).await?;
        commit_all(&session, &edge_statements(&flushed)).await?;
        db.flush().await?;
        commit_all(&session, &edge_statements(&l0)).await?;
        commit_all(&session, &[COPY_R_AS_U.to_string()]).await?;
        let reader = Reader::Session(&session);
        let ctx = format!("seed {seed}, mixed\n{g:?}");
        check_non_constant_direct(&reader, &ctx).await?;
        check_non_constant(&reader, &ctx).await?;
        db.shutdown().await?;
    }
    Ok(())
}

/// All in L0, and all in L1.
#[tokio::test(flavor = "multi_thread")]
async fn non_constant_pattern_maps_match_the_reference_l0_only_and_flushed_only() -> Result<()> {
    for flush in [false, true] {
        let g = random_graph(0x97_4000);
        let db = Uni::in_memory().build().await?;
        schema(&db).await?;
        let session = db.session();
        commit_all(&session, &[node_rows(&g)]).await?;
        commit_all(&session, &edge_statements(&g.edges)).await?;
        commit_all(&session, &[COPY_R_AS_U.to_string()]).await?;
        if flush {
            db.flush().await?;
        }
        let reader = Reader::Session(&session);
        let ctx = format!("flushed {flush}\n{g:?}");
        check_non_constant_direct(&reader, &ctx).await?;
        check_non_constant(&reader, &ctx).await?;
        db.shutdown().await?;
    }
    Ok(())
}

/// Through a fork whose own edges are in L0, and on primary beside it.
#[tokio::test(flavor = "multi_thread")]
async fn non_constant_pattern_maps_match_the_reference_on_a_fork() -> Result<()> {
    let g = random_graph(0x97_5000);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    let (base, extra) = split(&g);
    commit_all(&session, &[node_rows(&g)]).await?;
    commit_all(&session, &edge_statements(&base)).await?;
    commit_all(&session, &[COPY_R_AS_U.to_string()]).await?;
    db.flush().await?;
    let fork = session.fork("non_constant_map_fork").await?;
    commit_all(&fork, &edge_statements(&extra)).await?;
    check_non_constant(&Reader::Session(&fork), &format!("fork\n{g:?}")).await?;
    check_non_constant(&Reader::Session(&session), &format!("primary\n{g:?}")).await?;
    db.shutdown().await?;
    Ok(())
}

/// `MERGE` with a non-constant map matches on the map, and creates with it.
#[tokio::test(flavor = "multi_thread")]
async fn merge_with_a_non_constant_map_matches_and_creates_on_it() -> Result<()> {
    let g = random_graph(0x97_6000);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    commit_all(&session, &[node_rows(&g)]).await?;
    commit_all(&session, &edge_statements(&g.edges)).await?;
    for flush in [false, true] {
        if flush {
            db.flush().await?;
        }
        // A node: `a` itself always satisfies `{x: a.x}`, so this matches and
        // creates nothing.
        let tx = session.tx().await?;
        let want = bag(
            &Reader::Tx(&tx),
            "MATCH (a:N {id: 0}) MATCH (b:N) WHERE b.x = a.x RETURN b.id",
        )
        .await?;
        let got = bag(
            &Reader::Tx(&tx),
            "MATCH (a:N {id: 0}) MERGE (b:N {x: a.x}) RETURN b.id",
        )
        .await?;
        assert_eq!(got, want, "flushed {flush}: MERGE node");
        tx.rollback();

        // A relationship between every ordered pair of the first three nodes.
        for (s, d) in [(0, 1), (1, 0), (0, 0), (1, 2), (2, 1)] {
            let tx = session.tx().await?;
            let reader = Reader::Tx(&tx);
            let existing = bag(
                &reader,
                &format!(
                    "MATCH (a:N {{id: {s}}})-[r:R]->(b:N {{id: {d}}}) WHERE r.w = a.x RETURN r.k"
                ),
            )
            .await?;
            let res = reader
                .run(&format!(
                    "MATCH (a:N {{id: {s}}}), (b:N {{id: {d}}}) \
                     MERGE (a)-[r:R {{w: a.x}}]->(b) RETURN r.k AS k, r.w = a.x AS ok"
                ))
                .await?;
            let ctx = format!("flushed {flush}: MERGE ({s})-[:R {{w: a.x}}]->({d})");
            for row in res.rows() {
                assert!(row.get::<bool>("ok")?, "{ctx}");
            }
            if existing.is_empty() {
                assert_eq!(res.rows().len(), 1, "{ctx}: expected one created edge");
            } else {
                let mut got: Vec<String> = res.rows().iter().map(|r| cell(r.value("k"))).collect();
                got.sort();
                assert_eq!(got, existing, "{ctx}");
            }
            tx.rollback();
        }
    }
    db.shutdown().await?;
    Ok(())
}

/// A non-constant value where it has to gate a walk that no later filter can
/// stand in for is refused, not dropped: an element inside a quantified
/// sub-pattern that is not planned as a plain variable-length relationship
/// (its map applies per iteration). A shortest-path relationship's map with
/// such a value gates the search per row; see `shortest_path_row_dependent_map`.
#[tokio::test(flavor = "multi_thread")]
async fn non_constant_map_values_that_must_gate_a_walk_are_refused() -> Result<()> {
    let g = random_graph(0x97_7000);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    commit_all(&session, &[node_rows(&g)]).await?;
    commit_all(&session, &edge_statements(&g.edges)).await?;
    let reader = Reader::Session(&session);
    for (q, needle) in [
        (
            "MATCH (a:N) MATCH (a)((x)-[r:R {w: a.x}]->(y)){1,3}(b:N) RETURN a.id, b.id",
            "quantified path pattern",
        ),
        (
            "MATCH (a:N) MATCH (a)((x)-[r:R]->(y:N {x: a.x})){1,3}(b:N) RETURN a.id, b.id",
            "quantified path pattern",
        ),
    ] {
        let err = match reader.run(q).await {
            Ok(res) => panic!("{q}: expected a refusal, got {} rows", res.rows().len()),
            Err(e) => e.to_string(),
        };
        assert!(err.contains(needle), "{q}: unexpected error: {err}");
    }
    // The walk-constant forms of the same patterns still plan.
    for q in [
        "MATCH (a:N {id: 0}), (b:N) MATCH p = shortestPath((a)-[:R*1..4 {w: 1}]->(b)) \
         RETURN b.id",
        "MATCH (a:N) MATCH (a)((x)-[r:R {w: $p}]->(y:N {x: 1})){1,3}(b:N) RETURN a.id",
    ] {
        reader.run(q).await?;
    }
    db.shutdown().await?;
    Ok(())
}

/// A node map on a quantified sub-pattern's inner target constrains the node
/// every hop reaches, not only the last. A map on the inner source, which
/// would constrain the start of every iteration, is refused: only the first
/// iteration's start was ever checked.
#[tokio::test(flavor = "multi_thread")]
async fn quantified_inner_node_maps_match_the_reference() -> Result<()> {
    let g = random_graph(0x97_8000);
    let db = Uni::in_memory().build().await?;
    schema(&db).await?;
    let session = db.session();
    commit_all(&session, &[node_rows(&g)]).await?;
    commit_all(&session, &edge_statements(&g.edges)).await?;
    let reader = Reader::Session(&session);
    let every_target = "MATCH p = (a:N)-[:R*1..3]->(b:N) \
                        WHERE all(n IN tail(nodes(p)) WHERE n.x = 1) RETURN a.id, b.id";
    let mut failures = Vec::new();
    for (q, reference) in [
        (
            "MATCH (a:N) MATCH (a)(()-[:R]->(:N {x: 1})){1,3}(b:N) RETURN a.id, b.id",
            every_target,
        ),
        (
            "MATCH (a:N) MATCH (a)(()-[:R]->({x: 1})){1,3}(b:N) RETURN a.id, b.id",
            every_target,
        ),
        (
            "MATCH (a:N) MATCH (a)((x:N)-[r:R]->(y:N {x: 1})){1,3}(b:N) RETURN a.id, b.id",
            every_target,
        ),
    ] {
        let want = bag(&reader, reference).await?;
        match bag(&reader, q).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{q}\n  got:  {got:?}\n  want: {want:?}")),
            Err(e) => failures.push(format!("failed: {e}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    for q in [
        "MATCH (a:N) MATCH (a)(({x: 1})-[:R]->()){1,3}(b:N) RETURN a.id, b.id",
        "MATCH (a:N) MATCH (a)((x:N {x: 1})-[r:R]->(y:N)){1,3}(b:N) RETURN a.id, b.id",
    ] {
        let err = match reader.run(q).await {
            Ok(res) => panic!("{q}: expected a refusal, got {} rows", res.rows().len()),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("first node inside a quantified"), "{q}: {err}");
    }
    db.shutdown().await?;
    Ok(())
}
