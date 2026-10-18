// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A `vector_similarity` / `vector_distance` predicate in `WHERE` keeps its
//! per-row meaning whatever indexes exist.
//!
//! `vector_similarity(a, b)` is the cosine similarity of two vectors, and
//! `WHERE vector_similarity(d.emb, q) > t` keeps exactly the rows whose
//! cosine similarity exceeds `t`. The planner used to rewrite that predicate
//! into a top-100 index scan (`VectorKnn`), which (a) compared `t` against the
//! index's own score — `(1 + cos) / 2` for a cosine index, `1 / (1 + d)` for
//! L2, the raw dot product for dot — instead of the cosine, (b) kept every row
//! at `score >= t` for a strict `>`, and (c) silently dropped every match past
//! the 100th. The same rewrite fired on an outer variable inside `EXISTS { }`
//! and on a query vector with a negative component it could not evaluate.
//!
//! The oracle is a brute-force cosine over the fixture computed here in Rust,
//! with no database involved. Each query also runs behind a `WITH`
//! projection barrier (`WITH d, vector_similarity(..) AS s WHERE s > t`),
//! which no index rewrite can reach, so the oracle itself is cross-checked.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(vector_predicate_exactness)'

// Rust guideline compliant

use std::collections::BTreeSet;

use anyhow::{Result, anyhow};
use uni_db::{
    DataType, IndexType, Session, Uni, UniConfig, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
};

/// Hand-placed rows: `(id, emb)`.
///
/// Against `[0, 1]`: id 2 and id 6 have similarity exactly 1.0 (different
/// magnitudes, so L2 ranks them differently), id 7 `[3, 4]` exactly 0.8, id 1
/// and id 4 exactly 0.0, id 5 exactly -1.0. Id 8 has no vector.
const BASE: &[(i64, Option<[f64; 2]>)] = &[
    (1, Some([1.0, 0.0])),
    (2, Some([0.0, 1.0])),
    (3, Some([1.0, 1.0])),
    (4, Some([-1.0, 0.0])),
    (5, Some([0.0, -1.0])),
    (6, Some([0.0, 2.0])),
    (7, Some([3.0, 4.0])),
    (8, None),
];

/// Bulk rows `100 + i` with `emb = [1, i / 1024]` (exact in f32 and f64).
///
/// 150 rows all within 0.15 rad of `[1, 0]`, so a threshold below 0.98
/// against `[1, 0]` matches more rows than the old rewrite's fixed `k = 100`.
/// `i = 0` is exactly orthogonal to `[0, 1]`: a tie at threshold 0.0.
const BULK: i64 = 150;

/// Relationships `(src, dst)` for the subquery cases: id 1 (similarity 1.0
/// to `[1, 0]`) and id 100 (≈ 1.0) have an outgoing `:R`, id 4 (-1.0) has
/// one, id 3 (≈ 0.707) has none.
const EDGES: &[(i64, i64)] = &[(1, 2), (4, 1), (100, 1)];

/// Query vectors, as Cypher text and as values. `[-1.0, 0.0]` is a negative
/// literal, which the parser produces as a unary minus over a literal.
const QUERIES: &[(&str, [f64; 2])] = &[
    ("[0.0, 1.0]", [0.0, 1.0]),
    ("[1.0, 0.0]", [1.0, 0.0]),
    ("[-1.0, 0.0]", [-1.0, 0.0]),
    ("[1, 1]", [1.0, 1.0]),
];

const THRESHOLDS: &[(&str, f64)] = &[
    ("-0.5", -0.5),
    ("0.0", 0.0),
    ("0.5", 0.5),
    ("0.8", 0.8),
    ("1.0", 1.0),
];

/// How the store is built.
#[derive(Clone, Copy, Debug)]
enum Mode {
    /// No schema: `emb` is a plain list and no index exists.
    Schemaless,
    /// `emb` declared `VECTOR(2)` with a vector index of this kind.
    Indexed(Algo, Metric),
}

/// Index metric (`VectorMetric` is neither `Copy` nor `Debug`).
#[derive(Clone, Copy, Debug)]
enum Metric {
    Cosine,
    L2,
    Dot,
}

impl Metric {
    fn cfg(self) -> VectorMetric {
        match self {
            Self::Cosine => VectorMetric::Cosine,
            Self::L2 => VectorMetric::L2,
            Self::Dot => VectorMetric::Dot,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Algo {
    Flat,
    IvfFlat,
    Hnsw,
}

impl Algo {
    fn cfg(self) -> VectorAlgo {
        match self {
            Self::Flat => VectorAlgo::Flat,
            Self::IvfFlat => VectorAlgo::IvfFlat { partitions: 2 },
            Self::Hnsw => VectorAlgo::Hnsw {
                m: 8,
                ef_construction: 64,
                partitions: Some(1),
            },
        }
    }
}

/// Every vector row of the fixture, `(id, emb)`.
fn fixture() -> Vec<(i64, Option<[f64; 2]>)> {
    let mut rows = BASE.to_vec();
    rows.extend((0..BULK).map(|i| (100 + i, Some([1.0, i as f64 / 1024.0]))));
    rows
}

/// Cosine similarity with the engine's convention (0 when either is zero).
fn cosine(a: [f64; 2], b: [f64; 2]) -> f64 {
    let dot = a[0] * b[0] + a[1] * b[1];
    let (m1, m2) = (
        (a[0] * a[0] + a[1] * a[1]).sqrt(),
        (b[0] * b[0] + b[1] * b[1]).sqrt(),
    );
    if m1 == 0.0 || m2 == 0.0 {
        0.0
    } else {
        (dot / (m1 * m2)).clamp(-1.0, 1.0)
    }
}

fn euclid(a: [f64; 2], b: [f64; 2]) -> f64 {
    ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2)).sqrt()
}

/// A comparison operator, applied as `score OP threshold`.
#[derive(Clone, Copy, Debug)]
enum Op {
    Gt,
    Ge,
    Lt,
    Le,
}

impl Op {
    const ALL: [Op; 4] = [Op::Gt, Op::Ge, Op::Lt, Op::Le];

    fn text(self) -> &'static str {
        match self {
            Op::Gt => ">",
            Op::Ge => ">=",
            Op::Lt => "<",
            Op::Le => "<=",
        }
    }

    /// The operator that keeps the same rows with its operands swapped.
    fn flipped(self) -> &'static str {
        match self {
            Op::Gt => "<",
            Op::Ge => "<=",
            Op::Lt => ">",
            Op::Le => ">=",
        }
    }

    fn holds(self, score: f64, t: f64) -> bool {
        match self {
            Op::Gt => score > t,
            Op::Ge => score >= t,
            Op::Lt => score < t,
            Op::Le => score <= t,
        }
    }
}

async fn open(mode: Mode, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            ..Default::default()
        })
        .build()
        .await?;
    if let Mode::Indexed(algo, metric) = mode {
        db.schema()
            .label("N")
            .property("id", DataType::Int64)
            // Nullable: id 8 has no vector.
            .property_nullable("emb", DataType::Vector { dimensions: 2 })
            .index(
                "emb",
                IndexType::Vector(VectorIndexCfg {
                    algorithm: algo.cfg(),
                    metric: metric.cfg(),
                    embedding: None,
                }),
            )
            .done()
            .edge_type("R", &["N"], &["N"])
            .done()
            .apply()
            .await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    for (id, emb) in fixture() {
        match emb {
            Some([x, y]) => {
                tx.execute(&format!("CREATE (:N {{id: {id}, emb: [{x:?}, {y:?}]}})"))
                    .await?
            }
            None => tx.execute(&format!("CREATE (:N {{id: {id}}})")).await?,
        };
    }
    for (src, dst) in EDGES {
        tx.execute(&format!(
            "MATCH (a:N {{id: {src}}}), (b:N {{id: {dst}}}) CREATE (a)-[:R]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// Runs `query` and returns its single `INTEGER`-or-NULL column as a bag.
async fn ids(session: &Session, query: &str) -> Result<Vec<Option<i64>>> {
    let result = session
        .query(query)
        .await
        .map_err(|e| anyhow!("{query}: {e}"))?;
    let mut out = result
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Int(i) => Ok(Some(*i)),
            Value::Null => Ok(None),
            other => Err(anyhow!("{query}: unexpected value {other:?}")),
        })
        .collect::<Result<Vec<_>>>()?;
    out.sort();
    Ok(out)
}

/// Runs `query` with the query vector bound to `$q`.
async fn ids_with_q(session: &Session, query: &str, q: [f64; 2]) -> Result<Vec<Option<i64>>> {
    let result = session
        .query_with(query)
        .param(
            "q",
            Value::List(vec![Value::Float(q[0]), Value::Float(q[1])]),
        )
        .fetch_all()
        .await
        .map_err(|e| anyhow!("{query}: {e}"))?;
    let mut out = result
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Int(i) => Some(*i),
            _ => None,
        })
        .collect::<Vec<_>>();
    out.sort();
    Ok(out)
}

/// The ids whose vector satisfies `score(emb) OP t`, sorted.
fn oracle(score: impl Fn([f64; 2]) -> f64, op: Op, t: f64) -> Vec<Option<i64>> {
    let mut out: Vec<Option<i64>> = fixture()
        .into_iter()
        .filter_map(|(id, emb)| emb.filter(|e| op.holds(score(*e), t)).map(|_| Some(id)))
        .collect();
    out.sort();
    out
}

/// Compares `got` with `want`, recording a mismatch or an error.
fn record(
    failures: &mut Vec<String>,
    query: &str,
    got: Result<Vec<Option<i64>>>,
    want: &[Option<i64>],
) {
    match got {
        Ok(got) if got == want => {}
        Ok(got) => {
            let g: BTreeSet<_> = got.iter().collect();
            let w: BTreeSet<_> = want.iter().collect();
            failures.push(format!(
                "{query}\n  got {} rows, want {}; extra {:?}, missing {:?}",
                got.len(),
                want.len(),
                g.difference(&w).collect::<Vec<_>>(),
                w.difference(&g).collect::<Vec<_>>(),
            ));
        }
        Err(e) => failures.push(format!("{query}\n  error {e}")),
    }
}

/// The threshold matrix, the `$q` form, conjunct residuals and the
/// `vector_distance` forms, each against the brute-force oracle.
async fn check_predicates(session: &Session, failures: &mut Vec<String>) -> Result<()> {
    for (qtext, q) in QUERIES {
        let sim = |e: [f64; 2]| cosine(e, *q);
        for (ttext, t) in THRESHOLDS {
            for op in Op::ALL {
                let want = oracle(sim, op, *t);
                let call = format!("vector_similarity(d.emb, {qtext})");
                for pred in [
                    format!("{call} {} {ttext}", op.text()),
                    format!("{ttext} {} {call}", op.flipped()),
                ] {
                    let query = format!("MATCH (d:N) WHERE {pred} RETURN d.id");
                    record(failures, &query, ids(session, &query).await, &want);
                }
                // The oracle arm: a projection barrier no rewrite can see.
                let barrier = format!(
                    "MATCH (d:N) WITH d, {call} AS s WHERE s {} {ttext} RETURN d.id",
                    op.text()
                );
                record(failures, &barrier, ids(session, &barrier).await, &want);
            }
            // A residual conjunct on either side.
            let want: Vec<_> = oracle(sim, Op::Gt, *t)
                .into_iter()
                .filter(|id| id.is_some_and(|i| i < 200))
                .collect();
            for query in [
                format!(
                    "MATCH (d:N) WHERE vector_similarity(d.emb, {qtext}) > {ttext} \
                     AND d.id < 200 RETURN d.id"
                ),
                format!(
                    "MATCH (d:N) WHERE d.id < 200 AND \
                     vector_similarity(d.emb, {qtext}) > {ttext} RETURN d.id"
                ),
            ] {
                record(failures, &query, ids(session, &query).await, &want);
            }
            // Parameterised query vector.
            let query =
                format!("MATCH (d:N) WHERE vector_similarity(d.emb, $q) > {ttext} RETURN d.id");
            record(
                failures,
                &format!("{query} [q = {qtext}]"),
                ids_with_q(session, &query, *q).await,
                &oracle(sim, Op::Gt, *t),
            );
        }
        // Two stored vectors: id 2 is `[0, 1]`, so against it id 7 `[3, 4]`
        // scores exactly 0.8 — a tie a strict `>` must exclude.
        if *q == [0.0, 1.0] {
            for (ttext, t) in THRESHOLDS {
                let query = format!(
                    "MATCH (e:N {{id: 2}}), (d:N) \
                     WHERE vector_similarity(d.emb, e.emb) > {ttext} RETURN d.id"
                );
                record(
                    failures,
                    &query,
                    ids(session, &query).await,
                    &oracle(sim, Op::Gt, *t),
                );
            }
        }
        // vector_distance: cosine distance is `1 - cos`, l2 is Euclidean.
        for (dt, want) in [
            ("0.5", oracle(|e| 1.0 - cosine(e, *q), Op::Lt, 0.5)),
            ("1.0", oracle(|e| 1.0 - cosine(e, *q), Op::Le, 1.0)),
        ] {
            let op = if dt == "0.5" { "<" } else { "<=" };
            let query = format!(
                "MATCH (d:N) WHERE vector_distance(d.emb, {qtext}, 'cosine') {op} {dt} RETURN d.id"
            );
            record(failures, &query, ids(session, &query).await, &want);
        }
        let query =
            format!("MATCH (d:N) WHERE vector_distance(d.emb, {qtext}, 'l2') < 1.5 RETURN d.id");
        record(
            failures,
            &query,
            ids(session, &query).await,
            &oracle(|e| euclid(e, *q), Op::Lt, 1.5),
        );
    }
    Ok(())
}

/// `OPTIONAL MATCH` keeps one NULL row when nothing passes.
async fn check_optional(session: &Session, failures: &mut Vec<String>) -> Result<()> {
    for (qtext, q) in QUERIES {
        for (ttext, t) in THRESHOLDS {
            let mut want = oracle(|e| cosine(e, *q), Op::Gt, *t);
            if want.is_empty() {
                want.push(None);
            }
            let query = format!(
                "MATCH (x:N {{id: 1}}) OPTIONAL MATCH (d:N) \
                 WHERE vector_similarity(d.emb, {qtext}) > {ttext} RETURN d.id"
            );
            record(failures, &query, ids(session, &query).await, &want);
        }
    }
    Ok(())
}

/// The predicate on an outer variable inside a subquery.
async fn check_subqueries(session: &Session, failures: &mut Vec<String>) -> Result<()> {
    let has_edge = |id: i64| EDGES.iter().any(|(s, _)| *s == id);
    for (qtext, q) in QUERIES {
        for (ttext, t) in THRESHOLDS {
            let passes = |id: i64, emb: Option<[f64; 2]>| {
                has_edge(id) && emb.is_some_and(|e| cosine(e, *q) > *t)
            };
            let mut exists: Vec<Option<i64>> = fixture()
                .into_iter()
                .filter(|(id, emb)| passes(*id, *emb))
                .map(|(id, _)| Some(id))
                .collect();
            exists.sort();
            let mut not_exists: Vec<Option<i64>> = fixture()
                .into_iter()
                .filter(|(id, emb)| !passes(*id, *emb))
                .map(|(id, _)| Some(id))
                .collect();
            not_exists.sort();
            let body =
                format!("MATCH (a)-[:R]->(b:N) WHERE vector_similarity(a.emb, {qtext}) > {ttext}");
            for (query, want) in [
                (
                    format!("MATCH (a:N) WHERE EXISTS {{ {body} }} RETURN a.id"),
                    &exists,
                ),
                (
                    format!("MATCH (a:N) WHERE NOT EXISTS {{ {body} }} RETURN a.id"),
                    &not_exists,
                ),
                (
                    format!(
                        "MATCH (a:N) WITH a, EXISTS {{ {body} }} AS e \
                         WHERE e RETURN a.id"
                    ),
                    &exists,
                ),
                (
                    format!("MATCH (a:N) WHERE COUNT {{ {body} }} > 0 RETURN a.id"),
                    &exists,
                ),
            ] {
                record(failures, &query, ids(session, &query).await, want);
            }
        }
    }
    Ok(())
}

async fn check(mode: Mode, flush: bool) -> Result<()> {
    let db = open(mode, flush).await?;
    let session = db.session();
    let mut failures = Vec::new();
    check_predicates(&session, &mut failures).await?;
    check_optional(&session, &mut failures).await?;
    check_subqueries(&session, &mut failures).await?;

    // A query vector of the wrong dimension is an error on every path, never
    // an empty answer.
    for query in [
        "MATCH (d:N) WHERE vector_similarity(d.emb, [1.0, 0.0, 0.0]) > 0.5 RETURN d.id",
        "MATCH (d:N) WITH d, vector_similarity(d.emb, [1.0, 0.0, 0.0]) AS s \
         WHERE s > 0.5 RETURN d.id",
    ] {
        if let Ok(rows) = ids(&session, query).await {
            failures.push(format!(
                "{query}\n  expected a dimension error, got {rows:?}"
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "{mode:?}, flushed {flush}: {} failing:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

macro_rules! exactness_tests {
    ($($name:ident => $mode:expr;)*) => {
        $(
            mod $name {
                use super::*;

                #[tokio::test]
                async fn unflushed() -> Result<()> {
                    check($mode, false).await
                }

                #[tokio::test]
                async fn flushed() -> Result<()> {
                    check($mode, true).await
                }
            }
        )*
    };
}

exactness_tests! {
    schemaless => Mode::Schemaless;
    flat_cosine => Mode::Indexed(Algo::Flat, Metric::Cosine);
    flat_l2 => Mode::Indexed(Algo::Flat, Metric::L2);
    flat_dot => Mode::Indexed(Algo::Flat, Metric::Dot);
    ivf_flat_cosine => Mode::Indexed(Algo::IvfFlat, Metric::Cosine);
    ivf_flat_l2 => Mode::Indexed(Algo::IvfFlat, Metric::L2);
    hnsw_cosine => Mode::Indexed(Algo::Hnsw, Metric::Cosine);
    hnsw_dot => Mode::Indexed(Algo::Hnsw, Metric::Dot);
}

/// A fork reads the predicate through its branch: rows written on the fork
/// count, and primary does not see them.
#[tokio::test]
async fn fork_reads_the_predicate_exactly() -> Result<()> {
    let db = open(Mode::Indexed(Algo::Flat, Metric::Cosine), true).await?;
    let primary = db.session();
    let fork = primary.fork("vec").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:N {id: 9, emb: [0.0, 3.0]}), (:N {id: 10, emb: [3.0, 0.1]})")
        .await?;
    tx.commit().await?;

    let q = [0.0, 1.0];
    let base = oracle(|e| cosine(e, q), Op::Gt, 0.5);
    let mut on_fork = base.clone();
    on_fork.push(Some(9));
    on_fork.sort();

    let query = "MATCH (d:N) WHERE vector_similarity(d.emb, [0.0, 1.0]) > 0.5 RETURN d.id";
    let mut failures = Vec::new();
    record(
        &mut failures,
        &format!("fork: {query}"),
        ids(&fork, query).await,
        &on_fork,
    );
    record(
        &mut failures,
        &format!("primary: {query}"),
        ids(&primary, query).await,
        &base,
    );
    fork.flush().await?;
    record(
        &mut failures,
        &format!("fork flushed: {query}"),
        ids(&fork, query).await,
        &on_fork,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// `~=` is the top-K index scan; its query vector may be any constant: a
/// negative literal, arithmetic, or a parameter.
#[tokio::test]
async fn approx_eq_takes_any_constant_query_vector() -> Result<()> {
    for flush in [false, true] {
        let db = open(Mode::Indexed(Algo::Flat, Metric::Cosine), flush).await?;
        let session = db.session();
        let mut failures = Vec::new();
        for qtext in [
            "[-1.0, 0.0]",
            "[-1, 0]",
            "[0.0 - 1.0, 0.0]",
            "[-(1.0), -0.0]",
        ] {
            let query = format!(
                "MATCH (d:N) WHERE d.emb ~= {qtext} RETURN d.id ORDER BY d._score DESC LIMIT 1"
            );
            record(
                &mut failures,
                &query,
                ids(&session, &query).await,
                &[Some(4)],
            );
        }
        let query = "MATCH (d:N) WHERE d.emb ~= $q RETURN d.id ORDER BY d._score DESC LIMIT 1";
        record(
            &mut failures,
            query,
            ids_with_q(&session, query, [-1.0, 0.0]).await,
            &[Some(4)],
        );
        // The same constant as a procedure argument.
        let query = "CALL uni.vector.query('N', 'emb', [-1.0, 0.0], 1) YIELD node \
                     RETURN node.id";
        record(&mut failures, query, ids(&session, query).await, &[Some(4)]);
        assert!(
            failures.is_empty(),
            "flushed {flush}:\n{}",
            failures.join("\n")
        );
    }
    Ok(())
}
