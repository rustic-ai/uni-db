// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #330, step 1: `similar_to` and `vector_similarity` over two stored vectors
//! are scored from the Arrow buffers, without a `Value` per row.
//!
//! The scores must be the ones the `Value` path returns, bit for bit, so each
//! is checked against a brute-force oracle written out here and, for
//! `similar_to`, against the same call with an options map, which takes the
//! `Value` path. Covered: the metric of the index on the first argument
//! (cosine, L2 as `1 / (1 + d²)`, dot, and cosine when there is none), a NULL
//! vector (NULL score, the pair is dropped), a zero vector (0.0), flushed and
//! unflushed rows, a threshold equal to a pair's exact score, and the same
//! predicate in a Locy rule.

use std::collections::BTreeSet;

use anyhow::{Result, anyhow, bail};
use uni_db::{
    DataType, IndexType, Session, Uni, UniConfig, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
};
use uni_locy::CommandResult;

/// `(id, emb)`. Ids 1 and 2 have a cosine of exactly 0.8; id 3 is the zero
/// vector; id 4 has no vector.
const ROWS: &[(i64, Option<[f32; 2]>)] = &[
    (1, Some([3.0, 4.0])),
    (2, Some([0.0, 1.0])),
    (3, Some([0.0, 0.0])),
    (4, None),
    (5, Some([1.0, 0.0])),
    (6, Some([-1.0, 0.5])),
    (7, Some([0.3, 0.7])),
];

#[derive(Clone, Copy, Debug)]
enum Metric {
    /// No vector index: `similar_to` falls back to cosine.
    Unindexed,
    Cosine,
    L2,
    Dot,
}

impl Metric {
    fn cfg(self) -> Option<VectorMetric> {
        match self {
            Metric::Unindexed => None,
            Metric::Cosine => Some(VectorMetric::Cosine),
            Metric::L2 => Some(VectorMetric::L2),
            Metric::Dot => Some(VectorMetric::Dot),
        }
    }

    /// What `similar_to` returns for two stored vectors: an f32 score, widened.
    fn similar_to(self, a: [f32; 2], b: [f32; 2]) -> f64 {
        let score: f32 = match self {
            Metric::Unindexed | Metric::Cosine => cosine(a, b) as f32,
            Metric::L2 => 1.0 / (1.0 + ((a[0] - b[0]).powi(2) + (a[1] - b[1]).powi(2))),
            Metric::Dot => a[0] * b[0] + a[1] * b[1],
        };
        f64::from(score)
    }
}

/// What `vector_similarity` returns: the cosine in f64, whatever the index.
fn cosine(a: [f32; 2], b: [f32; 2]) -> f64 {
    let (mut dot, mut ma, mut mb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.into_iter().zip(b) {
        let (x, y) = (f64::from(x), f64::from(y));
        dot += x * y;
        ma += x * x;
        mb += y * y;
    }
    let (ma, mb) = (ma.sqrt(), mb.sqrt());
    if ma == 0.0 || mb == 0.0 {
        return 0.0;
    }
    (dot / (ma * mb)).clamp(-1.0, 1.0)
}

async fn open(metric: Metric, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            ..Default::default()
        })
        .build()
        .await?;
    let label = db
        .schema()
        .label("N")
        .property("id", DataType::Int64)
        .property_nullable("emb", DataType::Vector { dimensions: 2 });
    let label = match metric.cfg() {
        Some(metric) => label.index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric,
                embedding: None,
            }),
        ),
        None => label,
    };
    label.done().apply().await?;
    let tx = db.session().tx().await?;
    for (id, emb) in ROWS {
        match emb {
            Some([x, y]) => {
                tx.execute(&format!("CREATE (:N {{id: {id}, emb: [{x:?}, {y:?}]}})"))
                    .await?
            }
            None => tx.execute(&format!("CREATE (:N {{id: {id}}})")).await?,
        };
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// `(a.id, b.id) -> score bits`, NULL scores as `None`.
type Scores = Vec<((i64, i64), Option<u64>)>;

/// Every pair's score under `call`, an expression over `a.emb` and `b.emb`.
async fn scores(session: &Session, call: &str) -> Result<Scores> {
    let query = format!("MATCH (a:N), (b:N) RETURN a.id, b.id, {call} AS s");
    let result = session
        .query(&query)
        .await
        .map_err(|e| anyhow!("{query}: {e}"))?;
    let mut out = Vec::new();
    for row in result.rows() {
        let (Value::Int(a), Value::Int(b)) = (&row.values()[0], &row.values()[1]) else {
            bail!("{query}: unexpected ids {:?}", row.values());
        };
        let score = match &row.values()[2] {
            Value::Null => None,
            Value::Float(f) => Some(f.to_bits()),
            other => bail!("{query}: unexpected score {other:?}"),
        };
        out.push(((*a, *b), score));
    }
    out.sort();
    Ok(out)
}

/// The oracle for [`scores`].
fn expected(score: impl Fn([f32; 2], [f32; 2]) -> f64) -> Scores {
    let mut out = Vec::new();
    for (a, av) in ROWS {
        for (b, bv) in ROWS {
            let bits = av.zip(*bv).map(|(av, bv)| score(av, bv).to_bits());
            out.push(((*a, *b), bits));
        }
    }
    out.sort();
    out
}

/// The pairs a Cypher `WHERE` keeps.
async fn cypher_pairs(session: &Session, predicate: &str) -> Result<BTreeSet<(i64, i64)>> {
    let query = format!("MATCH (a:N), (b:N) WHERE {predicate} RETURN a.id, b.id");
    let result = session
        .query(&query)
        .await
        .map_err(|e| anyhow!("{query}: {e}"))?;
    result
        .rows()
        .iter()
        .map(|row| match (&row.values()[0], &row.values()[1]) {
            (Value::Int(a), Value::Int(b)) => Ok((*a, *b)),
            other => Err(anyhow!("{query}: unexpected ids {other:?}")),
        })
        .collect()
}

/// The pairs a Locy rule with the same `WHERE` derives.
async fn locy_pairs(session: &Session, predicate: &str) -> Result<BTreeSet<(i64, i64)>> {
    let program = format!(
        "CREATE RULE close AS MATCH (a:N), (b:N) WHERE {predicate} YIELD KEY a, KEY b \n\
         QUERY close RETURN a.id AS a, b.id AS b"
    );
    let result = session
        .locy(&program)
        .await
        .map_err(|e| anyhow!("{program}: {e}"))?;
    let [CommandResult::Query(rows)] = result.command_results.as_slice() else {
        bail!("{program}: expected one QUERY result");
    };
    rows.iter()
        .map(|row| match (row.get("a"), row.get("b")) {
            (Some(Value::Int(a)), Some(Value::Int(b))) => Ok((*a, *b)),
            other => Err(anyhow!("{program}: unexpected row {other:?}")),
        })
        .collect()
}

/// The oracle for the two functions above: pairs whose score passes `keep`.
/// A NULL score keeps nothing.
fn kept(
    score: impl Fn([f32; 2], [f32; 2]) -> f64,
    keep: impl Fn(f64) -> bool,
) -> BTreeSet<(i64, i64)> {
    expected(score)
        .into_iter()
        .filter_map(|(pair, bits)| bits.filter(|b| keep(f64::from_bits(*b))).map(|_| pair))
        .collect()
}

async fn check(metric: Metric, flush: bool) -> Result<()> {
    let db = open(metric, flush).await?;
    let session = db.session();
    let ctx = format!("{metric:?}, flush={flush}");

    // Scores, bit for bit.
    let fast = scores(&session, "similar_to(a.emb, b.emb)").await?;
    assert_eq!(
        fast,
        expected(|a, b| metric.similar_to(a, b)),
        "similar_to, {ctx}"
    );
    // An options map sends the same call down the `Value` path.
    let by_value = scores(&session, "similar_to(a.emb, b.emb, {fts_k: 1.0})").await?;
    assert_eq!(fast, by_value, "similar_to against the Value path, {ctx}");
    assert_eq!(
        scores(&session, "vector_similarity(a.emb, b.emb)").await?,
        expected(cosine),
        "vector_similarity, {ctx}"
    );

    // A threshold equal to a pair's exact score. Ids 1 and 2: the cosine is
    // exactly 0.8 in f64, and 0.800000011920929 once narrowed to f32, so
    // `vector_similarity > 0.8` rejects the pair and a cosine `similar_to`
    // keeps it.
    let t = metric.similar_to([3.0, 4.0], [0.0, 1.0]);
    for (op, keep) in [
        (">=", (|s, t| s >= t) as fn(f64, f64) -> bool),
        (">", |s, t| s > t),
    ] {
        for (call, score, t) in [
            (
                "similar_to(a.emb, b.emb)",
                (&|a, b| metric.similar_to(a, b)) as &dyn Fn([f32; 2], [f32; 2]) -> f64,
                t,
            ),
            (
                "similar_to(a.emb, b.emb)",
                &|a, b| metric.similar_to(a, b),
                0.8,
            ),
            ("vector_similarity(a.emb, b.emb)", &cosine, 0.8),
        ] {
            let predicate = format!("{call} {op} {t:?}");
            let want = kept(score, |s| keep(s, t));
            assert_eq!(
                cypher_pairs(&session, &predicate).await?,
                want,
                "Cypher, {predicate}, {ctx}"
            );
            assert_eq!(
                locy_pairs(&session, &predicate).await?,
                want,
                "Locy, {predicate}, {ctx}"
            );
        }
    }
    Ok(())
}

macro_rules! fast_path_tests {
    ($($name:ident => $metric:expr;)*) => {$(
        mod $name {
            use super::*;

            #[tokio::test]
            async fn unflushed() -> Result<()> {
                check($metric, false).await
            }

            #[tokio::test]
            async fn flushed() -> Result<()> {
                check($metric, true).await
            }
        }
    )*};
}

fast_path_tests! {
    unindexed => Metric::Unindexed;
    cosine => Metric::Cosine;
    l2 => Metric::L2;
    dot => Metric::Dot;
}

/// The exact-score pair, spelled out: the two functions disagree on it, and
/// the fast paths must not make them agree.
#[tokio::test]
async fn the_two_functions_still_differ_on_an_exact_score() -> Result<()> {
    let db = open(Metric::Cosine, true).await?;
    let session = db.session();
    let pair = (1, 2);
    assert!(
        cypher_pairs(&session, "similar_to(a.emb, b.emb) > 0.8")
            .await?
            .contains(&pair)
    );
    assert!(
        !cypher_pairs(&session, "vector_similarity(a.emb, b.emb) > 0.8")
            .await?
            .contains(&pair)
    );
    assert!(
        cypher_pairs(&session, "vector_similarity(a.emb, b.emb) >= 0.8")
            .await?
            .contains(&pair)
    );
    Ok(())
}
