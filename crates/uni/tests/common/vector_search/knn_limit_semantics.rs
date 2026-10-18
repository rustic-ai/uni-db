// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! How many neighbours `n.prop ~= q` asks the vector index for.
//!
//! `~=` is a top-K index scan, so `K` is part of its meaning. The contract
//! pinned here:
//!
//! - **`K` comes from the query's `LIMIT` (plus `SKIP`)** when the scan feeds
//!   that `LIMIT` directly: through projections, and through an `ORDER BY`
//!   whose leading key is the scan's own score, descending. `LIMIT 150`
//!   returns 150 rows, not the 100 a fixed cap used to allow.
//! - **Otherwise `K` is `UniConfig::vector_knn_default_k`** (100 by default),
//!   and when the scan returns exactly that many rows the result carries a
//!   warning, because more rows may have qualified. A residual filter
//!   (`~= q AND n.x > 3`) is the main "otherwise": a `LIMIT`-sized `K` would
//!   be filtered down to fewer rows than the `LIMIT` asked for.
//! - **The scan is never moved below a `LIMIT`** that its `WHERE` sits above.
//! - **Under `OPTIONAL MATCH`** the scan is still used, and a clause where
//!   nothing qualifies yields one NULL row.
//!
//! The oracle is a brute-force cosine ranking computed here in Rust. With a
//! Flat index the engine is exact, so answers are compared row for row; HNSW
//! and IVF are approximate, so only recall-independent properties (row
//! counts, score order, warnings) are asserted for them.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(knn_limit_semantics)'

// Rust guideline compliant

use anyhow::{Result, anyhow};
use uni_db::{
    DataType, IndexType, Session, Uni, UniConfig, Value, VectorAlgo, VectorIndexCfg, VectorMetric,
};

/// Rows on label `N`.
const ROWS: i64 = 300;

/// Angular grid step, in radians.
const STEP: f64 = 0.01;

/// The query angles used throughout: `0` (an exact grid point) and a point
/// between grid points whose distances to its neighbours are all distinct, so
/// the ranking has no ties.
const QUERY_ANGLES: [f64; 2] = [0.0, 1.5033];

/// Built-in default of `UniConfig::vector_knn_default_k`.
const DEFAULT_K: usize = 100;

/// Index flavour of a fixture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Algo {
    /// Exact: answers are compared row for row.
    Flat,
    /// Approximate: recall-independent properties only.
    IvfFlat,
    /// Approximate: recall-independent properties only.
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

    fn exact(self) -> bool {
        self == Self::Flat
    }
}

/// The angle of row `id`: a permutation of the grid, so neither id order nor
/// insertion order coincides with the ranking.
fn angle_of(id: i64) -> f64 {
    ((id * 137) % ROWS) as f64 * STEP
}

/// `x` of row `id`, the residual-filter column.
fn x_of(id: i64) -> i64 {
    id % 7
}

fn unit(theta: f64) -> [f64; 2] {
    [theta.cos(), theta.sin()]
}

fn cosine(a: [f64; 2], b: [f64; 2]) -> f64 {
    let dot = a[0] * b[0] + a[1] * b[1];
    let m = (a[0] * a[0] + a[1] * a[1]).sqrt() * (b[0] * b[0] + b[1] * b[1]).sqrt();
    dot / m
}

/// `(id, emb)` for every row of a fixture.
type Fixture = Vec<(i64, [f64; 2])>;

fn base_fixture() -> Fixture {
    (0..ROWS).map(|id| (id, unit(angle_of(id)))).collect()
}

/// Every id of `rows`, ranked by cosine similarity to `q`, best first.
fn ranked(rows: &Fixture, q: [f64; 2]) -> Vec<i64> {
    let mut scored: Vec<(f64, i64)> = rows.iter().map(|(id, e)| (cosine(*e, q), *id)).collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.into_iter().map(|(_, id)| id).collect()
}

fn vec_text(v: [f64; 2]) -> String {
    format!("[{:?}, {:?}]", v[0], v[1])
}

async fn open(algo: Algo, default_k: Option<usize>) -> Result<Uni> {
    let mut config = UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: usize::MAX,
        disable_fork_sweeper: true,
        disable_fork_index_builder: true,
        ..Default::default()
    };
    if let Some(k) = default_k {
        config.vector_knn_default_k = k;
    }
    let db = Uni::in_memory().config(config).build().await?;
    let index = |algo: Algo| {
        IndexType::Vector(VectorIndexCfg {
            algorithm: algo.cfg(),
            metric: VectorMetric::Cosine,
            embedding: None,
        })
    };
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property("x", DataType::Int64)
        .property("emb", DataType::Vector { dimensions: 2 })
        .index("emb", index(algo))
        .done()
        // Declared, indexed, never written: the OPTIONAL "nothing qualifies"
        // case.
        .label("E")
        .property("id", DataType::Int64)
        .property("emb", DataType::Vector { dimensions: 2 })
        .index("emb", index(algo))
        .done()
        .edge_type("R", &["N"], &["N"])
        .done()
        .apply()
        .await?;
    Ok(db)
}

async fn insert(session: &Session, rows: &Fixture) -> Result<()> {
    let tx = session.tx().await?;
    for (id, emb) in rows {
        tx.execute(&format!(
            "CREATE (:N {{id: {id}, x: {}, emb: {}}})",
            x_of(*id),
            vec_text(*emb)
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// One query's answer: the first column in returned order, and the warnings.
struct Answer {
    ids: Vec<Option<i64>>,
    warnings: Vec<String>,
}

impl Answer {
    fn knn_warned(&self) -> bool {
        self.warnings
            .iter()
            .any(|w| w.contains("vector_knn_default_k"))
    }
}

async fn run(session: &Session, query: &str) -> Result<Answer> {
    let result = session
        .query(query)
        .await
        .map_err(|e| anyhow!("{query}: {e}"))?;
    let ids = result
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Int(i) => Ok(Some(*i)),
            Value::Null => Ok(None),
            other => Err(anyhow!("{query}: unexpected value {other:?}")),
        })
        .collect::<Result<Vec<_>>>()?;
    let warnings = result.warnings().iter().map(ToString::to_string).collect();
    Ok(Answer { ids, warnings })
}

fn some(ids: &[i64]) -> Vec<Option<i64>> {
    ids.iter().copied().map(Some).collect()
}

fn sorted(mut v: Vec<Option<i64>>) -> Vec<Option<i64>> {
    v.sort();
    v
}

/// What a query must return.
enum Want {
    /// Exactly these rows, in this order.
    Ordered(Vec<Option<i64>>),
    /// Exactly these rows, in any order.
    Bag(Vec<Option<i64>>),
    /// Recall-independent: this many rows.
    Count(usize),
}

/// Runs `query` and checks rows and the truncation warning.
async fn expect(
    failures: &mut Vec<String>,
    session: &Session,
    label: &str,
    query: &str,
    want: Want,
    warned: bool,
) {
    let answer = match run(session, query).await {
        Ok(a) => a,
        Err(e) => {
            failures.push(format!("[{label}] {query}\n  error {e}"));
            return;
        }
    };
    let rows_ok = match &want {
        Want::Ordered(w) => &answer.ids == w,
        Want::Bag(w) => sorted(answer.ids.clone()) == sorted(w.clone()),
        Want::Count(n) => answer.ids.len() == *n,
    };
    if !rows_ok {
        let shown = match &want {
            Want::Ordered(w) | Want::Bag(w) => format!("{} rows {:?}", w.len(), w),
            Want::Count(n) => format!("{n} rows"),
        };
        failures.push(format!(
            "[{label}] {query}\n  got {} rows {:?}\n  want {shown}",
            answer.ids.len(),
            answer.ids
        ));
    }
    if answer.knn_warned() != warned {
        failures.push(format!(
            "[{label}] {query}\n  truncation warning: got {}, want {warned}; warnings {:?}",
            answer.knn_warned(),
            answer.warnings
        ));
    }
}

/// The `LIMIT`-derived, default-capped and residual-filter cases on one store.
async fn check_limit_cases(
    failures: &mut Vec<String>,
    session: &Session,
    rows: &Fixture,
    algo: Algo,
    label: &str,
) {
    let exact = algo.exact();
    for theta in QUERY_ANGLES {
        let q = unit(theta);
        let qt = vec_text(q);
        let rank = ranked(rows, q);
        let top = |n: usize| some(&rank[..n.min(rank.len())]);
        let pick = |w: Vec<Option<i64>>, n: usize| {
            if exact {
                Want::Ordered(w)
            } else {
                Want::Count(n)
            }
        };
        let bag = |w: Vec<Option<i64>>, n: usize| if exact { Want::Bag(w) } else { Want::Count(n) };

        // K = LIMIT.
        for limit in [5, 150] {
            let query = format!("MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id LIMIT {limit}");
            expect(
                failures,
                session,
                label,
                &query,
                pick(top(limit), limit),
                false,
            )
            .await;
        }
        // K = SKIP + LIMIT, then skip.
        for (skip, limit) in [(3, 5), (100, 20)] {
            let query =
                format!("MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id SKIP {skip} LIMIT {limit}");
            let want = some(&rank[skip..skip + limit]);
            expect(failures, session, label, &query, pick(want, limit), false).await;
        }
        // ORDER BY the scan's score, descending: still fed directly.
        for limit in [5, 150] {
            for query in [
                format!(
                    "MATCH (n:N) WHERE n.emb ~= {qt} \
                     RETURN n.id AS id, n._score AS score ORDER BY score DESC LIMIT {limit}"
                ),
                format!(
                    "MATCH (n:N) WHERE n.emb ~= {qt} \
                     RETURN n.id ORDER BY n._score DESC, n.id LIMIT {limit}"
                ),
            ] {
                expect(
                    failures,
                    session,
                    label,
                    &query,
                    pick(top(limit), limit),
                    false,
                )
                .await;
            }
        }
        // ORDER BY anything else: the LIMIT no longer selects by score, so K is
        // the default and the 5 smallest ids come from the default's top-K.
        {
            let query =
                format!("MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id ORDER BY n.id LIMIT 5");
            let mut pool = top(DEFAULT_K);
            pool.sort();
            pool.truncate(5);
            expect(failures, session, label, &query, pick(pool, 5), true).await;
            let query = format!(
                "MATCH (n:N) WHERE n.emb ~= {qt} \
                 RETURN n.id AS id, n._score AS s ORDER BY s ASC LIMIT 5"
            );
            let mut pool = top(DEFAULT_K);
            pool.reverse();
            pool.truncate(5);
            expect(failures, session, label, &query, pick(pool, 5), true).await;
        }
        // Residual filter: K must not come from LIMIT (5 nearest, then
        // filtered, would return fewer than 5). The default applies.
        {
            let query = format!("MATCH (n:N) WHERE n.emb ~= {qt} AND n.x > 3 RETURN n.id LIMIT 5");
            let want: Vec<Option<i64>> = rank[..DEFAULT_K]
                .iter()
                .filter(|id| x_of(**id) > 3)
                .take(5)
                .copied()
                .map(Some)
                .collect();
            expect(failures, session, label, &query, pick(want, 5), true).await;
            let query = format!(
                "MATCH (n:N) WHERE n.x > 3 AND n.emb ~= {qt} \
                 RETURN n.id ORDER BY n._score DESC LIMIT 5"
            );
            let want: Vec<Option<i64>> = rank[..DEFAULT_K]
                .iter()
                .filter(|id| x_of(**id) > 3)
                .take(5)
                .copied()
                .map(Some)
                .collect();
            expect(failures, session, label, &query, pick(want, 5), true).await;
            // An inline property map is a residual filter too.
            let query = format!("MATCH (n:N {{x: 2}}) WHERE n.emb ~= {qt} RETURN n.id LIMIT 5");
            let want: Vec<Option<i64>> = rank[..DEFAULT_K]
                .iter()
                .filter(|id| x_of(**id) == 2)
                .take(5)
                .copied()
                .map(Some)
                .collect();
            expect(failures, session, label, &query, pick(want, 5), true).await;
        }
        // No LIMIT: the default, and a warning because it was reached.
        {
            let query = format!("MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id");
            expect(
                failures,
                session,
                label,
                &query,
                bag(top(DEFAULT_K), DEFAULT_K),
                true,
            )
            .await;
        }
        // WITH ... LIMIT is a LIMIT the scan feeds directly.
        {
            let query = format!("MATCH (n:N) WHERE n.emb ~= {qt} WITH n LIMIT 150 RETURN n.id");
            expect(failures, session, label, &query, bag(top(150), 150), false).await;
            let query = format!(
                "MATCH (n:N) WHERE n.emb ~= {qt} WITH n, n._score AS s \
                 ORDER BY s DESC LIMIT 120 RETURN n.id"
            );
            expect(failures, session, label, &query, pick(top(120), 120), false).await;
            // A LIMIT above a traversal is not fed directly: the default
            // applies to the scan, and the warning reports it.
            let query = format!(
                "MATCH (n:N) WHERE n.emb ~= {qt} MATCH (n)-[:R]->(m:N) RETURN m.id LIMIT 1000"
            );
            expect(failures, session, label, &query, Want::Count(0), true).await;
        }
        // Inside a subquery: the warning reaches the outer result.
        {
            let query =
                format!("CALL {{ MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id AS id }} RETURN id");
            expect(
                failures,
                session,
                label,
                &query,
                bag(top(DEFAULT_K), DEFAULT_K),
                true,
            )
            .await;
            let query = format!(
                "MATCH (a:N) WHERE a.id < 3 CALL {{ WITH a MATCH (n:N) WHERE n.emb ~= {qt} \
                 RETURN count(n) AS c }} RETURN c"
            );
            let want = vec![Some(DEFAULT_K as i64); 3];
            expect(failures, session, label, &query, Want::Bag(want), true).await;
        }
        // UNION: each branch's own LIMIT.
        {
            let query = format!(
                "MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id AS id LIMIT 150 \
                 UNION ALL \
                 MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id AS id LIMIT 3"
            );
            let mut want = top(150);
            want.extend(top(3));
            expect(failures, session, label, &query, bag(want, 153), false).await;
        }
        // OPTIONAL MATCH over a populated label is a KNN with K = LIMIT.
        {
            let query = format!("OPTIONAL MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id LIMIT 150");
            expect(failures, session, label, &query, pick(top(150), 150), false).await;
            // A residual every row fails: one NULL row.
            let query =
                format!("OPTIONAL MATCH (n:N) WHERE n.emb ~= {qt} AND n.x > 1000 RETURN n.id");
            expect(
                failures,
                session,
                label,
                &query,
                Want::Bag(vec![None]),
                true,
            )
            .await;
            // An inline property map null-extends too, whether or not a row
            // passes it.
            let query = format!("OPTIONAL MATCH (n:N {{x: 1000}}) WHERE n.emb ~= {qt} RETURN n.id");
            expect(
                failures,
                session,
                label,
                &query,
                Want::Bag(vec![None]),
                true,
            )
            .await;
            let query = format!("OPTIONAL MATCH (n:N {{x: 2}}) WHERE n.emb ~= {qt} RETURN n.id");
            let want: Vec<Option<i64>> = rank[..DEFAULT_K]
                .iter()
                .filter(|id| x_of(**id) == 2)
                .copied()
                .map(Some)
                .collect();
            expect(
                failures,
                session,
                label,
                &query,
                bag(want.clone(), want.len()),
                true,
            )
            .await;
            // A residual on an earlier variable stays in the clause's filter.
            let query = format!(
                "MATCH (a:N {{id: 0}}) OPTIONAL MATCH (n:N) \
                 WHERE n.emb ~= {qt} AND n.x = a.x + 1 RETURN n.id"
            );
            let mut want: Vec<Option<i64>> = rank[..DEFAULT_K]
                .iter()
                .filter(|id| x_of(**id) == x_of(0) + 1)
                .copied()
                .map(Some)
                .collect();
            if want.is_empty() {
                want.push(None);
            }
            expect(
                failures,
                session,
                label,
                &query,
                bag(want.clone(), want.len()),
                true,
            )
            .await;
        }
        // OPTIONAL MATCH where nothing qualifies: one NULL row.
        {
            let query = format!("OPTIONAL MATCH (e:E) WHERE e.emb ~= {qt} RETURN e.id");
            expect(
                failures,
                session,
                label,
                &query,
                Want::Bag(vec![None]),
                false,
            )
            .await;
            let query = format!(
                "MATCH (a:N {{id: 7}}) OPTIONAL MATCH (e:E) WHERE e.emb ~= {qt} \
                 RETURN e.id"
            );
            expect(
                failures,
                session,
                label,
                &query,
                Want::Bag(vec![None]),
                false,
            )
            .await;
            let query = format!(
                "UNWIND [1, 2, 3] AS i OPTIONAL MATCH (e:E) WHERE e.emb ~= {qt} \
                 RETURN e.id"
            );
            expect(
                failures,
                session,
                label,
                &query,
                Want::Bag(vec![None, None, None]),
                false,
            )
            .await;
        }
    }
}

async fn check(algo: Algo, flush: bool) -> Result<()> {
    let db = open(algo, None).await?;
    let session = db.session();
    let rows = base_fixture();
    insert(&session, &rows).await?;
    if flush {
        db.flush().await?;
    }
    let mut failures = Vec::new();
    check_limit_cases(&mut failures, &session, &rows, algo, "primary").await;
    assert!(
        failures.is_empty(),
        "{algo:?}, flushed {flush}: {} failing:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

macro_rules! knn_limit_tests {
    ($($name:ident => $algo:expr;)*) => {
        $(
            mod $name {
                use super::*;

                #[tokio::test]
                async fn unflushed() -> Result<()> {
                    check($algo, false).await
                }

                #[tokio::test]
                async fn flushed() -> Result<()> {
                    check($algo, true).await
                }
            }
        )*
    };
}

knn_limit_tests! {
    flat => Algo::Flat;
    ivf_flat => Algo::IvfFlat;
    hnsw => Algo::Hnsw;
}

/// `UniConfig::vector_knn_default_k` sets the cap when no `LIMIT` does, and a
/// label smaller than the cap draws no warning.
#[tokio::test]
async fn default_k_comes_from_config() -> Result<()> {
    for flush in [false, true] {
        let db = open(Algo::Flat, Some(20)).await?;
        let session = db.session();
        let rows = base_fixture();
        insert(&session, &rows).await?;
        if flush {
            db.flush().await?;
        }
        let mut failures = Vec::new();
        for theta in QUERY_ANGLES {
            let q = unit(theta);
            let qt = vec_text(q);
            let rank = ranked(&rows, q);
            let query = format!("MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id");
            expect(
                &mut failures,
                &session,
                "k=20",
                &query,
                Want::Bag(some(&rank[..20])),
                true,
            )
            .await;
            // A LIMIT above the configured default still wins.
            let query = format!("MATCH (n:N) WHERE n.emb ~= {qt} RETURN n.id LIMIT 50");
            expect(
                &mut failures,
                &session,
                "k=20",
                &query,
                Want::Ordered(some(&rank[..50])),
                false,
            )
            .await;
        }
        assert!(
            failures.is_empty(),
            "flushed {flush}: {} failing:\n{}",
            failures.len(),
            failures.join("\n")
        );

        // A cap larger than the label: every row, no warning.
        let db = open(Algo::Flat, Some(1000)).await?;
        let session = db.session();
        insert(&session, &rows).await?;
        if flush {
            db.flush().await?;
        }
        let q = unit(0.0);
        let query = format!("MATCH (n:N) WHERE n.emb ~= {} RETURN n.id", vec_text(q));
        let mut failures = Vec::new();
        expect(
            &mut failures,
            &session,
            "k=1000",
            &query,
            Want::Bag(some(&ranked(&rows, q))),
            false,
        )
        .await;
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
    Ok(())
}

/// A `WHERE` above a `LIMIT` never has its scan moved below that `LIMIT`.
///
/// `MATCH (n:N) WITH n LIMIT 5 WHERE n.emb ~= q` asks for neighbours among 5
/// arbitrary rows. Rewriting the label scan under the `LIMIT` into the top-K
/// scan answered "the 5 nearest neighbours of the whole label" instead. There
/// is no top-K meaning over an arbitrary row set, so the query is refused with
/// an error that names the alternative.
#[tokio::test]
async fn knn_is_never_moved_below_a_limit() -> Result<()> {
    let db = open(Algo::Flat, None).await?;
    let session = db.session();
    insert(&session, &base_fixture()).await?;
    let q = vec_text(unit(0.0));
    for query in [
        format!("MATCH (n:N) WITH n LIMIT 5 WHERE n.emb ~= {q} RETURN n.id"),
        format!("MATCH (n:N) WITH n SKIP 2 LIMIT 5 WHERE n.emb ~= {q} RETURN n.id"),
        format!("MATCH (n:N) WITH n LIMIT 5 MATCH (n) WHERE n.emb ~= {q} RETURN n.id"),
        format!("MATCH (n:N) WITH n SKIP 2 LIMIT 5 MATCH (n) WHERE n.emb ~= {q} RETURN n.id"),
    ] {
        match session.query(&query).await {
            Ok(r) => panic!(
                "{query}: expected a planning error, got {} rows {:?}",
                r.rows().len(),
                r.rows()
                    .iter()
                    .map(|row| row.values()[0].clone())
                    .collect::<Vec<_>>()
            ),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("~=") && msg.contains("vector_similarity"),
                    "{query}: error should name `~=` and the per-row alternative, got: {msg}"
                );
            }
        }
    }
    Ok(())
}

/// A fork's `~=` ranks fork-local and inherited rows together, with the same
/// `K` rules.
#[tokio::test]
async fn fork_ranks_local_and_inherited_rows() -> Result<()> {
    for flush_fork in [false, true] {
        let db = open(Algo::Flat, None).await?;
        let primary = db.session();
        let base = base_fixture();
        insert(&primary, &base).await?;
        db.flush().await?;

        let fork = primary.fork(format!("knn_{flush_fork}")).await?;
        // Fork rows sit between the primary's grid points, so they interleave
        // with inherited rows in every ranking.
        let local: Fixture = (0..50)
            .map(|j| (1000 + j, unit(0.0047 + STEP * j as f64)))
            .collect();
        insert(&fork, &local).await?;
        if flush_fork {
            fork.flush().await?;
        }
        let mut all = base.clone();
        all.extend(local.iter().copied());

        let mut failures = Vec::new();
        check_limit_cases(&mut failures, &fork, &all, Algo::Flat, "fork").await;
        // Primary does not see the fork's rows.
        let q = unit(0.0);
        let query = format!(
            "MATCH (n:N) WHERE n.emb ~= {} RETURN n.id LIMIT 60",
            vec_text(q)
        );
        expect(
            &mut failures,
            &primary,
            "primary-after-fork",
            &query,
            Want::Ordered(some(&ranked(&base, q)[..60])),
            false,
        )
        .await;
        assert!(
            failures.is_empty(),
            "fork flushed {flush_fork}: {} failing:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
    Ok(())
}
