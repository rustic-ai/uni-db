// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #335: a Locy rule that pairs two unconnected labels by vector similarity reads
//! every pair: `|A| × |B|` rows, whatever the vector index could have told it.
//!
//! First reported as #330, which was closed once each pair and everything
//! around the pairing was made cheap (`docs/proposals/locy_similarity_join_330.md`,
//! `docs/proposals/locy_resolution_cost_2026_11_14.md`). The number of pairs
//! read is unchanged; #335 tracks reaching the vector index from a rule.
//!
//! ```locy
//! CREATE RULE close AS
//! MATCH (m:Mention), (e:Entity)
//! WHERE similar_to(m.embedding, e.embedding) >= 0.99
//! YIELD KEY m, KEY e
//! ```
//!
//! `MATCH (m:Mention), (e:Entity)` is a cartesian product, and a similarity
//! threshold is not an equality, so the `WHERE` can only be applied pair by
//! pair. Both labels carry a vector index that answers "the k nearest
//! entities to this mention" without visiting the rest, but a rule body has
//! no way to reach it: no Locy generator does a nearest-neighbour lookup.
//!
//! This is the similarity clause of the NLP entity-resolution program
//! (`website/docs/features/nlp-extraction.md`), stripped of everything else:
//! no models, no auto-embedding, explicit 8-dimensional vectors, and one rule.
//! Each mention's vector equals exactly one entity's, so the answer is
//! `N` facts, found by reading `N²` pairs.
//!
//! Run with:
//!   cargo test -p uni-db --test integration locy_similarity_cross_join_scale -- --ignored --nocapture

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use uni_db::{DataType, IndexType, Uni, VectorAlgo, VectorIndexCfg, VectorMetric};
use uni_locy::CommandResult;

const DIM: usize = 8;

/// A deterministic pseudo-random vector for `seed`; distinct seeds give
/// vectors whose cosine similarity is far below 0.99.
fn vector(seed: usize, dim: usize) -> Vec<f32> {
    let mut state = (seed as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (0..dim)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((state >> 33) as f32 / (1u64 << 31) as f32) * 2.0 - 1.0
        })
        .collect()
}

fn literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:.6}")).collect();
    format!("[{}]", parts.join(", "))
}

/// `n` mentions and `n` entities, both with an indexed `embedding` of `dim`
/// dimensions; mention `i` has entity `i`'s vector.
async fn db_with(n: usize, dim: usize) -> Result<Uni> {
    db_of(n, dim, false, false).await
}

/// [`db_with`], and when `typed` also a `type` on both labels, one of three
/// values: what the resolution program's clause reads. `refers_to` declares
/// the `REFERS_TO` edge type the clause's pattern predicate names; there are
/// no such edges either way.
async fn db_of(n: usize, dim: usize, typed: bool, refers_to: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let index = || {
        IndexType::Vector(VectorIndexCfg {
            algorithm: VectorAlgo::Flat,
            metric: VectorMetric::Cosine,
            embedding: None,
        })
    };
    db.schema()
        .label("Mention")
        .property("k", DataType::Int)
        .property("embedding", DataType::Vector { dimensions: dim })
        .index("embedding", index())
        .done()
        .label("Entity")
        .property("k", DataType::Int)
        .property("embedding", DataType::Vector { dimensions: dim })
        .index("embedding", index())
        .done()
        .apply()
        .await?;
    if typed {
        db.schema()
            .label("Mention")
            .property("type", DataType::String)
            .done()
            .label("Entity")
            .property("type", DataType::String)
            .done()
            .apply()
            .await?;
    }
    if refers_to {
        db.schema()
            .edge_type("REFERS_TO", &["Mention"], &["Entity"])
            .done()
            .apply()
            .await?;
    }
    let extra = |i: usize| match typed {
        true => format!(", type: 't{}'", i % 3),
        false => String::new(),
    };
    let rows: Vec<String> = (0..n)
        .map(|i| format!("{{k: {i}, v: {}{}}}", literal(&vector(i, dim)), extra(i)))
        .collect();
    let tx = db.session().tx().await?;
    // In chunks, so a wide vector does not make one statement of several megabytes.
    for chunk in rows.chunks(200) {
        tx.execute(&format!(
            "UNWIND [{}] AS r CREATE (:Mention {{k: r.k, embedding: r.v{t}}}) \
             CREATE (:Entity {{k: r.k, embedding: r.v{t}}})",
            chunk.join(", "),
            t = if typed { ", type: r.type" } else { "" }
        ))
        .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Runs `predicate` as the pairing rule under the default Locy limits and
/// returns how many pairs it derived and how long it took.
async fn pair(db: &Uni, predicate: &str) -> Result<(usize, Duration)> {
    let program = format!(
        "CREATE RULE close AS \
         MATCH (m:Mention), (e:Entity) WHERE {predicate} YIELD KEY m, KEY e \n\
         QUERY close RETURN m.k AS mk, e.k AS ek"
    );
    let started = Instant::now();
    let result = db.session().locy(&program).await?;
    let elapsed = started.elapsed();
    match result.command_results.as_slice() {
        [CommandResult::Query(rows)] => Ok((rows.len(), elapsed)),
        other => bail!("expected one QUERY result, got {other:?}"),
    }
}

const SIMILAR: &str = "similar_to(m.embedding, e.embedding) >= 0.99";

/// What the rule's pairing operators did in a profiled run of [`pair`]'s
/// program, summed over its iterations.
struct Pairing {
    /// Rows the `CrossJoinExec` operators produced.
    cross_join_rows: usize,
    /// Time in the `CrossJoinExec` operators, in milliseconds.
    cross_join_ms: f64,
    /// Time in the `FilterExec` operators, in milliseconds.
    filter_ms: f64,
}

async fn pairing(db: &Uni, predicate: &str) -> Result<Pairing> {
    let program = format!(
        "CREATE RULE close AS \
         MATCH (m:Mention), (e:Entity) WHERE {predicate} YIELD KEY m, KEY e \n\
         QUERY close RETURN m.k AS mk"
    );
    let (_, profile) = db.session().locy_with(&program).profile().await?;
    let mut out = Pairing {
        cross_join_rows: 0,
        cross_join_ms: 0.0,
        filter_ms: 0.0,
    };
    let operators = profile
        .profile
        .strata
        .iter()
        .flat_map(|s| &s.rules)
        .filter(|r| r.name == "close")
        .flat_map(|r| &r.iterations)
        .flat_map(|i| &i.operators);
    for o in operators {
        if o.operator.starts_with("CrossJoinExec") {
            out.cross_join_rows += o.actual_rows;
            out.cross_join_ms += o.time_ms;
        } else if o.operator.starts_with("FilterExec") {
            out.filter_ms += o.time_ms;
        }
    }
    Ok(out)
}

/// The repro. Fails today: the rule materialises all `N²` = 90,000 pairs to
/// find `N` = 300 matches. With a nearest-neighbour lookup through the
/// `embedding` index it would read about `N × k` candidates. Asserting on the
/// operator's row count rather than on time keeps it independent of the
/// build profile and machine.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "repro for #335: a similarity rule pairs every mention with every entity; fails \
            until Locy can reach the vector index from a rule"]
async fn similarity_rule_does_not_pair_every_mention_with_every_entity() -> Result<()> {
    const N: usize = 300;
    let db = db_with(N, DIM).await?;
    let (pairs, _) = pair(&db, SIMILAR).await?;
    assert_eq!(pairs, N, "each mention is identical to exactly one entity");
    let read = pairing(&db, SIMILAR).await?.cross_join_rows;
    assert!(
        read < N * N,
        "the rule read {read} mention-entity pairs to find {N} matches: every pair of \
         {N} mentions and {N} entities"
    );
    Ok(())
}

/// Not a pass/fail check: prints how the pairing grows with `N`, for the
/// similarity predicate and, for comparison, an equality on an integer key,
/// with the time the similarity rule spent in its cross join and its filter.
/// The second width is a realistic embedding's: the cross join copies every
/// vector for every pair, so its share grows with the width.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing probe: prints the growth of a cartesian Locy pairing; asserts only counts"]
async fn similarity_pairing_time_grows_with_the_product_of_both_sides() -> Result<()> {
    for (dim, sizes) in [(DIM, &[100, 200, 400, 1000][..]), (384, &[400, 1000][..])] {
        for &n in sizes {
            let db = db_with(n, dim).await?;
            // A debug build can exceed the default Locy timeout at the widest,
            // largest size; that is a measurement, not a failure of the probe.
            let (similar, t_similar) = match pair(&db, SIMILAR).await {
                Ok(done) => done,
                Err(e) => {
                    println!("dim={dim:3} n={n:5}  similar_to: {e}");
                    continue;
                }
            };
            let (equal, t_equal) = pair(&db, "m.k = e.k").await?;
            let p = pairing(&db, SIMILAR).await?;
            let rows_equal = pairing(&db, "m.k = e.k").await?.cross_join_rows;
            println!(
                "dim={dim:3} n={n:5}  similar_to: {t_similar:>10.2?} ({} pairs; cross join \
                 {:.1} ms, filter {:.1} ms)  equality: {t_equal:>10.2?} ({rows_equal} pairs)",
                p.cross_join_rows, p.cross_join_ms, p.filter_ms
            );
            assert_eq!((similar, equal), (n, n));
        }
    }
    Ok(())
}

/// Not a pass/fail check: the similarity clause of the resolution program at
/// 300 × 300 with three types. The equality makes it a hash join, with the
/// similarity and the pattern predicate as its residual filter and a second
/// scoring in the `YIELD`. Prints each operator's rows and time, for the
/// clause and for the clause less each residual predicate, with the
/// `REFERS_TO` edge type undeclared and declared: the pattern predicate costs
/// far more per row once the type exists. It reads `m` alone, so it is a
/// filter on the mention side, beneath the join.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing probe: prints the operators of the resolution program's similarity clause; \
            asserts only counts"]
async fn resolution_similarity_clause_operator_times() -> Result<()> {
    const N: usize = 300;
    for refers_to in [false, true] {
        let db = db_of(N, DIM, true, refers_to).await?;
        for (what, predicate) in [
            (
                "the clause",
                "NOT (m)-[:REFERS_TO]->() AND m.type = e.type \
                 AND similar_to(m.embedding, e.embedding) >= 0.5",
            ),
            (
                "without the pattern predicate",
                "m.type = e.type AND similar_to(m.embedding, e.embedding) >= 0.5",
            ),
            (
                "without the similarity",
                "NOT (m)-[:REFERS_TO]->() AND m.type = e.type",
            ),
        ] {
            let program = format!(
                "CREATE RULE match_evidence AS MATCH (m:Mention), (e:Entity) WHERE {predicate} \
                 YIELD KEY m, KEY e, similar_to(m.embedding, e.embedding) * 0.8 AS p \n\
                 QUERY match_evidence RETURN m.k AS mk, e.k AS ek"
            );
            let (result, profile) = db.session().locy_with(&program).profile().await?;
            let [CommandResult::Query(rows)] = result.command_results.as_slice() else {
                bail!("expected one QUERY result");
            };
            // At least each mention's identical entity, which shares its type.
            assert!(rows.len() >= N, "{} pairs", rows.len());
            println!("REFERS_TO declared: {refers_to}, {what}:");
            let operators = profile
                .profile
                .strata
                .iter()
                .flat_map(|s| &s.rules)
                .filter(|r| r.name == "match_evidence")
                .flat_map(|r| &r.iterations)
                .flat_map(|i| &i.operators);
            for o in operators {
                println!(
                    "{:>9} rows {:>9.1} ms  {}",
                    o.actual_rows, o.time_ms, o.operator
                );
            }
        }
    }
    Ok(())
}
