// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A pattern predicate that reads one side of a join is decided on that side.
//!
//! ```cypher
//! MATCH (a:A), (b:B)
//! WHERE NOT (a)-[:R]->() AND a.t = b.t
//! ```
//!
//! The equality makes this a hash join. `NOT (a)-[:R]->()` depends on `a`
//! alone, but the planner's side classification did not look inside a pattern
//! predicate, took it for a constant and left it in the filter above the join:
//! one adjacency lookup per joined pair, not per `a`. In the entity-resolution
//! program that was most of a pass once `REFERS_TO` edges existed (#330,
//! `docs/proposals/locy_similarity_join_330.md` §10.5).
//!
//! These tests pin the answers, for the predicates that move and for those
//! that must not, and that the join no longer receives the rows the predicate
//! rejects.

use std::collections::BTreeSet;

use anyhow::{Result, anyhow, bail};
use uni_db::{DataType, Session, Uni, UniConfig, Value};
use uni_locy::CommandResult;

const A: i64 = 6;
const B: i64 = 4;
/// `(a.k, b.k)` of each `R` edge.
const EDGES: &[(i64, i64)] = &[(0, 0), (3, 1)];

async fn open(flush: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("A")
        .property("k", DataType::Int64)
        .property("t", DataType::Int64)
        .done()
        .label("B")
        .property("k", DataType::Int64)
        .property("t", DataType::Int64)
        .done()
        .edge_type("R", &["A"], &["B"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS k CREATE (:A {{k: k, t: k % 2}})",
        A - 1
    ))
    .await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS k CREATE (:B {{k: k, t: k % 2}})",
        B - 1
    ))
    .await?;
    for (a, b) in EDGES {
        tx.execute(&format!(
            "MATCH (a:A {{k: {a}}}), (b:B {{k: {b}}}) CREATE (a)-[:R]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

type Pairs = BTreeSet<(i64, i64)>;

/// The type-matched pairs `keep` accepts.
fn expected(keep: impl Fn(i64, i64) -> bool) -> Pairs {
    (0..A)
        .flat_map(|a| (0..B).map(move |b| (a, b)))
        .filter(|(a, b)| a % 2 == b % 2 && keep(*a, *b))
        .collect()
}

async fn cypher(session: &Session, predicate: &str) -> Result<Pairs> {
    let query = format!("MATCH (a:A), (b:B) WHERE {predicate} RETURN a.k, b.k");
    let result = session
        .query(&query)
        .await
        .map_err(|e| anyhow!("{query}: {e}"))?;
    result
        .rows()
        .iter()
        .map(|row| match (&row.values()[0], &row.values()[1]) {
            (Value::Int(a), Value::Int(b)) => Ok((*a, *b)),
            other => Err(anyhow!("{query}: unexpected row {other:?}")),
        })
        .collect()
}

fn rule(predicate: &str) -> String {
    format!(
        "CREATE RULE pairs AS MATCH (a:A), (b:B) WHERE {predicate} YIELD KEY a, KEY b \n\
         QUERY pairs RETURN a.k AS a, b.k AS b"
    )
}

async fn locy(session: &Session, predicate: &str) -> Result<Pairs> {
    let program = rule(predicate);
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

fn linked(a: i64) -> bool {
    EDGES.iter().any(|(x, _)| *x == a)
}

fn pointed_at(b: i64) -> bool {
    EDGES.iter().any(|(_, y)| *y == b)
}

/// Each predicate with the pairs it must keep. The first four read one side
/// and move beneath the join; the rest must stay above it.
fn cases() -> Vec<(&'static str, Pairs)> {
    vec![
        (
            "NOT (a)-[:R]->() AND a.t = b.t",
            expected(|a, _| !linked(a)),
        ),
        ("(a)-[:R]->() AND a.t = b.t", expected(|a, _| linked(a))),
        // The pattern's side is the join's right.
        (
            "a.t = b.t AND NOT (b)<-[:R]-()",
            expected(|_, b| !pointed_at(b)),
        ),
        // Beside another conjunct of its side, and under an OR within it.
        (
            "a.t = b.t AND (a.k > 3 OR (a)-[:R]->())",
            expected(|a, _| a > 3 || linked(a)),
        ),
        // Both sides: a pair's own edge.
        (
            "NOT (a)-[:R]->(b) AND a.t = b.t",
            expected(|a, b| !EDGES.contains(&(a, b))),
        ),
        (
            "(a)-[:R]->(b) AND a.t = b.t",
            expected(|a, b| EDGES.contains(&(a, b))),
        ),
        // A property map holds an expression; it is left where it was.
        (
            "(a)-[:R]->({k: 1}) AND a.t = b.t",
            expected(|a, _| EDGES.contains(&(a, 1))),
        ),
        // One side's pattern OR-ed with the other side's property.
        (
            "a.t = b.t AND ((a)-[:R]->() OR b.k = 2)",
            expected(|a, b| linked(a) || b == 2),
        ),
    ]
}

async fn check(flush: bool) -> Result<()> {
    let db = open(flush).await?;
    let session = db.session();
    for (predicate, want) in cases() {
        assert_eq!(
            cypher(&session, predicate).await?,
            want,
            "Cypher, {predicate}, flush={flush}"
        );
        assert_eq!(
            locy(&session, predicate).await?,
            want,
            "Locy, {predicate}, flush={flush}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn one_sided_pattern_predicates_keep_their_answers_unflushed() -> Result<()> {
    check(false).await
}

#[tokio::test]
async fn one_sided_pattern_predicates_keep_their_answers_flushed() -> Result<()> {
    check(true).await
}

/// An edge the transaction has not committed counts, on the side below the
/// join as it did above it.
#[tokio::test]
async fn an_uncommitted_edge_is_seen_below_the_join() -> Result<()> {
    let db = open(true).await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:A {k: 5}), (b:B {k: 1}) CREATE (a)-[:R]->(b)")
        .await?;
    let result = tx
        .query("MATCH (a:A), (b:B) WHERE NOT (a)-[:R]->() AND a.t = b.t RETURN a.k, b.k")
        .await?;
    let got: Pairs = result
        .rows()
        .iter()
        .filter_map(|row| match (&row.values()[0], &row.values()[1]) {
            (Value::Int(a), Value::Int(b)) => Some((*a, *b)),
            _ => None,
        })
        .collect();
    assert_eq!(got, expected(|a, _| !linked(a) && a != 5));
    Ok(())
}

/// Rows the rule's `HashJoinExec` produced, for a profiled run.
async fn joined_rows(session: &Session, predicate: &str) -> Result<usize> {
    let (_, profile) = session.locy_with(&rule(predicate)).profile().await?;
    Ok(profile
        .profile
        .strata
        .iter()
        .flat_map(|s| &s.rules)
        .filter(|r| r.name == "pairs")
        .flat_map(|r| &r.iterations)
        .flat_map(|i| &i.operators)
        .filter(|o| o.operator.starts_with("HashJoinExec"))
        .map(|o| o.actual_rows)
        .sum())
}

/// The join receives only the `a` rows the pattern predicate keeps: it emits
/// the answer's 8 pairs, not all 12 type-matched ones for a filter above it
/// to cut down.
#[tokio::test]
async fn a_one_sided_pattern_predicate_runs_below_the_join() -> Result<()> {
    let db = open(true).await?;
    let session = db.session();
    let predicate = "NOT (a)-[:R]->() AND a.t = b.t";
    let kept = expected(|a, _| !linked(a)).len();
    assert_eq!(kept, 8);
    assert_eq!(joined_rows(&session, predicate).await?, kept);
    Ok(())
}

/// The negative twin: a pattern over both sides cannot move, so the join
/// emits every type-matched pair and the filter above it decides.
#[tokio::test]
async fn a_two_sided_pattern_predicate_stays_above_the_join() -> Result<()> {
    let db = open(true).await?;
    let session = db.session();
    let all = expected(|_, _| true).len();
    assert_eq!(all, 12);
    assert_eq!(
        joined_rows(&session, "NOT (a)-[:R]->(b) AND a.t = b.t").await?,
        all
    );
    Ok(())
}
