// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The clauses of one Locy rule agree on what the rule is, and a FOLD
//! aggregate honours `DISTINCT`.
//!
//! The planner maps each clause's YIELD onto the rule's columns by name and
//! reads the rule's FOLD, post-FOLD WHERE, REQUIRE and BEST BY from the first
//! clause that folds. The compiler checked neither, so:
//!
//! - a clause yielding `KEY a, KEY c` beside one yielding `KEY a, KEY b` had
//!   its `b` column filled from its own body variable `b`, not from `c`;
//! - a REQUIRE or post-FOLD WHERE on any clause but the first that folds was
//!   ignored, and two clauses folding one column with `SUM` and `MAX` failed
//!   in Arrow;
//! - a BEST BY in a rule without FOLD ranked the whole rule when clause 1
//!   stated it, and only its own clause otherwise;
//! - `COUNT(DISTINCT y)` counted every row.
//!
//! The compiler now requires the clauses to agree, by name rather than by
//! position, and the FOLD operator folds one row per distinct value. `DISTINCT`
//! is refused in a recursive rule, whose FOLD never sees a group's whole bag.
//! Plain Cypher is the oracle for `DISTINCT`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(clause_contract)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig};

/// `a -> b -> c -> d`, every weight 1.
const CHAIN: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (d:N {name: 'd'}), (a)-[:E {w: 1}]->(b), (b)-[:E {w: 1}]->(c), (c)-[:E {w: 1}]->(d)";

/// `a -> b` (5) and `a -> c` (7).
const FAN: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (a)-[:E {w: 5}]->(b), (a)-[:E {w: 7}]->(c)";

/// Repeated targets and weights: `a` has five edges to three nodes with
/// weights 1, 1, 2, 2, 3; `b` has two edges to `c`, both weight 4.
const PARALLEL: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (d:N {name: 'd'}), \
     (a)-[:E {w: 1}]->(b), (a)-[:E {w: 1}]->(b), (a)-[:E {w: 2}]->(c), \
     (a)-[:E {w: 2}]->(d), (a)-[:E {w: 3}]->(d), \
     (b)-[:E {w: 4}]->(c), (b)-[:E {w: 4}]->(c)";

/// A recursive path rule over CHAIN; `{base}` and `{rec}` are each clause's
/// tail after FOLD.
const RECURSIVE: &str = "CREATE RULE r AS MATCH (x:N)-[e:E]->(y:N) FOLD t = MSUM(e.w) {base} \
     YIELD KEY x, KEY y, t\n\
     CREATE RULE r AS MATCH (x:N)-[e:E]->(m:N) WHERE m IS r TO y FOLD t = MSUM(e.w) {rec} \
     YIELD KEY x, KEY y, t\n\
     QUERY r RETURN x.name AS x, y.name AS y, t";

fn config() -> LocyConfig {
    LocyConfig {
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

/// What a program must do.
enum Expect {
    /// Fail to compile, with every fragment in the message.
    Rejected(&'static [&'static str]),
    /// Run, and its last QUERY return these rows (sorted `column=value` cells).
    Rows(&'static [&'static str]),
}

async fn graph(setup: &str) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(setup).await?;
    tx.commit().await?;
    Ok(db)
}

async fn locy_rows(db: &Uni, program: &str) -> Result<Vec<String>> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    let Some(CommandResult::Query(rows)) = result.command_results.last() else {
        bail!("no QUERY result");
    };
    let mut out: Vec<String> = rows
        .iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cells.sort();
            cells.join(" ")
        })
        .collect();
    out.sort();
    Ok(out)
}

async fn check_all(cases: Vec<(&str, &str, String, Expect)>) -> Result<()> {
    let mut failures = Vec::new();
    for (label, setup, program, expect) in &cases {
        let outcome = locy_rows(&graph(setup).await?, program).await;
        let failure = match (expect, outcome) {
            (Expect::Rejected(fragments), Err(e)) => {
                let message = format!("{e:#}");
                if !message.contains("LocyCompileError") {
                    Some(format!("{label}: expected a compile error, got: {message}"))
                } else {
                    fragments
                        .iter()
                        .find(|f| !message.contains(*f))
                        .map(|f| format!("{label}: compile error lacks {f:?}: {message}"))
                }
            }
            (Expect::Rejected(_), Ok(rows)) => Some(format!("{label}: compiled and ran: {rows:?}")),
            (Expect::Rows(want), Ok(got)) => {
                (got != *want).then(|| format!("{label}: expected {want:?}, got {got:?}"))
            }
            (Expect::Rows(_), Err(e)) => Some(format!("{label}: failed: {e:#}")),
        };
        failures.extend(failure);
    }
    if !failures.is_empty() {
        bail!(
            "{} of {} cases failed:\n{}",
            failures.len(),
            cases.len(),
            failures.join("\n")
        );
    }
    Ok(())
}

/// Every pair `(u, v)` with a path of one or two edges on CHAIN.
const HOPS: &[&str] = &["a=a b=b", "a=a b=c", "a=b b=c", "a=b b=d", "a=c b=d"];

#[tokio::test]
async fn clause_contract_yield_columns_agree_by_name() -> Result<()> {
    let hop = |second: &str| {
        format!(
            "CREATE RULE hop AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
             CREATE RULE hop AS {second}\n\
             QUERY hop RETURN a.name AS a, b.name AS b"
        )
    };
    check_all(vec![
        (
            "a clause naming a column differently",
            CHAIN,
            hop("MATCH (a:N)-[:E]->(b:N)-[:E]->(c:N) YIELD KEY a, KEY c"),
            Expect::Rejected(&["schema mismatch", "clause 2 yields 'c'", "AS"]),
        ),
        (
            "the same clause, aliased",
            CHAIN,
            hop("MATCH (a:N)-[:E]->(b:N)-[:E]->(c:N) YIELD KEY a, KEY c AS b"),
            Expect::Rows(HOPS),
        ),
        (
            "the same columns in another order",
            CHAIN,
            hop("MATCH (a:N)-[:E]->(m:N)-[:E]->(b:N) YIELD KEY b, KEY a"),
            Expect::Rows(HOPS),
        ),
        (
            "a column KEY in one clause only",
            CHAIN,
            "CREATE RULE p AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, e.w AS w\n\
             CREATE RULE p AS MATCH (a:N)-[e:E]->(b:N) YIELD KEY a, KEY e.w AS w\n\
             QUERY p RETURN a.name AS a, w"
                .into(),
            Expect::Rejected(&["schema mismatch", "'w' is a KEY in one clause"]),
        ),
    ])
    .await
}

#[tokio::test]
async fn clause_contract_one_fold_tail_per_rule() -> Result<()> {
    let rec = |base: &str, rec: &str| RECURSIVE.replace("{base}", base).replace("{rec}", rec);
    check_all(vec![
        (
            "REQUIRE on both clauses",
            CHAIN,
            rec("REQUIRE t >= 2", "REQUIRE t >= 2"),
            Expect::Rows(&[]),
        ),
        (
            "REQUIRE on the recursive clause only",
            CHAIN,
            rec("", "REQUIRE t >= 2"),
            Expect::Rejected(&["disagree on REQUIRE", "clause 1 states `nothing`"]),
        ),
        (
            "REQUIRE on the base clause only",
            CHAIN,
            rec("REQUIRE t >= 2", ""),
            Expect::Rejected(&["disagree on REQUIRE"]),
        ),
        (
            "post-FOLD WHERE on the recursive clause only",
            CHAIN,
            rec("", "WHERE t >= 2"),
            Expect::Rejected(&["disagree on post-FOLD WHERE"]),
        ),
        (
            "post-FOLD WHERE on both clauses",
            CHAIN,
            rec("WHERE t >= 1", "WHERE t >= 1"),
            Expect::Rows(&[
                "t=1 x=a y=b",
                "t=1 x=a y=c",
                "t=1 x=a y=d",
                "t=1 x=b y=c",
                "t=1 x=b y=d",
                "t=1 x=c y=d",
            ]),
        ),
        (
            "two aggregates for one FOLD column",
            FAN,
            "CREATE RULE m AS MATCH (a:N)-[r:E]->(b:N) FOLD t = SUM(r.w) YIELD KEY a, t\n\
             CREATE RULE m AS MATCH (a:N)<-[r:E]-(b:N) FOLD t = MAX(r.w) YIELD KEY a, t\n\
             QUERY m RETURN a.name AS a, t"
                .into(),
            Expect::Rejected(&["disagree on FOLD", "t = SUM(...)", "t = MAX(...)"]),
        ),
        (
            "one aggregate over each clause's own argument",
            FAN,
            "CREATE RULE m AS MATCH (a:N)-[r:E]->(b:N) FOLD t = MAX(r.w) YIELD KEY a, t\n\
             CREATE RULE m AS MATCH (a:N)<-[e:E]-(b:N) FOLD t = MAX(e.w + 1) YIELD KEY a, t\n\
             QUERY m RETURN a.name AS a, t"
                .into(),
            Expect::Rows(&["a=a t=7", "a=b t=6", "a=c t=8"]),
        ),
        (
            "a seeding clause without FOLD",
            FAN,
            "CREATE RULE m AS MATCH (a:N) YIELD KEY a, NULL AS t\n\
             CREATE RULE m AS MATCH (a:N)-[r:E]->(b:N) FOLD t = MAX(r.w) YIELD KEY a, t\n\
             QUERY m RETURN a.name AS a, t"
                .into(),
            Expect::Rows(&["a=a t=7", "a=b t=null", "a=c t=null"]),
        ),
        (
            "a seeding clause stating BEST BY",
            FAN,
            "CREATE RULE m AS MATCH (a:N)-[r:E]->(b:N) BEST BY r.w ASC YIELD KEY a, r.w AS t\n\
             CREATE RULE m AS MATCH (a:N)-[r:E]->(b:N) FOLD t = MAX(r.w) YIELD KEY a, t\n\
             QUERY m RETURN a.name AS a, t"
                .into(),
            Expect::Rejected(&[
                "disagree on BEST BY",
                "clause 1 states BEST BY without FOLD",
            ]),
        ),
    ])
    .await
}

#[tokio::test]
async fn clause_contract_best_by_ranks_the_whole_rule() -> Result<()> {
    let program = |first: &str, second: &str| {
        format!(
            "CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE e.w < 6 {first} \
             YIELD KEY x, e.w AS w\n\
             CREATE RULE p AS MATCH (x:N)-[e:E]->(y:N) WHERE e.w > 6 {second} \
             YIELD KEY x, e.w AS w\n\
             QUERY p RETURN x.name AS x, w"
        )
    };
    check_all(vec![
        (
            "BEST BY on every clause",
            FAN,
            program("BEST BY w ASC", "BEST BY w ASC"),
            Expect::Rows(&["w=5 x=a"]),
        ),
        (
            "BEST BY on the first clause only",
            FAN,
            program("BEST BY w ASC", ""),
            Expect::Rejected(&["disagree on BEST BY", "clause 2 states `no BEST BY`"]),
        ),
        (
            "BEST BY on the second clause only",
            FAN,
            program("", "BEST BY w ASC"),
            Expect::Rejected(&["disagree on BEST BY"]),
        ),
        (
            "different BEST BY orders",
            FAN,
            program("BEST BY w ASC", "BEST BY w DESC"),
            Expect::Rejected(&["disagree on BEST BY", "BEST BY w DESC"]),
        ),
    ])
    .await
}

/// A numeric value as `f64`, anything else unchanged; a list sorted.
///
/// Locy's SUM and AVG return floats where Cypher's return an integer for an
/// integer input, and neither orders a `collect(DISTINCT ...)`. Neither
/// difference is what this test is about.
fn normalized(value: &Value) -> Value {
    match value {
        Value::Int(i) => Value::Float(*i as f64),
        Value::List(items) => {
            let mut items: Vec<Value> = items.iter().map(normalized).collect();
            items.sort_by_key(|v| format!("{v}"));
            Value::List(items)
        }
        other => other.clone(),
    }
}

#[tokio::test]
async fn clause_contract_fold_distinct_matches_cypher() -> Result<()> {
    let aggregates = [
        ("COUNT", "count", "y"),
        ("COUNT", "count", "e.w"),
        ("SUM", "sum", "e.w"),
        ("AVG", "avg", "e.w"),
        ("MIN", "min", "e.w"),
        ("MAX", "max", "e.w"),
        ("COLLECT", "collect", "e.w"),
        ("COLLECT", "collect", "y.name"),
    ];
    let db = graph(PARALLEL).await?;
    let mut failures = Vec::new();
    let mut cases = 0;
    for (fold, cypher, arg) in aggregates {
        for distinct in ["", "DISTINCT "] {
            cases += 1;
            let label = format!("{fold}({distinct}{arg})");
            let want = db
                .session()
                .query(&format!(
                    "MATCH (x:N)-[e:E]->(y:N) \
                     RETURN x.name AS x, {cypher}({distinct}{arg}) AS k ORDER BY x"
                ))
                .await?;
            let want: Vec<(Option<Value>, Option<Value>)> = want
                .rows()
                .iter()
                .map(|r| (r.value("x").cloned(), r.value("k").map(normalized)))
                .collect();
            let result = db
                .session()
                .locy_with(&format!(
                    "CREATE RULE n AS MATCH (x:N)-[e:E]->(y:N) \
                     FOLD k = {fold}({distinct}{arg}) YIELD KEY x, k\n\
                     QUERY n RETURN x.name AS x, k"
                ))
                .with_config(config())
                .run()
                .await;
            let got = match result {
                Ok(result) => match result.command_results.last() {
                    Some(CommandResult::Query(rows)) => {
                        let mut got: Vec<(Option<Value>, Option<Value>)> = rows
                            .iter()
                            .map(|r| (r.get("x").cloned(), r.get("k").map(normalized)))
                            .collect();
                        got.sort_by_key(|(x, _)| format!("{x:?}"));
                        got
                    }
                    _ => {
                        failures.push(format!("{label}: no QUERY result"));
                        continue;
                    }
                },
                Err(e) => {
                    failures.push(format!("{label}: Locy failed: {e:#}"));
                    continue;
                }
            };
            if got != want {
                failures.push(format!("{label}: Locy {got:?}, Cypher {want:?}"));
            }
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} of {cases} differ:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
    Ok(())
}

#[tokio::test]
async fn clause_contract_distinct_fold_refused_in_recursion() -> Result<()> {
    check_all(vec![(
        "DISTINCT in a recursive FOLD",
        CHAIN,
        "CREATE RULE r AS MATCH (x:N)-[e:E]->(y:N) FOLD t = MCOUNT(DISTINCT y) \
         YIELD KEY x, t\n\
         CREATE RULE r AS MATCH (x:N)-[e:E]->(m:N) WHERE m IS r FOLD t = MCOUNT(DISTINCT m) \
         YIELD KEY x, t\n\
         QUERY r RETURN x.name AS x, t"
            .into(),
        Expect::Rejected(&["FOLD 't' in recursive rule 'r' uses DISTINCT"]),
    )])
    .await
}
