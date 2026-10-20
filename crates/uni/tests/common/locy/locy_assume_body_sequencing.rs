// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The commands of an `ASSUME ... THEN { body }` run in sequence: a command
//! reads the hypothetical state every earlier command in the body left.
//!
//! Before, the body's facts were computed once, before its first command, so
//! a `QUERY` (or a second `DERIVE`) after a `DERIVE` or a Cypher write in the
//! same body answered from the state before that write — a silent stale read —
//! while a Cypher read after it saw the new edges. A body whose only command
//! was `DERIVE` also returned no rows: the DERIVE's `affected` count was
//! dropped.
//!
//! The oracle for every case is the same commands run one by one, each as its
//! own Locy program, in a transaction that applies the ASSUME's mutation
//! first and is rolled back afterwards. The ASSUME's rows must equal the
//! concatenated results of that sequence, a `DERIVE` contributing one
//! `{affected: n}` row as its top-level `CommandResult::Derive { affected }`
//! does.

// Rust guideline compliant

use anyhow::{Result, bail};
use uni_common::Value;
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyResult};

type FactRow = std::collections::HashMap<String, Value>;

/// `A -> B -> C -> D` over `KNOWS`.
async fn seeded() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'A'})-[:KNOWS]->(b:Person {name: 'B'})\
                -[:KNOWS]->(c:Person {name: 'C'})-[:KNOWS]->(d:Person {name: 'D'})",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// `link` derives `LINK` edges from `KNOWS`; `linked` (non-recursive) and
/// `reach` (recursive) read `LINK`; `link2` derives `LINK2` from `LINK`, and
/// `linked2` reads `LINK2`.
const RULES: &str = "\
CREATE RULE link AS MATCH (a:Person)-[:KNOWS]->(b:Person) DERIVE (a)-[:LINK]->(b)\n\
CREATE RULE linked AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b\n\
CREATE RULE reach AS MATCH (a:Person)-[:LINK]->(b:Person) YIELD KEY a, KEY b\n\
CREATE RULE reach AS MATCH (a:Person)-[:LINK]->(m:Person) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
CREATE RULE link2 AS MATCH (a:Person)-[:LINK]->(b:Person) DERIVE (a)-[:LINK2]->(b)\n\
CREATE RULE linked2 AS MATCH (a:Person)-[:LINK2]->(b:Person) YIELD KEY a, KEY b\n";

/// A mutation no rule reads: every parent stratum is reusable.
const UNRELATED: &str = "CREATE (:Marker {name: 'm'})";

/// A mutation `link` reads: a new `KNOWS` edge `D -> E`.
const EXTENDS_KNOWS: &str =
    "MATCH (d:Person {name: 'D'}) CREATE (d)-[:KNOWS]->(:Person {name: 'E'})";

const Q_LINKED: &str = "QUERY linked RETURN a.name AS a, b.name AS b";
const Q_REACH: &str = "QUERY reach RETURN a.name AS a, b.name AS b";
const Q_LINKED2: &str = "QUERY linked2 RETURN a.name AS a, b.name AS b";

/// A row rendered as sorted `column=value` cells.
fn render(row: &FactRow) -> String {
    let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v:?}")).collect();
    cells.sort();
    cells.join(" ")
}

/// The rows a command contributes to an enclosing ASSUME's result.
fn command_rows(result: &CommandResult) -> Result<Vec<String>> {
    Ok(match result {
        CommandResult::Query(rows) | CommandResult::Cypher(rows) | CommandResult::Assume(rows) => {
            rows.iter().map(render).collect()
        }
        CommandResult::Derive { affected } => {
            let row: FactRow = [("affected".to_string(), Value::Int(*affected as i64))].into();
            vec![render(&row)]
        }
        other => bail!("unexpected command result in an ASSUME body: {other:?}"),
    })
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// The ASSUME's rows, sorted.
async fn assume_answer(
    db: &Uni,
    mutation: &str,
    body_rules: &str,
    body: &[&str],
) -> Result<Vec<String>> {
    let program = format!(
        "{RULES}ASSUME {{ {mutation} }} THEN {{\n{body_rules}{}\n}}",
        body.join("\n")
    );
    let result: LocyResult = db.session().locy(&program).await?;
    match result.command_results.as_slice() {
        [CommandResult::Assume(rows)] => Ok(sorted(rows.iter().map(render).collect())),
        other => bail!("expected one ASSUME result, got {other:?}"),
    }
}

/// The oracle: `mutation`, then each command as its own program, in a
/// transaction that is rolled back.
async fn sequential_answer(
    db: &Uni,
    mutation: &str,
    body_rules: &str,
    body: &[&str],
) -> Result<Vec<String>> {
    let tx = db.session().tx().await?;
    tx.execute(mutation).await?;
    let mut rows = Vec::new();
    for command in body {
        if command.starts_with("MATCH") {
            // A Cypher write: a Locy program's Cypher commands are read-only.
            let written = tx.execute(command).await?;
            assert!(written.affected_rows() > 0, "`{command}` wrote nothing");
            continue;
        }
        let result = tx.locy(&format!("{RULES}{body_rules}{command}")).await?;
        for r in &result.command_results {
            rows.extend(command_rows(r)?);
        }
    }
    tx.rollback();
    Ok(sorted(rows))
}

/// Requires the ASSUME's rows to equal the oracle's, and the oracle to equal
/// `expected` (so a case cannot pass by both sides being empty or wrong the
/// same way). Also requires the stored graph to be unchanged afterwards.
async fn check(mutation: &str, body_rules: &str, body: &[&str], expected: &[&str]) -> Result<()> {
    let db = seeded().await?;
    let oracle = sequential_answer(&db, mutation, body_rules, body).await?;
    let mut want: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    want.sort();
    assert_eq!(oracle, want, "oracle for body {body:?}");
    let got = assume_answer(&db, mutation, body_rules, body).await?;
    assert_eq!(
        got, oracle,
        "ASSUME body {body:?} with mutation `{mutation}`"
    );
    let leaked = db
        .session()
        .query("MATCH ()-[r]->() WHERE type(r) <> 'KNOWS' RETURN count(r) AS n")
        .await?;
    assert_eq!(
        leaked.rows()[0].get::<i64>("n")?,
        0,
        "hypothetical edges leaked"
    );
    Ok(())
}

const AB: &str = "a=String(\"A\") b=String(\"B\")";
const BC: &str = "a=String(\"B\") b=String(\"C\")";
const CD: &str = "a=String(\"C\") b=String(\"D\")";
const DE: &str = "a=String(\"D\") b=String(\"E\")";
const AC: &str = "a=String(\"A\") b=String(\"C\")";
const AD: &str = "a=String(\"A\") b=String(\"D\")";
const BD: &str = "a=String(\"B\") b=String(\"D\")";

fn affected(n: i64) -> String {
    format!("affected=Int({n})")
}

#[tokio::test]
async fn derive_then_query_reads_the_derived_edges() -> Result<()> {
    let a3 = affected(3);
    check(
        UNRELATED,
        "",
        &["DERIVE link", Q_LINKED],
        &[&a3, AB, BC, CD],
    )
    .await
}

#[tokio::test]
async fn derive_then_query_after_a_mutation_the_rule_reads() -> Result<()> {
    let a4 = affected(4);
    check(
        EXTENDS_KNOWS,
        "",
        &["DERIVE link", Q_LINKED],
        &[&a4, AB, BC, CD, DE],
    )
    .await
}

#[tokio::test]
async fn query_derive_query_reads_before_then_after() -> Result<()> {
    let a3 = affected(3);
    check(
        UNRELATED,
        "",
        &[Q_LINKED, "DERIVE link", Q_LINKED],
        &[&a3, AB, BC, CD],
    )
    .await
}

#[tokio::test]
async fn derive_then_recursive_rule() -> Result<()> {
    let a3 = affected(3);
    check(
        UNRELATED,
        "",
        &["DERIVE link", Q_REACH],
        &[&a3, AB, BC, CD, AC, AD, BD],
    )
    .await
}

#[tokio::test]
async fn second_derive_reads_the_first_derives_edges() -> Result<()> {
    let a3 = affected(3);
    check(
        UNRELATED,
        "",
        &["DERIVE link", "DERIVE link2", Q_LINKED2],
        &[&a3, &a3, AB, BC, CD],
    )
    .await
}

#[tokio::test]
async fn derive_only_body_reports_affected() -> Result<()> {
    let a3 = affected(3);
    check(UNRELATED, "", &["DERIVE link"], &[&a3]).await
}

#[tokio::test]
async fn derive_feeding_a_body_rule() -> Result<()> {
    let a3 = affected(3);
    check(
        UNRELATED,
        "CREATE RULE via AS MATCH (a:Person) WHERE a IS linked TO b YIELD KEY a, KEY b\n",
        &["DERIVE link", "QUERY via RETURN a.name AS a, b.name AS b"],
        &[&a3, AB, BC, CD],
    )
    .await
}

#[tokio::test]
async fn cypher_write_then_query() -> Result<()> {
    check(
        UNRELATED,
        "",
        &[
            "MATCH (a:Person {name: 'A'}), (c:Person {name: 'C'}) CREATE (a)-[:LINK]->(c)",
            Q_LINKED,
        ],
        &[AC],
    )
    .await
}

#[tokio::test]
async fn nested_assume_after_derive_sees_it_and_is_discarded() -> Result<()> {
    let a3 = affected(3);
    // The nested ASSUME adds `A -LINK-> D`; the outer QUERY after it must not
    // see that edge, and must see the DERIVE's.
    check(
        UNRELATED,
        "",
        &[
            "DERIVE link",
            "ASSUME { MATCH (a:Person {name: 'A'}), (d:Person {name: 'D'}) \
             CREATE (a)-[:LINK]->(d) } THEN { QUERY linked RETURN a.name AS a, b.name AS b }",
            Q_LINKED,
        ],
        &[&a3, AB, BC, CD, AD, AB, BC, CD],
    )
    .await
}

#[tokio::test]
async fn derive_with_nothing_to_derive_changes_nothing() -> Result<()> {
    // `link2` reads LINK, which does not exist yet: zero affected.
    check(UNRELATED, "", &["DERIVE link2", Q_LINKED2], &[&affected(0)]).await
}
