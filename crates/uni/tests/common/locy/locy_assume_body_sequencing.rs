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
//! first and is rolled back afterwards. The ASSUME returns one result per
//! body command, and each must equal the result the same command returned in
//! that sequence: the same kind (a `DERIVE` reports
//! `CommandResult::Derive { affected }` in a body as at the top level) and the
//! same rows.

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

/// A command's result: its kind, and its rows sorted. A `DERIVE`'s one row
/// is its affected count; an ASSUME's rows are its body results' rows, each
/// prefixed with the body command's index (`0:`), and its kind lists theirs.
fn rendered(result: &CommandResult) -> Result<(String, Vec<String>)> {
    let (kind, mut rows) = match result {
        CommandResult::Query(rows) => ("Query".to_string(), rows.iter().map(render).collect()),
        CommandResult::Cypher(rows) => ("Cypher".to_string(), rows.iter().map(render).collect()),
        CommandResult::Derive { affected } => {
            let row: FactRow = [("affected".to_string(), Value::Int(*affected as i64))].into();
            ("Derive".to_string(), vec![render(&row)])
        }
        CommandResult::Assume { results, .. } => {
            let mut kinds = Vec::new();
            let mut rows = Vec::new();
            for (i, r) in results.iter().enumerate() {
                let (kind, body_rows) = rendered(r)?;
                kinds.push(kind);
                rows.extend(body_rows.into_iter().map(|row| format!("{i}:{row}")));
            }
            (format!("Assume[{}]", kinds.join(",")), rows)
        }
        other => bail!("unexpected command result in an ASSUME body: {other:?}"),
    };
    rows.sort();
    Ok((kind, rows))
}

/// The ASSUME's body results, one per body command.
async fn assume_answer(
    db: &Uni,
    mutation: &str,
    body_rules: &str,
    body: &[&str],
) -> Result<Vec<(String, Vec<String>)>> {
    let program = format!(
        "{RULES}ASSUME {{ {mutation} }} THEN {{\n{body_rules}{}\n}}",
        body.join("\n")
    );
    let result: LocyResult = db.session().locy(&program).await?;
    match result.command_results.as_slice() {
        [CommandResult::Assume { results, .. }] => results.iter().map(rendered).collect(),
        other => bail!("expected one ASSUME result, got {other:?}"),
    }
}

/// The oracle: `mutation`, then each command as its own program, in a
/// transaction that is rolled back. One result per command.
async fn sequential_answer(
    db: &Uni,
    mutation: &str,
    body_rules: &str,
    body: &[&str],
) -> Result<Vec<(String, Vec<String>)>> {
    let tx = db.session().tx().await?;
    tx.execute(mutation).await?;
    let mut results = Vec::new();
    for command in body {
        if command.starts_with("MATCH") {
            // A Cypher write: a Locy program's Cypher commands are read-only.
            // In a body it returns no rows.
            let written = tx.execute(command).await?;
            assert!(written.affected_rows() > 0, "`{command}` wrote nothing");
            results.push(("Cypher".to_string(), Vec::new()));
            continue;
        }
        let result = tx.locy(&format!("{RULES}{body_rules}{command}")).await?;
        match result.command_results.as_slice() {
            [one] => results.push(rendered(one)?),
            other => bail!("`{command}` alone returned {other:?}, not one result"),
        }
    }
    tx.rollback();
    Ok(results)
}

/// Requires each of the ASSUME's body results to equal the oracle's for the
/// same command, and the oracle's rows to equal `expected`, one list per
/// command (so a case cannot pass by both sides being empty or wrong the same
/// way). Also requires the stored graph to be unchanged afterwards.
async fn check(
    mutation: &str,
    body_rules: &str,
    body: &[&str],
    expected: &[&[&str]],
) -> Result<()> {
    let db = seeded().await?;
    let oracle = sequential_answer(&db, mutation, body_rules, body).await?;
    let want: Vec<Vec<String>> = expected
        .iter()
        .map(|rows| {
            let mut rows: Vec<String> = rows.iter().map(|s| s.to_string()).collect();
            rows.sort();
            rows
        })
        .collect();
    let oracle_rows: Vec<Vec<String>> = oracle.iter().map(|(_, rows)| rows.clone()).collect();
    assert_eq!(oracle_rows, want, "oracle for body {body:?}");
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

/// A row of a nested ASSUME's body command 0.
fn in_nested(row: &str) -> String {
    format!("0:{row}")
}

#[tokio::test]
async fn derive_then_query_reads_the_derived_edges() -> Result<()> {
    let a3 = affected(3);
    check(
        UNRELATED,
        "",
        &["DERIVE link", Q_LINKED],
        &[&[&a3], &[AB, BC, CD]],
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
        &[&[&a4], &[AB, BC, CD, DE]],
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
        &[&[], &[&a3], &[AB, BC, CD]],
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
        &[&[&a3], &[AB, BC, CD, AC, AD, BD]],
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
        &[&[&a3], &[&a3], &[AB, BC, CD]],
    )
    .await
}

#[tokio::test]
async fn derive_only_body_reports_affected() -> Result<()> {
    let a3 = affected(3);
    check(UNRELATED, "", &["DERIVE link"], &[&[&a3]]).await
}

#[tokio::test]
async fn derive_feeding_a_body_rule() -> Result<()> {
    let a3 = affected(3);
    check(
        UNRELATED,
        "CREATE RULE via AS MATCH (a:Person) WHERE a IS linked TO b YIELD KEY a, KEY b\n",
        &["DERIVE link", "QUERY via RETURN a.name AS a, b.name AS b"],
        &[&[&a3], &[AB, BC, CD]],
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
        &[&[], &[AC]],
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
        &[
            &[&a3],
            &[
                &in_nested(AB),
                &in_nested(BC),
                &in_nested(CD),
                &in_nested(AD),
            ],
            &[AB, BC, CD],
        ],
    )
    .await
}

#[tokio::test]
async fn derive_with_nothing_to_derive_changes_nothing() -> Result<()> {
    // `link2` reads LINK, which does not exist yet: zero affected.
    check(
        UNRELATED,
        "",
        &["DERIVE link2", Q_LINKED2],
        &[&[&affected(0)], &[]],
    )
    .await
}

/// The ASSUME's `derived` facts are those of the state its first body command
/// reads: after the mutation (`kn` sees the new `D -> E` edge) and before the
/// body's DERIVE (`linked` is still empty, though the QUERY after the DERIVE
/// reads 4 facts). A body rule's facts are among them.
#[tokio::test]
async fn derived_is_the_state_the_first_body_command_reads() -> Result<()> {
    let db = seeded().await?;
    let program = format!(
        "{RULES}ASSUME {{ {EXTENDS_KNOWS} }} THEN {{\n\
         CREATE RULE kn AS MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, KEY b\n\
         DERIVE link\n{Q_LINKED}\n}}"
    );
    let result: LocyResult = db.session().locy(&program).await?;
    let (results, derived) = match result.command_results.as_slice() {
        [one] => one.as_assume().expect("an ASSUME result"),
        other => bail!("expected one ASSUME result, got {other:?}"),
    };
    let rows: Vec<Vec<String>> = results
        .iter()
        .map(|r| rendered(r).map(|(_, rows)| rows))
        .collect::<Result<_>>()?;
    assert_eq!(
        rows,
        vec![
            vec![affected(4)],
            vec![AB.into(), BC.into(), CD.into(), DE.into()]
        ]
    );
    let count = |rule: &str| derived.get(rule).map(Vec::len);
    assert_eq!(count("kn"), Some(4), "the mutation's edge is in `derived`");
    assert_eq!(count("linked"), Some(0), "the body's DERIVE is not");
    // The program's own `derived` is the stored state: no body rule, and the
    // hypothetical edge is not in it.
    assert!(!result.derived_rules().await.unwrap().contains_key("kn"));
    assert_eq!(
        result
            .derived_rules()
            .await
            .unwrap()
            .get("linked")
            .map(Vec::len),
        Some(0)
    );
    // `rows()` is the first row-producing body result: the QUERY's.
    assert_eq!(result.rows().map(Vec::len), Some(4));
    Ok(())
}
