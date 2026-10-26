// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #309 triage: a tuple `IS` / prefix-`TO` reference that binds a
//! scalar KEY column (here `d`, a distance).
//!
//! Fixture: identity `i1` IDENTIFIES concept `c0`; `c2 -SUBTYPE_OF-> c1
//! -SUBTYPE_OF-> c0`. Every variant's expected answer is computed by hand.
//! The matrix varies: arity (2 vs 3 bound positions), recursion, reference
//! form (tuple vs prefix-TO), and whether the bound variable's name equals
//! the referenced column's name.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_309)' --no-capture

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Result;
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{Uni, Value};

async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (i1:Identity {sid: 'i1'}), (c0:Concept {sid: 'c0'}), \
                (c1:Concept {sid: 'c1'}), (c2:Concept {sid: 'c2'}), \
                (i1)-[:IDENTIFIES]->(c0), (c1)-[:SUBTYPE_OF]->(c0), \
                (c2)-[:SUBTYPE_OF]->(c1)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

fn cell(v: &Value) -> String {
    match v {
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:?}"),
        Value::String(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Runs `program` and returns the QUERY rows as `c|i|d` strings, or the error.
async fn run(program: &str) -> std::result::Result<BTreeSet<String>, String> {
    let db = fixture().await.map_err(|e| e.to_string())?;
    let result = db
        .session()
        .locy_with(program)
        .with_config(LocyConfig {
            max_iterations: 100,
            timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        })
        .run()
        .await
        .map_err(|e| e.to_string())?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| match c {
            CommandResult::Query(rows) => Some(rows.clone()),
            _ => None,
        })
        .ok_or("no QUERY result")?;
    Ok(rows
        .iter()
        .map(|r| {
            ["c", "i", "d"]
                .iter()
                .filter_map(|k| r.get(*k).map(cell))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect())
}

fn set(rows: &[&str]) -> BTreeSet<String> {
    rows.iter().map(|s| s.to_string()).collect()
}

const BASE3: &str = "CREATE RULE ia0 AS MATCH (i:Identity)-[:IDENTIFIES]->(c:Concept) \
                     YIELD KEY c, KEY i, KEY 0 AS d\n";
const BASE2: &str = "CREATE RULE ib0 AS MATCH (i:Identity)-[:IDENTIFIES]->(c:Concept) \
                     YIELD KEY c, KEY 0 AS d\n";

#[tokio::test]
async fn issue_309_scalar_key_binding_matrix() -> Result<()> {
    let rec3 = |reference: &str, var: &str| {
        format!(
            "CREATE RULE identity_at AS MATCH (i:Identity)-[:IDENTIFIES]->(c:Concept) \
               YIELD KEY c, KEY i, KEY 0 AS d\n\
             CREATE RULE identity_at AS MATCH (c:Concept)-[:SUBTYPE_OF]->(m:Concept) \
               WHERE {reference} YIELD KEY c, KEY i, KEY {var} + 1 AS d\n\
             QUERY identity_at RETURN c.sid AS c, i.sid AS i, d"
        )
    };
    let rec2 = |reference: &str, var: &str| {
        format!(
            "CREATE RULE at2 AS MATCH (i:Identity)-[:IDENTIFIES]->(c:Concept) \
               YIELD KEY c, KEY 0 AS d\n\
             CREATE RULE at2 AS MATCH (c:Concept)-[:SUBTYPE_OF]->(m:Concept) \
               WHERE {reference} YIELD KEY c, KEY {var} + 1 AS d\n\
             QUERY at2 RETURN c.sid AS c, d"
        )
    };
    let nonrec3 = |reference: &str, var: &str| {
        format!(
            "{BASE3}CREATE RULE ia1 AS MATCH (c:Concept)-[:SUBTYPE_OF]->(m:Concept) \
               WHERE {reference} YIELD KEY c, KEY i, KEY {var} + 1 AS d\n\
             QUERY ia1 RETURN c.sid AS c, i.sid AS i, d"
        )
    };
    let nonrec2 = |reference: &str, var: &str| {
        format!(
            "{BASE2}CREATE RULE ib1 AS MATCH (c:Concept)-[:SUBTYPE_OF]->(m:Concept) \
               WHERE {reference} YIELD KEY c, KEY {var} + 1 AS d\n\
             QUERY ib1 RETURN c.sid AS c, d"
        )
    };
    let full3 = set(&["c0|i1|0", "c1|i1|1", "c2|i1|2"]);
    let full2 = set(&["c0|0", "c1|1", "c2|2"]);
    let one3 = set(&["c1|i1|1"]);
    let one2 = set(&["c1|1"]);

    let cases: Vec<(&str, String, BTreeSet<String>)> = vec![
        (
            "R3 tuple   (m,i,e) IS r",
            rec3("(m, i, e) IS identity_at", "e"),
            full3.clone(),
        ),
        (
            "R3 tuple   (m,i,d) IS r [same name]",
            rec3("(m, i, d) IS identity_at", "d"),
            full3.clone(),
        ),
        (
            "R3 prefTO  (m,i) IS r TO e",
            rec3("(m, i) IS identity_at TO e", "e"),
            full3.clone(),
        ),
        (
            "R3 prefTO  (m,i) IS r TO d [same name]",
            rec3("(m, i) IS identity_at TO d", "d"),
            full3.clone(),
        ),
        (
            "N3 tuple   (m,i,e) IS ia0",
            nonrec3("(m, i, e) IS ia0", "e"),
            one3.clone(),
        ),
        (
            "N3 prefTO  (m,i) IS ia0 TO e",
            nonrec3("(m, i) IS ia0 TO e", "e"),
            one3.clone(),
        ),
        (
            "R2 tuple   (m,e) IS r",
            rec2("(m, e) IS at2", "e"),
            full2.clone(),
        ),
        (
            "R2 scalTO  m IS r TO e",
            rec2("m IS at2 TO e", "e"),
            full2.clone(),
        ),
        (
            "N2 tuple   (m,e) IS ib0",
            nonrec2("(m, e) IS ib0", "e"),
            one2.clone(),
        ),
        (
            "N2 scalTO  m IS ib0 TO e",
            nonrec2("m IS ib0 TO e", "e"),
            one2.clone(),
        ),
    ];

    let mut failures = Vec::new();
    for (name, program, expected) in cases {
        let got = run(&program).await;
        let ok = got.as_ref().is_ok_and(|g| *g == expected);
        println!(
            "{} {name}\n    expected {:?}\n    actual   {:?}",
            if ok { "PASS" } else { "FAIL" },
            expected,
            got
        );
        if !ok {
            failures.push(name);
        }
    }
    assert!(failures.is_empty(), "failing variants: {failures:?}");
    Ok(())
}

/// The issue's workaround (and its "minor note"): an `ALONG d = 0` /
/// `prev.d + 1` distance over integer operands. Prints the returned type.
#[tokio::test]
async fn issue_309_along_workaround_distance_type() -> Result<()> {
    let program = "CREATE RULE identity_at AS MATCH (i:Identity)-[:IDENTIFIES]->(c:Concept) \
           ALONG d = 0 YIELD KEY c, KEY i, d\n\
         CREATE RULE identity_at AS MATCH (c:Concept)-[:SUBTYPE_OF]->(m:Concept) \
           WHERE m IS identity_at TO i ALONG d = prev.d + 1 YIELD KEY c, KEY i, d\n\
         QUERY identity_at RETURN c.sid AS c, i.sid AS i, d";
    let got = run(program).await;
    println!("ALONG workaround: {got:?}");
    assert_eq!(
        got,
        Ok(set(&["c0|i1|0", "c1|i1|1", "c2|i1|2"])),
        "integer distance"
    );
    Ok(())
}
