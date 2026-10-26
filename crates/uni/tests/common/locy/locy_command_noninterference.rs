// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A command that does not write never changes what a later command reads.
//!
//! (Commands run in sequence, so a `DERIVE` or a Cypher write does change
//! what later commands read; `locy_toplevel_sequencing` covers that. The
//! commands placed here, `EXPLAIN RULE` and `ABDUCE`, write nothing.)
//!
//! `EXPLAIN RULE` falls back to re-deriving a rule through the in-memory
//! evaluator when it has no provenance to read: when its WHERE matches no
//! fact, or the rule has none. That evaluator does not implement FOLD,
//! BEST BY, post-FOLD WHERE or set semantics, and its rows were written into
//! the store every later `QUERY` reads. One `EXPLAIN` that matched nothing
//! therefore changed every answer after it: a FOLD came back as NULL rows, a
//! BEST BY loser reappeared, an `IS NOT` against a filtered FOLD lost a node,
//! a rule's facts became a bag, mutual recursion returned nothing, and an
//! `IS ... TO t` bound `t` to a weight instead of a node. `ABDUCE` builds its
//! tree through the same path.
//!
//! Each case states the program's answer without commands, so the relation
//! cannot pass vacuously, then requires the same answer with each command
//! placed before the `QUERY`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(command_noninterference)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::{Result, bail};
use uni_db::Uni;
use uni_db::locy::{CommandResult, LocyConfig};

/// `a -> b` (5) and `a -> c` (7).
const FAN: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (a)-[:E {w: 5}]->(b), (a)-[:E {w: 7}]->(c)";

/// `a -> b -> c -> d`.
const CHAIN: &str = "CREATE (a:N {name: 'a'}), (b:N {name: 'b'}), (c:N {name: 'c'}), \
     (d:N {name: 'd'}), (a)-[:E {w: 1}]->(b), (b)-[:E {w: 1}]->(c), (c)-[:E {w: 1}]->(d)";

fn config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

/// Runs `program` over `setup` and returns the last `QUERY`'s rows (or the
/// rows of a trailing `ASSUME`'s one body `QUERY`), each rendered as sorted `column=value`
/// cells, the rows themselves sorted.
async fn last_query(setup: &str, program: &str) -> Result<Vec<String>> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(setup).await?;
    tx.commit().await?;
    let result = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    let Some(rows) = result.command_results.iter().rev().find_map(|c| match c {
        CommandResult::Query(rows) => Some(rows),
        // The body's last QUERY: an ASSUME body may run an EXPLAIN or an
        // ABDUCE ahead of it.
        CommandResult::Assume { results, .. } => Some(
            results
                .iter()
                .rev()
                .find_map(|r| match r {
                    CommandResult::Query(rows) => Some(rows),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("the ASSUME body has no QUERY result: {results:?}")),
        ),
        _ => None,
    }) else {
        bail!("program has no QUERY: {program}");
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

/// The commands placed before the `QUERY`, for a rule `rule` keyed by `x`.
fn commands(rule: &str) -> Vec<String> {
    vec![
        format!("EXPLAIN RULE {rule} WHERE x.name = 'zzz'"),
        format!("EXPLAIN RULE {rule} WHERE x.name = 'a'"),
        format!("EXPLAIN RULE {rule}"),
        format!("ABDUCE NOT {rule} WHERE x.name = 'zzz'"),
        format!("ABDUCE NOT {rule} WHERE x.name = 'a'"),
    ]
}

/// Requires `rules + query` to return `expected`, and the same with each
/// command on `explained` placed between the rules and the query.
async fn assert_noninterference(
    setup: &str,
    rules: &str,
    explained: &str,
    query: &str,
    expected: &[&str],
) -> Result<()> {
    let baseline = last_query(setup, &format!("{rules}{query}")).await?;
    assert_eq!(baseline, expected, "control: {rules}{query}");
    for command in commands(explained) {
        let program = format!("{rules}{command}\n{query}");
        let after = last_query(setup, &program).await?;
        assert_eq!(
            after, baseline,
            "`{command}` changed the answer of `{query}`"
        );
    }
    Ok(())
}

#[tokio::test]
async fn command_noninterference_fold() -> Result<()> {
    assert_noninterference(
        FAN,
        "CREATE RULE tot AS MATCH (x:N)-[e:E]->(y:N) FOLD s = SUM(e.w) YIELD KEY x, s\n",
        "tot",
        "QUERY tot RETURN x.name AS x, s",
        &["s=12 x=a"],
    )
    .await
}

#[tokio::test]
async fn command_noninterference_best_by() -> Result<()> {
    assert_noninterference(
        FAN,
        "CREATE RULE cheap AS MATCH (x:N)-[e:E]->(y:N) BEST BY e.w ASC YIELD KEY x, e.w AS w\n",
        "cheap",
        "QUERY cheap RETURN x.name AS x, w",
        &["w=5 x=a"],
    )
    .await
}

#[tokio::test]
async fn command_noninterference_negation_over_filtered_fold() -> Result<()> {
    assert_noninterference(
        FAN,
        "CREATE RULE heavy AS MATCH (x:N)-[e:E]->(y:N) FOLD s = SUM(e.w) \
         WHERE s > 100 YIELD KEY x, s\n\
         CREATE RULE light AS MATCH (x:N) WHERE x IS NOT heavy YIELD KEY x\n",
        "light",
        "QUERY light RETURN x.name AS x",
        &["x=a", "x=b", "x=c"],
    )
    .await
}

#[tokio::test]
async fn command_noninterference_facts_are_a_set() -> Result<()> {
    assert_noninterference(
        FAN,
        "CREATE RULE src AS MATCH (x:N)-[:E]->(y:N) YIELD KEY x\n",
        "src",
        "QUERY src RETURN count(*) AS n",
        &["n=1"],
    )
    .await
}

#[tokio::test]
async fn command_noninterference_mutual_recursion() -> Result<()> {
    assert_noninterference(
        CHAIN,
        "CREATE RULE ev AS MATCH (x:N {name: 'a'}) YIELD KEY x\n\
         CREATE RULE od AS MATCH (p:N)-[:E]->(x:N) WHERE p IS ev YIELD KEY x\n\
         CREATE RULE ev AS MATCH (p:N)-[:E]->(x:N) WHERE p IS od YIELD KEY x\n",
        "ev",
        "QUERY od RETURN x.name AS x",
        &["x=b", "x=d"],
    )
    .await
}

#[tokio::test]
async fn command_noninterference_to_target_binding() -> Result<()> {
    assert_noninterference(
        FAN,
        "CREATE RULE link AS MATCH (x:N)-[e:E]->(y:N) YIELD KEY x, e.w AS w, KEY y\n\
         CREATE RULE via AS MATCH (x:N) WHERE x IS link TO t YIELD KEY x, t\n",
        "via",
        "QUERY via RETURN x.name AS x, t.name AS t",
        &["t=b x=a", "t=c x=a"],
    )
    .await
}

/// An `ASSUME` body reads the same facts whatever command ran before it:
/// an `EXPLAIN RULE` or `ABDUCE` at the top level ahead of the ASSUME, or
/// inside the body ahead of its `QUERY`.
#[tokio::test]
async fn command_noninterference_inside_assume() -> Result<()> {
    let rules = "CREATE RULE tot AS MATCH (x:N)-[e:E]->(y:N) FOLD s = SUM(e.w) YIELD KEY x, s\n";
    let query = "QUERY tot RETURN x.name AS x, s";
    let assume = |body: &str| format!("ASSUME {{ CREATE (:N {{name: 'z'}}) }} THEN {{ {body} }}");
    let baseline = last_query(FAN, &format!("{rules}{}", assume(query))).await?;
    assert_eq!(baseline, ["s=12 x=a"], "control");
    for command in commands("tot") {
        let after = last_query(FAN, &format!("{rules}{command}\n{}", assume(query))).await?;
        assert_eq!(
            after, baseline,
            "`{command}` before ASSUME changed the answer of `{query}` in its body"
        );
        let in_body = last_query(
            FAN,
            &format!("{rules}{}", assume(&format!("{command}\n{query}"))),
        )
        .await?;
        assert_eq!(
            in_body, baseline,
            "`{command}` inside the ASSUME body changed the answer of `{query}` after it"
        );
    }
    Ok(())
}
