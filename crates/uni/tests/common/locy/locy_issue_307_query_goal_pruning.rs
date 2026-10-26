// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #307: `QUERY supertype WHERE c.sid = …` cost the full fixpoint.
//!
//! The issue's program on a fan-out-10 `SUBTYPE_OF` tree of `n` concepts.
//!
//! * `default_config_query_work_follows_the_goal`: with the default
//!   configuration a one-subject QUERY derives the facts its goal reaches
//!   (the subject's ancestors), not the whole relation, and answers the same
//!   rows as full evaluation. Before `GoalDirected::Auto` became the default
//!   the default derived every concept's closure.
//! * `issue_307_query_where_cost_vs_relation`: the triage measurement,
//!   `#[ignore]`d (16k-node fixture). For `n` = 1k / 4k / 16k it reports the
//!   facts derived, rows scanned, answer rows and wall time of the whole
//!   relation and of a one-subject QUERY in every mode.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;
use uni_db::locy::{GoalDirected, LocyConfig};
use uni_db::{Uni, Value};

const FANOUT: usize = 10;

/// `n` concepts `C{i}`; `C{i}` (i > 0) is a subtype of `C{(i-1)/FANOUT}`.
async fn tree(n: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Concept (sid STRING)").await?;
    tx.execute("CREATE EDGE TYPE SUBTYPE_OF () FROM Concept TO Concept")
        .await?;
    tx.execute("CREATE INDEX idx_concept_sid FOR (c:Concept) ON (c.sid)")
        .await?;
    tx.commit().await?;
    let nodes: Vec<Value> = (0..n).map(|i| Value::String(format!("C{i}"))).collect();
    let edges: Vec<Value> = (1..n)
        .map(|i| {
            Value::Map(HashMap::from([
                ("c".to_string(), Value::String(format!("C{i}"))),
                (
                    "p".to_string(),
                    Value::String(format!("C{}", (i - 1) / FANOUT)),
                ),
            ]))
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $rows AS s CREATE (:Concept {sid: s})")
        .param("rows", Value::List(nodes))
        .run()
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute_with(
        "UNWIND $rows AS r MATCH (c:Concept {sid: r.c}), (p:Concept {sid: r.p}) \
         CREATE (c)-[:SUBTYPE_OF]->(p)",
    )
    .param("rows", Value::List(edges))
    .run()
    .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

const RULES: &str = "CREATE RULE supertype AS MATCH (c:Concept)-[:SUBTYPE_OF]->(p:Concept) \
       YIELD KEY c, KEY p\n\
     CREATE RULE supertype AS MATCH (c:Concept)-[:SUBTYPE_OF]->(m:Concept) \
       WHERE m IS supertype TO p YIELD KEY c, KEY p\n";

/// What one evaluation did.
#[derive(Debug)]
struct Work {
    /// Facts derived over every stratum.
    facts: usize,
    rows_scanned: u64,
    /// The QUERY's answer, sorted.
    answer: Vec<String>,
    ms: u128,
}

/// Evaluates `program` under `config` through `profile()`.
async fn run(db: &Uni, program: &str, config: LocyConfig) -> Result<Work> {
    let t = Instant::now();
    let (result, profile) = db
        .session()
        .locy_with(program)
        .with_config(config)
        .profile()
        .await?;
    let ms = t.elapsed().as_millis();
    let facts = profile.profile.strata.iter().map(|s| s.facts_derived).sum();
    let mut answer: Vec<String> = result
        .rows()
        .map(|rows| rows.iter().map(|r| format!("{:?}", r.get("p"))).collect())
        .unwrap_or_default();
    answer.sort();
    Ok(Work {
        facts,
        rows_scanned: result.metrics().rows_scanned as u64,
        answer,
        ms,
    })
}

fn mode(goal_directed: GoalDirected) -> LocyConfig {
    LocyConfig {
        goal_directed,
        ..Default::default()
    }
}

/// With the default configuration, a one-subject QUERY derives only what its
/// goal reaches.
///
/// `C999` of a 1000-concept tree has three ancestors (`C99`, `C9`, `C0`).
/// The goal-directed program derives the magic facts for those four nodes
/// and the six `supertype` facts among them, ten in all; the full fixpoint
/// derives every concept's closure, 2,877 facts. The bound below leaves room
/// for the rewrite's bookkeeping while staying two orders of magnitude under
/// the full relation.
#[tokio::test]
async fn default_config_query_work_follows_the_goal() -> Result<()> {
    let n = 1_000;
    let db = tree(n).await?;
    let program = format!(
        "{RULES}QUERY supertype WHERE c.sid = 'C{}' RETURN p.sid AS p",
        n - 1
    );

    let off = run(&db, &program, mode(GoalDirected::Off)).await?;
    let default = run(&db, &program, LocyConfig::default()).await?;
    eprintln!("#307 n={n}: off={off:?} default={default:?}");

    assert_eq!(
        off.answer,
        vec![
            "Some(String(\"C0\"))",
            "Some(String(\"C9\"))",
            "Some(String(\"C99\"))"
        ],
        "full evaluation's answer"
    );
    assert_eq!(
        default.answer, off.answer,
        "the default answers as Off does"
    );
    assert_eq!(off.facts, 2_877, "the full closure of the tree");
    assert!(
        default.facts <= 20,
        "the default derived {} facts for a goal reaching 4 nodes (off: {})",
        default.facts,
        off.facts
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "triage measurement for #307; 16k-node fixture"]
async fn issue_307_query_where_cost_vs_relation() -> Result<()> {
    for n in [1_000usize, 4_000, 16_000] {
        let db = tree(n).await?;
        let leaf = format!("C{}", n - 1);
        let whole = format!("{RULES}QUERY supertype RETURN c.sid AS c, p.sid AS p");
        let one = format!("{RULES}QUERY supertype WHERE c.sid = '{leaf}' RETURN p.sid AS p");
        // Warm-up so the first measured run does not pay cold caches.
        run(&db, &one, mode(GoalDirected::Off)).await?;
        let w = run(&db, &whole, mode(GoalDirected::Off)).await?;
        let off = run(&db, &one, mode(GoalDirected::Off)).await?;
        let default = run(&db, &one, LocyConfig::default()).await?;
        let on = run(&db, &one, mode(GoalDirected::On)).await?;
        let shadow = run(&db, &one, mode(GoalDirected::Shadow)).await?;
        eprintln!(
            "n={n}: whole={w:?}\n  one/off={off:?}\n  one/default={default:?}\n  \
             one/on={on:?}\n  one/shadow={shadow:?}"
        );
        assert_eq!(off.answer, on.answer, "n={n}: on/off answers differ");
        assert_eq!(
            off.answer, default.answer,
            "n={n}: default/off answers differ"
        );
        assert_eq!(
            off.answer, shadow.answer,
            "n={n}: shadow/off answers differ"
        );
    }
    Ok(())
}
