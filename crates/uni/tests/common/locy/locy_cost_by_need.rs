// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A program pays for provenance, hydration and writes by what it uses
//! (`docs/proposals/locy_resolution_cost_2026_11_14.md`).
//!
//! - One `MNOR` used to switch proof recording on for every rule of the
//!   program. It is recorded only when something can read it: an
//!   `EXPLAIN RULE`, top-k proof retention, or a fact whose proofs can share
//!   an event.
//! - Every rule's facts used to be turned into rows and hydrated for the
//!   commands and again for the result. Only the rules the commands read
//!   are, and `LocyConfig::returned_rules` chooses what the result carries.
//! - A `DERIVE` used to run one statement per fact.

use std::collections::BTreeSet;

use anyhow::Result;
use uni_db::locy::{LocyConfig, ReturnedRules};
use uni_db::{DataType, Uni, Value};

const N: i64 = 6;

/// `evidence` pairs every item with every tag of its colour and has no
/// `PROB` column; `score` folds it with `MNOR`; `chosen` keeps one tag per
/// item, which `link` writes.
const RULES: &str = "CREATE RULE evidence AS
  MATCH (i:Item), (t:Tag) WHERE i.colour = t.colour
  YIELD KEY i, KEY t, 0.5 AS p
CREATE RULE score AS
  MATCH (i:Item), (t:Tag)
  WHERE (i, t, p) IS evidence
  FOLD prob = MNOR(p)
  YIELD KEY i, KEY t, prob AS PROB
CREATE RULE chosen AS
  MATCH (i:Item), (t:Tag)
  WHERE (i, t, prob) IS score
  BEST BY prob DESC
  YIELD KEY i, t, prob
CREATE RULE link AS
  MATCH (i:Item), (t:Tag)
  WHERE (i, t, prob) IS chosen
  DERIVE (i)-[:TAGGED]->(t)
";

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Item")
        .property("k", DataType::Int64)
        .property("colour", DataType::Int64)
        .done()
        .label("Tag")
        .property("k", DataType::Int64)
        .property("colour", DataType::Int64)
        .done()
        .edge_type("TAGGED", &["Item"], &["Tag"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS k CREATE (:Item {{k: k, colour: k % 2}})",
        N - 1
    ))
    .await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS k CREATE (:Tag {{k: k, colour: k % 2}})",
        N - 1
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

/// The names of the synthetic operators a profile lists for `rule`.
fn rule_operators(profile: &uni_db::locy::LocyExecProfile, rule: &str) -> BTreeSet<String> {
    profile
        .strata
        .iter()
        .flat_map(|s| &s.rules)
        .filter(|r| r.name == rule)
        .flat_map(|r| &r.iterations)
        .flat_map(|i| &i.operators)
        .map(|o| o.operator.clone())
        .collect()
}

fn scores(result: &uni_locy::LocyResult) -> Vec<(i64, i64, f64)> {
    let mut rows: Vec<(i64, i64, f64)> = result
        .rows()
        .expect("QUERY rows")
        .iter()
        .map(|row| {
            let int = |c: &str| match row.get(c) {
                Some(Value::Int(v)) => *v,
                other => panic!("{c}: {other:?}"),
            } as i64;
            let prob = row.get("prob").and_then(Value::as_f64).expect("prob");
            (int("i"), int("t"), prob)
        })
        .collect();
    rows.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    rows
}

/// An `MNOR` over a rule without a `PROB` column: every row is its own
/// event, so nothing is recorded, nothing is warned of, and the values are
/// those exact inference gives.
#[tokio::test]
async fn a_fold_whose_rows_cannot_share_a_proof_records_no_provenance() -> Result<()> {
    let db = open().await?;
    let program = format!("{RULES}QUERY score RETURN i.k AS i, t.k AS t, prob");
    let session = db.session();
    let (result, profile) = session.locy_with(&program).profile().await?;
    for rule in ["evidence", "score", "chosen"] {
        let operators = rule_operators(&profile.profile, rule);
        assert!(
            !operators.contains("LocyProvenance") && !operators.contains("LocyLineage"),
            "{rule}: {operators:?}"
        );
    }
    assert!(result.warnings().is_empty(), "{:?}", result.warnings());

    // Exact inference keeps the tracker, and agrees.
    let (exact, exact_profile) = session
        .locy_with(&program)
        .with_config(LocyConfig {
            exact_probability: true,
            ..Default::default()
        })
        .profile()
        .await?;
    assert!(
        rule_operators(&exact_profile.profile, "score").contains("LocyLineage"),
        "exact inference analyses the lineage"
    );
    assert_eq!(scores(&result), scores(&exact));
    assert_eq!(scores(&result).len(), (N * N / 2) as usize);
    Ok(())
}

/// What can read the derivations still gets them recorded.
#[tokio::test]
async fn derivations_are_recorded_when_something_reads_them() -> Result<()> {
    let db = open().await?;
    let session = db.session();
    // An EXPLAIN RULE of the program.
    let explained = format!("{RULES}EXPLAIN RULE score WHERE i.k = 0");
    let (_, profile) = session.locy_with(&explained).profile().await?;
    assert!(
        rule_operators(&profile.profile, "evidence").contains("LocyProvenance"),
        "EXPLAIN RULE reads derivations"
    );
    // Top-k proof retention.
    let queried = format!("{RULES}QUERY score RETURN i.k AS i, t.k AS t, prob");
    let (_, profile) = session
        .locy_with(&queried)
        .with_config(LocyConfig {
            top_k_proofs: 1,
            ..Default::default()
        })
        .profile()
        .await?;
    assert!(
        rule_operators(&profile.profile, "score").contains("LocyProvenance"),
        "top-k retention keeps derivations"
    );
    Ok(())
}

/// A fold whose rows read a probabilistic fact keeps its lineage, and its
/// shared-dependency warning, without recording derivations nobody reads.
#[tokio::test]
async fn a_fold_over_probabilistic_facts_keeps_lineage_only() -> Result<()> {
    let db = open().await?;
    let program = "CREATE RULE lit AS
  MATCH (t:Tag) WHERE t.k = 0
  YIELD KEY t, 0.2 AS probability PROB
CREATE RULE exposed AS
  MATCH (i:Item), (t:Tag) WHERE i.colour = t.colour AND t IS lit
  YIELD KEY i, probability AS risk PROB
CREATE RULE total AS
  MATCH (i:Item), (t:Tag) WHERE t.k = 0 AND i.colour = t.colour AND i IS exposed
  FOLD p = MNOR(risk)
  YIELD KEY t, p AS probability PROB
QUERY total RETURN t.k AS t, probability";
    let (result, profile) = db.session().locy_with(program).profile().await?;
    let operators = rule_operators(&profile.profile, "total");
    assert!(operators.contains("LocyLineage"), "{operators:?}");
    assert!(!operators.contains("LocyProvenance"), "{operators:?}");
    assert!(
        result
            .warnings()
            .iter()
            .any(|w| w.code.as_str() == "shared_probabilistic_dependency"),
        "{:?}",
        result.warnings()
    );
    Ok(())
}

async fn tagged(db: &Uni) -> Result<BTreeSet<(i64, i64)>> {
    let rows = db
        .session()
        .query("MATCH (i:Item)-[:TAGGED]->(t:Tag) RETURN i.k AS i, t.k AS t")
        .await?;
    Ok(rows
        .rows()
        .iter()
        .map(|r| (r.get::<i64>("i").unwrap(), r.get::<i64>("t").unwrap()))
        .collect())
}

/// `returned_rules` decides what the result carries, not what the commands
/// read or write: a `DERIVE` over rules the result leaves out writes the
/// same edges, and the statistics still count every fact.
#[tokio::test]
async fn returned_rules_narrow_the_result_not_the_commands() -> Result<()> {
    let program = format!("{RULES}DERIVE link\nQUERY chosen RETURN i.k AS i, t.k AS t");
    let mut outcomes = Vec::new();
    for returned in [
        ReturnedRules::All,
        ReturnedRules::Nothing,
        ReturnedRules::Only(vec!["score".to_string()]),
    ] {
        let db = open().await?;
        let tx = db.session().tx().await?;
        let result = tx
            .locy_with(&program)
            .with_config(LocyConfig {
                returned_rules: returned.clone(),
                ..Default::default()
            })
            .run()
            .await?;
        let rules: BTreeSet<String> = result.derived_rules().await?.keys().cloned().collect();
        let want: BTreeSet<String> = match &returned {
            ReturnedRules::All => ["evidence", "score", "chosen"].map(String::from).into(),
            ReturnedRules::Nothing => BTreeSet::new(),
            ReturnedRules::Only(names) => names.iter().cloned().collect(),
        };
        assert_eq!(rules, want, "{returned:?}");
        if let Some(rows) = result.derived_rule("score").await? {
            assert!(
                rows.iter()
                    .all(|row| matches!(row.get("i"), Some(Value::Node(_)))),
                "{returned:?}: returned facts are hydrated"
            );
        }
        let queried = result.rows().expect("QUERY rows").len();
        let derived_nodes = result.stats().derived_nodes;
        tx.commit().await?;
        outcomes.push((tagged(&db).await?, queried, derived_nodes));
    }
    assert_eq!(outcomes[0].0.len(), N as usize, "one tag per item");
    assert_eq!(outcomes[0], outcomes[1]);
    assert_eq!(outcomes[0], outcomes[2]);
    Ok(())
}

/// A `DERIVE` of several facts is one statement, and still counts its facts.
#[tokio::test]
async fn derive_writes_its_facts_in_one_statement() -> Result<()> {
    let db = open().await?;
    let tx = db.session().tx().await?;
    let program = format!("{RULES}DERIVE link");
    let (result, profile) = tx.locy_with(&program).profile().await?;
    assert_eq!(result.stats().mutations_executed, N as usize);
    let [command] = profile.profile.commands.as_slice() else {
        panic!("one command: {:?}", profile.profile.commands);
    };
    assert_eq!(command.kind, "DERIVE");
    assert_eq!(command.facts_written, N as usize);
    // One conversion and one hydration for the commands, of `chosen` alone.
    let initial: Vec<_> = profile
        .profile
        .phases
        .iter()
        .filter(|p| p.pass == "initial")
        .collect();
    assert!(
        initial
            .iter()
            .all(|p| p.rules == 1 && p.facts == N as usize),
        "{initial:?}"
    );
    tx.commit().await?;
    let edges = tagged(&db).await?;
    assert_eq!(edges.len(), N as usize);
    assert!(edges.iter().all(|(i, t)| i % 2 == t % 2), "{edges:?}");
    Ok(())
}
