// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #314 repro: a classifier-backed PROB relation consumed through `IS`
//! by another rule. Controls: constant producer, direct classifier call.

use anyhow::Result;
use std::sync::Arc;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, Value};

const MODEL: &str = "CREATE MODEL delivery_model AS \
 INPUT (n) FEATURES n.route_days OUTPUT PROB risk \
 USING xervo('local/constant-v1') VERSION 'constant-v1' \n";
const PREDICTED: &str = "CREATE RULE predicted AS \
 MATCH (n:Supply) \
 YIELD KEY n, delivery_model(n.route_days) AS probability PROB \n";
const REVIEW: &str = "CREATE RULE reviewed AS \
 MATCH (r:Requirement)-[:USES]->(n:Supply) \
 WHERE n IS predicted, probability >= 0.5 \
 YIELD KEY r, probability \n\
 QUERY reviewed RETURN r.id AS requirement, probability";
const DIRECT: &str = "CREATE RULE reviewed AS \
 MATCH (r:Requirement)-[:USES]->(n:Supply) \
 YIELD KEY r, delivery_model(n.route_days) AS probability PROB \n\
 QUERY reviewed RETURN r.id AS requirement, probability";

fn config() -> LocyConfig {
    let classifier: Arc<dyn uni_locy::NeuralClassifier> =
        Arc::new(uni_locy::MockClassifier::constant("local/constant-v1", 0.7));
    let mut c = LocyConfig {
        max_iterations: 100,
        ..Default::default()
    };
    c.classifier_registry
        .insert("delivery_model".to_string(), classifier);
    c
}

async fn run(program: &str) -> Result<Vec<(String, f64)>> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (r:Requirement {id:'R'})-[:USES]->(n:Supply {id:'S', route_days:8})")
        .await?;
    tx.commit().await?;
    let r = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    Ok(r.rows()
        .expect("rows")
        .iter()
        .map(|row| {
            (
                row.get("requirement")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_string(),
                row.get("probability")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::NAN),
            )
        })
        .collect())
}

#[tokio::test]
async fn issue_314_constant_relation_control() -> Result<()> {
    let p = format!(
        "{MODEL}{}{REVIEW}",
        PREDICTED.replace("delivery_model(n.route_days)", "0.7")
    );
    assert_eq!(run(&p).await?, vec![("R".to_string(), 0.7)]);
    Ok(())
}

#[tokio::test]
async fn issue_314_direct_call_control() -> Result<()> {
    assert_eq!(
        run(&format!("{MODEL}{DIRECT}")).await?,
        vec![("R".to_string(), 0.7)]
    );
    Ok(())
}

/// Discriminating probe: a plain-variable feature argument lifts no hidden
/// `__feat_*` column (a literal is refused at compile time), so only the
/// consumer-side type of the model output column is exercised.
#[tokio::test]
async fn issue_314_probe_variable_feature_no_hidden_column() -> Result<()> {
    let p = format!(
        "{MODEL}{}{REVIEW}",
        PREDICTED.replace("delivery_model(n.route_days)", "delivery_model(n)")
    );
    let got = run(&p).await;
    println!("variable-feature producer via IS: {got:?}");
    assert_eq!(got?, vec![("R".to_string(), 0.7)]);
    Ok(())
}

/// The producer queried directly (no IS consumer) works: the hidden column
/// is stripped only on the user-visible row path.
#[tokio::test]
async fn issue_314_probe_query_producer_directly() -> Result<()> {
    let p = format!("{MODEL}{PREDICTED}QUERY predicted RETURN n.id AS requirement, probability");
    let got = run(&p).await;
    println!("producer queried directly: {got:?}");
    assert_eq!(got?, vec![("S".to_string(), 0.7)]);
    Ok(())
}

#[tokio::test]
async fn issue_314_classifier_relation_consumed_through_is() -> Result<()> {
    let got = run(&format!("{MODEL}{PREDICTED}{REVIEW}")).await;
    println!("classifier relation via IS: {got:?}");
    assert_eq!(got?, vec![("R".to_string(), 0.7)]);
    Ok(())
}

/// Runs `program` on the requirement/supply graph plus a customer ordering
/// the supply and a second supply it feeds, returning the raw result.
async fn run_result(program: &str) -> Result<uni_locy::LocyResult> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (r:Requirement {id:'R'})-[:USES]->(n:Supply {id:'S', route_days:8}), \
         (c:Customer {id:'C'})-[:ORDERS]->(n), \
         (n)-[:FEEDS]->(m:Supply {id:'T', route_days:3})",
    )
    .await?;
    tx.commit().await?;
    Ok(db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?
        .into_inner())
}

/// The value of `column` in the fact whose `key` node has id `id`.
fn float_of(rows: &[uni_locy::FactRow], key: &str, id: &str, column: &str) -> Option<f64> {
    rows.iter()
        .find(|r| match r.get(key) {
            Some(Value::Node(n)) => n.properties.get("id").and_then(Value::as_str) == Some(id),
            _ => false,
        })
        .and_then(|r| r.get(column))
        .and_then(Value::as_f64)
}

/// Two rules read the one model-scored relation, each getting the score.
#[tokio::test]
async fn issue_314_two_consumers_of_one_model_relation() -> Result<()> {
    let program = format!(
        "{MODEL}{PREDICTED}\
         CREATE RULE reviewed AS MATCH (r:Requirement)-[:USES]->(n:Supply) \
         WHERE n IS predicted, probability >= 0.5 YIELD KEY r, probability\n\
         CREATE RULE committed AS MATCH (c:Customer)-[:ORDERS]->(n:Supply) \
         WHERE n IS predicted YIELD KEY c, probability AS commitment\n"
    );
    let result = run_result(&program).await?;
    let reviewed = result.derived_rule("reviewed").await?.expect("reviewed");
    let committed = result.derived_rule("committed").await?.expect("committed");
    assert_eq!(float_of(reviewed, "r", "R", "probability"), Some(0.7));
    assert_eq!(float_of(committed, "c", "C", "commitment"), Some(0.7));
    Ok(())
}

/// The model output in arithmetic, in the producer and in a consumer: a
/// Float64 on both sides of `IS`.
#[tokio::test]
async fn issue_314_model_output_in_arithmetic() -> Result<()> {
    let program = format!(
        "{MODEL}CREATE RULE scaled AS MATCH (n:Supply) \
         YIELD KEY n, delivery_model(n.route_days) * 0.5 AS half\n\
         CREATE RULE doubled AS MATCH (r:Requirement)-[:USES]->(n:Supply) \
         WHERE n IS scaled YIELD KEY r, half * 4.0 AS twice\n"
    );
    let result = run_result(&program).await?;
    let scaled = result.derived_rule("scaled").await?.expect("scaled");
    let doubled = result.derived_rule("doubled").await?.expect("doubled");
    assert_eq!(float_of(scaled, "n", "S", "half"), Some(0.35));
    let twice = float_of(doubled, "r", "R", "twice").expect("twice");
    assert!((twice - 1.4).abs() < 1e-12, "{twice}");
    Ok(())
}

/// A recursive rule whose base clause is model-scored and whose recursive
/// clause reads itself through `IS`: the fixpoint's facts carry no feature
/// column either, and a third rule reads them.
#[tokio::test]
async fn issue_314_recursive_model_backed_rule() -> Result<()> {
    let program = format!(
        "{MODEL}CREATE RULE exposed AS MATCH (n:Supply) \
         YIELD KEY n, delivery_model(n.route_days) AS score\n\
         CREATE RULE exposed AS MATCH (m:Supply)-[:FEEDS]->(n:Supply) \
         WHERE m IS exposed YIELD KEY n, score\n\
         CREATE RULE flagged AS MATCH (r:Requirement)-[:USES]->(n:Supply) \
         WHERE n IS exposed, score >= 0.5 YIELD KEY r, score\n"
    );
    let result = run_result(&program).await?;
    let exposed = result.derived_rule("exposed").await?.expect("exposed");
    assert_eq!(float_of(exposed, "n", "S", "score"), Some(0.7));
    assert_eq!(float_of(exposed, "n", "T", "score"), Some(0.7));
    for row in exposed {
        assert!(
            row.keys()
                .all(|k| !k.starts_with(uni_locy::FEATURE_COL_PREFIX)),
            "a feature column reached a fact: {row:?}"
        );
    }
    let flagged = result.derived_rule("flagged").await?.expect("flagged");
    assert_eq!(float_of(flagged, "r", "R", "score"), Some(0.7));
    Ok(())
}

/// EXPLAIN still shows each fact's model call, though the facts no longer
/// carry the features it is looked up by: non-recursive and recursive.
#[tokio::test]
async fn issue_314_explain_keeps_model_provenance() -> Result<()> {
    let recursive = format!(
        "{MODEL}CREATE RULE predicted AS MATCH (n:Supply) \
         YIELD KEY n, delivery_model(n.route_days) AS probability\n\
         CREATE RULE predicted AS MATCH (m:Supply)-[:FEEDS]->(n:Supply) \
         WHERE m IS predicted YIELD KEY n, probability\n\
         EXPLAIN RULE predicted\n"
    );
    let non_recursive = format!("{MODEL}{PREDICTED}EXPLAIN RULE predicted\n");
    for (label, program) in [("non-recursive", non_recursive), ("recursive", recursive)] {
        let result = run_result(&program).await?;
        let node = result
            .command_results
            .iter()
            .find_map(|c| match c {
                uni_locy::CommandResult::Explain(node) => Some(node),
                _ => None,
            })
            .expect("an EXPLAIN result");
        let scored: Vec<_> = node
            .children
            .iter()
            .filter(|c| c.clause_index == 0)
            .collect();
        assert!(!scored.is_empty(), "{label}: no base-clause derivation");
        for child in scored {
            let call = child
                .neural_calls
                .iter()
                .find(|c| c.model_name == "delivery_model")
                .unwrap_or_else(|| panic!("{label}: no model call on {child:?}"));
            assert!(
                (call.raw_probability - 0.7).abs() < 1e-12,
                "{label}: {call:?}"
            );
        }
    }
    Ok(())
}
