// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #162: a PROB fold is correct in its own rule but arrives one factor
//! short at the rule that consumes it, and only when the folded inputs were
//! numerically equal.
//!
//! ```text
//! TOP --CONTAINS--> MID --CONTAINS--> L1   (supplied, reliability 0.5)
//!                       --CONTAINS--> L2   (supplied, reliability 0.5)
//! ```
//!
//! `MID = MPROD(0.5, 0.5) = 0.25` is right; `TOP = MPROD(MID)` reports 0.5.
//! With one leaf changed to 0.4 both rules agree, so equality of the folded
//! values is the trigger, not the depth or the shape.

use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::LocyConfig;

const PROGRAM: &str = "\
CREATE RULE avail AS \
  MATCH (p:P)<-[s:SUPPLIES]-(v:V) \
  FOLD a = MNOR(s.r) \
  YIELD KEY p, a PROB \n\
CREATE RULE assembly AS \
  MATCH (p:P)-[:CONTAINS]->(:P) \
  YIELD KEY p \n\
CREATE RULE build AS \
  MATCH (p:P) \
  WHERE p IS avail, p IS NOT assembly \
  YIELD KEY p, a AS b PROB \n\
CREATE RULE build AS \
  MATCH (p:P)-[:CONTAINS]->(c:P) \
  WHERE c IS build \
  FOLD b = MPROD(b) \
  YIELD KEY p, b PROB";

fn default_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

/// Same shape, but the recursive clause scales each child contribution before
/// multiplying. `MPROD` is associative, so a consumer that folds a child's
/// *contribution rows* instead of the child's *folded value* still lands on the
/// right answer for `PROGRAM` whenever the values differ — the scaling breaks
/// that coincidence and tells the two apart.
const PROGRAM_SCALED: &str = "\
CREATE RULE avail AS \
  MATCH (p:P)<-[s:SUPPLIES]-(v:V) \
  FOLD a = MNOR(s.r) \
  YIELD KEY p, a PROB \n\
CREATE RULE assembly AS \
  MATCH (p:P)-[:CONTAINS]->(:P) \
  YIELD KEY p \n\
CREATE RULE build AS \
  MATCH (p:P) \
  WHERE p IS avail, p IS NOT assembly \
  YIELD KEY p, a AS b PROB \n\
CREATE RULE build AS \
  MATCH (p:P)-[:CONTAINS]->(c:P) \
  WHERE c IS build \
  FOLD b = MPROD(b * 0.5) \
  YIELD KEY p, b PROB";

/// Build `TOP -> MID -> {L1, L2}` with the given leaf reliabilities and return
/// each part's derived `build` probability.
async fn run(r1: f64, r2: f64) -> Result<Vec<(String, f64)>> {
    run_program(PROGRAM, r1, r2).await
}

async fn run_program(program: &str, r1: f64, r2: f64) -> Result<Vec<(String, f64)>> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (top:P {{s: 'TOP'}}), (mid:P {{s: 'MID'}}), \
                (l1:P {{s: 'L1'}}), (l2:P {{s: 'L2'}}), (v:V {{n: 'v'}}), \
                (top)-[:CONTAINS]->(mid), (mid)-[:CONTAINS]->(l1), \
                (mid)-[:CONTAINS]->(l2), \
                (v)-[:SUPPLIES {{r: {r1}}}]->(l1), \
                (v)-[:SUPPLIES {{r: {r2}}}]->(l2)"
    ))
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(program)
        .with_config(default_config())
        .run()
        .await?;

    let build = result
        .derived
        .get("build")
        .expect("rule 'build' produced no derived facts");

    let mut out = Vec::new();
    for row in build {
        let name = match row.get("p") {
            Some(uni_common::Value::Node(n)) => n
                .properties
                .get("s")
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| format!("{:?}", row.get("p"))),
            other => format!("{other:?}"),
        };
        let prob = match row.get("b") {
            Some(uni_common::Value::Float(f)) => *f,
            other => panic!("expected Float 'b' for {name}, got {other:?}"),
        };
        out.push((name, prob));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn prob_of(rows: &[(String, f64)], part: &str) -> f64 {
    rows.iter()
        .find(|(n, _)| n == part)
        .unwrap_or_else(|| panic!("no build fact for {part} in {rows:?}"))
        .1
}

#[tokio::test]
async fn prob_fold_over_equal_values_reaches_its_consumer_intact() -> Result<()> {
    // Control: unequal leaves. Both MID and TOP must be 0.5 * 0.4 = 0.2.
    let unequal = run(0.5, 0.4).await?;
    assert!(
        (prob_of(&unequal, "MID") - 0.2).abs() < 1e-9,
        "control MID: expected 0.2, got {unequal:?}"
    );
    assert!(
        (prob_of(&unequal, "TOP") - 0.2).abs() < 1e-9,
        "control TOP: expected 0.2, got {unequal:?}"
    );

    // Equal leaves. Both MID and TOP must be 0.5 * 0.5 = 0.25.
    let equal = run(0.5, 0.5).await?;
    assert!(
        (prob_of(&equal, "MID") - 0.25).abs() < 1e-9,
        "MID: expected 0.25, got {equal:?}"
    );
    assert!(
        (prob_of(&equal, "TOP") - 0.25).abs() < 1e-9,
        "TOP consumes MID's folded 0.25, so MPROD over the single child is \
         0.25; got {equal:?}"
    );

    Ok(())
}

/// Diagnostic: does the consuming clause fold the child's *folded value*, or
/// the child's *contribution rows*?
///
/// Leaves 0.5 and 0.4 (deliberately unequal, so row dedup is out of the
/// picture). Under `MPROD(b * 0.5)`:
///
/// * `MID = (0.5 * 0.5) * (0.4 * 0.5) = 0.05`, either way.
/// * folding MID's value:            `TOP = 0.05 * 0.5  = 0.025`
/// * folding MID's contribution rows: `TOP = 0.125 * 0.1 = 0.0125`
#[tokio::test]
async fn consumer_folds_the_childs_value_not_its_contribution_rows() -> Result<()> {
    let rows = run_program(PROGRAM_SCALED, 0.5, 0.4).await?;
    assert!(
        (prob_of(&rows, "MID") - 0.05).abs() < 1e-9,
        "MID: expected 0.05, got {rows:?}"
    );
    assert!(
        (prob_of(&rows, "TOP") - 0.025).abs() < 1e-9,
        "TOP must fold MID's folded value (0.05 * 0.5 = 0.025); 0.0125 would \
         mean it folded MID's two pre-fold contribution rows. Got {rows:?}"
    );
    Ok(())
}

/// Scope check: a consumer in a *later stratum* reads the published, folded
/// facts, so it should be unaffected. `MID` folds to 0.05; `report` scales and
/// re-folds the single value, so `report(MID) = 0.025`. 0.0125 would mean the
/// pre-fold rows leaked across the stratum boundary too.
#[tokio::test]
async fn a_later_stratum_consumer_sees_the_folded_value() -> Result<()> {
    let program = format!(
        "{PROGRAM_SCALED}\nCREATE RULE report AS \
         MATCH (p:P) WHERE p IS build FOLD b = MPROD(b * 0.5) YIELD KEY p, b PROB"
    );
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (top:P {s: 'TOP'}), (mid:P {s: 'MID'}), \
                (l1:P {s: 'L1'}), (l2:P {s: 'L2'}), (v:V {n: 'v'}), \
                (top)-[:CONTAINS]->(mid), (mid)-[:CONTAINS]->(l1), \
                (mid)-[:CONTAINS]->(l2), \
                (v)-[:SUPPLIES {r: 0.5}]->(l1), (v)-[:SUPPLIES {r: 0.4}]->(l2)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(&program)
        .with_config(default_config())
        .run()
        .await?;
    let report = result.derived.get("report").expect("no 'report' facts");
    let mid = report
        .iter()
        .find(|r| {
            matches!(r.get("p"), Some(uni_common::Value::Node(n))
                if n.properties.get("s").and_then(|v| v.as_str()) == Some("MID"))
        })
        .expect("no report fact for MID");
    let got = match mid.get("b") {
        Some(uni_common::Value::Float(f)) => *f,
        other => panic!("expected Float, got {other:?}"),
    };
    assert!(
        (got - 0.025).abs() < 1e-9,
        "cross-stratum consumer must see MID's folded 0.05, giving 0.025; got {got}"
    );
    Ok(())
}
