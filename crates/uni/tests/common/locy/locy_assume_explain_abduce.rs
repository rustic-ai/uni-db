// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `EXPLAIN RULE` and `ABDUCE` inside an `ASSUME ... THEN` body, and the
//! probabilities a body reads.
//!
//! Both commands used to be refused there, for three gaps: a hypothetical
//! re-evaluation recorded no provenance, so a body `EXPLAIN` could only
//! rebuild a tree by re-execution; `ABDUCE` validated its candidates against
//! the parent's strata, leaving out the body's own rules; and the program
//! asked for a provenance tracker only when a top-level command needed one.
//!
//! The same missing tracker gave a wrong answer with no error: under
//! `exact_probability` the correction for shared proofs runs only with a
//! tracker, so a body `QUERY` read the independence approximation while the
//! same `QUERY` at the top level read the exact value.
//!
//! The oracle for each body command is the command run at the top level of
//! a program in a transaction that applied the ASSUME's mutation first and
//! is rolled back afterwards. Probabilities are also hand-computed.

// Rust guideline compliant

use anyhow::{Result, bail};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni, Value};
use uni_locy::{AbductionResult, DerivationNode, Modification};

/// `A -> B`, `A -> C`, `B -> D` over `EDGE`.
async fn seeded() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'}), \
         (d:Node {name: 'D'}), (a)-[:EDGE]->(b), (a)-[:EDGE]->(c), (b)-[:EDGE]->(d)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

const RULES: &str = "\
CREATE RULE linked AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b\n\
CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b\n\
CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(m:Node) WHERE m IS reach TO b YIELD KEY a, KEY b\n";

/// A new node `X` with an edge `D -> X`.
const ADD_X: &str = "MATCH (d:Node {name: 'D'}) CREATE (d)-[:EDGE]->(:Node {name: 'X'})";

/// A new node `X` with two edges into it, `D -> X` and `C -> X`: `A` reaches
/// `X` along two paths.
const ADD_X_TWICE: &str = "MATCH (d:Node {name: 'D'}), (c:Node {name: 'C'}) \
    CREATE (d)-[:EDGE]->(x:Node {name: 'X'}), (c)-[:EDGE]->(x)";

/// A body rule over a parent rule: the nodes that reach `X`.
const REACHES_X: &str =
    "CREATE RULE reaches_x AS MATCH (a:Node) WHERE a IS reach TO b, b.name = 'X' YIELD KEY a\n";

fn node_name(v: &Value) -> String {
    match v {
        Value::Node(n) => match n.properties.get("name") {
            Some(Value::String(s)) => s.clone(),
            other => format!("{other:?}"),
        },
        other => format!("{other:?}"),
    }
}

/// A derivation tree as nested text: each node's rule and its node
/// bindings by name, children sorted.
fn render_tree(node: &DerivationNode) -> String {
    let mut bindings: Vec<String> = node
        .bindings
        .iter()
        .filter(|(_, v)| matches!(v, Value::Node(_)))
        .map(|(k, v)| format!("{k}={}", node_name(v)))
        .collect();
    bindings.sort();
    let mut children: Vec<String> = node.children.iter().map(render_tree).collect();
    children.sort();
    format!(
        "{}[{}]{{{}}}",
        node.rule,
        bindings.join(","),
        children.join(";")
    )
}

/// Each candidate as its kind, edge type and endpoints by name, validated or
/// not, sorted. Endpoints are resolved through `names`, the seeded nodes; a
/// node a mutation created is `new`, since its id differs between the
/// hypothetical state and the oracle's transaction.
fn render_abduce(
    result: &AbductionResult,
    names: &std::collections::HashMap<u64, String>,
) -> Vec<String> {
    // A node the mutation created has a fresh id in each state.
    let name = |vid: &Option<uni_common::core::id::Vid>| {
        vid.and_then(|v| names.get(&v.as_u64()).cloned())
            .unwrap_or_else(|| "new".to_string())
    };
    let mut out: Vec<String> = result
        .modifications
        .iter()
        .map(|m| {
            let what = match &m.modification {
                Modification::RemoveEdge {
                    edge_type,
                    source_vid,
                    target_vid,
                    ..
                } => format!(
                    "remove {edge_type} {}->{}",
                    name(source_vid),
                    name(target_vid)
                ),
                other => format!("{other:?}"),
            };
            format!("{what} validated={}", m.validated)
        })
        .collect();
    out.sort();
    out
}

/// Every seeded node's id, by name.
async fn seeded_names(db: &Uni) -> Result<std::collections::HashMap<u64, String>> {
    let rows = db
        .session()
        .query("MATCH (n:Node) RETURN id(n) AS id, n.name AS name")
        .await?;
    Ok(rows
        .rows()
        .iter()
        .map(|r| Ok((r.get::<i64>("id")? as u64, r.get::<String>("name")?)))
        .collect::<Result<_>>()?)
}

/// The single body result of a program's single ASSUME.
async fn body_results(db: &Uni, program: &str, config: LocyConfig) -> Result<Vec<CommandResult>> {
    let result = db
        .session()
        .locy_with(program)
        .with_config(config)
        .run()
        .await?;
    match result.command_results.as_slice() {
        [CommandResult::Assume { results, .. }] => Ok(results.clone()),
        other => bail!("expected one ASSUME result, got {other:?}"),
    }
}

/// The oracle: `mutation` then `program` at the top level, in a
/// transaction that is rolled back.
async fn top_level(
    db: &Uni,
    mutations: &[&str],
    program: &str,
    config: LocyConfig,
) -> Result<Vec<CommandResult>> {
    let tx = db.session().tx().await?;
    for m in mutations {
        tx.execute(m).await?;
    }
    let result = tx.locy_with(program).with_config(config).run().await?;
    tx.rollback();
    Ok(result.command_results.clone())
}

fn explain_of(result: &CommandResult) -> Result<String> {
    match result {
        CommandResult::Explain(node) => Ok(render_tree(node)),
        other => bail!("expected an EXPLAIN result, got {other:?}"),
    }
}

fn abduce_of(result: &CommandResult) -> Result<&AbductionResult> {
    match result {
        CommandResult::Abduce(ab) => Ok(ab),
        other => bail!("expected an ABDUCE result, got {other:?}"),
    }
}

/// A body `EXPLAIN` explains the hypothetical facts: `reach(A, X)` exists
/// only after the mutation, through `B` and `D`. Same tree as a top-level
/// `EXPLAIN` after the same mutation, from recorded provenance.
#[tokio::test]
async fn explain_in_an_assume_body_explains_the_hypothetical_facts() -> Result<()> {
    let db = seeded().await?;
    let explain = "EXPLAIN RULE reach WHERE a.name = 'A' AND b.name = 'X'";
    let body = body_results(
        &db,
        &format!("{RULES}ASSUME {{ {ADD_X} }} THEN {{ {explain} }}"),
        LocyConfig::default(),
    )
    .await?;
    let oracle = top_level(
        &db,
        &[ADD_X],
        &format!("{RULES}{explain}"),
        LocyConfig::default(),
    )
    .await?;
    let (got, want) = (explain_of(&body[0])?, explain_of(&oracle[0])?);
    assert_eq!(got, want);
    // Hand-checked shape: the root and the one derivation of reach(A, X),
    // from recorded provenance, with the fact each derivation references
    // below it: reach(B, X), then reach(D, X), the edge D -> X. (Before
    // Wave Q a reference to a rule without a PROB column was not listed, so
    // the derivation had no children.)
    assert_eq!(
        want,
        "reach[]{reach[a=A,b=X]{reach[a=B,b=X]{reach[a=D,b=X]{}}}}"
    );
    Ok(())
}

/// A body `ABDUCE NOT` proposes and validates removals in the hypothetical
/// state, as at the top level after the same mutation: only removing an edge
/// on the one path `A -> B -> D -> X` breaks `reach(A, X)`.
#[tokio::test]
async fn abduce_in_an_assume_body_validates_in_the_hypothetical_state() -> Result<()> {
    let db = seeded().await?;
    let abduce = "ABDUCE NOT reach WHERE a.name = 'A' AND b.name = 'X'";
    let body = body_results(
        &db,
        &format!("{RULES}ASSUME {{ {ADD_X} }} THEN {{ {abduce} }}"),
        LocyConfig::default(),
    )
    .await?;
    let oracle = top_level(
        &db,
        &[ADD_X],
        &format!("{RULES}{abduce}"),
        LocyConfig::default(),
    )
    .await?;
    let names_body = seeded_names(&db).await?;
    let got = render_abduce(abduce_of(&body[0])?, &names_body);
    let want = render_abduce(abduce_of(&oracle[0])?, &names_body);
    assert_eq!(got, want);
    assert!(
        want.iter().any(|m| m.contains("validated=true")),
        "some removal breaks reach(A, X): {want:?}"
    );
    assert!(
        want.iter()
            .all(|m| !m.contains("A->C") || m.contains("validated=false")),
        "removing A -> C cannot break reach(A, X): {want:?}"
    );
    Ok(())
}

/// A body `ABDUCE` over the body's own rule validates against that rule.
/// `A` reaches `X` along two paths, so no single removal breaks
/// `reaches_x(A)`. Validating against the parent's strata alone found
/// `reaches_x` empty in every candidate state, so every removal "validated".
#[tokio::test]
async fn abduce_in_an_assume_body_validates_against_the_bodys_rules() -> Result<()> {
    let db = seeded().await?;
    let abduce = "ABDUCE NOT reaches_x WHERE a.name = 'A'";
    let body = body_results(
        &db,
        &format!("{RULES}ASSUME {{ {ADD_X_TWICE} }} THEN {{ {REACHES_X}{abduce} }}"),
        LocyConfig::default(),
    )
    .await?;
    let oracle = top_level(
        &db,
        &[ADD_X_TWICE],
        &format!("{RULES}{REACHES_X}{abduce}"),
        LocyConfig::default(),
    )
    .await?;
    let names = seeded_names(&db).await?;
    let got = render_abduce(abduce_of(&body[0])?, &names);
    let want = render_abduce(abduce_of(&oracle[0])?, &names);
    assert_eq!(got, want);
    assert!(!want.is_empty(), "candidates are proposed");
    assert!(
        want.iter().all(|m| m.contains("validated=false")),
        "no single removal breaks reaches_x(A): {want:?}"
    );
    Ok(())
}

/// A nested ASSUME's `ABDUCE` reads both mutations.
#[tokio::test]
async fn abduce_in_a_nested_assume_body_reads_both_mutations() -> Result<()> {
    let db = seeded().await?;
    let add_y = "MATCH (x:Node {name: 'X'}) CREATE (x)-[:EDGE]->(:Node {name: 'Y'})";
    let abduce = "ABDUCE NOT reach WHERE a.name = 'A' AND b.name = 'Y'";
    let program = format!(
        "{RULES}ASSUME {{ {ADD_X} }} THEN {{ QUERY linked RETURN a.name AS a \
         ASSUME {{ {add_y} }} THEN {{ {abduce} }} }}"
    );
    let outer = body_results(&db, &program, LocyConfig::default()).await?;
    let inner = match outer.as_slice() {
        [
            CommandResult::Query(_),
            CommandResult::Assume { results, .. },
        ] => results.clone(),
        other => bail!("unexpected body results {other:?}"),
    };
    let oracle = top_level(
        &db,
        &[ADD_X, add_y],
        &format!("{RULES}{abduce}"),
        LocyConfig::default(),
    )
    .await?;
    let names = seeded_names(&db).await?;
    let got = render_abduce(abduce_of(&inner[0])?, &names);
    let want = render_abduce(abduce_of(&oracle[0])?, &names);
    assert_eq!(got, want);
    assert!(
        want.iter().any(|m| m.contains("validated=true")),
        "{want:?}"
    );
    Ok(())
}

// ── Probabilities inside a body ──────────────────────────────────────────

/// `reach` with a noisy-OR over the edges' `p`, a recursive derivation
/// multiplying the referenced `reach` in.
const PROB_REACH: &str = "\
CREATE RULE preach AS MATCH (x:P)-[e:W]->(y:P) FOLD p = MNOR(e.p) YIELD KEY x, KEY y, p\n\
CREATE RULE preach AS MATCH (x:P)-[e:W]->(m:P) WHERE m IS preach TO y \
FOLD p = MNOR(e.p) YIELD KEY x, KEY y, p\n";

const Q_AD: &str = "QUERY preach WHERE x.name = 'A' AND y.name = 'D' RETURN p";

/// `A -> B`, `A -> C`, `B -> C`, `B -> D`, `C -> D`, every edge 0.5.
async fn seeded_prob() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("W", &["P"], &["P"])
        .property("p", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:P {name: 'A'}), (b:P {name: 'B'}), (c:P {name: 'C'}), (d:P {name: 'D'}), \
         (a)-[:W {p: 0.5}]->(b), (a)-[:W {p: 0.5}]->(c), (b)-[:W {p: 0.5}]->(c), \
         (b)-[:W {p: 0.5}]->(d), (c)-[:W {p: 0.5}]->(d)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

fn single_p(rows: &CommandResult) -> Result<f64> {
    match rows {
        CommandResult::Query(rows) if rows.len() == 1 => match rows[0].get("p") {
            Some(Value::Float(p)) => Ok(*p),
            other => bail!("p is {other:?}"),
        },
        other => bail!("expected one row, got {other:?}"),
    }
}

/// `preach(A, D)` is the probability that `D` is reachable from `A`:
/// `(AB ∧ (BD ∨ (BC ∧ CD))) ∨ (AC ∧ CD)` with every edge 0.5, which is
/// 0.46875 (condition on `CD`: 0.5 × 0.6875 + 0.5 × 0.25). The
/// independence approximation is 1 − (1 − 0.5 × 0.625)(1 − 0.25) =
/// 0.484375. A body `QUERY` reads the same value as a top-level one, exact
/// or not, under a mutation no rule reads.
#[tokio::test]
async fn a_body_query_reads_the_exact_probability() -> Result<()> {
    let db = seeded_prob().await?;
    for (exact, want) in [(true, 0.46875), (false, 0.484375)] {
        let config = || LocyConfig {
            max_iterations: 1000,
            exact_probability: exact,
            ..Default::default()
        };
        let top = db
            .session()
            .locy_with(&format!("{PROB_REACH}{Q_AD}"))
            .with_config(config())
            .run()
            .await?;
        let top_p = single_p(&top.command_results[0])?;
        let body = body_results(
            &db,
            &format!("{PROB_REACH}ASSUME {{ CREATE (:Isolated {{name: 'i'}}) }} THEN {{ {Q_AD} }}"),
            config(),
        )
        .await?;
        let body_p = single_p(&body[0])?;
        assert!(
            (top_p - want).abs() < 1e-9,
            "exact={exact}: top-level {top_p}, expected {want}"
        );
        assert!(
            (body_p - want).abs() < 1e-9,
            "exact={exact}: ASSUME body {body_p}, expected {want}"
        );
    }
    Ok(())
}

/// A body `EXPLAIN` over a probabilistic rule shows the same tree as the
/// top-level `EXPLAIN` after the same mutation.
#[tokio::test]
async fn a_body_explain_of_a_probabilistic_rule_matches_the_top_level() -> Result<()> {
    let db = seeded_prob().await?;
    let explain = "EXPLAIN RULE preach WHERE x.name = 'A' AND y.name = 'D'";
    let mutation = "MATCH (a:P {name: 'A'}), (d:P {name: 'D'}) CREATE (a)-[:W {p: 0.5}]->(d)";
    let config = || LocyConfig {
        max_iterations: 1000,
        exact_probability: true,
        ..Default::default()
    };
    let body = body_results(
        &db,
        &format!("{PROB_REACH}ASSUME {{ {mutation} }} THEN {{ {explain} }}"),
        config(),
    )
    .await?;
    let oracle = top_level(
        &db,
        &[mutation],
        &format!("{PROB_REACH}{explain}"),
        config(),
    )
    .await?;
    assert_eq!(explain_of(&body[0])?, explain_of(&oracle[0])?);
    Ok(())
}

// ── L5: CALIBRATE after a write ──────────────────────────────────────────

/// `CALIBRATE` after a Cypher write in the same program collects over the
/// state the write left: 20 samples seeded, 20 more written, 40 collected
/// (10 of them held out at `HOLDOUT 0.25`), at session and transaction level.
#[tokio::test]
async fn calibrate_after_a_write_reads_the_written_state() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let seed = db.session().tx().await?;
    seed.execute("UNWIND range(0, 19) AS i CREATE (:Sample {idx: i, label: i % 2 = 0})")
        .await?;
    seed.commit().await?;
    let config = || {
        let classifier: std::sync::Arc<dyn uni_locy::NeuralClassifier> =
            std::sync::Arc::new(uni_locy::MockClassifier::constant("classify/scorer", 0.95));
        let mut config = LocyConfig::default();
        config
            .classifier_registry
            .insert("scorer".to_string(), classifier);
        config
    };
    let program = "CREATE MODEL scorer AS INPUT (s) OUTPUT PROB risk USING xervo('classify/scorer')\n\
                   UNWIND range(20, 39) AS i CREATE (:Sample {idx: i, label: i % 2 = 0})\n\
                   CALIBRATE scorer ON MATCH (s:Sample) TARGET s.label METHOD platt_scaling HOLDOUT 0.25";
    let session = db
        .session()
        .locy_with(program)
        .with_config(config())
        .run()
        .await?;
    let tx = db.session().tx().await?;
    let in_tx = tx.locy_with(program).with_config(config()).run().await?;
    tx.rollback();
    for (what, result) in [("session", &session), ("tx", &in_tx)] {
        match result.command_results.as_slice() {
            [CommandResult::Cypher(_), CommandResult::Calibrate(c)] => {
                assert_eq!((c.n_samples, c.holdout_size), (40, 10), "{what}");
            }
            other => bail!("{what}: unexpected results {other:?}"),
        }
    }
    // The control: CALIBRATE alone collects the 20 seeded samples.
    let alone = db
        .session()
        .locy_with(
            "CREATE MODEL scorer AS INPUT (s) OUTPUT PROB risk USING xervo('classify/scorer')\n\
             CALIBRATE scorer ON MATCH (s:Sample) TARGET s.label METHOD platt_scaling HOLDOUT 0.25",
        )
        .with_config(config())
        .run()
        .await?;
    match alone.command_results.as_slice() {
        [CommandResult::Calibrate(c)] => assert_eq!(c.n_samples, 20),
        other => bail!("unexpected results {other:?}"),
    }
    Ok(())
}
