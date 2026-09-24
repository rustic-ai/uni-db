// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Diagnostic matrix for issue #162: which BOM shapes does the recursive
//! `MPROD` rollup get wrong? Prints a table; asserts nothing, so one run maps
//! the whole surface.

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

/// `edges`: parent → child CONTAINS pairs. `leaves`: (part, reliability).
async fn run(edges: &[(&str, &str)], leaves: &[(&str, f64)]) -> Result<Vec<(String, f64)>> {
    run_with(PROGRAM, edges, leaves, "build").await
}

/// As [`run`], but with an explicit program and derived relation.
async fn run_with(
    program: &str,
    edges: &[(&str, &str)],
    leaves: &[(&str, f64)],
    relation: &str,
) -> Result<Vec<(String, f64)>> {
    let db = Uni::in_memory().build().await?;
    let mut parts: Vec<&str> = edges
        .iter()
        .flat_map(|(a, b)| [*a, *b])
        .chain(leaves.iter().map(|(p, _)| *p))
        .collect();
    parts.sort_unstable();
    parts.dedup();

    let mut cypher = String::from("CREATE (v:V {n: 'v'})");
    for p in &parts {
        cypher.push_str(&format!(", (n_{p}:P {{s: '{p}'}})"));
    }
    for (a, b) in edges {
        cypher.push_str(&format!(", (n_{a})-[:CONTAINS]->(n_{b})"));
    }
    for (p, r) in leaves {
        cypher.push_str(&format!(", (v)-[:SUPPLIES {{r: {r}}}]->(n_{p})"));
    }

    let tx = db.session().tx().await?;
    tx.execute(&cypher).await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(program)
        .with_config(LocyConfig {
            max_iterations: 1000,
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await?;

    let mut out = Vec::new();
    for row in result.derived.get(relation).into_iter().flatten() {
        let name = match row.get("p") {
            Some(uni_common::Value::Node(n)) => n
                .properties
                .get("s")
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default(),
            _ => continue,
        };
        if let Some(uni_common::Value::Float(f)) = row.get("b") {
            out.push((name, (f * 1e9).round() / 1e9));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

#[tokio::test]
async fn issue_162_shape_matrix() -> Result<()> {
    // (label, edges, leaves, node under test, value both readings agree on)
    #[allow(clippy::type_complexity)]
    let cases: Vec<(&str, Vec<(&str, &str)>, Vec<(&str, f64)>, &str, f64)> = vec![
        (
            "S1 reported: TOP->MID->{L1,L2}, equal",
            vec![("TOP", "MID"), ("MID", "L1"), ("MID", "L2")],
            vec![("L1", 0.5), ("L2", 0.5)],
            "TOP",
            0.25,
        ),
        (
            "S2 control:  TOP->MID->{L1,L2}, unequal",
            vec![("TOP", "MID"), ("MID", "L1"), ("MID", "L2")],
            vec![("L1", 0.5), ("L2", 0.4)],
            "TOP",
            0.2,
        ),
        (
            "S3 flat:     TOP->{L1,L2}, equal",
            vec![("TOP", "L1"), ("TOP", "L2")],
            vec![("L1", 0.5), ("L2", 0.5)],
            "TOP",
            0.25,
        ),
        (
            "S4 chain3:   TOP->MID->X->{L1,L2}, equal",
            vec![("TOP", "MID"), ("MID", "X"), ("X", "L1"), ("X", "L2")],
            vec![("L1", 0.5), ("L2", 0.5)],
            "TOP",
            0.25,
        ),
        (
            "S5 bushy:    TOP->{A,B}, A->{L1,L2}, B->{L3,L4}, equal",
            vec![
                ("TOP", "A"),
                ("TOP", "B"),
                ("A", "L1"),
                ("A", "L2"),
                ("B", "L3"),
                ("B", "L4"),
            ],
            vec![("L1", 0.5), ("L2", 0.5), ("L3", 0.5), ("L4", 0.5)],
            "TOP",
            0.0625,
        ),
        (
            "S6 3 kids:   TOP->MID->{L1,L2,L3}, equal",
            vec![("TOP", "MID"), ("MID", "L1"), ("MID", "L2"), ("MID", "L3")],
            vec![("L1", 0.5), ("L2", 0.5), ("L3", 0.5)],
            "TOP",
            0.125,
        ),
        (
            "S7 mixed:    TOP->{MID,L3}, MID->{L1,L2}, equal",
            vec![("TOP", "MID"), ("TOP", "L3"), ("MID", "L1"), ("MID", "L2")],
            vec![("L1", 0.5), ("L2", 0.5), ("L3", 0.5)],
            "TOP",
            0.125,
        ),
    ];

    println!("\n{:<48} {:>10} {:>10}  {}", "shape", "want", "got", "all");
    let mut bad = 0;
    for (label, edges, leaves, node, want) in cases {
        let rows = run(&edges, &leaves).await?;
        let got = rows
            .iter()
            .find(|(n, _)| n == node)
            .map(|(_, v)| *v)
            .unwrap_or(f64::NAN);
        let ok = (got - want).abs() < 1e-9;
        if !ok {
            bad += 1;
        }
        println!(
            "{label:<48} {want:>10} {got:>10}  {}  {rows:?}",
            if ok { "ok " } else { "BAD" }
        );
    }
    println!("\n{bad} shape(s) wrong\n");
    Ok(())
}

/// Termination check for the per-iteration folded view: on a cycle a folded
/// value can now move in place every iteration, where before it was pinned by
/// whole-row dedup over a finite discriminator domain. `MPROD` decays toward 0
/// and `MNOR` saturates toward 1, so both must settle within the 12-decimal
/// rounding the merge applies — not spin to `max_iterations`.
#[tokio::test]
async fn issue_162_cyclic_recursive_fold_terminates() -> Result<()> {
    // A -> B -> C -> A, with A additionally containing a supplied leaf.
    let rows = run(
        &[("A", "B"), ("B", "C"), ("C", "A"), ("A", "L")],
        &[("L", 0.5)],
    )
    .await?;
    println!("cyclic MPROD: {rows:?}");
    for (name, v) in &rows {
        assert!(
            v.is_finite() && (0.0..=1.0).contains(v),
            "{name} left the probability domain: {v}"
        );
    }
    Ok(())
}

/// ALONG on one clause must not disable the folded view on a *sibling* clause.
///
/// The view is chosen per clause, not per rule: here the base clause carries
/// `ALONG` (no self-reference, so nothing to choose) while the recursive clause
/// folds an inherited value and therefore reads the folded view. A rule-level
/// "has ALONG ⇒ opt out" would put `TOP` back at 0.5.
#[tokio::test]
async fn issue_162_along_on_a_sibling_clause_keeps_the_folded_view() -> Result<()> {
    const PROGRAM_ALONG_BASE: &str = "\
CREATE RULE assembly AS \
  MATCH (p:P)-[:CONTAINS]->(:P) \
  YIELD KEY p \n\
CREATE RULE build AS \
  MATCH (p:P)<-[s:SUPPLIES]-(v:V) \
  WHERE p IS NOT assembly \
  ALONG b = s.r \
  YIELD KEY p, b PROB \n\
CREATE RULE build AS \
  MATCH (p:P)-[:CONTAINS]->(c:P) \
  WHERE c IS build \
  FOLD b = MPROD(b) \
  YIELD KEY p, b PROB";

    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (top:P {s: 'TOP'}), (mid:P {s: 'MID'}), (l1:P {s: 'L1'}), \
                (l2:P {s: 'L2'}), (v:V {n: 'v'}), \
                (top)-[:CONTAINS]->(mid), (mid)-[:CONTAINS]->(l1), \
                (mid)-[:CONTAINS]->(l2), \
                (v)-[:SUPPLIES {r: 0.5}]->(l1), (v)-[:SUPPLIES {r: 0.5}]->(l2)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(PROGRAM_ALONG_BASE)
        .with_config(LocyConfig {
            max_iterations: 1000,
            timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        })
        .run()
        .await?;

    let mut got = Vec::new();
    for row in result.derived.get("build").into_iter().flatten() {
        if let (Some(uni_common::Value::Node(n)), Some(uni_common::Value::Float(f))) =
            (row.get("p"), row.get("b"))
        {
            got.push((
                n.properties
                    .get("s")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                (f * 1e9).round() / 1e9,
            ));
        }
    }
    got.sort_by(|a, b| a.0.cmp(&b.0));
    println!("ALONG-on-sibling: {got:?}");
    let top = got
        .iter()
        .find(|(n, _)| n == "TOP")
        .map(|(_, v)| *v)
        .expect("no TOP fact");
    assert!(
        (top - 0.25).abs() < 1e-9,
        "TOP must fold MID's folded 0.25 even though a sibling clause has \
         ALONG; got {got:?}"
    );
    Ok(())
}

/// W3: a contribution must be **replaced** when the child's folded value moves,
/// not appended alongside the stale one.
///
/// `TOP→MID`, `MID→L1`, `MID→X`, `X→L2` staggers the derivations: `MID` folds
/// to 0.5 from `L1` alone before `X` has a value, so `TOP` derives against 0.5;
/// one iteration later `MID` gains `X` and becomes 0.25, and `TOP` must
/// re-derive *over* its earlier row.
///
/// * replace (correct): `TOP = 0.25`
/// * append: `TOP = MPROD(0.5, 0.25) = 0.125`
#[tokio::test]
async fn issue_162_a_moved_child_value_replaces_the_stale_contribution() -> Result<()> {
    let rows = run(
        &[("TOP", "MID"), ("MID", "L1"), ("MID", "X"), ("X", "L2")],
        &[("L1", 0.5), ("L2", 0.5)],
    )
    .await?;
    println!("staggered DAG: {rows:?}");
    let top = rows
        .iter()
        .find(|(n, _)| n == "TOP")
        .map(|(_, v)| *v)
        .expect("no TOP fact");
    assert!(
        (top - 0.25).abs() < 1e-9,
        "TOP must fold MID's final 0.25, not its stale 0.5 as well \
         (0.125 = stale kept alongside fresh). Got {rows:?}"
    );
    Ok(())
}

/// W2: HAVING stays post-fixpoint — it is **not** applied to the per-iteration
/// folded snapshot.
///
/// `TOP→{MID,L3}`, `MID→{L1,L2}`, all leaves 0.5, folding with `MNOR`:
/// `MID = 1-(1-.5)² = 0.75`, `TOP = 1-(1-.75)(1-.5) = 0.875`. With
/// `WHERE b > 0.8`, `MID` is filtered out of the final answer but must still
/// have been visible to `TOP` during iteration.
///
/// * HAVING post-fixpoint (correct): `TOP = 0.875`, no `MID` fact
/// * HAVING per iteration: `MID` never enters the snapshot, `TOP` sees only
///   `L3` → 0.5 → itself filtered → no `TOP` fact at all
#[tokio::test]
async fn issue_162_having_is_not_applied_to_the_per_iteration_snapshot() -> Result<()> {
    const PROGRAM_HAVING: &str = "\
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
  FOLD b = MNOR(b) \
  WHERE b > 0.8 \
  YIELD KEY p, b PROB";

    let rows = run_with(
        PROGRAM_HAVING,
        &[("TOP", "MID"), ("TOP", "L3"), ("MID", "L1"), ("MID", "L2")],
        &[("L1", 0.5), ("L2", 0.5), ("L3", 0.5)],
        "build",
    )
    .await?;
    println!("HAVING post-fixpoint: {rows:?}");

    let top = rows.iter().find(|(n, _)| n == "TOP").map(|(_, v)| *v);
    assert!(
        top.is_some_and(|v| (v - 0.875).abs() < 1e-9),
        "TOP must be 0.875 — it had to see MID's 0.75 during iteration even \
         though HAVING removes MID from the answer. Got {rows:?}"
    );
    assert!(
        !rows.iter().any(|(n, _)| n == "MID"),
        "MID (0.75) must be filtered by the post-FOLD WHERE. Got {rows:?}"
    );
    Ok(())
}
