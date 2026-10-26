// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression for <https://github.com/rustic-ai/uni-db/issues/159>
//!
//! Aggregation inside a **recursive** rule dropped rows whose values were
//! numerically equal: one parent with N children each contributing `1.0` summed
//! to `1.0`, not `N`, so a bill-of-materials rollup was not expressible. The
//! same fold was always correct in a non-recursive rule.
//!
//! ## Root cause
//!
//! Every dedup path in the semi-naive fixpoint keys on **all columns**
//! (`dedup_batches_all_columns`, `RowDedupState::compute_delta`,
//! `FixpointState::compute_delta_legacy`, `arrow_left_anti_dedup`), and the
//! FOLD runs once after convergence via `apply_post_fixpoint_chain`, so it only
//! ever observed the already-deduped fact set. A non-recursive rule bypasses
//! `FixpointState` entirely, which is why the controls below always passed —
//! and why the non-recursive control was effectively the specification: a fold
//! aggregates the **bag of body rows**, and the recursive path disagreed.
//!
//! This is the set-vs-bag tension in Datalog. Set semantics on derived facts is
//! what makes semi-naive evaluation terminate; rows feeding an aggregate need
//! bag semantics. One policy was serving both jobs.
//!
//! ## The fix
//!
//! Split the two jobs rather than choosing between them. A recursive FOLD/ALONG
//! rule now projects hidden derivation-discriminator columns
//! (`__deriv_clause`, `__deriv_vid_{var}` — see `FOLD_DISCRIMINATOR_COL_PREFIX`)
//! holding the clause index and the vids of MATCH-bound node variables the
//! YIELD does not project. Those are exactly the bindings that distinguish one
//! derivation from another, so the dedup key still suppresses *re-derivations*
//! (preserving termination) while keeping *distinct derivations* apart. They
//! are stripped after the fixpoint, so nothing downstream sees them.
//!
//! Two simpler fixes were prototyped and refuted, recorded here so they are not
//! retried: aggregating per iteration and replacing per KEY breaks because a
//! self-ref reads pre-fold contribution rows; and skipping dedup entirely for
//! FOLD rules hits the iteration limit **even on a DAG**, because a
//! non-recursive base clause re-emits its rows every iteration and dedup is
//! what suppresses them.
//!
//! ## Scope
//!
//! Only rules carrying FOLD or ALONG in a recursive stratum. A key-only
//! recursive rule genuinely wants set semantics —
//! `bugs::locy_is_not_complement_recursion` derives 1350 facts and would derive
//! 58 050 under a global bag switch, and cyclic fixtures would stop
//! terminating. That test plus the `RowDedupState` unit tests are the tripwire
//! that the scoping held.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration \
//!     -E 'test(issue_159)' --run-ignored all

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Builds a product graph: `costs` maps sku -> unit cost, `edges` are
/// `(parent, child, quantity)` `HAS` relationships.
async fn build(edges: &[(&str, &str, f64)], costs: &[(&str, f64)]) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("sku", DataType::String)
        .property("cost", DataType::Float64)
        .done()
        .edge_type("HAS", &["P"], &["P"])
        .property("q", DataType::Float64)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    for (sku, cost) in costs {
        tx.execute_with("CREATE (:P {sku: $s, cost: $c})")
            .param("s", *sku)
            .param("c", *cost)
            .run()
            .await?;
    }
    for (a, b, q) in edges {
        tx.execute_with("MATCH (a:P {sku: $a}), (b:P {sku: $b}) CREATE (a)-[:HAS {q: $q}]->(b)")
            .param("a", *a)
            .param("b", *b)
            .param("q", *q)
            .run()
            .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// Bottom-up cost rollup: leaves contribute their own cost, assemblies sum
/// `child_cost * quantity` over their children.
const ROLLUP: &str = r#"
CREATE RULE assembly AS
  MATCH (p:P)-[:HAS]->(:P)
  YIELD KEY p
CREATE RULE roll AS
  MATCH (p:P)
  WHERE p IS NOT assembly
  YIELD KEY p, p.cost AS cost
CREATE RULE roll AS
  MATCH (p:P)-[e:HAS]->(c:P)
  WHERE c IS roll
  FOLD cost = MSUM(cost * e.q)
  YIELD KEY p, cost
"#;

/// Extracts the rolled-up cost for `sku` from the `roll` relation.
async fn cost_of(result: &uni_db::locy::LocyResult, sku: &str) -> Option<f64> {
    let empty = vec![];
    result
        .derived_rule("roll")
        .await
        .unwrap()
        .unwrap_or(&empty)
        .iter()
        .find(|row| match row.get("p") {
            Some(Value::Node(node)) => {
                matches!(node.properties.get("sku"), Some(Value::String(s)) if s == sku)
            }
            _ => false,
        })
        .and_then(|row| match row.get("cost") {
            Some(Value::Float(f)) => Some(*f),
            _ => None,
        })
}

// ---------------------------------------------------------------------------
// Green: controls that pin the trigger to (recursive AND equal-valued).
// ---------------------------------------------------------------------------

/// Distinct child costs sum correctly in the very same recursive rule, proving
/// the fold itself works and the loss is specifically the dedup of equal rows.
#[tokio::test]
async fn issue_159_recursive_fold_with_distinct_values_is_correct() -> Result<()> {
    let costs = [
        ("TOP", 0.0),
        ("L0", 1.0),
        ("L1", 2.0),
        ("L2", 4.0),
        ("L3", 8.0),
    ];
    let edges = [
        ("TOP", "L0", 1.0),
        ("TOP", "L1", 1.0),
        ("TOP", "L2", 1.0),
        ("TOP", "L3", 1.0),
    ];
    let db = build(&edges, &costs).await?;
    let result = db.session().locy(ROLLUP).await?;
    let top = cost_of(&result, "TOP").await.expect("TOP must be derived");
    assert!(
        (top - 15.0).abs() < 1e-9,
        "distinct child costs must sum: got {top}, want 15.0"
    );
    Ok(())
}

/// The identical aggregation over equal values is correct when the rule is
/// **not** recursive — it bypasses `FixpointState` and never hits the dedup.
#[tokio::test]
async fn issue_159_non_recursive_fold_over_equal_values_is_correct() -> Result<()> {
    let costs = [("TOP", 0.0), ("L0", 1.0), ("L1", 1.0), ("L2", 1.0)];
    let edges = [("TOP", "L0", 1.0), ("TOP", "L1", 1.0), ("TOP", "L2", 1.0)];
    let db = build(&edges, &costs).await?;

    for agg in ["SUM", "MSUM", "COUNT", "MCOUNT"] {
        let arg = if agg.contains("COUNT") { "c" } else { "c.cost" };
        let program = format!(
            "CREATE RULE flat AS MATCH (p:P)-[:HAS]->(c:P) \
             FOLD v = {agg}({arg}) YIELD KEY p, v"
        );
        let result = db.session().locy(&program).await?;
        let empty = vec![];
        let facts = result.derived_rule("flat").await.unwrap().unwrap_or(&empty);
        let v = facts
            .first()
            .and_then(|row| match row.get("v") {
                Some(Value::Float(f)) => Some(*f),
                Some(Value::Int(i)) => Some(*i as f64),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{agg}: no value derived"));
        assert!(
            (v - 3.0).abs() < 1e-9,
            "non-recursive {agg} over 3 equal values must be 3, got {v}"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The bug itself.
// ---------------------------------------------------------------------------

/// N children each contributing exactly `1.0` must sum to N. This returned
/// `1.0` for every N, because the N identical `(TOP, 1.0)` rows were collapsed
/// to one by all-column dedup before the fold ran.
#[tokio::test]
async fn issue_159_recursive_fold_must_not_dedup_equal_values() -> Result<()> {
    for n in 2..=6usize {
        let mut costs = vec![("TOP".to_string(), 0.0)];
        let mut edges = Vec::new();
        for i in 0..n {
            costs.push((format!("L{i}"), 1.0));
            edges.push(("TOP".to_string(), format!("L{i}"), 1.0));
        }
        let costs_ref: Vec<(&str, f64)> = costs.iter().map(|(s, c)| (s.as_str(), *c)).collect();
        let edges_ref: Vec<(&str, &str, f64)> = edges
            .iter()
            .map(|(a, b, q)| (a.as_str(), b.as_str(), *q))
            .collect();

        let db = build(&edges_ref, &costs_ref).await?;
        let result = db.session().locy(ROLLUP).await?;
        let top = cost_of(&result, "TOP").await.expect("TOP must be derived");
        assert!(
            (top - n as f64).abs() < 1e-9,
            "N={n}: {n} children of cost 1.0 must roll up to {n}, got {top} — \
             the equal-valued sibling rows were deduplicated before the fold"
        );
    }
    Ok(())
}

/// The path-enumeration formulation fails the same way: two distinct DAG paths
/// carrying an equal accumulated quantity collapse into one row, so a
/// downstream sum under-counts.
///
/// `T->M1->X (q=2)`, `T->M2->X (q=2)`, `T->M3->X (q=5)` must yield three
/// `(T, X)` rows totalling 9.0; the two `2.0` paths used to merge, giving 7.0.
///
/// ALONG is the half of #159 that has no FOLD to drop the hidden columns, so
/// this is also the regression test for the post-fixpoint strip.
#[tokio::test]
async fn issue_159_along_must_not_collapse_equal_valued_paths() -> Result<()> {
    let costs = [
        ("T", 0.0),
        ("M1", 0.0),
        ("M2", 0.0),
        ("M3", 0.0),
        ("X", 0.0),
    ];
    let edges = [
        ("T", "M1", 1.0),
        ("T", "M2", 1.0),
        ("T", "M3", 1.0),
        ("M1", "X", 2.0),
        ("M2", "X", 2.0),
        ("M3", "X", 5.0),
    ];
    let db = build(&edges, &costs).await?;

    const EXPLODE: &str = r#"
CREATE RULE expl AS
  MATCH (p:P)-[e:HAS]->(c:P)
  ALONG qty = e.q
  YIELD KEY p, KEY c, qty
CREATE RULE expl AS
  MATCH (p:P)-[e:HAS]->(m:P)
  WHERE m IS expl TO c
  ALONG qty = prev.qty * e.q
  YIELD KEY p, KEY c, qty
"#;
    let result = db.session().locy(EXPLODE).await?;
    let empty = vec![];
    let sku = |row: &std::collections::HashMap<String, Value>, col: &str| match row.get(col) {
        Some(Value::Node(node)) => match node.properties.get("sku") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        },
        _ => None,
    };
    let mut qtys: Vec<f64> = result
        .derived_rule("expl")
        .await
        .unwrap()
        .unwrap_or(&empty)
        .iter()
        .filter(|row| {
            sku(row, "p").as_deref() == Some("T") && sku(row, "c").as_deref() == Some("X")
        })
        .filter_map(|row| match row.get("qty") {
            Some(Value::Float(f)) => Some(*f),
            _ => None,
        })
        .collect();
    qtys.sort_by(|a, b| a.partial_cmp(b).expect("no NaN quantities"));

    let total: f64 = qtys.iter().sum();
    assert!(
        (total - 9.0).abs() < 1e-9,
        "three distinct T->X paths (2, 2, 5) must total 9.0, got {total} from \
         rows {qtys:?} — the two equal 2.0 paths collapsed"
    );
    Ok(())
}

/// The detection already exists — the *reporting* is what fails.
///
/// `check_fold_in_recursive_path` (`uni-locy/src/compiler/typecheck.rs:643-671`)
/// does fire on this program, so the compiler knows the rule is suspect. Two
/// things nonetheless leave the user with a silent under-count:
///
/// 1. **The advice is wrong.** The message suggests adding `ALONG` "for
///    per-path aggregation", but `issue_159_along_must_not_collapse_equal_valued_paths`
///    proves ALONG collapses equal-valued paths too — and the check is
///    *suppressed* when ALONG is present (`typecheck.rs:653`), so following the
///    advice moves the user to an equally-wrong formulation that emits no
///    warning at all.
/// 2. **The channel is not plumbed to Python.** `bindings/uni-db/src/convert.rs:986`
///    maps `result.warnings` (runtime) but never `result.compile_warnings`, so
///    `LocyResult` has no `compile_warnings` attribute in Python. The issue was
///    filed from Python, where this warning is invisible.
///
/// This test pins the current detection so a fix cannot silently remove it
/// while reworking the message.
#[tokio::test]
async fn issue_159_fold_in_recursive_path_is_detected_at_compile_time() -> Result<()> {
    use uni_locy::types::WarningCode;

    let costs = [("TOP", 0.0), ("L0", 1.0), ("L1", 1.0)];
    let edges = [("TOP", "L0", 1.0), ("TOP", "L1", 1.0)];
    let db = build(&edges, &costs).await?;
    let result = db.session().locy(ROLLUP).await?;

    assert!(
        result
            .compile_warnings
            .iter()
            .any(|w| matches!(w.code, WarningCode::FoldInRecursivePath)),
        "the compiler must keep detecting FOLD-in-recursive-path; got {:?}",
        result.compile_warnings
    );
    Ok(())
}
