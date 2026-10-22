// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A derivation's `proof_probability` is its own weight, and top-k proof
//! retention keeps the `k` heaviest derivations of each fact.
//!
//! Before, every recorded derivation's `proof_probability` was `None`. The
//! weight was looked up in a map keyed by the provenance hash of each
//! *recorded row*, under a literal `"PROB"` column, by the `base_fact_id` of
//! each support term, which is the referenced fact's KEY hash: the two keys
//! never matched, and a leaf (no support) got no weight at all. Retention
//! then sorted equal zeros, and it was keyed per recorded row rather than per
//! fact, so `top_k_proofs = 1` kept every derivation of a fact.
//!
//! The weight of a derivation is the row's own pre-fold PROB value: its local
//! weight times every probability it referenced (each carried on its
//! `ProofTerm`). Every value below is hand-computed from the edge weights.

// Rust guideline compliant

use anyhow::{Result, bail};
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{DataType, Uni, Value};
use uni_locy::DerivationNode;

/// `reach`: one hop, or a hop followed by a `reach` fact. A recursive
/// derivation multiplies the referenced `reach` probability in.
const REACH: &str = "\
CREATE RULE reach AS MATCH (x:N)-[e:E]->(y:N) FOLD p = MNOR(e.p) YIELD KEY x, KEY y, p\n\
CREATE RULE reach AS MATCH (x:N)-[e:E]->(m:N) WHERE m IS reach TO y \
FOLD p = MNOR(e.p) YIELD KEY x, KEY y, p\n";

/// `edges` as `(from, to, p)`.
async fn seeded(edges: &[(&str, &str, f64)]) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("E", &["N"], &["N"])
        .property("p", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    let mut names: Vec<&str> = edges.iter().flat_map(|(a, b, _)| [*a, *b]).collect();
    names.sort_unstable();
    names.dedup();
    for name in names {
        tx.execute(&format!("CREATE (:N {{name: '{name}'}})"))
            .await?;
    }
    for (from, to, p) in edges {
        tx.execute(&format!(
            "MATCH (a:N {{name: '{from}'}}), (b:N {{name: '{to}'}}) CREATE (a)-[:E {{p: {p}}}]->(b)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// The EXPLAIN tree of `reach(from, to)` under `top_k_proofs = k`.
async fn explain(db: &Uni, from: &str, to: &str, k: usize) -> Result<DerivationNode> {
    let program = format!("{REACH}EXPLAIN RULE reach WHERE x.name = '{from}' AND y.name = '{to}'");
    let config = LocyConfig {
        max_iterations: 1000,
        top_k_proofs: k,
        ..Default::default()
    };
    let result = db
        .session()
        .locy_with(&program)
        .with_config(config)
        .run()
        .await?;
    match result.command_results.as_slice() {
        [CommandResult::Explain(node)] => Ok(node.clone()),
        other => bail!("expected one EXPLAIN result, got {other:?}"),
    }
}

/// The node's `name` binding of `var`.
fn name_of(node: &DerivationNode, var: &str) -> String {
    match node.bindings.get(var) {
        Some(Value::Node(n)) => format!("{:?}", n.properties.get("name")),
        Some(Value::Map(m)) => format!("{:?}", m.get("name")),
        other => format!("{other:?}"),
    }
}

/// Each derivation child of the root, as `(proof_probability, rendered)`,
/// heaviest first.
fn derivations(root: &DerivationNode) -> Vec<(Option<f64>, String)> {
    let mut out: Vec<(Option<f64>, String)> = root
        .children
        .iter()
        .map(|c| {
            let support: Vec<String> = c
                .children
                .iter()
                .map(|s| {
                    format!(
                        "{}({}->{})={:?}",
                        s.rule,
                        name_of(s, "x"),
                        name_of(s, "y"),
                        s.proof_probability.map(|p| (p * 1e6).round() / 1e6)
                    )
                })
                .collect();
            (c.proof_probability, support.join(","))
        })
        .collect();
    out.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    out
}

fn assert_close(actual: Option<f64>, expected: f64, what: &str) {
    match actual {
        Some(p) => assert!(
            (p - expected).abs() < 1e-9,
            "{what}: proof_probability {p}, expected {expected}"
        ),
        None => panic!("{what}: proof_probability is None, expected {expected}"),
    }
}

/// A one-hop fact's only derivation weighs its edge, `0.9`.
#[tokio::test]
async fn a_leaf_derivation_weighs_its_own_prob_value() -> Result<()> {
    let db = seeded(&[("A", "B", 0.9)]).await?;
    for k in [0, 3] {
        let root = explain(&db, "A", "B", k).await?;
        let found = derivations(&root);
        assert_eq!(found.len(), 1, "k={k}: {found:?}");
        assert_close(found[0].0, 0.9, &format!("k={k} reach(A,B)"));
    }
    Ok(())
}

/// `reach(A, C)` has two derivations: the edge `A -> C` (0.3), and
/// `A -> B` (0.5) times `reach(B, C)` = `B -> C` (0.8), 0.4. The recursive
/// one's support child is `reach(B, C)` weighing 0.8.
#[tokio::test]
async fn a_recursive_derivation_weighs_its_edge_times_what_it_references() -> Result<()> {
    let db = seeded(&[("A", "B", 0.5), ("B", "C", 0.8), ("A", "C", 0.3)]).await?;
    let root = explain(&db, "A", "C", 0).await?;
    let found = derivations(&root);
    assert_eq!(found.len(), 2, "{found:?}");
    assert_close(found[0].0, 0.4, "reach(A,C) via B");
    assert!(
        found[0]
            .1
            .contains("reach(Some(String(\"B\"))->Some(String(\"C\")))=Some(0.8)"),
        "the recursive derivation references reach(B, C) at 0.8: {found:?}"
    );
    assert_close(found[1].0, 0.3, "reach(A,C) direct");
    Ok(())
}

/// Top-k retention keeps the heaviest derivations of each fact: `k = 1`
/// keeps the 0.4 derivation of `reach(A, C)`, not the 0.3 one recorded
/// first (iteration 0), and `k = 2` keeps both, heaviest first.
#[tokio::test]
async fn top_k_retention_keeps_the_heaviest_derivations() -> Result<()> {
    let db = seeded(&[("A", "B", 0.5), ("B", "C", 0.8), ("A", "C", 0.3)]).await?;

    let one = derivations(&explain(&db, "A", "C", 1).await?);
    assert_eq!(one.len(), 1, "k=1 keeps one derivation: {one:?}");
    assert_close(one[0].0, 0.4, "k=1 keeps the heaviest");

    let two = derivations(&explain(&db, "A", "C", 2).await?);
    assert_eq!(two.len(), 2, "k=2 keeps both: {two:?}");
    assert_close(two[0].0, 0.4, "k=2 first");
    assert_close(two[1].0, 0.3, "k=2 second");
    Ok(())
}

/// Two parallel edges `A -> B` (0.7 and 0.6) are two derivations of one
/// fact; `k = 1` keeps the 0.7 one whichever was recorded first.
#[tokio::test]
async fn top_k_retention_is_per_fact_for_parallel_edges() -> Result<()> {
    for edges in [
        [("A", "B", 0.6), ("A", "B", 0.7)],
        [("A", "B", 0.7), ("A", "B", 0.6)],
    ] {
        let db = seeded(&edges).await?;
        let one = derivations(&explain(&db, "A", "B", 1).await?);
        assert_eq!(one.len(), 1, "{edges:?}: {one:?}");
        assert_close(one[0].0, 0.7, &format!("{edges:?}"));
        let all = derivations(&explain(&db, "A", "B", 0).await?);
        assert_eq!(all.len(), 2, "unlimited keeps both: {all:?}");
    }
    Ok(())
}
