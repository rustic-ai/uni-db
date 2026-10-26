// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #308 triage: EXPLAIN RULE on a fact derived by a recursive clause.
//!
//! Chain `A → B → C`; `depends(A, C)` is derived only through clause 1
//! (`(A)-[:DEPENDS_ON]->(B)` plus `depends(B, C)`). The derivation node for
//! clause 1 should list `depends(B, C)` as a child. Varies: EXPLAIN with and
//! without a `WHERE` filter, default vs large `max_explain_depth`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_308)' --no-capture

use anyhow::Result;
use uni_db::locy::{CommandResult, LocyConfig};
use uni_db::{Uni, Value};
use uni_locy::DerivationNode;

const RULES: &str = "CREATE RULE depends AS \
       MATCH (a)-[:DEPENDS_ON]->(b) YIELD KEY a, KEY b\n\
     CREATE RULE depends AS \
       MATCH (a)-[:DEPENDS_ON]->(mid) WHERE mid IS depends TO b YIELD KEY a, KEY b\n";

fn id(node: &DerivationNode, var: &str) -> String {
    match node.bindings.get(var) {
        Some(Value::Node(n)) => n
            .properties
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string(),
        other => format!("{other:?}"),
    }
}

fn render(node: &DerivationNode, depth: usize, out: &mut String) {
    out.push_str(&format!(
        "{}{} #{} a={} b={} fact={:?}\n",
        "  ".repeat(depth),
        node.rule,
        node.clause_index,
        id(node, "a"),
        id(node, "b"),
        node.graph_fact
    ));
    for c in &node.children {
        render(c, depth + 1, out);
    }
}

/// Finds the derivation of `depends(a, b)` anywhere in the tree.
fn find<'t>(node: &'t DerivationNode, a: &str, b: &str) -> Option<&'t DerivationNode> {
    if node.graph_fact.is_some() && id(node, "a") == a && id(node, "b") == b {
        return Some(node);
    }
    node.children.iter().find_map(|c| find(c, a, b))
}

async fn explain(explain: &str, depth: Option<usize>) -> Result<DerivationNode> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:X {id:'A'})-[:DEPENDS_ON]->(:X {id:'B'})-[:DEPENDS_ON]->(:X {id:'C'})")
        .await?;
    tx.commit().await?;
    let mut config = LocyConfig::default();
    if let Some(d) = depth {
        config.max_explain_depth = d;
    }
    let result = db
        .session()
        .locy_with(&format!("{RULES}{explain}"))
        .with_config(config)
        .run()
        .await?;
    Ok(result
        .command_results()
        .iter()
        .find_map(|c| match c {
            CommandResult::Explain(n) => Some(n.clone()),
            _ => None,
        })
        .expect("an EXPLAIN result"))
}

#[tokio::test]
async fn issue_308_recursive_derivation_lists_its_supporting_fact() -> Result<()> {
    let mut failures = Vec::new();
    for (label, cmd) in [
        (
            "filtered",
            "EXPLAIN RULE depends WHERE a.id = 'A' AND b.id = 'C'",
        ),
        ("unfiltered", "EXPLAIN RULE depends"),
    ] {
        for depth in [None, Some(100)] {
            let tree = explain(cmd, depth).await?;
            let mut text = String::new();
            render(&tree, 0, &mut text);
            println!("--- {label}, max_explain_depth={depth:?}\n{text}");
            let ac = find(&tree, "A", "C").expect("depends(A, C) is explained");
            let ok = ac.clause_index == 1
                && ac
                    .children
                    .iter()
                    .any(|c| c.rule == "depends" && id(c, "a") == "B" && id(c, "b") == "C");
            if !ok {
                failures.push(format!("{label}/{depth:?}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "depends(A, C) via clause 1 lacks child depends(B, C) in: {failures:?}"
    );
    Ok(())
}

/// The issue's own shape: declared labels, a synthetic tree of fan-out 2 and
/// depth 4, and `supertype(C_0_0_0_0, C)` explained with a WHERE filter. The
/// tree must descend `C_0_0_0_0 → C_0_0_0 → C_0_0 → C_0 → C`, the last step
/// a clause-0 (edge) derivation.
#[tokio::test]
async fn issue_308_depth_four_chain_expands_to_the_base_fact() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Concept")
        .property("sid", uni_db::DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("SUBTYPE_OF", &["Concept"], &["Concept"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Concept {sid: 'urn:synth:C'})").await?;
    let mut frontier = vec!["C".to_string()];
    for _ in 0..4 {
        let mut next = Vec::new();
        for p in &frontier {
            for k in 0..2 {
                let c = format!("{p}_{k}");
                tx.execute(&format!(
                    "MATCH (p:Concept {{sid: 'urn:synth:{p}'}}) \
                     CREATE (:Concept {{sid: 'urn:synth:{c}'}})-[:SUBTYPE_OF]->(p)"
                ))
                .await?;
                next.push(c);
            }
        }
        frontier = next;
    }
    tx.commit().await?;
    let result = db
        .session()
        .locy_with(
            "CREATE RULE supertype AS MATCH (c:Concept)-[:SUBTYPE_OF]->(p:Concept) \
               YIELD KEY c, KEY p\n\
             CREATE RULE supertype AS MATCH (c:Concept)-[:SUBTYPE_OF]->(m:Concept) \
               WHERE m IS supertype TO p YIELD KEY c, KEY p\n\
             EXPLAIN RULE supertype \
               WHERE c.sid = 'urn:synth:C_0_0_0_0' AND p.sid = 'urn:synth:C'",
        )
        .with_config(LocyConfig::default())
        .run()
        .await?;
    let tree = result
        .command_results()
        .iter()
        .find_map(|c| match c {
            CommandResult::Explain(n) => Some(n.clone()),
            _ => None,
        })
        .expect("an EXPLAIN result");
    let sid = |n: &DerivationNode, v: &str| match n.bindings.get(v) {
        Some(Value::Node(node)) => node
            .properties
            .get("sid")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .trim_start_matches("urn:synth:")
            .to_string(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        tree.children.len(),
        1,
        "the WHERE filter selects exactly one supertype fact"
    );
    let mut chain = Vec::new();
    let mut node = &tree.children[0];
    loop {
        println!(
            "{}supertype #{} c={} p={} fact={:?}",
            "  ".repeat(chain.len()),
            node.clause_index,
            sid(node, "c"),
            sid(node, "p"),
            node.graph_fact
        );
        chain.push((sid(node, "c"), sid(node, "p"), node.clause_index));
        match node.children.as_slice() {
            [only] => node = only,
            [] => break,
            many => panic!("one derivation per level expected, got {}", many.len()),
        }
    }
    let expected: Vec<(String, String, usize)> =
        [("C_0_0_0_0", 1), ("C_0_0_0", 1), ("C_0_0", 1), ("C_0", 0)]
            .iter()
            .map(|(c, i)| (c.to_string(), "C".to_string(), *i))
            .collect();
    assert_eq!(chain, expected);
    Ok(())
}
