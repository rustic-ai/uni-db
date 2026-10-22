// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! EXPLAIN RULE shows, under every derivation, the facts it references.
//!
//! Mode A (the recorded provenance) listed a derivation's referenced facts
//! only for a probabilistic reference: one to a PROB rule from an
//! `MNOR`/`MPROD` rule, whose lineage column names the fact. A reference to
//! a rule without a PROB column, or from a PROB rule without a fold, had no
//! such column, so its derivations showed no child: a transitive closure's
//! tree was flat. Every positive reference now names its fact. A fact of a
//! rule without a PROB column is certain: its derivations weigh 1.
//!
//! Mode B (re-running the clauses, when no provenance was recorded for the
//! rule) is probed over the same programs: it must explain the facts the
//! program derived, each derivation's references being facts of the
//! referenced rule.

// Rust guideline compliant

use std::collections::BTreeSet;

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{DataType, Uni, Value};
use uni_locy::DerivationNode;

/// A chain with a branch and a cycle: a→b→c→d, b→e, e→b.
const EDGES: &[(&str, &str, f64)] = &[
    ("a", "b", 0.5),
    ("b", "c", 0.4),
    ("c", "d", 0.3),
    ("b", "e", 0.6),
    ("e", "b", 0.7),
];

async fn seeded() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .property("size", DataType::Int64)
        .apply()
        .await?;
    db.schema()
        .edge_type("E", &["N"], &["N"])
        .property("p", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for (i, n) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tx.execute(&format!("CREATE (:N {{name: '{n}', size: {i}}})"))
            .await?;
    }
    for (a, b, p) in EDGES {
        tx.execute(&format!(
            "MATCH (x:N {{name:'{a}'}}), (y:N {{name:'{b}'}}) CREATE (x)-[:E {{p: {p}}}]->(y)"
        ))
        .await?;
    }
    tx.commit().await?;
    Ok(db)
}

/// The name of the node bound to `var` in `node`.
fn name(node: &DerivationNode, var: &str) -> String {
    match node.bindings.get(var) {
        Some(Value::Node(n)) => n
            .properties
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string(),
        other => panic!("`{var}` is not a node in {:?}: {other:?}", node.bindings),
    }
}

async fn explain(db: &Uni, program: &str, rule: &str) -> Result<DerivationNode> {
    let result = db
        .session()
        .locy_with(&format!("{program}\nEXPLAIN RULE {rule}"))
        .with_config(LocyConfig {
            max_iterations: 1000,
            ..Default::default()
        })
        .run()
        .await?;
    Ok(result
        .command_results()
        .iter()
        .find_map(|c| c.as_explain())
        .expect("an EXPLAIN result")
        .clone())
}

/// `b` is reachable from `a` by a path of at least one edge.
fn reaches(a: &str, b: &str) -> bool {
    let mut frontier = vec![a];
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    while let Some(n) = frontier.pop() {
        for (x, y, _) in EDGES {
            if *x == n && seen.insert(y) {
                frontier.push(y);
            }
        }
    }
    seen.contains(b)
}

const PATH: &str = "CREATE RULE path AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
     CREATE RULE path AS MATCH (a:N)-[:E]->(m:N) WHERE m IS path TO b YIELD KEY a, KEY b";

/// A transitive closure (set semantics, recursive, `m` projected away):
/// every derivation through `m IS path TO b` has the referenced `path(m,
/// b)` as its child, `a → m` an edge, and every node weighs 1.
#[tokio::test]
async fn a_set_semantics_closure_shows_the_referenced_fact() -> Result<()> {
    let db = seeded().await?;
    let tree = explain(&db, PATH, "path").await?;
    let mut facts = BTreeSet::new();
    let mut through = 0;
    for d in &tree.children {
        let (a, b) = (name(d, "a"), name(d, "b"));
        assert!(reaches(&a, &b), "path({a}, {b}) is no path");
        assert_eq!(d.proof_probability, Some(1.0), "path({a}, {b}) is certain");
        facts.insert((a.clone(), b.clone()));
        if d.clause_index == 0 {
            assert!(d.children.is_empty(), "an edge references nothing");
            continue;
        }
        through += 1;
        assert_eq!(
            d.children.len(),
            1,
            "path({a}, {b}) through clause 1: {d:#?}"
        );
        let child = &d.children[0];
        assert_eq!(child.rule, "path");
        let m = name(child, "a");
        assert_eq!(name(child, "b"), b, "the child is path(m, {b})");
        assert!(
            EDGES.iter().any(|(x, y, _)| *x == a && *y == m),
            "path({a}, {b}) through {m}: no edge {a} → {m}"
        );
        if child
            .graph_fact
            .as_deref()
            .is_some_and(|f| f.starts_with('['))
        {
            // Expanded: a derivation of path(m, b), weighing 1.
            assert_eq!(child.proof_probability, Some(1.0));
        }
    }
    let expected: BTreeSet<(String, String)> = ["a", "b", "c", "d", "e"]
        .iter()
        .flat_map(|x| ["a", "b", "c", "d", "e"].map(|y| (x.to_string(), y.to_string())))
        .filter(|(x, y)| reaches(x, y))
        .collect();
    assert_eq!(facts, expected);
    assert!(through > 0, "some fact is derived through a reference");
    Ok(())
}

const BIG: &str = "CREATE RULE big AS MATCH (h:N) WHERE h.size >= 2 YIELD KEY h\n";

/// A non-recursive rule referencing a rule without a PROB column, the
/// subject `h` not yielded: each derivation's child is the `big(h)` it read.
#[tokio::test]
async fn a_reference_to_a_rule_without_prob_is_a_child() -> Result<()> {
    let db = seeded().await?;
    let program =
        format!("{BIG}CREATE RULE near AS MATCH (a:N)-[:E]->(h:N) WHERE h IS big YIELD KEY a");
    let tree = explain(&db, &program, "near").await?;
    let mut seen = BTreeSet::new();
    for d in &tree.children {
        let a = name(d, "a");
        assert_eq!(d.children.len(), 1, "near({a}): {d:#?}");
        let child = &d.children[0];
        assert_eq!(child.rule, "big");
        let h = name(child, "h");
        assert!(
            EDGES.iter().any(|(x, y, _)| *x == a && *y == h),
            "near({a}) read big({h}) with no edge {a} → {h}"
        );
        assert_eq!(child.proof_probability, Some(1.0), "big({h}) is certain");
        seen.insert(a);
    }
    // c → d (size 3), b → c (size 2): near(b), near(c).
    assert_eq!(seen, BTreeSet::from(["b".to_string(), "c".to_string()]));
    Ok(())
}

/// An `MNOR` rule referencing a rule without a PROB column: the child is
/// listed (the reference is no event, so the value is unchanged), at 1.
#[tokio::test]
async fn an_mnor_rule_lists_a_certain_reference() -> Result<()> {
    let db = seeded().await?;
    let program = format!(
        "{BIG}CREATE RULE risky AS MATCH (a:N)-[e:E]->(h:N) WHERE h IS big \
         FOLD p = MNOR(e.p) YIELD KEY a, p"
    );
    let tree = explain(&db, &program, "risky").await?;
    assert!(!tree.children.is_empty());
    for d in &tree.children {
        let a = name(d, "a");
        assert_eq!(d.children.len(), 1, "risky({a}): {d:#?}");
        assert_eq!(d.children[0].rule, "big");
        assert_eq!(d.children[0].proof_probability, Some(1.0));
        let p = d
            .proof_probability
            .expect("risky's derivation weighs its edge");
        let h = name(&d.children[0], "h");
        let edge = EDGES
            .iter()
            .find(|(x, y, _)| *x == a && *y == h)
            .expect("an edge");
        assert!((p - edge.2).abs() < 1e-9, "risky({a}) via {h}: {p}");
    }
    Ok(())
}

const REACH: &str = "CREATE RULE reach AS MATCH (a:N)-[e:E]->(b:N) \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob\n\
     CREATE RULE reach AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS reach TO b \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob\n";

/// A PROB rule without a fold referencing a PROB rule: the child is the
/// `reach` fact the row read, and the derivation weighs the row's PROB value
/// (its edge times that fact's probability).
#[tokio::test]
async fn a_prob_rule_without_a_fold_lists_its_reference() -> Result<()> {
    let db = seeded().await?;
    let program = format!(
        "{REACH}CREATE RULE hop AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS reach TO b \
         YIELD KEY a, KEY b, KEY m, e.p AS p PROB"
    );
    let tree = explain(&db, &program, "hop").await?;
    assert!(!tree.children.is_empty());
    for d in &tree.children {
        let (a, b, m) = (name(d, "a"), name(d, "b"), name(d, "m"));
        let reach: Vec<&DerivationNode> = d.children.iter().filter(|c| c.rule == "reach").collect();
        assert!(
            !reach.is_empty(),
            "hop({a}, {b}, {m}) lists reach({m}, {b}): {d:#?}"
        );
        for c in &reach {
            assert_eq!((name(c, "a"), name(c, "b")), (m.clone(), b.clone()));
        }
        let p = d.proof_probability.expect("hop weighs its PROB value");
        match d.bindings.get("p") {
            Some(Value::Float(v)) => assert!((p - v).abs() < 1e-9, "{p} vs {v}"),
            other => panic!("hop's p: {other:?}"),
        }
    }
    Ok(())
}

// ── Mode B: the tree ABDUCE builds by re-running the clauses ──────────────

/// The `ABDUCE NOT` result of `program` over a fresh graph built by `graph`.
async fn abduce(graph: &str, program: &str) -> Result<uni_locy::AbductionResult> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(graph).await?;
    tx.commit().await?;
    let result = db.session().locy(program).await?;
    for cr in &result.command_results {
        if let uni_db::locy::CommandResult::Abduce(ab) = cr {
            return Ok(ab.clone());
        }
    }
    anyhow::bail!("no ABDUCE result")
}

/// The validated `RemoveEdge` candidates' edge types.
fn validated_removals(ab: &uni_locy::AbductionResult) -> Vec<String> {
    ab.modifications
        .iter()
        .filter(|m| m.validated)
        .filter_map(|m| match &m.modification {
            uni_locy::Modification::RemoveEdge { edge_type, .. } => Some(edge_type.clone()),
            _ => None,
        })
        .collect()
}

/// A fact whose only derivation reads one `E` edge (and a fact through
/// recursion, and through `COUNT`) is removed by removing that edge, so
/// the tree must hold the clause and row that derived it.
#[tokio::test]
async fn mode_b_explains_recursion_negation_and_fold() -> Result<()> {
    let graph = "CREATE (x:N {name: 'x'})-[:E]->(y:N {name: 'y'})-[:E]->(z:N {name: 'z'}), \
                 (w:N {name: 'w', flag: true})";
    for (what, program) in [
        (
            "recursion",
            "CREATE RULE reach AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, KEY b\n\
             CREATE RULE reach AS MATCH (a:N)-[:E]->(m:N) WHERE m IS reach TO b YIELD KEY a, KEY b\n\
             ABDUCE NOT reach WHERE a.name = 'x' AND b.name = 'z'",
        ),
        (
            "boolean negation",
            "CREATE RULE blocked AS MATCH (b:N) WHERE b.flag = true YIELD KEY b\n\
             CREATE RULE ok AS MATCH (a:N)-[:E]->(b:N) WHERE b IS NOT blocked YIELD KEY a, KEY b\n\
             ABDUCE NOT ok WHERE a.name = 'x'",
        ),
        (
            "fold",
            "CREATE RULE deg AS MATCH (a:N)-[:E]->(b:N) FOLD n = COUNT(*) YIELD KEY a, n\n\
             ABDUCE NOT deg WHERE a.name = 'x'",
        ),
    ] {
        let ab = abduce(graph, program).await?;
        assert!(
            validated_removals(&ab).iter().any(|t| t == "E"),
            "{what}: no validated edge removal in {:?}",
            ab.modifications
        );
    }
    Ok(())
}

/// A PROB rule negating a PROB rule keeps the row with its complement
/// (`p · (1 − risky)`): `safe(x, y)` holds although `risky(y)` does too,
/// with probability 0.5. Re-running the clause as a boolean anti-join
/// dropped the row, so the tree had no evidence for `safe(x, y)`, and no
/// removal was proposed.
#[tokio::test]
async fn mode_b_keeps_a_row_a_probabilistic_negation_keeps() -> Result<()> {
    let graph = "CREATE (x:N {name: 'x'})-[:E {p: 0.8}]->(y:N {name: 'y'})-[:R {p: 0.5}]->(z:N {name: 'z'})";
    let program = "CREATE RULE risky AS MATCH (a:N)-[r:R]->(b:N) YIELD KEY a, r.p AS p PROB\n\
         CREATE RULE safe AS MATCH (a:N)-[e:E]->(b:N) WHERE b IS NOT risky \
         YIELD KEY a, KEY b, e.p AS p PROB\n\
         ABDUCE NOT safe WHERE a.name = 'x'";
    let ab = abduce(graph, program).await?;
    assert!(
        validated_removals(&ab).iter().any(|t| t == "E"),
        "no validated E removal in {:?}",
        ab.modifications
    );
    Ok(())
}

/// PRIORITY keeps, per KEY, the rows of the highest-priority clause that
/// derived it: `kind(x) = 'high'` comes from the `F` clause although the
/// `E` clause, listed first, derives `x` too. The tree must explain the
/// clause the fact came from, or ABDUCE proposes removing the `E` edge,
/// which leaves `kind(x) = 'high'`, and never the `F` edge.
#[tokio::test]
async fn mode_b_explains_the_clause_priority_kept() -> Result<()> {
    let graph = "CREATE (x:N {name: 'x'})-[:E]->(:N {name: 'y'}), (x)-[:F]->(:N {name: 'z'})";
    let program = "CREATE RULE kind PRIORITY 1 AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, 'low' AS k\n\
         CREATE RULE kind PRIORITY 2 AS MATCH (a:N)-[:F]->(b:N) YIELD KEY a, 'high' AS k\n\
         ABDUCE NOT kind WHERE a.name = 'x' AND k = 'high'";
    let ab = abduce(graph, program).await?;
    assert_eq!(
        validated_removals(&ab),
        ["F".to_string()],
        "{:?}",
        ab.modifications
    );
    Ok(())
}

/// BEST BY keeps, per KEY, the best row: `cheap(x)` is the 1-weight edge.
/// The tree must explain that row, not the first one sharing its KEY (the
/// 5-weight edge, created first), or ABDUCE proposes removing the 5-weight
/// edge, which leaves `cheap(x) = 1`.
#[tokio::test]
async fn mode_b_explains_the_row_best_by_kept() -> Result<()> {
    let graph = "CREATE (x:N {name: 'x'})-[:E {w: 5}]->(:N {name: 'y'}), \
                 (x)-[:G {w: 1}]->(:N {name: 'z'})";
    let program = "CREATE RULE cheap AS MATCH (a:N)-[e:E|G]->(b:N) BEST BY w ASC \
         YIELD KEY a, e.w AS w\n\
         ABDUCE NOT cheap WHERE a.name = 'x' AND w = 1";
    let ab = abduce(graph, program).await?;
    assert_eq!(
        validated_removals(&ab),
        ["G".to_string()],
        "{:?}",
        ab.modifications
    );
    Ok(())
}
