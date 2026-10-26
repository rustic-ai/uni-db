// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #313 repro: exact-mode MNOR over a portfolio whose suppliers depend
//! on distinct independent facility events.
//!
//! Hand oracle (F1 = 0.2, F2 = 0.1 independent):
//!   P(F1 or F2) = 1 - (1 - 0.2)(1 - 0.1) = 1 - 0.8 * 0.9 = 1 - 0.72 = 0.28
//!   Two paths to F1 only: P(F1) = 0.2
//!   Distinct F1 = F2 = 0.2: 1 - 0.8 * 0.8 = 0.36

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, Value};

const PROGRAM: &str = "CREATE RULE event AS \
 MATCH (f:Facility) \
 YIELD KEY f, f.prob AS probability PROB \n\
CREATE RULE exposed AS \
 MATCH (s:Supplier)-[:DEPENDS_ON]->(f:Facility) \
 WHERE f IS event \
 YIELD KEY s, probability AS risk PROB \n\
CREATE RULE portfolio AS \
 MATCH (p:Portfolio)-[:EXPOSED_TO]->(s:Supplier) \
 WHERE s IS exposed \
 FOLD combined = MNOR(risk) \
 YIELD KEY p, combined AS probability PROB \n\
QUERY portfolio RETURN p.id AS portfolio, probability";

async fn run(facilities: &[(&str, f64)], edges: &[(&str, &str)], exact: bool) -> Result<f64> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    for (f, p) in facilities {
        tx.execute(&format!("CREATE (:Facility {{id:'{f}', prob:{p}}})"))
            .await?;
    }
    tx.execute("CREATE (:Portfolio {id:'P'})").await?;
    for (s, f) in edges {
        tx.execute(&format!(
            "MATCH (f:Facility {{id:'{f}'}}), (p:Portfolio {{id:'P'}}) \
             CREATE (p)-[:EXPOSED_TO]->(:Supplier {{id:'{s}'}})-[:DEPENDS_ON]->(f)"
        ))
        .await?;
    }
    tx.commit().await?;
    let config = LocyConfig {
        exact_probability: exact,
        strict_probability_domain: true,
        max_bdd_variables: 100,
        max_iterations: 100,
        ..Default::default()
    };
    let r = db
        .session()
        .locy_with(PROGRAM)
        .with_config(config)
        .run()
        .await?;
    let codes: Vec<String> = r
        .warnings()
        .iter()
        .map(|w| format!("{:?}", w.code))
        .collect();
    println!("    exact={exact} warnings={codes:?}");
    let rows = r.rows().expect("rows");
    assert_eq!(rows.len(), 1, "{rows:?}");
    Ok(rows[0]
        .get("probability")
        .and_then(Value::as_f64)
        .expect("prob"))
}

#[tokio::test]
async fn issue_313_exact_mnor_distinct_events() -> Result<()> {
    let f12 = [("F1", 0.2), ("F2", 0.1)];
    let f21 = [("F2", 0.1), ("F1", 0.2)];
    #[allow(clippy::type_complexity)]
    let cases: Vec<(&str, Vec<(&str, f64)>, Vec<(&str, &str)>, f64)> = vec![
        (
            "one_shared",
            vec![("F1", 0.2)],
            vec![("S1", "F1"), ("S2", "F1")],
            0.2,
        ),
        (
            "two_independent",
            f12.to_vec(),
            vec![("S1", "F1"), ("S3", "F2")],
            0.28,
        ),
        (
            "two_independent_rev_nodes",
            f21.to_vec(),
            vec![("S1", "F1"), ("S3", "F2")],
            0.28,
        ),
        (
            "two_independent_rev_edges",
            f12.to_vec(),
            vec![("S3", "F2"), ("S1", "F1")],
            0.28,
        ),
        (
            "mixed",
            f12.to_vec(),
            vec![("S1", "F1"), ("S2", "F1"), ("S3", "F2")],
            0.28,
        ),
        (
            "mixed_rev_nodes",
            f21.to_vec(),
            vec![("S1", "F1"), ("S2", "F1"), ("S3", "F2")],
            0.28,
        ),
        (
            "two_shared_groups",
            f12.to_vec(),
            vec![("S1", "F1"), ("S2", "F1"), ("S3", "F2"), ("S4", "F2")],
            0.28,
        ),
        (
            "equal_probs_distinct",
            vec![("F1", 0.2), ("F2", 0.2)],
            vec![("S1", "F1"), ("S3", "F2")],
            0.36,
        ),
    ];
    let mut bad = Vec::new();
    for (name, fac, edges, want) in &cases {
        for exact in [false, true] {
            let got = run(fac, edges, exact).await?;
            println!("{name:28} exact={exact:5} got={got:.6} oracle={want}");
            if exact && (got - want).abs() > 1e-9 {
                bad.push(format!("{name}: exact got {got}, want {want}"));
            }
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
    Ok(())
}
