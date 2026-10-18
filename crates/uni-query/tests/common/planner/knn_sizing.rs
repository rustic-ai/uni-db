// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Where `n.prop ~= q` becomes a `VectorKnn`, and what sizes its `K`.
//!
//! Plan-shape companion to `uni-db`'s `vector_search::knn_limit_semantics`,
//! which checks the answers. `K` is `Some(skip + fetch)` only when the KNN
//! feeds the `LIMIT` through row-preserving, score-ordered nodes; otherwise
//! `None` (the configured default). The scan is never rewritten below a
//! `LIMIT`, and under `OPTIONAL MATCH` the KNN carries `optional: true`.

// Rust guideline compliant

use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_query::query::planner::QueryPlanner;

async fn planner() -> (QueryPlanner, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let sm = SchemaManager::load(&dir.path().join("schema.json"))
        .await
        .unwrap();
    for label in ["N", "M"] {
        sm.add_label(label).unwrap();
        sm.add_property(label, "id", DataType::Int64, false)
            .unwrap();
        sm.add_property(label, "x", DataType::Int64, false).unwrap();
        sm.add_property(label, "emb", DataType::Vector { dimensions: 2 }, false)
            .unwrap();
    }
    sm.add_edge_type("R", vec!["N".into()], vec!["N".into()])
        .unwrap();
    (QueryPlanner::new(sm.schema()), dir)
}

fn plan_debug(planner: &QueryPlanner, cypher: &str) -> String {
    let ast = uni_cypher::parse(cypher).unwrap_or_else(|e| panic!("{cypher}: {e}"));
    let plan = planner
        .plan(ast)
        .unwrap_or_else(|e| panic!("{cypher}: {e}"));
    format!("{plan:?}")
}

/// Every `VectorKnn`'s `(k, optional)`, in Debug order.
fn knns(debug: &str) -> Vec<String> {
    debug
        .match_indices("VectorKnn {")
        .map(|(i, _)| {
            let rest = &debug[i..];
            let k = rest.find("k: ").map(|p| &rest[p..]).unwrap();
            let end = k.find(", optional: ").unwrap();
            let opt = &k[end + ", optional: ".len()..];
            let opt_end = opt.find([' ', ',', '}']).unwrap();
            format!("{} optional={}", &k[3..end], &opt[..opt_end])
        })
        .collect()
}

#[tokio::test]
async fn limit_sizes_k_only_when_fed_directly() {
    let (p, _dir) = planner().await;
    let q = "[1.0, 0.0]";
    let cases: &[(&str, &[&str])] = &[
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id LIMIT 5",
            &["Some(5) optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id SKIP 3 LIMIT 5",
            &["Some(8) optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id AS id, n._score AS s \
             ORDER BY s DESC LIMIT 7",
            &["Some(7) optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id ORDER BY n._score DESC, n.id LIMIT 7",
            &["Some(7) optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q WITH n, n._score AS s ORDER BY s DESC LIMIT 9 \
             RETURN n.id",
            &["Some(9) optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q WITH n LIMIT 4 RETURN n.id LIMIT 2",
            &["Some(4) optional=false"],
        ),
        // Not fed directly: the default.
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id",
            &["None optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q AND n.x > 3 RETURN n.id LIMIT 5",
            &["None optional=false"],
        ),
        (
            "MATCH (n:N {x: 1}) WHERE n.emb ~= Q RETURN n.id LIMIT 5",
            &["None optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id ORDER BY n.id LIMIT 5",
            &["None optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id ORDER BY n._score ASC LIMIT 5",
            &["None optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN DISTINCT n.x LIMIT 5",
            &["None optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id SKIP 5",
            &["None optional=false"],
        ),
        (
            "MATCH (n:N) WHERE n.emb ~= Q MATCH (n)-[:R]->(m:N) RETURN m.id LIMIT 5",
            &["None optional=false"],
        ),
        // Each UNION branch is sized by its own LIMIT.
        (
            "MATCH (n:N) WHERE n.emb ~= Q RETURN n.id AS id LIMIT 3 \
             UNION ALL MATCH (n:N) WHERE n.emb ~= Q RETURN n.id AS id",
            &["Some(3) optional=false", "None optional=false"],
        ),
        // OPTIONAL MATCH on a clause-created variable.
        (
            "OPTIONAL MATCH (n:N) WHERE n.emb ~= Q RETURN n.id LIMIT 6",
            &["Some(6) optional=true"],
        ),
        (
            "MATCH (a:M) OPTIONAL MATCH (n:N) WHERE n.emb ~= Q AND n.x = a.x RETURN n.id",
            &["None optional=true"],
        ),
    ];
    let mut failures = Vec::new();
    for (cypher, want) in cases {
        let cypher = cypher.replace('Q', q);
        let got = knns(&plan_debug(&p, &cypher));
        if got != *want {
            failures.push(format!("{cypher}\n  got {got:?}\n  want {want:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A `WHERE` above a `LIMIT` is never answered by a KNN below it, and a `~=`
/// that cannot become a KNN is refused at planning.
#[tokio::test]
async fn approx_eq_that_cannot_replace_its_scan_is_refused() {
    let (p, _dir) = planner().await;
    for cypher in [
        "MATCH (n:N) WITH n LIMIT 5 WHERE n.emb ~= [1.0, 0.0] RETURN n.id",
        "MATCH (n:N) WITH n LIMIT 5 MATCH (n) WHERE n.emb ~= [1.0, 0.0] RETURN n.id",
        "MATCH (n:N) WITH n SKIP 1 LIMIT 5 MATCH (n) WHERE n.emb ~= [1.0, 0.0] RETURN n.id",
        "MATCH (a:N)-[:R]->(n:N) WHERE n.emb ~= [1.0, 0.0] RETURN n.id",
        "MATCH (a:N), (n:N) WHERE n.emb ~= a.emb RETURN n.id",
        "MATCH (n:N) WHERE n.emb ~= [1.0, 0.0] OR n.x = 1 RETURN n.id",
        "MATCH (a:N) OPTIONAL MATCH (a)-[:R]->(n:N) WHERE a.emb ~= [1.0, 0.0] RETURN n.id",
    ] {
        let ast = uni_cypher::parse(cypher).unwrap();
        match p.plan(ast) {
            Ok(plan) => panic!("{cypher}: expected a planning error, got {plan:?}"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("~=") && msg.contains("vector_similarity"),
                    "{cypher}: {msg}"
                );
            }
        }
    }
}
