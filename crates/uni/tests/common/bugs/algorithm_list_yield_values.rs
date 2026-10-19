// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An algorithm's list and map yields are real lists and maps to every
//! expression that consumes them, standalone and per row.
//!
//! They were emitted as a `Utf8` column holding the value's display text, so
//! `size(clique)` counted characters, `UNWIND` saw one string, and indexing
//! and `IN` failed or matched nothing. Only the final row conversion parsed
//! the text back, which is why `RETURN clique` looked right. A node yield
//! (a node id) was text in the same way, so `id(n) = node` never matched.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(algorithm_list_yield_values)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// A path `P1 -> P2 -> P3` and a triangle `T1, T2, T3` linked both ways.
async fn graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .label("T")
        .property("id", DataType::Int)
        .edge_type("LINK", &["P"], &["P"])
        .edge_type("TL", &["T"], &["T"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {id: 1})-[:LINK]->(:P {id: 2})-[:LINK]->(:P {id: 3})")
        .await?;
    tx.execute(
        "CREATE (a:T {id: 1}), (b:T {id: 2}), (c:T {id: 3}), \
         (a)-[:TL]->(b), (b)-[:TL]->(a), (b)-[:TL]->(c), (c)-[:TL]->(b), \
         (a)-[:TL]->(c), (c)-[:TL]->(a)",
    )
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn rows(db: &Uni, query: &str) -> Result<Vec<Vec<Value>>> {
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
    Ok(result.rows().iter().map(|r| r.values().to_vec()).collect())
}

#[tokio::test]
async fn algorithm_list_yield_values_are_lists_standalone() -> Result<()> {
    let db = graph().await?;
    let clique = "CALL uni.algo.maximalCliques(['T'], ['TL'], 3) YIELD clique";
    let cases: Vec<(String, Vec<Vec<Value>>)> = vec![
        (
            format!("{clique} RETURN size(clique) AS n"),
            vec![vec![Value::Int(3)]],
        ),
        (
            format!("{clique} UNWIND clique AS v RETURN count(v) AS n"),
            vec![vec![Value::Int(3)]],
        ),
        (
            format!(
                "{clique} RETURN clique[0] IN clique AS has_first, \
                 size([v IN clique WHERE v >= 0]) AS ints"
            ),
            vec![vec![Value::Bool(true), Value::Int(3)]],
        ),
        (
            format!(
                "{clique} MATCH (t:T) WHERE id(t) IN clique \
                 RETURN count(t) AS n"
            ),
            vec![vec![Value::Int(3)]],
        ),
        (
            "CALL uni.algo.isBipartite(['P'], ['LINK']) YIELD partition AS p \
             RETURN size(keys(p)) AS n"
                .to_owned(),
            vec![vec![Value::Int(3)]],
        ),
    ];
    for (query, want) in cases {
        assert_eq!(rows(&db, &query).await?, want, "{query}");
    }
    // The yield itself still returns as a list.
    let got = rows(&db, &format!("{clique} RETURN clique")).await?;
    assert!(
        matches!(&got[0][0], Value::List(l) if l.len() == 3),
        "clique must be a list of three ids: {got:?}"
    );
    Ok(())
}

#[tokio::test]
async fn algorithm_list_yield_values_are_lists_per_row() -> Result<()> {
    let db = graph().await?;
    let call = "MATCH (a:P {id: 1}), (b:P {id: 3}) \
                CALL uni.algo.shortestPath(a, b, ['LINK']) YIELD nodeIds";
    let cases: Vec<(String, Vec<Vec<Value>>)> = vec![
        (
            format!(
                "{call} RETURN size(nodeIds) AS n, id(a) IN nodeIds AS has_a, \
                 nodeIds[0] = id(a) AS first_is_a"
            ),
            vec![vec![Value::Int(3), Value::Bool(true), Value::Bool(true)]],
        ),
        (
            format!("{call} UNWIND nodeIds AS v RETURN count(v) AS n"),
            vec![vec![Value::Int(3)]],
        ),
    ];
    for (query, want) in cases {
        assert_eq!(rows(&db, &query).await?, want, "{query}");
    }
    Ok(())
}

/// A node yield is the node's id inside the query too, not its text.
#[tokio::test]
async fn algorithm_list_yield_values_node_yield_is_an_id() -> Result<()> {
    let db = graph().await?;
    let got = rows(
        &db,
        "CALL uni.algo.articulationPoints(['P'], ['LINK']) YIELD node \
         MATCH (p:P) WHERE id(p) = node RETURN p.id AS id",
    )
    .await?;
    assert_eq!(got, vec![vec![Value::Int(2)]], "P2 is the cut vertex");
    Ok(())
}
