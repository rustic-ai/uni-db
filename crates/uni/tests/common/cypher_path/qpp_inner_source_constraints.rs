// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The first node inside a quantified path pattern.
//!
//! In `(a) ((x:L)-[:R]->(y)){1,3} (b)` the label on `x` holds for the node
//! that starts every iteration: `a` itself, and each node where one
//! iteration ends and the next begins. It was checked only on the first
//! iteration, and only when no outer node preceded the pattern — so with
//! `(a)` in front, or after the first hop, it was not checked at all. The
//! walk now checks it on every transition that starts an iteration. The
//! same defect, unquantified, dropped the label of `(a) ((x:M)-->(y)) (b)`.
//!
//! Three neighbours in the same code:
//!
//! * Under `OPTIONAL MATCH`, an inner source constraint on an already bound
//!   node filtered the input instead of nulling the clause (the W4 class),
//!   dropping rows.
//! * An inner *target* label the schema does not declare was ignored, so
//!   `(y:Undeclared)` matched every node.
//!
//! Each quantified pattern is compared with a variable-length oracle that
//! spells the same constraint with `labels(nodes(p)[i])`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(qpp_inner_source_constraints)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// `1:L -> 2:L -> 3:M -> 4:L -> 5:L -> 1`, plus `2 -> 6:M`, all `R`.
async fn open(flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("L")
        .property("id", DataType::Int)
        .label("M")
        .property("id", DataType::Int)
        .edge_type("R", &["L", "M"], &["L", "M"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n1:L {id: 1}), (n2:L {id: 2}), (n3:M {id: 3}), (n4:L {id: 4}), \
         (n5:L {id: 5}), (n6:M {id: 6}), \
         (n1)-[:R]->(n2), (n2)-[:R]->(n3), (n3)-[:R]->(n4), (n4)-[:R]->(n5), \
         (n5)-[:R]->(n1), (n2)-[:R]->(n6)",
    )
    .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

async fn bag(db: &Uni, query: &str) -> Result<Vec<String>> {
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
    let mut rows: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

/// Every node that starts a hop — all but the last — has label `L`.
const STARTS_ARE_L: &str = "all(n IN nodes(p)[0..length(p)] WHERE 'L' IN labels(n))";

#[tokio::test]
async fn qpp_inner_source_constraints_match_the_oracle() -> Result<()> {
    let pairs: Vec<(String, String)> = vec![
        (
            "MATCH (a) ((x:L)-[:R]->(y)){1,3} (b) RETURN a.id, b.id".into(),
            format!("MATCH p = (a)-[:R*1..3]->(b) WHERE {STARTS_ARE_L} RETURN a.id, b.id"),
        ),
        (
            "MATCH (a) ((:L)-[:R]->()){1,3} (b) RETURN a.id, b.id".into(),
            format!("MATCH p = (a)-[:R*1..3]->(b) WHERE {STARTS_ARE_L} RETURN a.id, b.id"),
        ),
        (
            "MATCH ((x:L)-[:R]->(y)){2,3} (b) RETURN b.id".into(),
            format!("MATCH p = ()-[:R*2..3]->(b) WHERE {STARTS_ARE_L} RETURN b.id"),
        ),
        (
            "MATCH ((x:L)-[:R]->(y)){2} RETURN [n IN x | n.id] AS xs".into(),
            format!(
                "MATCH p = ()-[:R*2]->() WHERE {STARTS_ARE_L} \
                 RETURN [n IN nodes(p)[0..2] | n.id] AS xs"
            ),
        ),
        // Two hops per iteration: the label holds at even positions only.
        (
            "MATCH (a) ((x:L)-[:R]->(y)-[:R]->(z)){1,2} (b) RETURN a.id, b.id".into(),
            "MATCH p = (a)-[:R*2..4]->(b) WHERE length(p) % 2 = 0 \
             AND all(i IN range(0, length(p) - 1, 2) WHERE 'L' IN labels(nodes(p)[i])) \
             RETURN a.id, b.id"
                .into(),
        ),
        (
            "MATCH (a) OPTIONAL MATCH (a) ((x:L)-[:R]->(y)){1,3} (b) RETURN a.id, b.id".into(),
            format!(
                "MATCH (a) OPTIONAL MATCH p = (a)-[:R*1..3]->(b) WHERE {STARTS_ARE_L} \
                 RETURN a.id, b.id"
            ),
        ),
        // Unquantified: the label constrains the one source, `a` itself.
        (
            "MATCH (a) ((x:M)-[:R]->(y)) (b) RETURN a.id, b.id".into(),
            "MATCH (a:M)-[:R]->(b) RETURN a.id, b.id".into(),
        ),
        (
            "MATCH (a) OPTIONAL MATCH (a) ((x:M)-[:R]->(y)) (b) RETURN a.id, b.id".into(),
            "MATCH (a) OPTIONAL MATCH (a)-[:R]->(b) WHERE a:M RETURN a.id, b.id".into(),
        ),
    ];
    for flush in [false, true] {
        let db = open(flush).await?;
        for (qpp, oracle) in &pairs {
            let want = bag(&db, oracle).await?;
            assert!(!want.is_empty(), "vacuous oracle: {oracle}");
            assert_eq!(bag(&db, qpp).await?, want, "flush={flush}: {qpp}");
        }
    }
    Ok(())
}

/// An inner source map or label on a bound node, under `OPTIONAL MATCH`,
/// nulls the clause for a row that fails it rather than dropping the row.
#[tokio::test]
async fn qpp_inner_source_constraints_keep_optional_rows() -> Result<()> {
    let cases: &[(&str, &[&str])] = &[
        (
            "MATCH (a:L) OPTIONAL MATCH (a) ((x {id: 1})-[:R]->(y)) (b) RETURN a.id, b.id",
            &[
                "[Int(1), Int(2)]",
                "[Int(2), Null]",
                "[Int(4), Null]",
                "[Int(5), Null]",
            ],
        ),
        (
            "MATCH (x:L) OPTIONAL MATCH ((x {id: 2})-[:R]->(y)) RETURN x.id, y.id",
            &[
                "[Int(1), Null]",
                "[Int(2), Int(3)]",
                "[Int(2), Int(6)]",
                "[Int(4), Null]",
                "[Int(5), Null]",
            ],
        ),
        (
            "MATCH (a:L) OPTIONAL MATCH (a) ((x:M)-[:R]->(y)) (b) RETURN a.id, b.id",
            &[
                "[Int(1), Null]",
                "[Int(2), Null]",
                "[Int(4), Null]",
                "[Int(5), Null]",
            ],
        ),
    ];
    for flush in [false, true] {
        let db = open(flush).await?;
        for (query, want) in cases {
            assert_eq!(bag(&db, query).await?, *want, "flush={flush}: {query}");
        }
    }
    Ok(())
}

/// A label the schema does not declare is still a constraint.
#[tokio::test]
async fn qpp_inner_source_constraints_undeclared_labels_constrain() -> Result<()> {
    let db = open(false).await?;
    for query in [
        "MATCH (a {id: 1}) ((x)-[:R]->(y:Nope)){1,2} (b) RETURN b.id",
        "MATCH (a {id: 1}) (()-[:R]->(:Nope)){1,3} (b) RETURN b.id",
        "MATCH (a {id: 1}) ((x:Nope)-[:R]->(y)){1,2} (b) RETURN b.id",
    ] {
        assert!(bag(&db, query).await?.is_empty(), "{query}");
    }
    Ok(())
}

/// Like the other inner nodes, the first carries at most one label.
#[tokio::test]
async fn qpp_inner_source_constraints_refuse_two_labels() -> Result<()> {
    let db = open(false).await?;
    let err = db
        .session()
        .query("MATCH (a) ((x:L:M)-[:R]->(y)){1,2} (b) RETURN b.id")
        .await
        .expect_err("two labels on the first inner node are refused");
    assert!(err.to_string().contains("at most one label"), "{err}");
    Ok(())
}
