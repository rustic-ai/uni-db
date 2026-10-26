// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A Locy rule keeps its values' types.
//!
//! The planner typed every `FOLD` output except `COUNT` as Float64, and every
//! property it could not place on a label (an unlabelled node, any
//! relationship) as Float64 too, casting the rule body's values before they
//! reached the aggregate. So `MIN(b.id)` over integers returned `1.0`,
//! `COLLECT(b.id)` a list of floats, `b.id AS bid` on an unlabelled `b` and
//! `r.w AS w` returned floats — and above 2^53 the value itself was wrong.
//! `MIN`/`MAX` (and `MMIN`/`MMAX`) now take their argument's type, `COLLECT`'s
//! input is not cast, a property whose every schema declaration agrees takes
//! that type, and a property declared nowhere (a schemaless graph) is left as
//! the stored value — found by the W5 random-program oracle, where `b` bound
//! by `a IS reach TO b` still came back as a float.
//!
//! Found by the `locy_fold` relation of `metamorphic::dqp::topo` (W4).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_value_types)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

/// Above 2^53, where an `f64` cannot hold every integer.
const BIG: i64 = 9_007_199_254_740_993;

async fn open() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .property("big", DataType::Int)
        .edge_type("K", &["P"], &["P"])
        .property("w", DataType::Int)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (a:P {{id: 1, big: 0}}), (b:P {{id: 2, big: {BIG}}}), (c:P {{id: 3, big: 5}}), \
         (a)-[:K {{w: 7}}]->(b), (a)-[:K {{w: 4}}]->(c), (b)-[:K {{w: 1}}]->(c)"
    ))
    .await?;
    tx.commit().await?;
    Ok(db)
}

async fn query_row(db: &Uni, program: &str) -> Result<Vec<Value>> {
    let result = db.session().locy(program).await?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("program has a QUERY");
    assert_eq!(rows.len(), 1, "{program}");
    let mut columns: Vec<_> = rows[0].iter().collect();
    columns.sort_by_key(|(name, _)| (*name).clone());
    Ok(columns.into_iter().map(|(_, v)| v.clone()).collect())
}

#[tokio::test]
async fn locy_value_types_fold_min_max_collect() -> Result<()> {
    let db = open().await?;
    // Labelled and unlabelled targets alike.
    for target in ["(b:P)", "(b)"] {
        let program = format!(
            "CREATE RULE f AS MATCH (a:P)-[:K]->{target} \
             FOLD lo = MIN(b.id), hi = MAX(b.big), c = COLLECT(b.id) YIELD KEY a, lo, hi, c \
             QUERY f WHERE a.id = 1 RETURN lo AS c0, hi AS c1, c AS c2"
        );
        let row = query_row(&db, &program).await?;
        assert_eq!(row[0], Value::Int(2), "{program}");
        assert_eq!(row[1], Value::Int(BIG), "{program}");
        let Value::List(mut ids) = row[2].clone() else {
            panic!("COLLECT returned {:?}", row[2]);
        };
        ids.sort_by_key(|v| format!("{v:?}"));
        assert_eq!(ids, vec![Value::Int(2), Value::Int(3)], "{program}");
    }
    Ok(())
}

#[tokio::test]
async fn locy_value_types_yielded_properties() -> Result<()> {
    let db = open().await?;
    // An unlabelled node's property and a relationship's, without any FOLD.
    let row = query_row(
        &db,
        "CREATE RULE f AS MATCH (a:P)-[r:K]->(b) YIELD KEY a, KEY b, b.big AS big, r.w AS w \
         QUERY f WHERE b.id = 2 RETURN big AS c0, w AS c1",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(BIG), Value::Int(7)]);
    let row = query_row(
        &db,
        "CREATE RULE f AS MATCH (a:P)-[r:K]->(b:P) FOLD hi = MAX(r.w) YIELD KEY a, hi \
         QUERY f WHERE a.id = 1 RETURN hi AS c0",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(7)]);
    Ok(())
}

/// A recursive rule's monotone `MMAX`, over the reach of a node.
#[tokio::test]
async fn locy_value_types_recursive_max() -> Result<()> {
    let db = open().await?;
    let row = query_row(
        &db,
        "CREATE RULE reach AS MATCH (a)-[:K]->(b) YIELD KEY a, KEY b \
         CREATE RULE reach AS MATCH (a)-[:K]->(m) WHERE m IS reach TO b YIELD KEY a, KEY b \
         CREATE RULE far AS MATCH (a:P) WHERE a IS reach TO b FOLD hi = MAX(b.big) YIELD KEY a, hi \
         QUERY far WHERE a.id = 1 RETURN hi AS c0",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(BIG)]);
    Ok(())
}

/// A schemaless graph, with the target bound by an `IS ... TO` reference rather
/// than by `MATCH`.
#[tokio::test]
async fn locy_value_types_schemaless() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n0:Node {id: 0}), (n1:Node {id: 1}), \
         (n0)-[:EDGE {w: 1}]->(n1), (n0)-[:EDGE {w: 4}]->(n0)",
    )
    .await?;
    tx.commit().await?;
    let row = query_row(
        &db,
        "CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b \
         CREATE RULE reach AS MATCH (a:Node)-[:EDGE]->(m:Node) WHERE m IS reach TO b \
         YIELD KEY a, KEY b \
         CREATE RULE span AS MATCH (a:Node) WHERE a IS reach TO b \
         FOLD hi = MAX(b.id), lo = MMIN(b.id) YIELD KEY a, hi, lo \
         QUERY span RETURN hi AS c0, lo AS c1",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(1), Value::Int(0)]);
    let row = query_row(
        &db,
        "CREATE RULE d AS MATCH (a:Node)-[e:EDGE]->(b:Node) FOLD lo = MIN(e.w), hi = MAX(e.w) \
         YIELD KEY a, lo, hi QUERY d RETURN lo AS c0, hi AS c1",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(1), Value::Int(4)]);
    Ok(())
}

/// An ALONG accumulation keeps its type, unified over the rule's clauses: an
/// integer path cost is an integer (every ALONG column was Float64), one float
/// step makes it a float, and over a schemaless graph it is the stored value,
/// which a downstream SUM can still read. Found by the W5 random-program oracle.
#[tokio::test]
async fn locy_value_types_along() -> Result<()> {
    let db = open().await?;
    let base = "CREATE RULE c AS MATCH (a:P)-[e:K]->(b:P) ALONG q = e.w YIELD KEY a, KEY b, q ";
    let row = query_row(
        &db,
        &format!(
            "{base}CREATE RULE c AS MATCH (a:P)-[e:K]->(m:P) WHERE m IS c TO b \
             ALONG q = prev.q + e.w YIELD KEY a, KEY b, q \
             QUERY c WHERE a.id = 1 AND b.id = 3 AND q > 7 RETURN q AS c0"
        ),
    )
    .await?;
    assert_eq!(row, vec![Value::Int(8)]);
    let row = query_row(
        &db,
        &format!(
            "{base}CREATE RULE c AS MATCH (a:P)-[e:K]->(m:P) WHERE m IS c TO b \
             ALONG q = prev.q * 0.5 + e.w YIELD KEY a, KEY b, q \
             QUERY c WHERE a.id = 1 AND b.id = 3 AND q > 4 RETURN q AS c0"
        ),
    )
    .await?;
    assert_eq!(row, vec![Value::Float(7.5)]);
    // Above 2^53.
    let row = query_row(
        &db,
        &format!(
            "CREATE RULE c AS MATCH (a:P)-[e:K]->(b:P) ALONG q = b.big + 0 YIELD KEY a, KEY b, q \
             QUERY c WHERE b.id = 2 RETURN q AS c0"
        ),
    )
    .await?;
    assert_eq!(row, vec![Value::Int(BIG)]);

    // Schemaless.
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:N {id: 0})-[:E {w: 2}]->(b:N {id: 1})-[:E {w: 3}]->(c:N {id: 2})")
        .await?;
    tx.commit().await?;
    let row = query_row(
        &db,
        "CREATE RULE c AS MATCH (a:N)-[e:E]->(b:N) ALONG q = e.w YIELD KEY a, KEY b, q \
         CREATE RULE c AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS c TO b ALONG q = prev.q + e.w \
         YIELD KEY a, KEY b, q \
         CREATE RULE t AS MATCH (a:N) WHERE a IS c TO b FOLD s = SUM(q) YIELD KEY a, s \
         QUERY c WHERE a.id = 0 AND b.id = 2 RETURN q AS c0",
    )
    .await?;
    assert_eq!(row, vec![Value::Int(5)]);
    let result = db
        .session()
        .locy(
            "CREATE RULE c AS MATCH (a:N)-[e:E]->(b:N) ALONG q = e.w YIELD KEY a, KEY b, q \
             CREATE RULE c AS MATCH (a:N)-[e:E]->(m:N) WHERE m IS c TO b ALONG q = prev.q + e.w \
             YIELD KEY a, KEY b, q \
             CREATE RULE t AS MATCH (a:N) WHERE a IS c TO b FOLD s = SUM(q) YIELD KEY a, s \
             QUERY t WHERE a.id = 0 RETURN s AS c0",
        )
        .await?;
    let rows = result
        .command_results()
        .iter()
        .find_map(|c| c.as_query())
        .expect("a QUERY");
    // An integer ALONG value sums to an integer, as Cypher's `sum` does (issue
    // #316); it was the float `7.0`.
    assert_eq!(rows[0].get("c0"), Some(&Value::Int(7)));
    Ok(())
}
