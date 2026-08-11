// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_quantified_pattern_fixed() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Node")
        .property("id", DataType::Int64)
        .edge_type("NEXT", &["Node"], &["Node"])
        .apply()
        .await?;

    // Create chain: (1)->(2)->(3)->(4)->(5)
    let tx = db.session().tx().await?;
    tx.execute("CREATE (n1:Node {id: 1}), (n2:Node {id: 2}), (n3:Node {id: 3}), (n4:Node {id: 4}), (n5:Node {id: 5})").await?;
    tx.execute("MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:NEXT]->(n2)")
        .await?;
    tx.execute("MATCH (n2:Node {id: 2}), (n3:Node {id: 3}) CREATE (n2)-[:NEXT]->(n3)")
        .await?;
    tx.execute("MATCH (n3:Node {id: 3}), (n4:Node {id: 4}) CREATE (n3)-[:NEXT]->(n4)")
        .await?;
    tx.execute("MATCH (n4:Node {id: 4}), (n5:Node {id: 5}) CREATE (n4)-[:NEXT]->(n5)")
        .await?;
    tx.commit().await?;

    // 2 hops: (1)->(3), (2)->(4), (3)->(5).
    //
    // The endpoints are the outer `(s)` and `(e)`. `a` and `b` are declared
    // inside the quantifier and so are GQL group variables — lists with one
    // element per iteration — not the pattern's start and end. The inner
    // `:Node` label still constrains each iteration's source.
    let query = "MATCH (s:Node)((a:Node)-[:NEXT]->(b)){2}(e) \
                 RETURN s.id as start, e.id as end ORDER BY start";
    let results = db.session().query(query).await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results.rows()[0].get::<i64>("start")?, 1);
    assert_eq!(results.rows()[0].get::<i64>("end")?, 3);
    assert_eq!(results.rows()[1].get::<i64>("start")?, 2);
    assert_eq!(results.rows()[1].get::<i64>("end")?, 4);
    assert_eq!(results.rows()[2].get::<i64>("start")?, 3);
    assert_eq!(results.rows()[2].get::<i64>("end")?, 5);

    Ok(())
}

/// The same pattern read through its group variables: each holds one element
/// per iteration, and `b`'s first element is `a`'s second because consecutive
/// iterations share a node.
#[tokio::test]
async fn test_quantified_pattern_group_variables() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Node")
        .property("id", DataType::Int64)
        .edge_type("NEXT", &["Node"], &["Node"])
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (n1:Node {id: 1}), (n2:Node {id: 2}), (n3:Node {id: 3})")
        .await?;
    tx.execute("MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:NEXT]->(n2)")
        .await?;
    tx.execute("MATCH (n2:Node {id: 2}), (n3:Node {id: 3}) CREATE (n2)-[:NEXT]->(n3)")
        .await?;
    tx.commit().await?;

    let results = db
        .session()
        .query(
            "MATCH (s:Node {id: 1})((a:Node)-[:NEXT]->(b)){2}(e) \
             RETURN [n IN a | n.id] AS a_ids, [n IN b | n.id] AS b_ids",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let ids = |col: &str| -> Vec<i64> {
        match results.rows()[0].value(col).unwrap() {
            uni_query::Value::List(items) => items
                .iter()
                .map(|v| match v {
                    uni_query::Value::Int(i) => *i,
                    other => panic!("expected an int, got {other:?}"),
                })
                .collect(),
            other => panic!("expected a list, got {other:?}"),
        }
    };
    assert_eq!(ids("a_ids"), vec![1, 2]);
    assert_eq!(ids("b_ids"), vec![2, 3]);

    Ok(())
}

#[tokio::test]
async fn test_quantified_pattern_variable() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Node")
        .property("id", DataType::Int64)
        .edge_type("NEXT", &["Node"], &["Node"])
        .apply()
        .await?;

    // (1)->(2)->(3)
    let tx = db.session().tx().await?;
    tx.execute("CREATE (n1:Node {id: 1}), (n2:Node {id: 2}), (n3:Node {id: 3})")
        .await?;
    tx.execute("MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:NEXT]->(n2)")
        .await?;
    tx.execute("MATCH (n2:Node {id: 2}), (n3:Node {id: 3}) CREATE (n2)-[:NEXT]->(n3)")
        .await?;
    tx.commit().await?;

    // 1 to 2 hops from 1. The property map that used to sit on the inner
    // source node moves to the outer anchor, which is what it always meant:
    // it constrains where the traversal starts, not every iteration.
    let query = "MATCH (s:Node {id: 1})((a:Node)-[:NEXT]->(b)){1,2}(e) \
                 RETURN e.id as end ORDER BY end";
    let results = db.session().query(query).await?;

    assert_eq!(results.len(), 2);
    assert_eq!(results.rows()[0].get::<i64>("end")?, 2); // 1 hop
    assert_eq!(results.rows()[1].get::<i64>("end")?, 3); // 2 hops

    Ok(())
}
