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
    db.execute("CREATE (n1:Node {id: 1}), (n2:Node {id: 2}), (n3:Node {id: 3}), (n4:Node {id: 4}), (n5:Node {id: 5})").await?;
    db.execute("MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:NEXT]->(n2)")
        .await?;
    db.execute("MATCH (n2:Node {id: 2}), (n3:Node {id: 3}) CREATE (n2)-[:NEXT]->(n3)")
        .await?;
    db.execute("MATCH (n3:Node {id: 3}), (n4:Node {id: 4}) CREATE (n3)-[:NEXT]->(n4)")
        .await?;
    db.execute("MATCH (n4:Node {id: 4}), (n5:Node {id: 5}) CREATE (n4)-[:NEXT]->(n5)")
        .await?;

    // 2 hops: (1)->(3), (2)->(4), (3)->(5)
    let query = "MATCH ((a:Node)-[:NEXT]->(b)){2} RETURN a.id as start, b.id as end ORDER BY a.id";
    let results = db.query(query).await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results.rows[0].get::<i64>("start")?, 1);
    assert_eq!(results.rows[0].get::<i64>("end")?, 3);
    assert_eq!(results.rows[1].get::<i64>("start")?, 2);
    assert_eq!(results.rows[1].get::<i64>("end")?, 4);
    assert_eq!(results.rows[2].get::<i64>("start")?, 3);
    assert_eq!(results.rows[2].get::<i64>("end")?, 5);

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
    db.execute("CREATE (n1:Node {id: 1}), (n2:Node {id: 2}), (n3:Node {id: 3})")
        .await?;
    db.execute("MATCH (n1:Node {id: 1}), (n2:Node {id: 2}) CREATE (n1)-[:NEXT]->(n2)")
        .await?;
    db.execute("MATCH (n2:Node {id: 2}), (n3:Node {id: 3}) CREATE (n2)-[:NEXT]->(n3)")
        .await?;

    // 1 to 2 hops from 1
    let query = "MATCH ((a:Node {id: 1})-[:NEXT]->(b)){1,2} RETURN b.id as end ORDER BY b.id";
    let results = db.query(query).await?;

    assert_eq!(results.len(), 2);
    assert_eq!(results.rows[0].get::<i64>("end")?, 2); // 1 hop
    assert_eq!(results.rows[1].get::<i64>("end")?, 3); // 2 hops

    Ok(())
}
