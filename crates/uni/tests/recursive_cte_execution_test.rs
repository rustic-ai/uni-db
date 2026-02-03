// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_recursive_cte_execution() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Use schema API instead of DDL to avoid parser issues
    db.schema()
        .label("Item")
        .property("id", DataType::Int32)
        .edge_type("CHILD", &["Item"], &["Item"])
        .apply()
        .await?;

    db.execute("CREATE (n0:Item {id: 0})").await?;
    db.execute("CREATE (n1:Item {id: 1})").await?;
    db.execute("CREATE (n2:Item {id: 2})").await?;

    db.execute("MATCH (n0:Item {id: 0}), (n1:Item {id: 1}) CREATE (n0)-[:CHILD]->(n1)")
        .await?;
    db.execute("MATCH (n1:Item {id: 1}), (n2:Item {id: 2}) CREATE (n1)-[:CHILD]->(n2)")
        .await?;

    // Query: Start at 0, follow CHILD recursively
    let query = "
        WITH RECURSIVE hierarchy AS (
            MATCH (root:Item {id: 0}) RETURN root
            UNION
            MATCH (parent:Item)-[:CHILD]->(child:Item)
            WHERE parent IN hierarchy
            RETURN child
        )
        MATCH (n:Item) WHERE n IN hierarchy
        RETURN n.id AS id ORDER BY id
    ";

    let result = db.query(query).await?;
    assert_eq!(result.len(), 3);

    let rows = result.rows();
    assert_eq!(rows[0].get::<i32>("id")?, 0);
    assert_eq!(rows[1].get::<i32>("id")?, 1);
    assert_eq!(rows[2].get::<i32>("id")?, 2);

    Ok(())
}
