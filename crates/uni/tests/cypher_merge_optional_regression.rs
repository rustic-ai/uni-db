//! Regression tests for MERGE + OPTIONAL MATCH interactions.
//!
//! Bug 4: OPTIONAL MATCH drops unmatched rows when both endpoints are bound.

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_merge_optional_match_count() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("B")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema().edge_type("KNOWS", &[], &[]).apply().await?;
    db.schema().edge_type("HATES", &[], &[]).apply().await?;

    // TCK Match8 #2 reproduction:
    // Create 2 nodes (a1, a2) with edges:
    // a1-[:KNOWS]->a2 and a1-[:HATES]->a2
    db.execute(
        r#"
        CREATE (a:A {name: 'a1'}), (b:A {name: 'a2'})
        CREATE (a)-[:KNOWS]->(b)
        CREATE (a)-[:HATES]->(b)
    "#,
    )
    .await?;

    // MATCH (a) finds 2 nodes
    // MERGE (b:B {name: 'b1'}) creates/merges 1 node → cross product = 2 rows
    // OPTIONAL MATCH (a)--(b) finds 0 edges (no edges from A to B label)
    // count(*) should be 2 (preserving unmatched rows)
    let result = db
        .query(
            "MATCH (a:A) MERGE (b:B {name: 'b1'}) WITH a, b OPTIONAL MATCH (a)--(b) RETURN count(*) AS cnt",
        )
        .await?;
    assert_eq!(result.rows().len(), 1, "Should return 1 row with count");
    let cnt = result.rows()[0].get::<i64>("cnt")?;
    assert_eq!(
        cnt, 2,
        "OPTIONAL MATCH with no edges should preserve all 2 input rows"
    );

    Ok(())
}

#[tokio::test]
async fn test_optional_match_both_bound_no_edge() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("B")
        .property("name", DataType::String)
        .apply()
        .await?;

    // Create disconnected nodes
    db.execute("CREATE (:A {name: 'alice'})").await?;
    db.execute("CREATE (:B {name: 'bob'})").await?;

    // Both a and b are bound from MATCH. OPTIONAL MATCH should preserve the row
    // with NULLs for the relationship.
    let result = db
        .query("MATCH (a:A), (b:B) OPTIONAL MATCH (a)-[r]-(b) RETURN a.name, b.name, r")
        .await?;
    assert_eq!(
        result.rows().len(),
        1,
        "OPTIONAL MATCH with no edge should still return the input row"
    );
    assert_eq!(result.rows()[0].get::<String>("a.name")?, "alice");
    assert_eq!(result.rows()[0].get::<String>("b.name")?, "bob");

    Ok(())
}
