//! Regression tests for schemaless fixed-length edge property filtering.
//!
//! Bug 3: In schemaless mode, edge property predicates on fixed-length patterns
//! are not applied (the filter expression is dropped).

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_schemaless_edge_property_filter() -> Result<()> {
    // No schema setup — fully schemaless mode
    let db = Uni::in_memory().build().await?;

    db.execute(
        r#"
        CREATE (a {name: 'root'})
        CREATE (x {name: 'monkey_friend'})
        CREATE (y {name: 'woot_friend'})
        CREATE (a)-[:KNOWS {name: 'monkey'}]->(x)
        CREATE (a)-[:KNOWS {name: 'woot'}]->(y)
    "#,
    )
    .await?;

    // Edge property filter: only edges with name='monkey'
    let result = db
        .query("MATCH (n {name: 'root'})-[r:KNOWS {name: 'monkey'}]->(a) RETURN a.name")
        .await?;
    assert_eq!(
        result.rows().len(),
        1,
        "Schemaless edge property filter should select only matching edges"
    );
    assert_eq!(result.rows()[0].get::<String>("a.name")?, "monkey_friend");

    Ok(())
}

#[tokio::test]
async fn test_schemaless_edge_property_no_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute(
        r#"
        CREATE (a {name: 'root'})
        CREATE (x {name: 'other'})
        CREATE (a)-[:KNOWS {name: 'monkey'}]->(x)
    "#,
    )
    .await?;

    // No edge has name='nope'
    let result = db
        .query("MATCH (n {name: 'root'})-[r:KNOWS {name: 'nope'}]->(a) RETURN a.name")
        .await?;
    assert_eq!(result.rows().len(), 0, "No edges match name='nope'");

    Ok(())
}
