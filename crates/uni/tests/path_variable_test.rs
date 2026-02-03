// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_path_variable() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .property("since", DataType::Int64)
        .apply()
        .await?;

    db.execute("CREATE (a:Person {name: 'Alice'})").await?;
    db.execute("CREATE (b:Person {name: 'Bob'})").await?;
    db.execute("CREATE (c:Person {name: 'Charlie'})").await?;
    db.execute("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)").await?;
    db.execute("MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS {since: 2021}]->(c)").await?;

    // 1. Path variable in Variable Length Traversal
    // MATCH p = (a)-[:KNOWS*1..2]->(b) WHERE a.name = 'Alice' RETURN p
    let result = db.query("MATCH p = (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(b) RETURN p, length(p) AS len ORDER BY len").await?;

    // Should have 2 paths: Alice->Bob (len 1), Alice->Bob->Charlie (len 2)
    assert_eq!(result.len(), 2);

    // Path 1 (len 1)
    let row1 = &result.rows()[0];
    let len1: i64 = row1.get("len")?;
    assert_eq!(len1, 1);

    // Path 2 (len 2)
    let row2 = &result.rows()[1];
    let len2: i64 = row2.get("len")?;
    assert_eq!(len2, 2);

    // Verify Path object structure (via JSON/Value inspection if possible, or specialized getters)
    // Currently public API Row::get returns FromValue types.
    // types::Path is public.
    // Let's see if we can get it as Path
    // uni_db::Path is re-exported from uni-query::types::Path

    let p1: uni_db::Path = row1.get("p")?;
    assert_eq!(p1.nodes.len(), 2); // Alice, Bob
    assert_eq!(p1.edges.len(), 1); // KNOWS

    let p2: uni_db::Path = row2.get("p")?;
    assert_eq!(p2.nodes.len(), 3); // Alice, Bob, Charlie
    assert_eq!(p2.edges.len(), 2); // KNOWS, KNOWS

    // 2. NODES() and RELATIONSHIPS() functions
    let result = db.query("MATCH p = (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN nodes(p) AS ns, relationships(p) AS rels").await?;
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];

    // nodes(p) returns List<Node>
    // but Row::get returns FromValue.
    // Vec<Node> implements FromValue via Vec<T>.
    let ns: Vec<uni_db::Node> = row.get("ns")?;
    assert_eq!(ns.len(), 2);
    // Note: Node objects reconstructed from Path currently have empty labels/properties in executor
    // because fetch logic is not implemented inside build_traverse_match for efficiency/complexity reasons yet.
    // They contain VIDs.
    // We can verify IDs match.

    // relationships(p) returns List<Edge>
    let rels: Vec<uni_db::Edge> = row.get("rels")?;
    assert_eq!(rels.len(), 1);

    Ok(())
}
