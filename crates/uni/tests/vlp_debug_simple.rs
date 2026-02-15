use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_vlp_simple() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create simple chain: A -> B
    db.execute("CREATE (a:A)-[:REL]->(b:B)").await?;

    // First verify we have 2 nodes
    let all_nodes = db.query("MATCH (n) RETURN n").await?;
    println!("Total nodes: {}", all_nodes.len());
    assert_eq!(all_nodes.len(), 2, "Should have 2 nodes");

    // Test cartesian product
    let cartesian = db.query("MATCH (n), (m) RETURN n, m").await?;
    println!("Cartesian product: {}", cartesian.len());
    for (i, row) in cartesian.iter().enumerate() {
        println!("Row {}: {:?}", i, row);
    }
    assert_eq!(cartesian.len(), 4, "Cartesian should be 4 (2x2)");

    // Test single-hop pattern predicate (this works)
    let single_hop = db
        .query("MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m")
        .await?;
    println!("Single-hop pattern predicate: {}", single_hop.len());
    for (i, row) in single_hop.iter().enumerate() {
        println!("Single-hop row {}: {:?}", i, row);
    }
    assert_eq!(single_hop.len(), 1, "Should have 1 match (A->B)");

    // Test VLP pattern predicate (this fails)
    let vlp = db
        .query("MATCH (n), (m) WHERE (n)-[:REL*1..1]->(m) RETURN n, m")
        .await?;
    println!("VLP pattern predicate: {}", vlp.len());
    for (i, row) in vlp.iter().enumerate() {
        println!("VLP row {}: {:?}", i, row);
    }
    assert_eq!(vlp.len(), 1, "Should have 1 match (A->B)");

    Ok(())
}
