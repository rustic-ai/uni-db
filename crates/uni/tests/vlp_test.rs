use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_basic_vlp_works() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create chain: A -> B -> C
    db.execute("CREATE (a:A)-[:REL]->(b:B)-[:REL]->(c:C)")
        .await?;

    // Test 1: Regular VLP (non-pattern-predicate)
    let results = db.query("MATCH (n)-[:REL*1..2]->(m) RETURN n, m").await?;
    println!("Test 1 - Regular VLP: {} rows", results.len());
    for (i, row) in results.iter().enumerate() {
        println!("  Row {}: {:?}", i, row);
    }

    // Test 2: Pattern predicate VLP
    let results2 = db
        .query("MATCH (n), (m) WHERE (n)-[:REL*1..2]->(m) RETURN n, m")
        .await?;
    println!("\nTest 2 - Pattern predicate VLP: {} rows", results2.len());
    for (i, row) in results2.iter().enumerate() {
        println!("  Row {}: {:?}", i, row);
    }

    Ok(())
}
