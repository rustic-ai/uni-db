// Comprehensive tests for pattern predicates with two bound nodes
// Covers Pattern1 TCK tests [13], [14], [15], [16], [17], [18]
use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_pattern_two_nodes_vlp_undirected() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create graph: (a:A)-[:REL1]->(b:B), (b)-[:REL2]->(a), (a)-[:REL3]->(c:C), (a)-[:REL1]->(d:D)
    db.execute(
        r#"
        CREATE (a:A)-[:REL1]->(b:B), (b)-[:REL2]->(a), (a)-[:REL3]->(:C), (a)-[:REL1]->(:D)
        "#,
    )
    .await?;

    // Query: MATCH (n), (m) WHERE (n)-[:REL1*2]-(m) RETURN n, m
    // Expected: D and B (via path D <-REL1- A -REL1-> B)
    let results = db
        .query(
            r#"
            MATCH (n), (m) WHERE (n)-[:REL1*2]-(m) RETURN n, m
            "#,
        )
        .await?;

    println!("Results: {:#?}", results);
    println!("Row count: {}", results.len());

    // Should have 2 rows: (D, B) and (B, D)
    assert_eq!(results.len(), 2, "Expected 2 results");

    Ok(())
}

#[tokio::test]
async fn test_pattern_two_nodes_simple() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Simpler test: just two nodes with a single edge
    db.execute(
        r#"
        CREATE (a:A)-[:REL]->(b:B)
        "#,
    )
    .await?;

    // First, let's see what the Cartesian product gives us
    let cartesian = db.query("MATCH (n), (m) RETURN n, m").await?;
    println!("Cartesian product rows: {}", cartesian.len());
    for (i, row) in cartesian.iter().enumerate() {
        println!("  Row {}: {:?}", i, row);
    }

    // Query: MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m
    let results = db
        .query(
            r#"
            MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m
            "#,
        )
        .await?;

    println!("Filtered results: {:#?}", results);
    println!("Filtered row count: {}", results.len());

    // Should have 1 row: (A, B)
    assert_eq!(results.len(), 1, "Expected 1 result");

    Ok(())
}

// Pattern1 [14]: Two nodes on single outgoing directed connection
#[tokio::test]
async fn test_pattern_two_nodes_directed() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:A)-[:REL]->(b:B)").await?;

    // MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m
    // Expected: 1 row (A, B)
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m")
        .await?;

    assert_eq!(results.len(), 1, "Expected 1 result for directed edge");

    Ok(())
}

// Pattern1 [13]: Two nodes on single undirected connection
#[tokio::test]
async fn test_pattern_two_nodes_undirected() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:A)-[:REL]->(b:B)").await?;

    // MATCH (n), (m) WHERE (n)-[:REL]-(m) RETURN n, m
    // Expected: 2 rows (A, B) and (B, A)
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:REL]-(m) RETURN n, m")
        .await?;

    assert_eq!(results.len(), 2, "Expected 2 results for undirected edge");

    Ok(())
}

// Pattern1 [15]: Two nodes on single undirected connection with type
#[tokio::test]
async fn test_pattern_two_nodes_typed_undirected() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create multiple edge types
    db.execute("CREATE (a:A)-[:REL]->(b:B), (a)-[:OTHER]->(c:C)")
        .await?;

    // MATCH (n), (m) WHERE (n)-[:REL]-(m) RETURN n, m
    // Expected: 2 rows (A, B) and (B, A), NOT (A, C)
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:REL]-(m) RETURN n, m")
        .await?;

    assert_eq!(results.len(), 2, "Expected 2 results for typed undirected");

    Ok(())
}

// Pattern1 [16]: Two nodes on VLP outgoing directed
#[tokio::test]
async fn test_pattern_two_nodes_vlp_directed() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create chain: A -> B -> C
    db.execute("CREATE (a:A)-[:REL]->(b:B)-[:REL]->(c:C)")
        .await?;

    // MATCH (n), (m) WHERE (n)-[:REL*1..2]->(m) RETURN n, m
    // Expected: 3 rows (A, B), (B, C), (A, C)
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:REL*1..2]->(m) RETURN n, m")
        .await?;

    assert_eq!(results.len(), 3, "Expected 3 results for VLP directed");

    Ok(())
}

// Pattern1 [17]: Two nodes on VLP undirected
#[tokio::test]
async fn test_pattern_two_nodes_vlp_undirected_single() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:A)-[:REL]->(b:B)").await?;

    // MATCH (n), (m) WHERE (n)-[:REL*1..2]-(m) RETURN n, m
    // Expected: 2 rows (A, B) and (B, A)
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:REL*1..2]-(m) RETURN n, m")
        .await?;

    assert_eq!(
        results.len(),
        2,
        "Expected 2 results for VLP undirected single edge"
    );

    Ok(())
}

// No match case: verify empty result set
#[tokio::test]
async fn test_pattern_two_nodes_no_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE (a:A), (b:B)").await?;

    // MATCH (n), (m) WHERE (n)-[:NONEXISTENT]->(m) RETURN n, m
    // Expected: 0 rows
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:NONEXISTENT]->(m) RETURN n, m")
        .await?;

    assert_eq!(results.len(), 0, "Expected 0 results for non-existent edge");

    Ok(())
}

// Self-loop case: verify self-loops are included when they exist
#[tokio::test]
async fn test_pattern_two_nodes_self_loop() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create self-loop and regular edge
    db.execute("CREATE (a:A)-[:REL]->(a), (a)-[:REL]->(b:B)")
        .await?;

    // MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m
    // Expected: 2 rows (A, A) and (A, B)
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:REL]->(m) RETURN n, m")
        .await?;

    assert_eq!(results.len(), 2, "Expected 2 results including self-loop");

    Ok(())
}

// Multiple edge types with VLP
#[tokio::test]
async fn test_pattern_two_nodes_vlp_multiple_types() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create graph with multiple types
    db.execute("CREATE (a:A)-[:REL1]->(b:B), (a)-[:REL2]->(c:C)")
        .await?;

    // MATCH (n), (m) WHERE (n)-[:REL1*1..1]->(m) RETURN n, m
    // Expected: 1 row (A, B), NOT (A, C)
    let results = db
        .query("MATCH (n), (m) WHERE (n)-[:REL1*1..1]->(m) RETURN n, m")
        .await?;

    assert_eq!(
        results.len(),
        1,
        "Expected 1 result for VLP with type filter"
    );

    Ok(())
}
