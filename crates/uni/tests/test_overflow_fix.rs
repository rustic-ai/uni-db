// Quick test to verify overflow_json fix for post-flush scenarios
use anyhow::Result;
use tempfile::tempdir;
use uni_db::Uni;

#[tokio::test]
async fn test_overflow_json_post_flush() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create schemaless label
    db.schema().label("Person").apply().await?;

    // Create with overflow properties
    db.execute("CREATE (:Person {name: 'Alice', city: 'NYC', age: 30})")
        .await?;

    println!("✓ Created vertex with overflow properties");

    // Verify before flush (L0)
    let results = db
        .query("MATCH (p:Person) RETURN p.name, p.city, p.age")
        .await?;
    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];
    assert_eq!(row.get::<String>("p.name")?, "Alice");
    assert_eq!(row.get::<String>("p.city")?, "NYC");

    println!("✓ Properties accessible from L0");

    // Flush to storage
    db.flush().await?;
    println!("✓ Flushed to storage");

    // Verify after flush (this is what we're fixing!)
    let results = db
        .query("MATCH (p:Person) RETURN p.name, p.city, p.age")
        .await?;
    println!("Results after flush: {} rows", results.len());
    if results.is_empty() {
        // Try simpler query
        let results2 = db.query("MATCH (p:Person) RETURN p").await?;
        println!("Simpler query results: {} rows", results2.len());

        // Try just count
        let results3 = db.query("MATCH (p:Person) RETURN count(p) as cnt").await?;
        if !results3.is_empty() {
            println!("Count query: {}", results3.rows()[0].get::<i64>("cnt")?);
        }
    }
    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];
    assert_eq!(row.get::<String>("p.name")?, "Alice");
    assert_eq!(row.get::<String>("p.city")?, "NYC");

    println!("✓ Properties accessible from storage after flush!");
    println!("\n✅ All tests passed! The overflow_json fix works correctly.");

    Ok(())
}

#[tokio::test]
async fn test_overflow_properties_returned() -> Result<()> {
    use tempfile::tempdir;
    use uni_db::Uni;

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    db.schema().label("Product").apply().await?;

    db.execute("CREATE (:Product {name: 'Book A', category: 'books'})")
        .await?;
    db.flush().await?;

    // First verify we can return overflow properties at all
    let results = db
        .query("MATCH (p:Product) RETURN p.name, p.category")
        .await?;

    println!("Simple return query results: {} rows", results.len());
    if !results.is_empty() {
        let row = &results.rows()[0];
        println!(
            "Row data: name={:?}, category={:?}",
            row.value("p.name"),
            row.value("p.category")
        );
    }

    assert_eq!(results.len(), 1, "Should return 1 row");

    Ok(())
}

#[tokio::test]
async fn test_where_clause_on_overflow_property() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create schemaless label
    db.schema().label("Product").apply().await?;

    // Create test data
    db.execute("CREATE (:Product {name: 'Book A', category: 'books', price: 10})")
        .await?;
    db.execute("CREATE (:Product {name: 'Book B', category: 'books', price: 20})")
        .await?;
    db.execute("CREATE (:Product {name: 'Phone', category: 'electronics', price: 500})")
        .await?;

    println!("✓ Created 3 products with overflow properties");

    // Flush to storage
    db.flush().await?;
    println!("✓ Flushed to storage");

    // Test WHERE clause on overflow property (requires query rewriting)
    let results = db
        .query("MATCH (p:Product) WHERE p.category = 'books' RETURN p.name, p.price")
        .await?;

    println!("Results: {} rows", results.len());
    assert_eq!(results.len(), 2, "Should find 2 books");

    // Verify correct products were returned
    let names: Vec<String> = results
        .rows()
        .iter()
        .map(|r| r.get::<String>("p.name").unwrap())
        .collect();

    assert!(names.contains(&"Book A".to_string()));
    assert!(names.contains(&"Book B".to_string()));
    assert!(!names.contains(&"Phone".to_string()));

    println!("✓ WHERE clause filtering on overflow property works!");

    Ok(())
}
