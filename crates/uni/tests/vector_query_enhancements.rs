// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for vector query enhancements:
//! - Pre-filtering with SQL WHERE clauses
//! - Distance threshold filtering
//! - Yield order preservation
//! - Conditional property loading performance
//! - Large VID handling

use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_db::Uni;

#[tokio::test]
async fn test_vector_search_with_filter() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema with vector property
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Product")?;
    schema_manager.add_property("Product", "name", DataType::String, false)?;
    schema_manager.add_property("Product", "price", DataType::Float, false)?;
    schema_manager.add_property(
        "Product",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    // 2. Create database and insert products with varying prices
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    db.execute("CREATE (p1:Product {name: 'Cheap Laptop', embedding: [1.0, 0.0], price: 500.0})")
        .await?;
    db.execute("CREATE (p2:Product {name: 'Mid Laptop', embedding: [0.9, 0.1], price: 1500.0})")
        .await?;
    db.execute(
        "CREATE (p3:Product {name: 'Expensive Laptop', embedding: [0.95, 0.05], price: 3000.0})",
    )
    .await?;
    db.execute("CREATE (p4:Product {name: 'Budget Mouse', embedding: [0.85, 0.15], price: 20.0})")
        .await?;

    db.flush().await?;

    // 3. Search with price filter (only products under $1000)
    let result = db
        .query(
            "CALL uni.vector.query('Product', 'embedding', [1.0, 0.0], 10, 'price < 1000.0')
             YIELD node
             RETURN node.name AS name, node.price AS price",
        )
        .await?;

    // Should only return Cheap Laptop and Budget Mouse
    assert_eq!(result.len(), 2);

    let names: Vec<String> = result
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("name").ok())
        .collect();

    assert!(names.contains(&"Cheap Laptop".to_string()));
    assert!(names.contains(&"Budget Mouse".to_string()));
    assert!(!names.contains(&"Mid Laptop".to_string()));
    assert!(!names.contains(&"Expensive Laptop".to_string()));

    // Verify all prices are under $1000
    for row in result.rows() {
        let price: f64 = row.get("price")?;
        assert!(price < 1000.0, "Price {} should be under 1000", price);
    }

    Ok(())
}

#[tokio::test]
async fn test_vector_search_with_threshold() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Item")?;
    schema_manager.add_property("Item", "name", DataType::String, false)?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    // 2. Create items with known distances from query point [1.0, 0.0]
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    db.execute("CREATE (i1:Item {name: 'Very Close', embedding: [1.0, 0.0]})")
        .await?; // Distance: 0.0
    db.execute("CREATE (i2:Item {name: 'Close', embedding: [0.9, 0.1]})")
        .await?; // Distance: ~0.14
    db.execute("CREATE (i3:Item {name: 'Medium', embedding: [0.7, 0.3]})")
        .await?; // Distance: ~0.42
    db.execute("CREATE (i4:Item {name: 'Far', embedding: [0.0, 1.0]})")
        .await?; // Distance: ~1.41

    db.flush().await?;

    // 3. Search with distance threshold of 0.5
    let result = db
        .query(
            "CALL uni.vector.query('Item', 'embedding', [1.0, 0.0], 100, NULL, 0.5)
             YIELD vid, distance
             RETURN vid, distance",
        )
        .await?;

    // Should only return items within distance 0.5
    assert!(result.len() >= 2 && result.len() <= 3);

    // Verify all distances <= 0.5
    for row in result.rows() {
        let distance: f64 = row.get("distance")?;
        assert!(
            distance <= 0.5,
            "Distance {} exceeds threshold 0.5",
            distance
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_vector_search_yield_order() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "text", DataType::String, false)?;
    schema_manager.add_property(
        "Doc",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    // 2. Create test data
    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.execute("CREATE (d:Doc {text: 'Hello', embedding: [1.0, 0.0]})")
        .await?;
    db.flush().await?;

    // 3. Test different yield orders
    let result1 = db
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5)
             YIELD distance, vid, score
             RETURN distance, vid, score",
        )
        .await?;

    // Verify column order matches YIELD order
    assert_eq!(result1.columns(), &["distance", "vid", "score"]);

    let result2 = db
        .query(
            "CALL uni.vector.query('Doc', 'embedding', [1.0, 0.0], 5)
             YIELD score, distance, vid
             RETURN score, distance, vid",
        )
        .await?;

    // Verify column order matches different YIELD order
    assert_eq!(result2.columns(), &["score", "distance", "vid"]);

    Ok(())
}

#[tokio::test]
async fn test_vector_search_conditional_loading() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema with multiple properties
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Article")?;
    schema_manager.add_property("Article", "title", DataType::String, false)?;
    schema_manager.add_property("Article", "content", DataType::String, false)?;
    schema_manager.add_property("Article", "author", DataType::String, false)?;
    schema_manager.add_property(
        "Article",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    // 2. Create articles
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    for i in 0..10 {
        db.execute(&format!(
            "CREATE (a:Article {{title: 'Article {}', content: 'Content {}', author: 'Author {}', embedding: [{}, 0.0]}})",
            i, i, i, 1.0 - (i as f32 * 0.05)
        )).await?;
    }

    db.flush().await?;

    // 3. Test YIELD vid only (should not load properties)
    let result_vid = db
        .query(
            "CALL uni.vector.query('Article', 'embedding', [1.0, 0.0], 10)
             YIELD vid
             RETURN vid",
        )
        .await?;

    assert_eq!(result_vid.len(), 10);
    assert_eq!(result_vid.columns(), &["vid"]);

    // 4. Test YIELD node (should load all properties)
    let result_node = db
        .query(
            "CALL uni.vector.query('Article', 'embedding', [1.0, 0.0], 10)
             YIELD node
             RETURN node.title AS title, node.author AS author",
        )
        .await?;

    assert_eq!(result_node.len(), 10);

    // Verify properties are loaded
    for row in result_node.rows() {
        let title: String = row.get("title")?;
        let author: String = row.get("author")?;
        assert!(title.starts_with("Article "));
        assert!(author.starts_with("Author "));
    }

    Ok(())
}

#[tokio::test]
async fn test_vector_search_score_normalization() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Test with Cosine metric
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("CosineDoc")?;
    schema_manager.add_property(
        "CosineDoc",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.execute("CREATE (d:CosineDoc {embedding: [1.0, 0.0]})")
        .await?;
    db.flush().await?;

    let result = db
        .query(
            "CALL uni.vector.query('CosineDoc', 'embedding', [1.0, 0.0], 5)
             YIELD score, distance
             RETURN score, distance",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let score: f64 = result.rows()[0].get("score")?;
    let distance: f64 = result.rows()[0].get("distance")?;

    // For identical vectors, distance should be ~0 and score should be ~1
    assert!(
        distance < 0.01,
        "Distance should be near 0, got {}",
        distance
    );
    assert!(score > 0.99, "Score should be near 1.0, got {}", score);

    Ok(())
}

#[tokio::test]
async fn test_vector_search_combined_filter_and_threshold() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Product")?;
    schema_manager.add_property("Product", "name", DataType::String, false)?;
    schema_manager.add_property("Product", "category", DataType::String, false)?;
    schema_manager.add_property(
        "Product",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    // 2. Create products in different categories
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    db.execute(
        "CREATE (p:Product {name: 'Laptop A', category: 'Electronics', embedding: [1.0, 0.0]})",
    )
    .await?;
    db.execute(
        "CREATE (p:Product {name: 'Laptop B', category: 'Electronics', embedding: [0.9, 0.1]})",
    )
    .await?;
    db.execute("CREATE (p:Product {name: 'Book A', category: 'Books', embedding: [0.95, 0.05]})")
        .await?;
    db.execute("CREATE (p:Product {name: 'Book B', category: 'Books', embedding: [0.0, 1.0]})")
        .await?;

    db.flush().await?;

    // 3. Search with both filter (Electronics only) AND threshold (distance < 0.3)
    let result = db
        .query(
            "CALL uni.vector.query('Product', 'embedding', [1.0, 0.0], 100, 'category = \"Electronics\"', 0.3)
             YIELD node, distance
             RETURN node.name AS name, node.category AS category, distance"
        )
        .await?;

    // Should only return Electronics products within distance threshold
    for row in result.rows() {
        let category: String = row.get("category")?;
        let distance: f64 = row.get("distance")?;

        assert_eq!(category, "Electronics", "Should only return Electronics");
        assert!(distance <= 0.3, "Distance {} should be <= 0.3", distance);
    }

    // Verify we got at least one result
    assert!(
        !result.is_empty(),
        "Should have at least one matching result"
    );

    Ok(())
}

#[tokio::test]
async fn test_vector_search_null_filter_and_threshold() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Item")?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.execute("CREATE (i:Item {embedding: [1.0, 0.0]})")
        .await?;
    db.flush().await?;

    // 2. Test with explicit NULL for filter and threshold
    let result = db
        .query(
            "CALL uni.vector.query('Item', 'embedding', [1.0, 0.0], 5, NULL, NULL)
             YIELD vid
             RETURN vid",
        )
        .await?;

    assert_eq!(result.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_vector_search_all_yield_types() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Test")?;
    schema_manager.add_property("Test", "value", DataType::String, false)?;
    schema_manager.add_property(
        "Test",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    db.execute("CREATE (t:Test {value: 'test', embedding: [1.0, 0.0]})")
        .await?;
    db.flush().await?;

    // 2. Test all yield types together
    let result = db
        .query(
            "CALL uni.vector.query('Test', 'embedding', [1.0, 0.0], 5)
             YIELD node, vid, distance, score
             RETURN node.value AS value, vid, distance, score",
        )
        .await?;

    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];

    // Verify all yields are present and valid
    let value: String = row.get("value")?;
    let distance: f64 = row.get("distance")?;
    let score: f64 = row.get("score")?;

    // Verify vid column exists and has a value
    assert!(
        result.columns().contains(&"vid".to_string()),
        "VID column should be present"
    );

    assert_eq!(value, "test");
    assert!(distance >= 0.0);
    assert!(
        (0.0..=1.0).contains(&score),
        "Score should be between 0.0 and 1.0, got {}",
        score
    );

    Ok(())
}
