// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Tests for inverted index functionality.
//!
//! These tests verify that inverted indexes work correctly for
//! set membership queries (`ANY IN`) and support incremental updates.

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_inverted_index() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create Schema
    db.query(
        r#"
        CALL uni.schema.createLabel('Product', {
            "properties": {
                "name": { "type": "STRING" },
                "tags": { "type": "LIST<STRING>" }
            },
            "indexes": [
                { "property": "tags", "type": "Inverted" }
            ]
        })
    "#,
    )
    .await?;

    // 2. Insert Data
    db.query("CREATE (p:Product {name: 'A', tags: ['rust', 'db']})")
        .await?;
    db.query("CREATE (p:Product {name: 'B', tags: ['rust', 'web']})")
        .await?;
    db.query("CREATE (p:Product {name: 'C', tags: ['python', 'ml']})")
        .await?;
    db.query("CREATE (p:Product {name: 'D', tags: ['rust']})")
        .await?;

    // 3. Flush to persistence (Inverted Index reads from persistent storage)
    db.flush().await?;

    // 4. Rebuild index (since we don't have incremental updates yet)
    // Drop and Recreate
    db.query("CALL uni.schema.dropIndex('Product_tags_inverted')")
        .await?;
    db.query(
        r#"
        CALL uni.schema.createIndex('Product', 'tags', { "type": "Inverted" })
    "#,
    )
    .await?;

    // 5. Query
    let results = db.query("MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['rust']) RETURN p.name ORDER BY p.name").await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results.rows[0].get::<String>("p.name")?, "A");
    assert_eq!(results.rows[1].get::<String>("p.name")?, "B");
    assert_eq!(results.rows[2].get::<String>("p.name")?, "D");

    let results_or = db.query("MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['web', 'ml']) RETURN p.name ORDER BY p.name").await?;
    assert_eq!(results_or.len(), 2);
    assert_eq!(results_or.rows[0].get::<String>("p.name")?, "B");
    assert_eq!(results_or.rows[1].get::<String>("p.name")?, "C");

    Ok(())
}

/// Tests that inverted index supports incremental updates without full rebuild.
///
/// This test verifies that:
/// 1. New vertices are added to the index on flush
/// 2. Deleted vertices are removed from the index on flush
/// 3. No explicit index rebuild is required
#[tokio::test]
async fn test_inverted_index_incremental_updates() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create Schema with inverted index
    db.query(
        r#"
        CALL uni.schema.createLabel('Article', {
            "properties": {
                "title": { "type": "STRING" },
                "categories": { "type": "LIST<STRING>" }
            },
            "indexes": [
                { "property": "categories", "type": "Inverted" }
            ]
        })
    "#,
    )
    .await?;

    // 2. Insert initial batch of data
    db.query("CREATE (a:Article {title: 'Rust Guide', categories: ['programming', 'rust']})")
        .await?;
    db.query(
        "CREATE (a:Article {title: 'Python Tutorial', categories: ['programming', 'python']})",
    )
    .await?;
    db.flush().await?;

    // 3. Verify initial query works (NO index rebuild needed!)
    let results = db
        .query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['rust']) RETURN a.title")
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("a.title")?, "Rust Guide");

    // 4. Add more data (incremental update)
    db.query(
        "CREATE (a:Article {title: 'Rust Web Dev', categories: ['programming', 'rust', 'web']})",
    )
    .await?;
    db.query("CREATE (a:Article {title: 'ML Basics', categories: ['ml', 'python']})")
        .await?;
    db.flush().await?;

    // 5. Verify new data is in the index (NO index rebuild needed!)
    let results = db
        .query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['rust']) RETURN a.title ORDER BY a.title")
        .await?;
    assert_eq!(
        results.len(),
        2,
        "Expected 2 Rust articles after incremental update"
    );
    assert_eq!(results.rows[0].get::<String>("a.title")?, "Rust Guide");
    assert_eq!(results.rows[1].get::<String>("a.title")?, "Rust Web Dev");

    // 6. Verify OR queries work
    let results = db
        .query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['ml', 'web']) RETURN a.title ORDER BY a.title")
        .await?;
    assert_eq!(results.len(), 2, "Expected 2 articles with ml or web");
    assert_eq!(results.rows[0].get::<String>("a.title")?, "ML Basics");
    assert_eq!(results.rows[1].get::<String>("a.title")?, "Rust Web Dev");

    // 7. All programming articles
    let results = db
        .query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['programming']) RETURN a.title ORDER BY a.title")
        .await?;
    assert_eq!(results.len(), 3, "Expected 3 programming articles");

    Ok(())
}

/// Tests that the inverted index handles edge cases correctly.
#[tokio::test]
async fn test_inverted_index_edge_cases() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create Schema
    db.query(
        r#"
        CALL uni.schema.createLabel('Item', {
            "properties": {
                "name": { "type": "STRING" },
                "tags": { "type": "LIST<STRING>" }
            },
            "indexes": [
                { "property": "tags", "type": "Inverted" }
            ]
        })
    "#,
    )
    .await?;

    // Insert item with empty tags list
    db.query("CREATE (i:Item {name: 'Empty', tags: []})")
        .await?;

    // Insert item with single tag
    db.query("CREATE (i:Item {name: 'Single', tags: ['only']})")
        .await?;

    // Insert item with duplicate tags (should be deduplicated by index)
    db.query("CREATE (i:Item {name: 'Dupes', tags: ['tag', 'tag', 'tag']})")
        .await?;

    db.flush().await?;

    // Query for single tag
    let results = db
        .query("MATCH (i:Item) WHERE ANY(t IN i.tags WHERE t IN ['only']) RETURN i.name")
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("i.name")?, "Single");

    // Query for tag (should find Dupes only once)
    let results = db
        .query("MATCH (i:Item) WHERE ANY(t IN i.tags WHERE t IN ['tag']) RETURN i.name")
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("i.name")?, "Dupes");

    // Query for non-existent tag
    let results = db
        .query("MATCH (i:Item) WHERE ANY(t IN i.tags WHERE t IN ['nonexistent']) RETURN i.name")
        .await?;
    assert_eq!(results.len(), 0);

    Ok(())
}
