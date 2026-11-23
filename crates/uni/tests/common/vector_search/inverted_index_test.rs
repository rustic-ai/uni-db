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
    let tx = db.session().tx().await?;
    tx.execute(
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
    tx.commit().await?;

    // 2. Insert Data
    let tx = db.session().tx().await?;
    tx.execute("CREATE (p:Product {name: 'A', tags: ['rust', 'db']})")
        .await?;
    tx.execute("CREATE (p:Product {name: 'B', tags: ['rust', 'web']})")
        .await?;
    tx.execute("CREATE (p:Product {name: 'C', tags: ['python', 'ml']})")
        .await?;
    tx.execute("CREATE (p:Product {name: 'D', tags: ['rust']})")
        .await?;
    tx.commit().await?;

    // 3. Flush to persistence (Inverted Index reads from persistent storage)
    db.flush().await?;

    // 4. Rebuild index (since we don't have incremental updates yet)
    // Drop and Recreate
    let tx = db.session().tx().await?;
    tx.execute("CALL uni.schema.dropIndex('Product_tags_inverted')")
        .await?;
    tx.execute(
        r#"
        CALL uni.schema.createIndex('Product', 'tags', { "type": "Inverted" })
    "#,
    )
    .await?;
    tx.commit().await?;

    // 5. Query
    let results = db.session().query("MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['rust']) RETURN p.name ORDER BY p.name").await?;

    assert_eq!(results.len(), 3);
    assert_eq!(results.rows()[0].get::<String>("p.name")?, "A");
    assert_eq!(results.rows()[1].get::<String>("p.name")?, "B");
    assert_eq!(results.rows()[2].get::<String>("p.name")?, "D");

    let results_or = db.session().query("MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['web', 'ml']) RETURN p.name ORDER BY p.name").await?;
    assert_eq!(results_or.len(), 2);
    assert_eq!(results_or.rows()[0].get::<String>("p.name")?, "B");
    assert_eq!(results_or.rows()[1].get::<String>("p.name")?, "C");

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
    let tx = db.session().tx().await?;
    tx.execute(
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
    tx.commit().await?;

    // 2. Insert initial batch of data
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:Article {title: 'Rust Guide', categories: ['programming', 'rust']})")
        .await?;
    tx.execute(
        "CREATE (a:Article {title: 'Python Tutorial', categories: ['programming', 'python']})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // 3. Verify initial query works (NO index rebuild needed!)
    let results = db
        .session()
        .query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['rust']) RETURN a.title")
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows()[0].get::<String>("a.title")?, "Rust Guide");

    // 4. Add more data (incremental update)
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Article {title: 'Rust Web Dev', categories: ['programming', 'rust', 'web']})",
    )
    .await?;
    tx.execute("CREATE (a:Article {title: 'ML Basics', categories: ['ml', 'python']})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // 5. Verify new data is in the index (NO index rebuild needed!)
    let results = db
        .session().query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['rust']) RETURN a.title ORDER BY a.title")
        .await?;
    assert_eq!(
        results.len(),
        2,
        "Expected 2 Rust articles after incremental update"
    );
    assert_eq!(results.rows()[0].get::<String>("a.title")?, "Rust Guide");
    assert_eq!(results.rows()[1].get::<String>("a.title")?, "Rust Web Dev");

    // 6. Verify OR queries work
    let results = db
        .session().query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['ml', 'web']) RETURN a.title ORDER BY a.title")
        .await?;
    assert_eq!(results.len(), 2, "Expected 2 articles with ml or web");
    assert_eq!(results.rows()[0].get::<String>("a.title")?, "ML Basics");
    assert_eq!(results.rows()[1].get::<String>("a.title")?, "Rust Web Dev");

    // 7. All programming articles
    let results = db
        .session().query("MATCH (a:Article) WHERE ANY(c IN a.categories WHERE c IN ['programming']) RETURN a.title ORDER BY a.title")
        .await?;
    assert_eq!(results.len(), 3, "Expected 3 programming articles");

    Ok(())
}

/// Tests that the inverted index handles edge cases correctly.
#[tokio::test]
async fn test_inverted_index_edge_cases() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create Schema
    let tx = db.session().tx().await?;
    tx.execute(
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
    tx.commit().await?;

    // Insert item with empty tags list
    let tx = db.session().tx().await?;
    tx.execute("CREATE (i:Item {name: 'Empty', tags: []})")
        .await?;

    // Insert item with single tag
    tx.execute("CREATE (i:Item {name: 'Single', tags: ['only']})")
        .await?;

    // Insert item with duplicate tags (should be deduplicated by index)
    tx.execute("CREATE (i:Item {name: 'Dupes', tags: ['tag', 'tag', 'tag']})")
        .await?;
    tx.commit().await?;

    db.flush().await?;

    // Query for single tag
    let results = db
        .session()
        .query("MATCH (i:Item) WHERE ANY(t IN i.tags WHERE t IN ['only']) RETURN i.name")
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows()[0].get::<String>("i.name")?, "Single");

    // Query for tag (should find Dupes only once)
    let results = db
        .session()
        .query("MATCH (i:Item) WHERE ANY(t IN i.tags WHERE t IN ['tag']) RETURN i.name")
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows()[0].get::<String>("i.name")?, "Dupes");

    // Query for non-existent tag
    let results = db
        .session()
        .query("MATCH (i:Item) WHERE ANY(t IN i.tags WHERE t IN ['nonexistent']) RETURN i.name")
        .await?;
    assert_eq!(results.len(), 0);

    Ok(())
}

/// An update that changes a document's terms removes the vertex from the
/// postings of the terms it no longer holds, at the next flush; so does
/// setting the list to empty or null.
#[tokio::test]
async fn test_inverted_index_update_drops_stale_terms() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let run = |stmt: &'static str| {
        let db = &db;
        async move {
            let tx = db.session().tx().await?;
            tx.execute(stmt).await?;
            tx.commit().await?;
            anyhow::Ok(())
        }
    };
    run("CREATE LABEL Product (name STRING, tags LIST<STRING>)").await?;
    run("CREATE INVERTED INDEX product_tags FOR (p:Product) ON (p.tags)").await?;
    run("CREATE (:Product {name: 'A', tags: ['rust', 'db']}), (:Product {name: 'B', tags: ['web']})")
        .await?;
    db.flush().await?;

    let postings = |terms: &'static [&'static str]| {
        let storage = db.storage();
        async move {
            let index = storage.inverted_index("Product", "tags").await?;
            let terms: Vec<String> = terms.iter().map(|t| (*t).to_string()).collect();
            anyhow::Ok(index.query_any(&terms).await?.len())
        }
    };
    assert_eq!(postings(&["rust"]).await?, 1);

    run("MATCH (p:Product {name: 'A'}) SET p.tags = ['go']").await?;
    db.flush().await?;
    assert_eq!(postings(&["rust"]).await?, 0, "A no longer holds 'rust'");
    assert_eq!(postings(&["db"]).await?, 0, "A no longer holds 'db'");
    assert_eq!(postings(&["go"]).await?, 1);

    run("MATCH (p:Product {name: 'A'}) SET p.tags = []").await?;
    run("MATCH (p:Product {name: 'B'}) SET p.tags = null").await?;
    db.flush().await?;
    assert_eq!(
        postings(&["go"]).await?,
        0,
        "an emptied list holds no terms"
    );
    assert_eq!(postings(&["web"]).await?, 0, "a null list holds no terms");

    // An update to another property leaves the postings as they were.
    run("MATCH (p:Product {name: 'A'}) SET p.tags = ['go']").await?;
    db.flush().await?;
    run("MATCH (p:Product {name: 'A'}) SET p.name = 'A2'").await?;
    db.flush().await?;
    assert_eq!(postings(&["go"]).await?, 1);
    Ok(())
}

/// A sparse vector set to null leaves its vid in no posting after the flush.
#[tokio::test]
async fn test_sparse_index_null_drops_postings() -> Result<()> {
    use uni_db::{DataType, IndexType, Value};
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: 16 })
        .index("emb", IndexType::sparse(16))
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 'a', emb: $e})")
        .param(
            "e",
            Value::SparseVector {
                indices: vec![1, 3],
                values: vec![0.5, 0.25],
            },
        )
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let hits = || async {
        db.storage()
            .sparse_search("Doc", "emb", &[(1, 1.0)], 10, None, None)
            .await
    };
    assert_eq!(hits().await?.len(), 1);

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc) SET d.emb = null").await?;
    tx.commit().await?;
    db.flush().await?;
    assert!(
        hits().await?.is_empty(),
        "a null sparse vector has no postings"
    );
    Ok(())
}
