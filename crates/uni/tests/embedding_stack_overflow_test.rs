// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for embedding service implementations.
// Candle-based embeddings are the default (no stack overflow issues).
// FastEmbed tests require the 'fastembed' feature flag.

use anyhow::Result;
use uni_db::Uni;

/// Test that Candle embedding works for vector search.
/// Candle is the default embedding provider.
#[tokio::test]
#[ignore] // Requires model download from HuggingFace Hub
async fn test_candle_embedding_basic() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create label with content property
    db.execute("CREATE LABEL Document (content STRING)").await?;

    // 2. Create vector index with Candle auto-embedding
    // all-MiniLM-L6-v2 produces 384-dimensional embeddings
    db.execute(
        r#"
        CREATE VECTOR INDEX doc_embed_idx
        FOR (d:Document) ON (d.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: {
                provider: 'Candle',
                model: 'all-MiniLM-L6-v2',
                source: ['content']
            }
        }
    "#,
    )
    .await?;

    // 3. Insert a document - this triggers auto-embedding
    db.execute(r#"CREATE (:Document {content: "Test content for embedding generation."})"#)
        .await?;

    // 4. Flush to persist the data
    db.flush().await?;

    // 5. Verify the embedding was generated
    let result = db
        .query("MATCH (d:Document) RETURN count(d) AS cnt")
        .await?;
    let count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(count, 1, "Expected 1 document");

    // Verify embedding was stored
    let result = db
        .query("MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) AS cnt")
        .await?;
    let emb_count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(emb_count, 1, "Document should have embedding");

    Ok(())
}

/// Test multiple Candle embeddings to ensure stability.
#[tokio::test]
#[ignore] // Requires model download from HuggingFace Hub
async fn test_candle_multiple_embeddings() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Article (title STRING, body STRING)")
        .await?;

    db.execute(
        r#"
        CREATE VECTOR INDEX article_embed_idx
        FOR (a:Article) ON (a.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: {
                provider: 'Candle',
                model: 'all-MiniLM-L6-v2',
                source: ['title', 'body']
            }
        }
    "#,
    )
    .await?;

    // Insert multiple documents
    for i in 1..=5 {
        db.execute(&format!(
            r#"CREATE (:Article {{title: "Article {}", body: "This is the body of article number {}."}})"#,
            i, i
        ))
        .await?;
    }

    db.flush().await?;

    // Verify all documents have embeddings
    let result = db.query("MATCH (a:Article) RETURN count(a) AS cnt").await?;
    let count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(count, 5, "Expected 5 articles");

    // Verify embeddings were generated for all
    let result = db
        .query("MATCH (a:Article) WHERE a.embedding IS NOT NULL RETURN count(a) AS cnt")
        .await?;
    let emb_count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(emb_count, 5, "All 5 articles should have embeddings");

    Ok(())
}

// FastEmbed tests (only compiled when fastembed feature is enabled)
#[cfg(feature = "fastembed")]
mod fastembed_tests {
    use super::*;

    /// Test that fastembed embedding works without stack overflow.
    /// This test triggers auto-embedding via CREATE with a vector index
    /// that has embedding_config set. Without the fix (explicit 8MB stack),
    /// this would cause a stack overflow on the Tokio blocking thread pool.
    #[tokio::test]
    async fn test_fastembed_no_stack_overflow() -> Result<()> {
        let db = Uni::temporary().build().await?;

        // 1. Create label with content property
        db.execute("CREATE LABEL Document (content STRING)").await?;

        // 2. Create vector index with fastembed auto-embedding
        // BGESmallENV15 produces 384-dimensional embeddings
        db.execute(
            r#"
            CREATE VECTOR INDEX doc_embed_idx
            FOR (d:Document) ON (d.embedding)
            OPTIONS {
                metric: 'cosine',
                embedding: {
                    provider: 'FastEmbed',
                    model: 'BGESmallENV15',
                    source: ['content']
                }
            }
        "#,
        )
        .await?;

        // 3. Insert a document - this triggers auto-embedding
        // Without the stack overflow fix, this would crash
        db.execute(r#"CREATE (:Document {content: "Test content for embedding generation."})"#)
            .await?;

        // 4. Flush to persist the data
        db.flush().await?;

        // 5. Verify the embedding was generated (reaching this point means no stack overflow)
        let result = db
            .query("MATCH (d:Document) RETURN count(d) AS cnt")
            .await?;
        let count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(count, 1, "Expected 1 document");

        // Verify embedding was stored
        let result = db
            .query("MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 1, "Document should have embedding");

        Ok(())
    }

    /// Test multiple embeddings to ensure thread spawning is stable.
    #[tokio::test]
    async fn test_fastembed_multiple_embeddings() -> Result<()> {
        let db = Uni::temporary().build().await?;

        db.execute("CREATE LABEL Article (title STRING, body STRING)")
            .await?;

        db.execute(
            r#"
            CREATE VECTOR INDEX article_embed_idx
            FOR (a:Article) ON (a.embedding)
            OPTIONS {
                metric: 'cosine',
                embedding: {
                    provider: 'FastEmbed',
                    model: 'AllMiniLML6V2',
                    source: ['title', 'body']
                }
            }
        "#,
        )
        .await?;

        // Insert multiple documents
        for i in 1..=5 {
            db.execute(&format!(
                r#"CREATE (:Article {{title: "Article {}", body: "This is the body of article number {}."}})"#,
                i, i
            ))
            .await?;
        }

        db.flush().await?;

        // Verify all documents have embeddings
        let result = db.query("MATCH (a:Article) RETURN count(a) AS cnt").await?;
        let count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(count, 5, "Expected 5 articles");

        // Verify embeddings were generated for all
        let result = db
            .query("MATCH (a:Article) WHERE a.embedding IS NOT NULL RETURN count(a) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 5, "All 5 articles should have embeddings");

        Ok(())
    }
}
