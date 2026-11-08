// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Tests for embedding service implementations.
// Candle-based embeddings are the default (no stack overflow issues).
// ONNX-based embed tests require the 'provider-onnx' feature flag.

use anyhow::Result;
use serde_json::json;
use uni_db::Uni;
use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

/// Build a [`ModelAliasSpec`] for a Candle text-embedding model.
///
/// The inline `CREATE VECTOR INDEX … embedding: { alias, source }` DDL
/// references a model by catalog alias; the alias must be registered on the
/// runtime via `Uni::temporary().xervo_catalog(...)`. `model_id` is a Candle
/// model name such as `all-MiniLM-L6-v2`.
fn candle_alias(alias: &str, model_id: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task: ModelTask::Embed,
        provider_id: "local/candle".to_string(),
        model_id: model_id.to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: json!({}),
    }
}

/// Test that Candle embedding works for vector search.
/// Candle is the default embedding provider.
#[tokio::test]
#[ignore] // Requires model download from HuggingFace Hub
async fn test_candle_embedding_basic() -> Result<()> {
    let db = Uni::temporary()
        .xervo_catalog(vec![candle_alias("embed/default", "all-MiniLM-L6-v2")])
        .build()
        .await?;

    // 1. Create label with content property
    // 2. Create vector index with Candle auto-embedding
    // all-MiniLM-L6-v2 produces 384-dimensional embeddings
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Document (content STRING)").await?;
    tx.execute(
        r#"
        CREATE VECTOR INDEX doc_embed_idx
        FOR (d:Document) ON (d.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: {
                alias: 'embed/default',
                source: ['content']
            }
        }
    "#,
    )
    .await?;
    // 3. Insert a document - this triggers auto-embedding
    tx.execute(r#"CREATE (:Document {content: "Test content for embedding generation."})"#)
        .await?;
    tx.commit().await?;

    // 4. Flush to persist the data
    db.flush().await?;

    // 5. Verify the embedding was generated
    let result = db
        .session()
        .query("MATCH (d:Document) RETURN count(d) AS cnt")
        .await?;
    let count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(count, 1, "Expected 1 document");

    // Verify embedding was stored
    let result = db
        .session()
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
    let db = Uni::temporary()
        .xervo_catalog(vec![candle_alias("embed/default", "all-MiniLM-L6-v2")])
        .build()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Article (title STRING, body STRING)")
        .await?;
    tx.execute(
        r#"
        CREATE VECTOR INDEX article_embed_idx
        FOR (a:Article) ON (a.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: {
                alias: 'embed/default',
                source: ['title', 'body']
            }
        }
    "#,
    )
    .await?;
    tx.commit().await?;

    // Insert multiple documents
    for i in 1..=5 {
        let tx = db.session().tx().await?;
        tx.execute(&format!(
            r#"CREATE (:Article {{title: "Article {}", body: "This is the body of article number {}."}})"#,
            i, i
        ))
        .await?;
        tx.commit().await?;
    }

    db.flush().await?;

    // Verify all documents have embeddings
    let result = db
        .session()
        .query("MATCH (a:Article) RETURN count(a) AS cnt")
        .await?;
    let count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(count, 5, "Expected 5 articles");

    // Verify embeddings were generated for all
    let result = db
        .session()
        .query("MATCH (a:Article) WHERE a.embedding IS NOT NULL RETURN count(a) AS cnt")
        .await?;
    let emb_count: i64 = result.rows()[0].get("cnt")?;
    assert_eq!(emb_count, 5, "All 5 articles should have embeddings");

    Ok(())
}

#[cfg(feature = "provider-onnx")]
mod fastembed_tests {
    use super::*;
    use serde_json::json;
    use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

    /// Build a [`ModelAliasSpec`] for a fastembed model.
    ///
    /// `model_id` is a fastembed-rs `TextEmbedding` enum value (e.g.
    /// `BGESmallENV15`, `AllMiniLML6V2`). Auto-registered via
    /// `Uni::temporary().xervo_catalog(...)` in each test.
    fn fastembed_alias(alias: &str, model_id: &str) -> ModelAliasSpec {
        ModelAliasSpec {
            alias: alias.to_string(),
            task: ModelTask::Embed,
            provider_id: "local/onnx".to_string(),
            model_id: model_id.to_string(),
            revision: None,
            warmup: WarmupPolicy::Lazy,
            required: false,
            timeout: None,
            load_timeout: None,
            retry: None,
            options: json!({}),
        }
    }

    /// End-to-end fastembed auto-embedding: `CREATE` against a vector index
    /// carrying an `embedding` config fills the vector in on write.
    ///
    /// The name is historical and its original claim was wrong. It read
    /// "without the fix (explicit 8MB stack), this would cause a stack overflow
    /// on the Tokio blocking thread pool" — but that stack size is set in the
    /// PyO3 module initialiser (`bindings/uni-db/src/lib.rs`), for the query
    /// executor's nested async state machines rather than for fastembed, and a
    /// `#[tokio::test]` in this crate never loads that module. So this test ran
    /// on a default stack throughout and cannot have been guarding it; the
    /// bindings' own runtime is covered by the `Python Tests` job.
    #[tokio::test]
    #[ignore] // Requires model download from HuggingFace Hub
    async fn test_fastembed_no_stack_overflow() -> Result<()> {
        // BGESmallENV15 produces 384-dimensional embeddings.
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias("embed/default", "BGESmallENV15")])
            .build()
            .await?;

        // 1. Create label with content property
        // 2. Create vector index with fastembed auto-embedding
        // 3. Insert a document - this triggers auto-embedding
        // Without the stack overflow fix, this would crash
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Document (content STRING)").await?;
        tx.execute(
            r#"
            CREATE VECTOR INDEX doc_embed_idx
            FOR (d:Document) ON (d.embedding)
            OPTIONS {
                metric: 'cosine',
                embedding: {
                    alias: 'embed/default',
                    source: ['content']
                }
            }
        "#,
        )
        .await?;
        tx.execute(r#"CREATE (:Document {content: "Test content for embedding generation."})"#)
            .await?;
        tx.commit().await?;

        // 4. Flush to persist the data
        db.flush().await?;

        // 5. Verify the embedding was generated (reaching this point means no stack overflow)
        let result = db
            .session()
            .query("MATCH (d:Document) RETURN count(d) AS cnt")
            .await?;
        let count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(count, 1, "Expected 1 document");

        // Verify embedding was stored
        let result = db
            .session()
            .query("MATCH (d:Document) WHERE d.embedding IS NOT NULL RETURN count(d) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 1, "Document should have embedding");

        Ok(())
    }

    /// Several auto-embedded writes in sequence, to exercise repeated loads of
    /// the same model rather than a single cold one.
    #[tokio::test]
    #[ignore] // Requires model download from HuggingFace Hub
    async fn test_fastembed_multiple_embeddings() -> Result<()> {
        let db = Uni::temporary()
            .xervo_catalog(vec![fastembed_alias("embed/default", "AllMiniLML6V2")])
            .build()
            .await?;

        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL Article (title STRING, body STRING)")
            .await?;
        tx.execute(
            r#"
            CREATE VECTOR INDEX article_embed_idx
            FOR (a:Article) ON (a.embedding)
            OPTIONS {
                metric: 'cosine',
                embedding: {
                    alias: 'embed/default',
                    source: ['title', 'body']
                }
            }
        "#,
        )
        .await?;
        tx.commit().await?;

        // Insert multiple documents
        for i in 1..=5 {
            let tx = db.session().tx().await?;
            tx.execute(&format!(
                r#"CREATE (:Article {{title: "Article {}", body: "This is the body of article number {}."}})"#,
                i, i
            ))
            .await?;
            tx.commit().await?;
        }

        db.flush().await?;

        // Verify all documents have embeddings
        let result = db
            .session()
            .query("MATCH (a:Article) RETURN count(a) AS cnt")
            .await?;
        let count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(count, 5, "Expected 5 articles");

        // Verify embeddings were generated for all
        let result = db
            .session()
            .query("MATCH (a:Article) WHERE a.embedding IS NOT NULL RETURN count(a) AS cnt")
            .await?;
        let emb_count: i64 = result.rows()[0].get("cnt")?;
        assert_eq!(emb_count, 5, "All 5 articles should have embeddings");

        Ok(())
    }
}
