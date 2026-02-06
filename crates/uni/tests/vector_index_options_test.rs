// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Regression test for vector index embedding_config preservation.
// Verifies that embedding options passed to CREATE VECTOR INDEX ... OPTIONS
// are correctly stored in the schema.

use anyhow::Result;
use uni_common::core::schema::{EmbeddingModel, IndexDefinition};
use uni_db::Uni;

/// Test that CREATE VECTOR INDEX with embedding OPTIONS preserves the embedding_config.
#[tokio::test]
async fn test_vector_index_preserves_embedding_config() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create label with a content property
    db.execute("CREATE LABEL Document (content STRING)").await?;

    // 2. Create vector index with embedding options
    db.execute(
        r#"
        CREATE VECTOR INDEX doc_embed_idx
        FOR (d:Document) ON (d.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: {
                provider: 'fastembed',
                model: 'AllMiniLML6V2',
                source: ['content']
            }
        }
    "#,
    )
    .await?;

    // 3. Retrieve schema and verify
    let schema = db.get_schema();
    let index = schema
        .indexes
        .iter()
        .find(|idx| matches!(idx, IndexDefinition::Vector(v) if v.name == "doc_embed_idx"))
        .expect("Index not found");

    if let IndexDefinition::Vector(config) = index {
        assert!(
            config.embedding_config.is_some(),
            "embedding_config should not be None"
        );
        let emb = config.embedding_config.as_ref().unwrap();

        // Verify model is FastEmbed variant
        assert!(
            matches!(emb.model, EmbeddingModel::FastEmbed { .. }),
            "Expected FastEmbed model"
        );

        // Verify source properties
        assert_eq!(emb.source_properties, vec!["content"]);

        // Verify model name
        if let EmbeddingModel::FastEmbed { model_name, .. } = &emb.model {
            assert_eq!(model_name, "AllMiniLML6V2");
        }
    } else {
        panic!("Expected Vector index definition");
    }

    Ok(())
}

/// Test that CREATE VECTOR INDEX without embedding OPTIONS has None for embedding_config.
#[tokio::test]
async fn test_vector_index_without_embedding_options() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create label
    db.execute("CREATE LABEL Product (name STRING)").await?;

    // 2. Create vector index without embedding options
    db.execute(
        r#"
        CREATE VECTOR INDEX product_vec_idx
        FOR (p:Product) ON (p.embedding)
        OPTIONS { metric: 'l2' }
    "#,
    )
    .await?;

    // 3. Verify embedding_config is None
    let schema = db.get_schema();
    let index = schema
        .indexes
        .iter()
        .find(|idx| matches!(idx, IndexDefinition::Vector(v) if v.name == "product_vec_idx"))
        .expect("Index not found");

    if let IndexDefinition::Vector(config) = index {
        assert!(
            config.embedding_config.is_none(),
            "embedding_config should be None when not specified"
        );
    } else {
        panic!("Expected Vector index definition");
    }

    Ok(())
}

/// Test embedding config via procedure API (CALL uni.schema.createIndex).
#[tokio::test]
async fn test_procedure_api_embedding_config() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create label
    db.execute("CREATE LABEL Article (body STRING)").await?;

    // 2. Create vector index with embedding via procedure
    db.query(
        r#"
        CALL uni.schema.createIndex('Article', 'embedding', {
            "type": "VECTOR",
            "name": "article_embed_idx",
            "embedding": {
                "provider": "openai",
                "model": "text-embedding-3-small",
                "source": ["body"]
            }
        })
    "#,
    )
    .await?;

    // 3. Verify embedding_config is set
    let schema = db.get_schema();
    let index = schema
        .indexes
        .iter()
        .find(|idx| matches!(idx, IndexDefinition::Vector(v) if v.name == "article_embed_idx"))
        .expect("Index not found");

    if let IndexDefinition::Vector(config) = index {
        assert!(
            config.embedding_config.is_some(),
            "embedding_config should be set via procedure API"
        );
        let emb = config.embedding_config.as_ref().unwrap();

        // Verify OpenAI model
        assert!(
            matches!(emb.model, EmbeddingModel::OpenAI { .. }),
            "Expected OpenAI model"
        );

        if let EmbeddingModel::OpenAI { model, .. } = &emb.model {
            assert_eq!(model, "text-embedding-3-small");
        }

        assert_eq!(emb.source_properties, vec!["body"]);
    } else {
        panic!("Expected Vector index definition");
    }

    Ok(())
}

/// Test auto-embed in uni.vector.query when index has no embedding_config.
/// This should return a clear error message.
#[tokio::test]
async fn test_auto_embed_error_without_config() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create label and vector index WITHOUT embedding config
    db.execute("CREATE LABEL Item (name STRING)").await?;
    db.execute(
        r#"
        CREATE VECTOR INDEX item_vec_idx
        FOR (i:Item) ON (i.embedding)
        OPTIONS { metric: 'cosine' }
    "#,
    )
    .await?;

    // 2. Try to use auto-embed (string query instead of vector)
    let result = db
        .query(
            r#"
            CALL uni.vector.query('Item', 'embedding', 'search text', 5)
            YIELD vid, score
        "#,
        )
        .await;

    // 3. Should fail with clear error message
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("no embedding_config"),
        "Error should mention missing embedding_config: {}",
        err_msg
    );

    Ok(())
}

/// Test Ollama embedding provider via procedure API.
#[tokio::test]
async fn test_ollama_embedding_provider() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // 1. Create label
    db.execute("CREATE LABEL Note (text STRING)").await?;

    // 2. Create vector index with Ollama embedding
    db.query(
        r#"
        CALL uni.schema.createIndex('Note', 'vector', {
            "type": "VECTOR",
            "name": "note_ollama_idx",
            "embedding": {
                "provider": "ollama",
                "model": "nomic-embed-text",
                "source": ["text"]
            }
        })
    "#,
    )
    .await?;

    // 3. Verify Ollama model
    let schema = db.get_schema();
    let index = schema
        .indexes
        .iter()
        .find(|idx| matches!(idx, IndexDefinition::Vector(v) if v.name == "note_ollama_idx"))
        .expect("Index not found");

    if let IndexDefinition::Vector(config) = index {
        let emb = config
            .embedding_config
            .as_ref()
            .expect("embedding_config should be set");

        assert!(
            matches!(emb.model, EmbeddingModel::Ollama { .. }),
            "Expected Ollama model"
        );

        if let EmbeddingModel::Ollama { model, host } = &emb.model {
            assert_eq!(model, "nomic-embed-text");
            assert_eq!(host, "http://localhost:11434");
        }
    } else {
        panic!("Expected Vector index definition");
    }

    Ok(())
}
