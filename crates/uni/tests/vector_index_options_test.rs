// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use serde_json::json;
use uni_common::core::schema::{DataType, IndexDefinition};
use uni_db::Uni;
use uni_db::api::schema::{EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};
use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

fn mistral_embed_alias(alias: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task: ModelTask::Embed,
        provider_id: "local/mistralrs".to_string(),
        model_id: "nomic-embed-text-v1.5".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: json!({}),
    }
}

#[tokio::test]
async fn test_vector_index_preserves_embedding_alias_config() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Document")
        .property("content", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: Some(EmbeddingCfg {
                    alias: "embed/default".to_string(),
                    source_properties: vec!["content".to_string()],
                    batch_size: 32,
                }),
            }),
        )
        .apply()
        .await?;

    let schema = db.get_schema();
    let index = schema
        .indexes
        .iter()
        .find(|idx| matches!(idx, IndexDefinition::Vector(v) if v.label == "Document"))
        .expect("Vector index not found");

    if let IndexDefinition::Vector(config) = index {
        let emb = config
            .embedding_config
            .as_ref()
            .expect("embedding_config should be present");
        assert_eq!(emb.alias, "embed/default");
        assert_eq!(emb.source_properties, vec!["content"]);
        assert_eq!(emb.batch_size, 32);
    } else {
        panic!("Expected vector index");
    }

    Ok(())
}

#[tokio::test]
async fn test_procedure_api_embedding_alias_config() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Article")
        .property("body", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .apply()
        .await?;

    db.query(
        r#"
        CALL uni.schema.createIndex('Article', 'embedding', {
            "type": "VECTOR",
            "name": "article_embed_idx",
            "embedding": {
                "alias": "embed/default",
                "source": ["body"],
                "batch_size": 8
            }
        })
    "#,
    )
    .await?;

    let schema = db.get_schema();
    let index = schema
        .indexes
        .iter()
        .find(|idx| matches!(idx, IndexDefinition::Vector(v) if v.name == "article_embed_idx"))
        .expect("Index not found");

    if let IndexDefinition::Vector(config) = index {
        let emb = config
            .embedding_config
            .as_ref()
            .expect("embedding_config should be set");
        assert_eq!(emb.alias, "embed/default");
        assert_eq!(emb.source_properties, vec!["body"]);
        assert_eq!(emb.batch_size, 8);
    } else {
        panic!("Expected vector index");
    }

    Ok(())
}

#[tokio::test]
async fn test_auto_embed_string_query_requires_xervo_runtime() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Item")
        .property("content", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .apply()
        .await?;

    db.execute(
        r#"
        CREATE VECTOR INDEX item_vec_idx
        FOR (i:Item) ON (i.embedding)
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

    let result = db
        .query(
            r#"
            CALL uni.vector.query('Item', 'embedding', 'search text', 5)
            YIELD vid, score
        "#,
        )
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Uni-Xervo runtime not configured"),
        "Unexpected error: {err_msg}"
    );

    Ok(())
}

#[tokio::test]
async fn test_vector_e2e_lifecycle_create_insert_flush_query_delete_query() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Doc")
        .property("id", DataType::Int64)
        .property("content", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L2,
                embedding: Some(EmbeddingCfg {
                    alias: "embed/default".to_string(),
                    source_properties: vec!["content".to_string()],
                    batch_size: 16,
                }),
            }),
        )
        .apply()
        .await?;

    db.execute("CREATE (d:Doc {id: 1, content: 'alpha', embedding: [0.0, 0.0]})")
        .await?;
    db.execute("CREATE (d:Doc {id: 2, content: 'beta', embedding: [1.0, 1.0]})")
        .await?;

    db.flush().await?;

    let before = db.query("MATCH (d:Doc) RETURN count(d) AS c").await?;
    assert_eq!(before.rows()[0].get::<i64>("c")?, 2);

    let nearest = db
        .query_with(
            "
            MATCH (d:Doc)
            WHERE d.embedding ~= $q
            RETURN d.id
            LIMIT 1
            ",
        )
        .param("q", vec![0.1, 0.1])
        .fetch_all()
        .await?;
    assert_eq!(nearest.rows()[0].get::<i64>("d.id")?, 1);

    db.execute("MATCH (d:Doc {id: 1}) DETACH DELETE d").await?;
    db.flush().await?;

    let after = db.query("MATCH (d:Doc) RETURN count(d) AS c").await?;
    assert_eq!(after.rows()[0].get::<i64>("c")?, 1);

    let remaining = db.query("MATCH (d:Doc) RETURN d.id AS id").await?;
    assert_eq!(remaining.rows()[0].get::<i64>("id")?, 2);

    Ok(())
}

#[tokio::test]
async fn test_vector_match_operator_with_embedding_alias_config() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Item")
        .property("id", DataType::Int64)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L2,
                embedding: Some(EmbeddingCfg {
                    alias: "embed/default".to_string(),
                    source_properties: vec!["id".to_string()],
                    batch_size: 4,
                }),
            }),
        )
        .apply()
        .await?;

    db.execute("CREATE (i:Item {id: 1, embedding: [0.0, 0.0]})")
        .await?;
    db.execute("CREATE (i:Item {id: 2, embedding: [2.0, 2.0]})")
        .await?;
    db.flush().await?;

    let results = db
        .query_with(
            "
            MATCH (i:Item)
            WHERE i.embedding ~= $q
            RETURN i.id
            LIMIT 1
            ",
        )
        .param("q", vec![0.2, 0.2])
        .fetch_all()
        .await?;

    assert_eq!(results.rows.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("i.id")?, 1);

    Ok(())
}

#[tokio::test]
async fn test_uni_xervo_facade_exposed_when_catalog_configured() -> Result<()> {
    let db = Uni::temporary()
        .xervo_catalog(vec![mistral_embed_alias("embed/default")])
        .build()
        .await?;
    db.xervo()?;
    Ok(())
}

#[tokio::test]
async fn test_reopen_fails_fast_when_schema_has_alias_but_catalog_missing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let db_uri = db_path.to_string_lossy().to_string();

    let db = Uni::open(&db_uri).build().await?;
    db.schema()
        .label("R")
        .property("txt", DataType::String)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding: Some(EmbeddingCfg {
                    alias: "embed/default".to_string(),
                    source_properties: vec!["txt".to_string()],
                    batch_size: 4,
                }),
            }),
        )
        .apply()
        .await?;
    drop(db);

    let reopen = Uni::open(&db_uri).build().await;
    let err = match reopen {
        Ok(_) => panic!("Expected reopen without catalog to fail"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("Uni-Xervo catalog is required"),
        "Unexpected error: {err}"
    );

    Ok(())
}
