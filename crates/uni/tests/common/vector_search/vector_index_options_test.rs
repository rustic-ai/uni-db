// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_common::core::schema::{DataType, IndexDefinition, VectorIndexType};
use uni_db::Uni;
use uni_db::api::schema::{EmbeddingCfg, IndexType, VectorAlgo, VectorIndexCfg, VectorMetric};

#[cfg(feature = "provider-mistralrs")]
use serde_json::json;
#[cfg(feature = "provider-mistralrs")]
use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

#[cfg(feature = "provider-mistralrs")]
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
                    document_prefix: None,
                    query_prefix: None,
                }),
            }),
        )
        .apply()
        .await?;

    let schema = db.schema().current();
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

    db.session()
        .query(
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

    let schema = db.schema().current();
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

    let tx = db.session().tx().await?;
    tx.execute(
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
    tx.commit().await?;

    let result = db
        .session()
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
                    document_prefix: None,
                    query_prefix: None,
                }),
            }),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (d:Doc {id: 1, content: 'alpha', embedding: [0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (d:Doc {id: 2, content: 'beta', embedding: [1.0, 1.0]})")
        .await?;
    tx.commit().await?;

    db.flush().await?;

    let before = db
        .session()
        .query("MATCH (d:Doc) RETURN count(d) AS c")
        .await?;
    assert_eq!(before.rows()[0].get::<i64>("c")?, 2);

    let nearest = db
        .session()
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

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {id: 1}) DETACH DELETE d").await?;
    tx.commit().await?;
    db.flush().await?;

    let after = db
        .session()
        .query("MATCH (d:Doc) RETURN count(d) AS c")
        .await?;
    assert_eq!(after.rows()[0].get::<i64>("c")?, 1);

    let remaining = db
        .session()
        .query("MATCH (d:Doc) RETURN d.id AS id")
        .await?;
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
                    document_prefix: None,
                    query_prefix: None,
                }),
            }),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (i:Item {id: 1, embedding: [0.0, 0.0]})")
        .await?;
    tx.execute("CREATE (i:Item {id: 2, embedding: [2.0, 2.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let results = db
        .session()
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

    assert_eq!(results.len(), 1);
    assert_eq!(results.rows()[0].get::<i64>("i.id")?, 1);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "provider-mistralrs")]
async fn test_uni_xervo_facade_exposed_when_catalog_configured() -> Result<()> {
    let db = Uni::temporary()
        .xervo_catalog(vec![mistral_embed_alias("embed/default")])
        .build()
        .await?;
    assert!(db.xervo().is_available());
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
                    document_prefix: None,
                    query_prefix: None,
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

// ---------------------------------------------------------------------------
// T1: Schema round-trip — verify each VectorAlgo persists correct VectorIndexType
// ---------------------------------------------------------------------------

async fn assert_schema_stores_index_type(algo: VectorAlgo, expected: VectorIndexType) {
    let db = Uni::temporary().build().await.unwrap();
    db.schema()
        .label("V")
        .property("emb", DataType::Vector { dimensions: 8 })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: algo,
                metric: VectorMetric::Cosine,
                embedding: None,
            }),
        )
        .apply()
        .await
        .unwrap();

    let schema = db.schema().current();
    let idx = schema
        .indexes
        .iter()
        .find(|i| matches!(i, IndexDefinition::Vector(v) if v.label == "V"))
        .expect("vector index not found");

    if let IndexDefinition::Vector(cfg) = idx {
        assert_eq!(cfg.index_type, expected, "index_type mismatch for label V");
    } else {
        panic!("Expected vector index");
    }
}

#[tokio::test]
async fn test_schema_round_trip_flat() {
    assert_schema_stores_index_type(VectorAlgo::Flat, VectorIndexType::Flat).await;
}

#[tokio::test]
async fn test_schema_round_trip_ivf_flat() {
    assert_schema_stores_index_type(
        VectorAlgo::IvfFlat { partitions: 4 },
        VectorIndexType::IvfFlat { num_partitions: 4 },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_ivf_pq() {
    assert_schema_stores_index_type(
        VectorAlgo::IvfPq {
            partitions: 8,
            sub_vectors: 4,
        },
        VectorIndexType::IvfPq {
            num_partitions: 8,
            num_sub_vectors: 4,
            bits_per_subvector: 8,
        },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_ivf_sq() {
    assert_schema_stores_index_type(
        VectorAlgo::IvfSq { partitions: 16 },
        VectorIndexType::IvfSq { num_partitions: 16 },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_ivf_rq() {
    assert_schema_stores_index_type(
        VectorAlgo::IvfRq {
            partitions: 32,
            num_bits: None,
        },
        VectorIndexType::IvfRq {
            num_partitions: 32,
            num_bits: None,
        },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_hnsw() {
    assert_schema_stores_index_type(
        VectorAlgo::Hnsw {
            m: 12,
            ef_construction: 100,
            partitions: None,
        },
        VectorIndexType::HnswSq {
            m: 12,
            ef_construction: 100,
            num_partitions: None,
        },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_hnsw_sq() {
    assert_schema_stores_index_type(
        VectorAlgo::HnswSq {
            m: 8,
            ef_construction: 64,
            partitions: None,
        },
        VectorIndexType::HnswSq {
            m: 8,
            ef_construction: 64,
            num_partitions: None,
        },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_hnsw_pq() {
    assert_schema_stores_index_type(
        VectorAlgo::HnswPq {
            m: 16,
            ef_construction: 200,
            sub_vectors: 4,
            partitions: None,
        },
        VectorIndexType::HnswPq {
            m: 16,
            ef_construction: 200,
            num_sub_vectors: 4,
            num_partitions: None,
        },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_hnsw_flat() {
    // HnswFlat is its own distinct arm (unlike Hnsw/HnswSq which collapse to HnswSq):
    // it maps to Lance IvfHnswFlat — graph search with no quantization.
    assert_schema_stores_index_type(
        VectorAlgo::HnswFlat {
            m: 16,
            ef_construction: 200,
            partitions: None,
        },
        VectorIndexType::HnswFlat {
            m: 16,
            ef_construction: 200,
            num_partitions: None,
        },
    )
    .await;
}

#[tokio::test]
async fn test_schema_round_trip_ivf_rq_4bit() {
    // IvfRq = RaBitQ quantization; `num_bits: Some(n)` selects the per-dimension bit
    // width (the existing ivf_rq test only covers the backend-default `None`).
    assert_schema_stores_index_type(
        VectorAlgo::IvfRq {
            partitions: 32,
            num_bits: Some(4),
        },
        VectorIndexType::IvfRq {
            num_partitions: 32,
            num_bits: Some(4),
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// T2: Integration — insert data, build index, query nearest neighbor
// ---------------------------------------------------------------------------

/// Insert 20 8-dim vectors, flush, and query for the nearest to [0.1; 8].
/// The closest vector is id=0 at [0.0; 8].
async fn assert_vector_query_works(algo: VectorAlgo) {
    let db = Uni::temporary().build().await.unwrap();

    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property("emb", DataType::Vector { dimensions: 8 })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: algo,
                metric: VectorMetric::L2,
                embedding: None,
            }),
        )
        .apply()
        .await
        .unwrap();

    // Insert 20 vectors — id i has component values i as f32
    let tx = db.session().tx().await.unwrap();
    for i in 0..20i64 {
        let v = format!("[{0}.0,{0}.0,{0}.0,{0}.0,{0}.0,{0}.0,{0}.0,{0}.0]", i);
        tx.execute(&format!("CREATE (n:N {{id: {i}, emb: {v}}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    let result = db
        .session()
        .query_with("MATCH (n:N) WHERE n.emb ~= $q RETURN n.id LIMIT 1")
        .param("q", vec![0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1])
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(result.rows()[0].get::<i64>("n.id").unwrap(), 0);
}

#[tokio::test]
async fn test_vector_query_flat() {
    assert_vector_query_works(VectorAlgo::Flat).await;
}

#[tokio::test]
async fn test_vector_query_ivf_flat() {
    assert_vector_query_works(VectorAlgo::IvfFlat { partitions: 2 }).await;
}

#[tokio::test]
async fn test_vector_query_ivf_pq() {
    assert_vector_query_works(VectorAlgo::IvfPq {
        partitions: 2,
        sub_vectors: 4,
    })
    .await;
}

#[tokio::test]
async fn test_vector_query_ivf_sq() {
    assert_vector_query_works(VectorAlgo::IvfSq { partitions: 2 }).await;
}

#[tokio::test]
async fn test_vector_query_ivf_rq() {
    assert_vector_query_works(VectorAlgo::IvfRq {
        partitions: 2,
        num_bits: None,
    })
    .await;
}

#[tokio::test]
async fn test_vector_query_hnsw_sq() {
    assert_vector_query_works(VectorAlgo::HnswSq {
        m: 4,
        ef_construction: 16,
        partitions: None,
    })
    .await;
}

#[tokio::test]
async fn test_vector_query_hnsw_pq() {
    assert_vector_query_works(VectorAlgo::HnswPq {
        m: 4,
        ef_construction: 16,
        sub_vectors: 4,
        partitions: None,
    })
    .await;
}

#[tokio::test]
async fn test_vector_query_hnsw_flat() {
    assert_vector_query_works(VectorAlgo::HnswFlat {
        m: 4,
        ef_construction: 16,
        partitions: None,
    })
    .await;
}

#[tokio::test]
async fn test_vector_query_ivf_rq_4bit() {
    assert_vector_query_works(VectorAlgo::IvfRq {
        partitions: 2,
        num_bits: Some(4),
    })
    .await;
}

// ---------------------------------------------------------------------------
// T3: DDL procedure — verify algorithm selection via Cypher procedure call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ddl_procedure_algorithm_ivf_sq() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Doc")
        .property("emb", DataType::Vector { dimensions: 8 })
        .apply()
        .await?;

    db.session()
        .query(
            r#"CALL uni.schema.createIndex('Doc', 'emb', {
                "type": "VECTOR",
                "algorithm": "ivf_sq",
                "partitions": 4
            })"#,
        )
        .await?;

    let schema = db.schema().current();
    let idx = schema
        .indexes
        .iter()
        .find(|i| matches!(i, IndexDefinition::Vector(v) if v.label == "Doc"))
        .expect("index not found");

    if let IndexDefinition::Vector(cfg) = idx {
        assert_eq!(cfg.index_type, VectorIndexType::IvfSq { num_partitions: 4 });
    } else {
        panic!("Expected vector index");
    }

    Ok(())
}

#[tokio::test]
async fn test_ddl_procedure_algorithm_hnsw_pq() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Doc")
        .property("emb", DataType::Vector { dimensions: 8 })
        .apply()
        .await?;

    db.session()
        .query(
            r#"CALL uni.schema.createIndex('Doc', 'emb', {
                "type": "VECTOR",
                "algorithm": "hnsw_pq",
                "m": 8,
                "ef_construction": 100,
                "sub_vectors": 4
            })"#,
        )
        .await?;

    let schema = db.schema().current();
    let idx = schema
        .indexes
        .iter()
        .find(|i| matches!(i, IndexDefinition::Vector(v) if v.label == "Doc"))
        .expect("index not found");

    if let IndexDefinition::Vector(cfg) = idx {
        assert_eq!(
            cfg.index_type,
            VectorIndexType::HnswPq {
                m: 8,
                ef_construction: 100,
                num_sub_vectors: 4,
                num_partitions: None,
            }
        );
    } else {
        panic!("Expected vector index");
    }

    Ok(())
}

#[tokio::test]
async fn test_ddl_procedure_default_algorithm_is_ivf_pq() -> Result<()> {
    // The `uni.schema.createIndex` procedure and the Cypher DDL now share ONE option
    // parser (`uni_common::vector_index_opts`), so a vector index created without an
    // explicit algorithm uses the canonical default IVF_PQ on BOTH paths. (Previously the
    // procedure defaulted to HNSW_SQ while the DDL defaulted to IVF_PQ — this asserts that
    // divergence is gone.)
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Doc")
        .property("emb", DataType::Vector { dimensions: 8 })
        .apply()
        .await?;

    db.session()
        .query(
            r#"CALL uni.schema.createIndex('Doc', 'emb', {
                "type": "VECTOR"
            })"#,
        )
        .await?;

    let schema = db.schema().current();
    let idx = schema
        .indexes
        .iter()
        .find(|i| matches!(i, IndexDefinition::Vector(v) if v.label == "Doc"))
        .expect("index not found");

    if let IndexDefinition::Vector(cfg) = idx {
        assert_eq!(
            cfg.index_type,
            VectorIndexType::IvfPq {
                num_partitions: 256,
                num_sub_vectors: 16,
                bits_per_subvector: 8,
            }
        );
    } else {
        panic!("Expected vector index");
    }

    Ok(())
}

/// L1/Manhattan vector search picks a different nearest neighbor than L2 would,
/// proving the L1 metric is actually applied (exact/brute-force, no ANN index).
#[tokio::test]
async fn test_l1_metric_nearest_differs_from_l2() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Declare an L1 vector column. The `Flat` algorithm builds no physical ANN
    // index for L1 — the config only records the metric; search is brute-force.
    db.schema()
        .label("Doc")
        .property("id", DataType::Int64)
        .property("embedding", DataType::Vector { dimensions: 2 })
        .index(
            "embedding",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::L1,
                embedding: None,
            }),
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    // Query point is [0, 0]. Distances to the query:
    //   A=[1,1]   → L1 = 2.0,  L2 = 1.41
    //   B=[1.5,0] → L1 = 1.5,  L2 = 1.5
    // Under L1, B is nearest (1.5 < 2.0). Under L2, A would be nearest. So a
    // nearest-of B proves L1 (not L2) ordering.
    tx.execute("CREATE (:Doc {id: 1, embedding: [1.0, 1.0]})")
        .await?;
    tx.execute("CREATE (:Doc {id: 2, embedding: [1.5, 0.0]})")
        .await?;
    tx.execute("CREATE (:Doc {id: 3, embedding: [0.0, 3.0]})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let nearest = db
        .session()
        .query_with("MATCH (d:Doc) WHERE d.embedding ~= $q RETURN d.id LIMIT 1")
        .param("q", vec![0.0_f32, 0.0])
        .fetch_all()
        .await?;
    assert_eq!(
        nearest.rows()[0].get::<i64>("d.id")?,
        2,
        "L1 nearest to [0,0] is [1.5,0] (id 2); L2 would pick [1,1] (id 1)"
    );
    Ok(())
}

/// A wide-embedding index must pick `sub_vectors` from the column's dimension,
/// not the historical fixed 16.
///
/// At 768-d the old default compressed 192x (768*4/16) and measured 0.30 recall
/// before any refine pass; 96 sub-vectors holds it at 32x. The value is asserted
/// off the *persisted* schema, because the whole point is that the sentinel is
/// resolved before it is stored.
#[tokio::test]
async fn default_sub_vectors_is_dimension_aware_for_wide_embeddings() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Wide")
        .property("emb", DataType::Vector { dimensions: 768 })
        .apply()
        .await?;

    db.session()
        .query(
            r#"CALL uni.schema.createIndex('Wide', 'emb', {
                "type": "VECTOR"
            })"#,
        )
        .await?;

    let schema = db.schema().current();
    let idx = schema
        .indexes
        .iter()
        .find(|i| matches!(i, IndexDefinition::Vector(v) if v.label == "Wide"))
        .expect("index not found");

    let IndexDefinition::Vector(cfg) = idx else {
        panic!("expected a vector index");
    };

    // 768 * 4 / 96 == 32x, and 96 divides 768 so Lance can encode it.
    assert_eq!(
        cfg.index_type,
        VectorIndexType::IvfPq {
            num_partitions: 256,
            num_sub_vectors: 96,
            bits_per_subvector: 8,
        },
        "wide embeddings must not keep the dimension-blind default"
    );

    // A quantized index also carries a refine default, so a query that passes no
    // `refine_factor` still re-scores against the original vectors.
    assert!(
        cfg.default_refine_factor.is_some_and(|r| r >= 8),
        "quantized index should carry a refine default, got {:?}",
        cfg.default_refine_factor
    );

    Ok(())
}
