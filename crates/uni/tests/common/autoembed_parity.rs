// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Auto-embed PARITY suite across embedding modalities (dense / multi-vector / sparse / hybrid).
//!
//! Fills the cross-modality coverage matrix with shared, deterministic, call-counting mocks so
//! every modality exercises the same scenarios: deferred write, query-time text embed, error paths
//! (no runtime / no embedding config), explicit-not-overwritten, multi-source, document_prefix,
//! persistence across reopen, SET re-embed, and batched inference. Per-modality happy-path basics
//! live in `{multivec,hybrid,sparse}_autoembed.rs`; this file closes the gaps and asserts parity.
//!
//! Mock convention: each whitespace word contributes a term. dense = `[word_count; DIM]`;
//! multi = one `[i+1; DIM]` per word; sparse = `(char_len, 1.0)` per word.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use tempfile::TempDir;
use uni_common::UniConfig;
use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo,
    VectorIndexCfg, VectorMetric, WarmupPolicy,
};
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::multivector::{MultiVectorEmbedResult, MultiVectorEmbeddingModel};
use uni_xervo::traits::sparse::{SparseEmbedResult, SparseEmbeddingModel};
use uni_xervo::traits::{
    EmbedResult, EmbeddingModel, LoadedModelHandle, ModelInfo, ModelProvider, ProviderCapabilities,
    ProviderHealth,
};

const DIM: usize = 4;
const VOCAB: usize = 100;

// ─────────────────────────── mocks (call-counting) ───────────────────────────

/// Dense: `[word_count; DIM]`. `prefix_marker` lets a prefix test detect prefixing
/// (a `document_prefix`/`query_prefix` adds its own words to the count).
struct DenseMock {
    calls: Arc<AtomicUsize>,
}
impl ModelInfo for DenseMock {
    fn model_id(&self) -> &str {
        "mock-dense"
    }
}
#[async_trait]
impl EmbeddingModel for DenseMock {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<EmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EmbedResult {
            vectors: texts
                .iter()
                .map(|t| vec![t.split_whitespace().count() as f32; DIM])
                .collect(),
            usage: None,
        })
    }
    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}

struct SparseMock {
    calls: Arc<AtomicUsize>,
}
impl ModelInfo for SparseMock {
    fn model_id(&self) -> &str {
        "mock-sparse"
    }
}
#[async_trait]
impl SparseEmbeddingModel for SparseMock {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<SparseEmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(SparseEmbedResult {
            vectors: texts
                .iter()
                .map(|t| {
                    t.split_whitespace()
                        .map(|w| (w.chars().count() as u32, 1.0f32))
                        .collect()
                })
                .collect(),
            usage: None,
        })
    }
    fn vocab_size(&self) -> u32 {
        VOCAB as u32
    }
}

struct MultiMock {
    calls: Arc<AtomicUsize>,
}
impl ModelInfo for MultiMock {
    fn model_id(&self) -> &str {
        "mock-multi"
    }
}
#[async_trait]
impl MultiVectorEmbeddingModel for MultiMock {
    async fn embed(&self, texts: &[&str]) -> uni_xervo::error::Result<MultiVectorEmbedResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(MultiVectorEmbedResult {
            vectors: texts
                .iter()
                .map(|t| {
                    let n = t.split_whitespace().count().max(1);
                    (0..n).map(|i| vec![(i + 1) as f32; DIM]).collect()
                })
                .collect(),
            usage: None,
        })
    }
    fn dimensions(&self) -> u32 {
        DIM as u32
    }
}

/// Provider for a single dedicated head, parameterized by task + the handle factory.
macro_rules! single_head_provider {
    ($name:ident, $task:expr, $id:literal, $make:expr) => {
        struct $name {
            calls: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl ModelProvider for $name {
            fn provider_id(&self) -> &'static str {
                $id
            }
            fn capabilities(&self) -> ProviderCapabilities {
                ProviderCapabilities {
                    supported_tasks: vec![$task],
                }
            }
            async fn load(
                &self,
                _spec: &ModelAliasSpec,
            ) -> uni_xervo::error::Result<LoadedModelHandle> {
                let h = ($make)(self.calls.clone());
                Ok(Arc::new(h) as LoadedModelHandle)
            }
            async fn health(&self) -> ProviderHealth {
                ProviderHealth::Healthy
            }
        }
    };
}

single_head_provider!(DenseProvider, ModelTask::Embed, "mock/dense", |c| {
    let h: Arc<dyn EmbeddingModel> = Arc::new(DenseMock { calls: c });
    h
});
single_head_provider!(SparseProvider, ModelTask::EmbedSparse, "mock/sparse", |c| {
    let h: Arc<dyn SparseEmbeddingModel> = Arc::new(SparseMock { calls: c });
    h
});
single_head_provider!(
    MultiProvider,
    ModelTask::EmbedMultiVector,
    "mock/multi",
    |c| {
        let h: Arc<dyn MultiVectorEmbeddingModel> = Arc::new(MultiMock { calls: c });
        h
    }
);

fn spec(alias: &str, task: ModelTask, provider: &str, model: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task,
        provider_id: provider.to_string(),
        model_id: model.to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({}),
    }
}

fn emb_cfg(alias: &str, sources: &[&str], doc_prefix: Option<&str>) -> EmbeddingCfg {
    EmbeddingCfg {
        alias: alias.to_string(),
        source_properties: sources.iter().map(|s| s.to_string()).collect(),
        batch_size: 16,
        document_prefix: doc_prefix.map(|s| s.to_string()),
        query_prefix: None,
    }
}

/// Read a dense `emb` column back as `Vec<f64>` (`[]`-decoded). Empty if null/missing.
async fn read_dense(db: &Uni, title: &str) -> anyhow::Result<Vec<f64>> {
    let res = db
        .session()
        .query(&format!(
            "MATCH (d:Doc {{title: '{title}'}}) RETURN d.emb AS e"
        ))
        .await?;
    let v = res.rows()[0].value("e").unwrap().clone();
    Ok(serde_json::from_value(v.into()).unwrap_or_default())
}

// ─────────────────────────── DENSE ───────────────────────────

async fn dense_runtime(calls: Arc<AtomicUsize>) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(DenseProvider { calls })
        .catalog(vec![spec(
            "d/mock",
            ModelTask::Embed,
            "mock/dense",
            "mock-dense",
        )])
        .build()
        .await
        .unwrap()
}

async fn dense_schema(db: &Uni, embedding: Option<EmbeddingCfg>) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable("emb", DataType::Vector { dimensions: DIM })
        .index(
            "emb",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding,
            }),
        )
        .apply()
        .await?;
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_deferred() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let cfg = UniConfig {
        defer_embeddings: true,
        ..UniConfig::default()
    };
    let db = Uni::in_memory()
        .config(cfg)
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?; // 3 words
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        read_dense(&db, "t").await?,
        vec![3.0; DIM],
        "deferred dense embed"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_explicit_not_overwritten() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute_with("CREATE (:Doc {title: 't', content: 'a b c', emb: $e})")
        .param("e", Value::List(vec![Value::Float(9.0); DIM]))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        read_dense(&db, "t").await?,
        vec![9.0; DIM],
        "explicit emb must win"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_set_reembeds() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(read_dense(&db, "t").await?, vec![3.0; DIM]);

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 't'}) SET d.content = 'a b'")
        .await?; // 2 words
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        read_dense(&db, "t").await?,
        vec![2.0; DIM],
        "SET must re-embed dense"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_persistence_across_reopen() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let db = Uni::open(&path)
            .xervo_runtime(dense_runtime(calls).await)
            .build()
            .await?;
        dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
    }
    // Reopen with a fresh runtime (a schema with embedding aliases requires the catalog at
    // open). The embedding was materialized at write time, so reading must NOT re-embed — the
    // call counter stays 0 and the persisted vector comes back unchanged.
    let reopen_calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::open(&path)
        .xervo_runtime(dense_runtime(reopen_calls.clone()).await)
        .build()
        .await?;
    assert_eq!(
        read_dense(&db, "t").await?,
        vec![3.0; DIM],
        "embed must survive reopen"
    );
    assert_eq!(
        reopen_calls.load(Ordering::SeqCst),
        0,
        "reopen must not re-embed"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_document_prefix_applied() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    // document_prefix "x y " adds 2 words → 'a b c' embeds as count 5, not 3.
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], Some("x y ")))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        read_dense(&db, "t").await?,
        vec![5.0; DIM],
        "document_prefix must be prepended before embedding"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_batches_one_inference() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::in_memory()
        .config(UniConfig {
            defer_embeddings: true,
            ..UniConfig::default()
        })
        .xervo_runtime(dense_runtime(calls.clone()).await)
        .build()
        .await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute(&format!("CREATE (:Doc {{title: 't{i}', content: 'a b'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    // Deferred path batches a label's rows into ONE embed call.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "5 docs must embed in one batched inference"
    );
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_string_query_requires_runtime() -> anyhow::Result<()> {
    // Embedding config present but NO runtime → a text query must error clearly.
    let db = Uni::temporary().build().await?;
    dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
    let err = db
        .session()
        .query("CALL uni.vector.query('Doc', 'emb', 'some text', 3) YIELD node RETURN node")
        .await
        .err();
    assert!(err.is_some(), "string query without runtime must error");
    Ok(())
}

#[tokio::test]
async fn dense_autoembed_string_query_requires_config() -> anyhow::Result<()> {
    // Index has NO embedding_config → a text query cannot auto-embed → error.
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(dense_runtime(calls).await)
        .build()
        .await?;
    dense_schema(&db, None).await?;
    let err = db
        .session()
        .query("CALL uni.vector.query('Doc', 'emb', 'some text', 3) YIELD node RETURN node")
        .await
        .err();
    assert!(
        err.is_some(),
        "string query without embedding_config must error"
    );
    Ok(())
}

// ─────────────────────────── MULTI-VECTOR ───────────────────────────

async fn multi_runtime(calls: Arc<AtomicUsize>) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(MultiProvider { calls })
        .catalog(vec![spec(
            "m/mock",
            ModelTask::EmbedMultiVector,
            "mock/multi",
            "mock-multi",
        )])
        .build()
        .await
        .unwrap()
}

async fn multi_schema(db: &Uni, embedding: Option<EmbeddingCfg>) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index(
            "tokens",
            IndexType::Vector(VectorIndexCfg {
                algorithm: VectorAlgo::Flat,
                metric: VectorMetric::Cosine,
                embedding,
            }),
        )
        .apply()
        .await?;
    Ok(())
}

async fn read_multi(db: &Uni, title: &str) -> anyhow::Result<Vec<Vec<f64>>> {
    let res = db
        .session()
        .query(&format!(
            "MATCH (d:Doc {{title: '{title}'}}) RETURN d.tokens AS t"
        ))
        .await?;
    let v = res.rows()[0].value("t").unwrap().clone();
    Ok(serde_json::from_value(v.into()).unwrap_or_default())
}

fn expect_tokens(n: usize) -> Vec<Vec<f64>> {
    (0..n).map(|i| vec![(i + 1) as f64; DIM]).collect()
}

#[tokio::test]
async fn multi_autoembed_set_reembeds() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(multi_runtime(calls).await)
        .build()
        .await?;
    multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(read_multi(&db, "t").await?, expect_tokens(3));

    let tx = db.session().tx().await?;
    tx.execute("MATCH (d:Doc {title: 't'}) SET d.content = 'a b'")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        read_multi(&db, "t").await?,
        expect_tokens(2),
        "SET must re-embed multi"
    );
    Ok(())
}

#[tokio::test]
async fn multi_autoembed_persistence_across_reopen() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let db = Uni::open(&path)
            .xervo_runtime(multi_runtime(calls).await)
            .build()
            .await?;
        multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
    }
    let reopen = Arc::new(AtomicUsize::new(0));
    let db = Uni::open(&path)
        .xervo_runtime(multi_runtime(reopen.clone()).await)
        .build()
        .await?;
    assert_eq!(
        read_multi(&db, "t").await?,
        expect_tokens(3),
        "multi embed survives reopen"
    );
    assert_eq!(reopen.load(Ordering::SeqCst), 0, "reopen must not re-embed");
    Ok(())
}

#[tokio::test]
async fn multi_autoembed_string_query_requires_runtime() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;
    let err = db
        .session()
        .query("CALL uni.vector.query('Doc', 'tokens', 'some text', 3) YIELD node RETURN node")
        .await
        .err();
    assert!(
        err.is_some(),
        "multi string query without runtime must error"
    );
    Ok(())
}

// ─────────────────────────── SPARSE ───────────────────────────

async fn sparse_runtime(calls: Arc<AtomicUsize>) -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(SparseProvider { calls })
        .catalog(vec![spec(
            "s/mock",
            ModelTask::EmbedSparse,
            "mock/sparse",
            "mock-sparse",
        )])
        .build()
        .await
        .unwrap()
}

async fn sparse_schema(db: &Uni, embedding: Option<EmbeddingCfg>) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("content", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize: true,
                embedding,
                idf: false,
            },
        )
        .apply()
        .await?;
    Ok(())
}

/// Top sparse query result title for an explicit `{indices,values}` probe.
async fn sparse_top(
    db: &Uni,
    indices: Vec<u32>,
    values: Vec<f32>,
) -> anyhow::Result<Option<String>> {
    let res = db
        .session()
        .query_with(
            "CALL uni.sparse.query('Doc', 'emb', $q, 5, null, null, {}) \
             YIELD node, score RETURN node.title AS title",
        )
        .param("q", Value::SparseVector { indices, values })
        .param("k", Value::Int(5))
        .fetch_all()
        .await?;
    Ok(res
        .rows()
        .first()
        .map(|r| r.get::<String>("title").unwrap()))
}

#[tokio::test]
async fn sparse_autoembed_persistence_across_reopen() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let db = Uni::open(&path)
            .xervo_runtime(sparse_runtime(calls).await)
            .build()
            .await?;
        sparse_schema(&db, Some(emb_cfg("s/mock", &["content"], None))).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 't', content: 'a bb ccc'})")
            .await?; // {1,2,3}
        tx.commit().await?;
        db.flush().await?;
    }
    let reopen = Arc::new(AtomicUsize::new(0));
    let db = Uni::open(&path)
        .xervo_runtime(sparse_runtime(reopen.clone()).await)
        .build()
        .await?;
    assert_eq!(
        sparse_top(&db, vec![1, 2, 3], vec![1.0, 1.0, 1.0]).await?,
        Some("t".to_string()),
        "sparse embed survives reopen"
    );
    assert_eq!(reopen.load(Ordering::SeqCst), 0, "reopen must not re-embed");
    Ok(())
}

#[tokio::test]
async fn sparse_autoembed_multi_source() -> anyhow::Result<()> {
    // Two source columns are joined before embedding: 'a' + 'bb ccc' → 'a bb ccc' → {1,2,3}.
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::temporary()
        .xervo_runtime(sparse_runtime(calls).await)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("title", DataType::String)
        .property("a", DataType::String)
        .property("b", DataType::String)
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .index(
            "emb",
            IndexType::Sparse {
                dimensions: VOCAB,
                quantize: true,
                embedding: Some(emb_cfg("s/mock", &["a", "b"], None)),
                idf: false,
            },
        )
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 't', a: 'a', b: 'bb ccc'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        sparse_top(&db, vec![1, 2, 3], vec![1.0, 1.0, 1.0]).await?,
        Some("t".to_string()),
        "multi-source must join both columns before embedding"
    );
    Ok(())
}

// ───────────────────── SHARED RUNTIME (issue #155) ─────────────────────
//
// One `Arc<ModelRuntime>` handed to several databases must load each model
// once, not once per database — the property the Python `ModelRuntime` handle
// exists to expose. These also pin the alias-presence check that guards the
// prebuilt-runtime path, which used to accept any runtime without inspection.

/// Dense provider that counts `load()` calls rather than embed calls, so a
/// second deserialization of the same weights is observable.
struct LoadCountingProvider {
    loads: Arc<AtomicUsize>,
}

#[async_trait]
impl ModelProvider for LoadCountingProvider {
    fn provider_id(&self) -> &'static str {
        "mock/dense"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supported_tasks: vec![ModelTask::Embed],
        }
    }
    async fn load(&self, _spec: &ModelAliasSpec) -> uni_xervo::error::Result<LoadedModelHandle> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        let h: Arc<dyn EmbeddingModel> = Arc::new(DenseMock {
            calls: Arc::new(AtomicUsize::new(0)),
        });
        Ok(Arc::new(h) as LoadedModelHandle)
    }
    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Healthy
    }
}

/// Two databases sharing one runtime deserialize the model once.
///
/// This already holds at the Rust level — it is the property the Python
/// binding is being built to expose — so it is a characterization test, not a
/// regression repro. Its job is to fail if `build_model_runtime`'s extraction
/// ever changes provider registration or the per-runtime model cache.
#[tokio::test]
async fn one_runtime_shared_by_two_databases_loads_the_model_once() -> anyhow::Result<()> {
    let loads = Arc::new(AtomicUsize::new(0));
    let runtime = ModelRuntime::builder()
        .register_provider(LoadCountingProvider {
            loads: loads.clone(),
        })
        .catalog(vec![spec(
            "d/mock",
            ModelTask::Embed,
            "mock/dense",
            "mock-dense",
        )])
        .build()
        .await
        .unwrap();

    for _ in 0..2 {
        let db = Uni::in_memory()
            .xervo_runtime(runtime.clone())
            .build()
            .await?;
        dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 't', content: 'a b c'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        assert_eq!(read_dense(&db, "t").await?, vec![3.0; DIM]);
    }

    assert_eq!(
        loads.load(Ordering::SeqCst),
        1,
        "a shared runtime must deserialize the model once, not once per database"
    );
    Ok(())
}

/// Reopening with a runtime whose catalog lacks a schema-required alias fails
/// at open.
///
/// Before the fix the prebuilt-runtime arm returned the runtime unexamined, so
/// `build()` returned `Ok` and the mismatch surfaced much later as an
/// `AliasNotFound` from inside the writer on the first auto-embedded write.
#[tokio::test]
async fn prebuilt_runtime_missing_a_required_alias_is_rejected_at_open() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path)
            .xervo_runtime(dense_runtime(Arc::new(AtomicUsize::new(0))).await)
            .build()
            .await?;
        dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
        db.flush().await?;
    }

    // Same provider, same task — only the alias differs from the one the
    // persisted schema binds its vector index to.
    let wrong = ModelRuntime::builder()
        .register_provider(DenseProvider {
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .catalog(vec![spec(
            "d/other",
            ModelTask::Embed,
            "mock/dense",
            "mock-dense",
        )])
        .build()
        .await
        .unwrap();

    let Err(err) = Uni::open(&path).xervo_runtime(wrong).build().await else {
        panic!("a runtime lacking the schema's embedding alias must be refused at open");
    };
    assert!(
        err.to_string().contains("Missing Uni-Xervo alias 'd/mock'"),
        "expected the same message the catalog path produces, got: {err}"
    );
    Ok(())
}

/// The presence check must not reject a runtime that does carry the alias.
#[tokio::test]
async fn prebuilt_runtime_carrying_the_required_alias_opens() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path)
            .xervo_runtime(dense_runtime(Arc::new(AtomicUsize::new(0))).await)
            .build()
            .await?;
        dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
        db.flush().await?;
    }

    let db = Uni::open(&path)
        .xervo_runtime(dense_runtime(Arc::new(AtomicUsize::new(0))).await)
        .build()
        .await?;
    assert!(db.xervo().is_available());
    assert!(
        db.xervo().raw_runtime().is_some(),
        "the injected runtime must be handed back out for sharing"
    );
    Ok(())
}

// ──────────────── #122 residual cells ────────────────
//
// Two gaps the parity matrix still had. Both are coverage; neither implicates
// production behaviour.

/// Multi-vector: a **string** query embeds and returns rows when a runtime is
/// present (#122).
///
/// Its sibling `multi_autoembed_string_query_requires_runtime` covers only the
/// error path — that a string query without a runtime is refused. That test
/// passes whether or not the success path works at all, which is exactly the
/// gap: an unwired query-time embed would look identical from there.
#[tokio::test]
async fn multi_autoembed_string_query_succeeds_with_runtime() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::in_memory()
        .xervo_runtime(multi_runtime(calls.clone()).await)
        .build()
        .await?;
    multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {title: 'a', content: 'alpha'})")
        .await?;
    tx.execute("CREATE (:Doc {title: 'b', content: 'beta'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let before = calls.load(Ordering::SeqCst);
    let res = db
        .session()
        .query("CALL uni.vector.query('Doc', 'tokens', 'some text', 3) YIELD node RETURN node.title AS title")
        .await?;

    assert!(
        !res.rows().is_empty(),
        "a string query with a runtime present must embed and return rows"
    );
    assert!(
        calls.load(Ordering::SeqCst) > before,
        "the string query must have driven an embed call; {before} before, {} after — \
         equal means the text was not embedded and the rows came from somewhere else",
        calls.load(Ordering::SeqCst)
    );
    Ok(())
}

/// Sparse: a label's rows embed in one batched inference (#122).
///
/// The dense path asserts this (`dense_autoembed_batches_one_inference`); its
/// sparse sibling did not, so a regression to per-row inference here would be
/// invisible.
#[tokio::test]
async fn sparse_autoembed_batches_one_inference() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::in_memory()
        .config(UniConfig {
            defer_embeddings: true,
            ..UniConfig::default()
        })
        .xervo_runtime(sparse_runtime(calls.clone()).await)
        .build()
        .await?;
    sparse_schema(&db, Some(emb_cfg("s/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute(&format!("CREATE (:Doc {{title: 't{i}', content: 'a b'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "5 docs must embed in one batched sparse inference"
    );
    Ok(())
}

/// Multi-vector: a label's rows embed in one batched inference (#122).
#[tokio::test]
async fn multi_autoembed_batches_one_inference() -> anyhow::Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let db = Uni::in_memory()
        .config(UniConfig {
            defer_embeddings: true,
            ..UniConfig::default()
        })
        .xervo_runtime(multi_runtime(calls.clone()).await)
        .build()
        .await?;
    multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;

    let tx = db.session().tx().await?;
    for i in 0..5 {
        tx.execute(&format!("CREATE (:Doc {{title: 't{i}', content: 'a b'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "5 docs must embed in one batched multi-vector inference"
    );
    Ok(())
}

// ─────────────────────────── FORK PROMOTE ───────────────────────────
//
// An embedding the writer fills in must be stored in L0 in the column's
// declared form (`Vector`, or a list of `Vector`s), as a value written through
// Cypher is (`to_declared_form`). Auto-embed used to put lists of floats there,
// and the flush hashes the L0 map into the vertex's content UID: a `List`
// hashes its numbers, a `Vector` only its width. Reads convert to the declared
// form, so nothing looked wrong, but the registered UID differed from the one
// the stored row hashes to: fork promote re-inserted auto-embedded vertices
// already on primary, and skipped edges to them as having no endpoint.

#[tokio::test(flavor = "multi_thread")]
async fn promote_matches_auto_embedded_vertices_on_primary() -> anyhow::Result<()> {
    use uni_db::api::fork_diff::PromotePattern;
    for (multi_vector, defer) in [(false, false), (true, false), (false, true), (true, true)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let cfg = UniConfig {
            defer_embeddings: defer,
            ..UniConfig::default()
        };
        let db = if multi_vector {
            let db = Uni::temporary()
                .config(cfg)
                .xervo_runtime(multi_runtime(calls).await)
                .build()
                .await?;
            multi_schema(&db, Some(emb_cfg("m/mock", &["content"], None))).await?;
            db
        } else {
            let db = Uni::temporary()
                .config(cfg)
                .xervo_runtime(dense_runtime(calls).await)
                .build()
                .await?;
            dense_schema(&db, Some(emb_cfg("d/mock", &["content"], None))).await?;
            db
        };
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {title: 'a', content: 'x y'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        {
            let fork = db.session().fork("f").await?;
            let tx = fork.tx().await?;
            tx.execute(
                "MATCH (a:Doc {title: 'a'}) \
                 CREATE (a)-[:CITES]->(:Doc {title: 'b', content: 'z'})",
            )
            .await?;
            tx.commit().await?;
        }
        let report = db
            .promote_from_fork(
                "f",
                &[
                    PromotePattern::label("Doc"),
                    PromotePattern::edge_type("CITES"),
                ],
            )
            .await?;
        // `a` is already on primary; only `b` and the edge are new.
        let what = format!(
            "{}{}",
            if multi_vector {
                "multi-vector"
            } else {
                "dense"
            },
            if defer { ", deferred" } else { "" }
        );
        assert_eq!(report.vertices_inserted, 1, "{what}");
        assert_eq!(report.edges_inserted, 1, "{what}");
        assert_eq!(report.edges_skipped_no_endpoint, 0, "{what}");
        let rows = db
            .session()
            .query("MATCH (a:Doc)-[:CITES]->(b:Doc) RETURN a.title + b.title AS p")
            .await?;
        assert_eq!(rows.rows()[0].get::<String>("p")?, "ab", "{what}");
        assert_eq!(
            db.session()
                .query("MATCH (d:Doc) RETURN count(d) AS n")
                .await?
                .rows()[0]
                .get::<i64>("n")?,
            2,
            "{what}"
        );
    }
    Ok(())
}
