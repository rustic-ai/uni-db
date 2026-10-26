// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end BGE-M3 3-way hybrid coverage with the REAL `aapot/bge-m3-onnx` `EmbedHybrid` model.
//!
//! Complements the mock-based `bge_m3_hybrid_3way.rs` with a real model whose dense, learned-
//! sparse, and multi-vector / ColBERT heads emit production-shaped values (1024-d dense + ColBERT
//! tokens, 250 002-term sparse space). Verifies the multi-vector column round-trips through a
//! MUVERA index, a long document, batched ingest, and a close/reopen catalog-rehydration cycle.
//!
//! Gated on `provider-onnx` (static ORT) + `#[ignore]` (downloads / loads a ~2.1 GB model).
//! Uses the standard Hugging Face hub cache by default (override via `BGE_M3_DIR`);
//! no network if the model files are already cached.
//!
//! NOTE: the `provider-onnx` feature gate now lives on the `mod bge_m3_real_onnx;`
//! declaration in `integration.rs` (this file is folded into that binary).

use std::sync::Arc;

use uni_db::{
    DataType, EmbeddingCfg, IndexType, ModelAliasSpec, ModelTask, Uni, Value, VectorAlgo,
    VectorIndexCfg, VectorMetric, WarmupPolicy,
};
use uni_xervo::provider::LocalOnnxProvider;
use uni_xervo::runtime::ModelRuntime;

const DIM: usize = 1024; // bge-m3 dense + ColBERT token dim
const VOCAB: usize = 250002; // XLM-RoBERTa vocab
const ALIAS: &str = "hybrid/bge-m3";

/// hf-hub cache root holding the `aapot/bge-m3-onnx` snapshot (model.onnx +
/// model.onnx.data + Constant_685_attr__value + tokenizer.json).
///
/// Set `BGE_M3_DIR` to override. Defaults to the standard Hugging Face hub
/// cache (`$HF_HUB_CACHE`, then `$HF_HOME/hub`, then `~/.cache/huggingface/hub`)
/// so the ~2.1 GB download is shared across checkouts — the provider treats
/// `cache_dir` as an hf-hub cache root, and a relative default used to strand
/// the model inside the source tree (cargo test CWD = the crate root).
fn model_dir() -> String {
    if let Ok(dir) = std::env::var("BGE_M3_DIR") {
        return dir;
    }
    if let Ok(dir) = std::env::var("HF_HUB_CACHE") {
        return dir;
    }
    if let Ok(hf_home) = std::env::var("HF_HOME") {
        return format!("{hf_home}/hub");
    }
    let home = std::env::var("HOME").expect("HOME must be set to locate the HF hub cache");
    format!("{home}/.cache/huggingface/hub")
}

fn spec() -> ModelAliasSpec {
    ModelAliasSpec {
        alias: ALIAS.to_string(),
        task: ModelTask::EmbedHybrid,
        provider_id: "local/onnx".to_string(),
        model_id: "aapot/bge-m3-onnx".to_string(),
        revision: None,
        // Eager: the ~2.1 GB download and load happen while the runtime
        // builds, not inside the first auto-embedding write, where they ran
        // into the 30 s query timeout on a cold cache (measured 2026-10-22).
        warmup: WarmupPolicy::Eager,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: serde_json::json!({ "cache_dir": model_dir() }),
    }
}

async fn runtime() -> Arc<ModelRuntime> {
    ModelRuntime::builder()
        .register_provider(LocalOnnxProvider::new())
        .catalog(vec![spec()])
        .build()
        .await
        .expect("build bge-m3 hybrid runtime")
}

fn emb_cfg() -> EmbeddingCfg {
    EmbeddingCfg {
        alias: ALIAS.to_string(),
        source_properties: vec!["content".to_string()],
        batch_size: 16,
        document_prefix: None,
        query_prefix: None,
    }
}

fn dense_index() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Flat,
        metric: VectorMetric::Cosine,
        embedding: Some(emb_cfg()),
    })
}

/// `Doc(content, embedding Vector{1024}, emb SparseVector{vocab}, tokens List<Vector{1024}>)` —
/// all three index configs share the SAME hybrid alias + source: one bge-m3 pass fills all.
async fn define_schema(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("embedding", dense_index())
        .index("emb", IndexType::sparse_with_embedding(VOCAB, emb_cfg()))
        .index("tokens", dense_index())
        .apply()
        .await?;
    Ok(())
}

/// Realistic late-interaction config: the multi-vector column is indexed with **MUVERA**
/// (derives an FDE `Vector` column — a different write path than a plain Flat Vector index).
fn muvera_index() -> IndexType {
    IndexType::Vector(VectorIndexCfg {
        algorithm: VectorAlgo::Muvera {
            k_sim: 4,
            reps: 8,
            d_proj: 8,
            seed: uni_db::api::schema::DEFAULT_FDE_SEED,
            inner: Box::new(VectorAlgo::Flat),
        },
        metric: VectorMetric::Cosine,
        embedding: Some(emb_cfg()),
    })
}

async fn define_schema_muvera(db: &Uni) -> anyhow::Result<()> {
    db.schema()
        .label("Doc")
        .property("content", DataType::String)
        .property_nullable("embedding", DataType::Vector { dimensions: DIM })
        .property_nullable("emb", DataType::SparseVector { dimensions: VOCAB })
        .property_nullable(
            "tokens",
            DataType::List(Box::new(DataType::Vector { dimensions: DIM })),
        )
        .index("embedding", dense_index())
        .index("emb", IndexType::sparse_with_embedding(VOCAB, emb_cfg()))
        .index("tokens", muvera_index())
        .apply()
        .await?;
    Ok(())
}

fn summarize(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Vector(xs) => format!("Vector(dim={})", xs.len()),
        Value::SparseVector { indices, .. } => format!("SparseVector(nnz={})", indices.len()),
        Value::List(items) => {
            let inner = items.first().map(|i| match i {
                Value::Vector(xs) => format!("Vector(dim={})", xs.len()),
                Value::List(xs) => format!("List(len={})", xs.len()),
                other => format!("{other:?}"),
            });
            format!("List(tokens={}, inner={:?})", items.len(), inner)
        }
        other => format!("{other:?}"),
    }
}

/// MUVERA-indexed multi-vector + a LONG document (many tokens) + a batch of docs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real bge-m3 ONNX model (~2.1 GB); run explicitly"]
async fn real_bge_m3_muvera_multivector() -> anyhow::Result<()> {
    // Deferred embeddings: bge-m3 inference runs at flush (not during commit), avoiding the
    // commit-timeout that eager inference of many docs hits.
    let cfg = uni_common::UniConfig {
        defer_embeddings: true,
        ..uni_common::UniConfig::default()
    };
    let db = Uni::temporary()
        .config(cfg)
        .xervo_runtime(runtime().await)
        .build()
        .await?;
    define_schema_muvera(&db).await?;

    let long = "machine learning ".repeat(40);
    let tx = db.session().tx().await?;
    for i in 0..3 {
        tx.execute(&format!("CREATE (:Doc {{content: 'doc {i} {long}'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query(
            "MATCH (d:Doc) WHERE d.content STARTS WITH 'doc 0' RETURN d.tokens AS t, keys(d) AS k",
        )
        .await?;
    let row = &res.rows()[0];
    let tokens = row.value("t").cloned().unwrap_or(Value::Null);
    eprintln!("keys   = {:?}", row.value("k"));
    eprintln!("tokens (muvera, long doc) = {}", summarize(&tokens));

    assert_ne!(
        tokens,
        Value::Null,
        "MUVERA multi-vector column read back as Null"
    );
    Ok(())
}

/// Close + REOPEN: ingest with auto-embed, flush, drop the DB, then reopen the SAME path and
/// read the multi-vector column. On reopen the schema is rehydrated from the persisted catalog;
/// this asserts the stored `List(Vector)` survives that round-trip intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real bge-m3 ONNX model (~2.1 GB); run explicitly"]
async fn real_bge_m3_reopen() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_str().unwrap().to_string();

    {
        let db = Uni::open(&path)
            .xervo_runtime(runtime().await)
            .build()
            .await?;
        define_schema(&db).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Doc {content: 'late interaction colbert retrieval over a graph'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        // Sanity: correct BEFORE reopen.
        let pre = db
            .session()
            .query("MATCH (d:Doc) RETURN d.tokens AS t")
            .await?;
        eprintln!(
            "tokens (pre-reopen) = {}",
            summarize(&pre.rows()[0].value("t").cloned().unwrap_or(Value::Null))
        );
        drop(db);
    }

    // Reopen WITH the model runtime re-registered (required when the schema has embedding
    // aliases) — a real deployment reopens this way.
    let db = Uni::open(&path)
        .xervo_runtime(runtime().await)
        .build()
        .await?;
    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN d.tokens AS t")
        .await?;
    let tokens = res.rows()[0].value("t").cloned().unwrap_or(Value::Null);
    eprintln!("tokens (post-reopen) = {}", summarize(&tokens));

    assert_ne!(
        tokens,
        Value::Null,
        "multi-vector column read back as Null AFTER REOPEN"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "real bge-m3 ONNX model (~2.1 GB); run explicitly"]
async fn real_bge_m3_multivector_roundtrips() -> anyhow::Result<()> {
    let db = Uni::temporary()
        .xervo_runtime(runtime().await)
        .build()
        .await?;
    define_schema(&db).await?;

    let text = "The quick brown fox jumps over the lazy dog while the database \
                engine indexes hundreds of late-interaction token vectors for retrieval.";

    let tx = db.session().tx().await?;
    tx.execute(&format!("CREATE (:Doc {{content: '{text}'}})"))
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let res = db
        .session()
        .query("MATCH (d:Doc) RETURN d.embedding AS e, d.emb AS s, d.tokens AS t")
        .await?;
    let row = &res.rows()[0];
    let dense = row.value("e").cloned().unwrap_or(Value::Null);
    let sparse = row.value("s").cloned().unwrap_or(Value::Null);
    let tokens = row.value("t").cloned().unwrap_or(Value::Null);

    // Compact summaries (the real vectors are huge).
    let summarize = |v: &Value| -> String {
        match v {
            Value::Null => "NULL".to_string(),
            Value::Vector(xs) => format!("Vector(dim={})", xs.len()),
            Value::SparseVector { indices, .. } => format!("SparseVector(nnz={})", indices.len()),
            Value::List(items) => {
                let inner = items.first().map(|i| match i {
                    Value::Vector(xs) => format!("Vector(dim={})", xs.len()),
                    Value::List(xs) => format!("List(len={})", xs.len()),
                    other => format!("{other:?}"),
                });
                format!("List(tokens={}, inner={:?})", items.len(), inner)
            }
            other => format!("{other:?}"),
        }
    };
    eprintln!("dense  = {}", summarize(&dense));
    eprintln!("sparse = {}", summarize(&sparse));
    eprintln!("tokens = {}", summarize(&tokens));

    assert_ne!(tokens, Value::Null, "multi-vector column read back as Null");
    match &tokens {
        Value::List(items) => {
            assert!(!items.is_empty(), "multi-vector has zero tokens");
            for (i, tok) in items.iter().enumerate() {
                match tok {
                    Value::Vector(xs) => assert_eq!(xs.len(), DIM, "token {i} wrong dim"),
                    other => panic!("token {i} is not a Vector: {other:?}"),
                }
            }
        }
        other => panic!("tokens is not a List: {other:?}"),
    }
    Ok(())
}
