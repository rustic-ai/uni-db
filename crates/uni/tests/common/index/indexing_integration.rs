// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use serde_json::json;
use uni_db::{Uni, Value};
use uni_xervo::api::{ModelAliasSpec, ModelTask, WarmupPolicy};

/// The Candle `all-MiniLM-L6-v2` text-embedding model, registered as `alias`.
fn minilm_alias(alias: &str) -> ModelAliasSpec {
    ModelAliasSpec {
        alias: alias.to_string(),
        task: ModelTask::Embed,
        provider_id: "local/candle".to_string(),
        model_id: "all-MiniLM-L6-v2".to_string(),
        revision: None,
        warmup: WarmupPolicy::Lazy,
        required: false,
        timeout: None,
        load_timeout: None,
        retry: None,
        options: json!({}),
    }
}

/// A vertex written without its vector gets one from the index's embedding
/// alias, visible in the creating statement, and the vector index finds it.
///
/// Rewritten 2026-10-22 against the public API: the original drove the
/// planner and executor by hand with an index DDL that named a provider and
/// model inline (`embedding: { provider, model, source }`), which the engine
/// has since replaced with a catalog alias. No lane ran it, so it failed with
/// `embedding.alias is required` until the nightly ignored-test sweep did.
#[tokio::test]
#[ignore = "downloads the all-MiniLM-L6-v2 Candle model from the HuggingFace Hub"]
async fn test_auto_embedding_and_vector_search() -> Result<()> {
    const CONTENT: &str = "This is a test document about graphs and vectors.";
    let db = Uni::temporary()
        .xervo_catalog(vec![minilm_alias("embed/default")])
        .build()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Document (content STRING)").await?;
    tx.execute(
        r#"
        CREATE VECTOR INDEX doc_embed_idx
        FOR (d:Document) ON (d.embedding)
        OPTIONS {
            metric: 'cosine',
            embedding: { alias: 'embed/default', source: ['content'] }
        }
    "#,
    )
    .await?;
    // Written without `embedding`: the write embeds `content`, and the
    // creating statement already sees the vector.
    let created = tx
        .query(&format!(
            "CREATE (d:Document {{content: '{CONTENT}'}}) RETURN d.embedding AS e"
        ))
        .await?;
    let embedding = created.rows()[0].value("e").cloned();
    match embedding {
        Some(Value::Vector(v)) => assert_eq!(v.len(), 384, "all-MiniLM-L6-v2 is 384-d"),
        Some(Value::List(v)) => assert_eq!(v.len(), 384, "all-MiniLM-L6-v2 is 384-d"),
        other => panic!("expected the auto-generated embedding, got {other:?}"),
    }
    tx.commit().await?;
    db.flush().await?;

    // `~=` is the top-K index search: the only document is the nearest.
    let found = db
        .session()
        .query_with("MATCH (d:Document) WHERE d.embedding ~= $q RETURN d.content AS c")
        .param("q", Value::Vector(vec![0.01; 384]))
        .fetch_all()
        .await?;
    assert_eq!(found.rows().len(), 1);
    assert_eq!(found.rows()[0].get::<String>("c")?, CONTENT);
    Ok(())
}
