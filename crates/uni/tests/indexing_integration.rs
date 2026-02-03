// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_auto_embedding_and_vector_search() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    // 1. Setup
    let dir = tempdir()?;
    let base_path = dir.path().to_str().unwrap();
    let schema_path = dir.path().join("schema.json");

    let schema_manager = SchemaManager::load(&schema_path).await?;

    // Add Label and Properties
    let _label_id = schema_manager.add_label("Document")?;
    schema_manager.add_property("Document", "content", DataType::String, false)?;
    // We expect "embedding" to be added automatically? No, we must define it in schema usually?
    // Auto-embedding logic inserts it into properties. Writer checks if property exists in schema?
    // Writer uses L0. L0 does not enforce schema strictly, but Flush to L1 does build record batch based on schema.
    // So we MUST add the embedding property to schema for persistence to work.
    schema_manager.add_property(
        "Document",
        "embedding",
        DataType::Vector { dimensions: 384 },
        true,
    )?; // Nullable initially? Or not?
    // Auto-embedding runs BEFORE insert. So property will be present.

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(base_path, schema_manager.clone()).await?);
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // 2. Create Vector Index with Embedding Config via DDL
    // We use Planner and Executor to run DDL
    let ddl = r#"
        CREATE VECTOR INDEX doc_embed_idx
        FOR (d:Document) ON d.embedding
        OPTIONS {
            type: 'hnsw',
            embedding: {
                provider: 'fastembed',
                model: 'AllMiniLML6V2',
                source: ['content']
            }
        }
    "#;

    {
        let query = uni_query::parse_cypher(ddl)?;
        let planner = QueryPlanner::new(schema_manager.schema().into());
        let plan = planner.plan(query)?;

        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
        let executor = Executor::new_with_writer(storage.clone(), writer.clone());
        let params = HashMap::new();

        executor.execute(plan, &prop_manager, &params).await?;
    }

    // 3. Insert Vertex (Auto-Embedding)
    // We insert a vertex with 'content' but NO 'embedding'.
    let insert_query = r#"
        CREATE (d:Document { content: "This is a test document about graphs and vectors." })
        RETURN d.embedding
    "#;
    // Note: RETURN d.embedding might return null if read immediately after create in same statement if not handled?
    // Logic: execute_create_pattern inserts to Writer. Writer.insert_vertex calls process_embeddings.
    // So embedding should be in L0 immediately.

    {
        let query = uni_query::parse_cypher(insert_query)?;
        let planner = QueryPlanner::new(schema_manager.schema().into());
        let plan = planner.plan(query)?;

        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
        let executor = Executor::new_with_writer(storage.clone(), writer.clone());
        let params = HashMap::new();

        let results = executor.execute(plan, &prop_manager, &params).await?;

        assert_eq!(results.len(), 1);
        let embedding_val = results[0].get("d.embedding").unwrap();

        // Verify embedding is generated
        assert!(embedding_val.is_array());
        let vec = embedding_val.as_array().unwrap();
        assert_eq!(vec.len(), 384); // AllMiniLML6V2 is 384 dim
    }

    // 4. Flush to persist
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // 5. Vector Search
    // We search using a similar concept
    let _search_query = r#"
        MATCH (d:Document)
        WHERE vector_similarity(d.embedding, [0.0, 0.0, 0.0]) > 0.0
        RETURN d.content
    "#;
    // Note: We need a valid query vector of size 384. [0.0, 0.0...] won't work in Parser/Planner if it expects array literal.
    // Planner parses list. `parse_vector` expects numbers.
    // Constructing a 384-float array in query string is tedious.
    // Use parameter!

    let search_query_param = r#"
        MATCH (d:Document)
        WHERE vector_similarity(d.embedding, $q) > 0.0
        RETURN d.content
    "#;

    let mut params = HashMap::new();
    let zero_vec: Vec<f32> = vec![0.01; 384]; // non-zero to avoid div/0 in cosine?
    params.insert("q".to_string(), json!(zero_vec));

    {
        let query = uni_query::parse_cypher(search_query_param)?;
        let planner = QueryPlanner::new(schema_manager.schema().into());
        let plan = planner.plan(query)?;

        // Plan should contain VectorKnn if optimization worked?
        // Or Scan+Filter if not.
        // If we want to verify VectorKnn plan usage, we might need to inspect plan, but integration test checks result.
        // However, `vector_similarity` with threshold IS optimized to `VectorKnn` logical plan in `planner.rs`.

        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
        let executor = Executor::new_with_writer(storage.clone(), writer.clone());

        let results = executor.execute(plan, &prop_manager, &params).await?;

        // We should find our document (threshold 0.0 is low)
        assert_eq!(results.len(), 1);
        let content = results[0].get("d.content").unwrap().as_str().unwrap();
        assert_eq!(content, "This is a test document about graphs and vectors.");
    }

    Ok(())
}
