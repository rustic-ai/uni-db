// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
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
async fn test_fts_query() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    // 1. Setup
    let dir = tempdir()?;
    let base_path = dir.path().to_str().unwrap();
    let schema_path = dir.path().join("schema.json");

    let schema_manager = SchemaManager::load(&schema_path).await?;
    schema_manager.add_label("Article")?;
    schema_manager.add_property("Article", "title", DataType::String, false)?;
    schema_manager.add_property("Article", "body", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(StorageManager::new(base_path, schema_manager.clone()).await?);
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    // 2. Insert Data
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema_manager.schema().into());

    let inserts = vec![
        r#"CREATE (:Article { title: "Rust Lang", body: "Rust is a systems programming language." })"#,
        r#"CREATE (:Article { title: "Python", body: "Python is great for data science." })"#,
        r#"CREATE (:Article { title: "Databases", body: "Graph databases are versatile." })"#,
    ];

    for q in inserts {
        let query = uni_query::parse_cypher(q)?;
        let plan = planner.plan(query)?;
        executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;
    }

    // 3. Create FTS Index
    let ddl = r#"
        CREATE FULLTEXT INDEX article_body_fts
        FOR (a:Article) ON EACH [a.body]
    "#;
    {
        let query = uni_query::parse_cypher(ddl)?;
        let plan = planner.plan(query)?;
        executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;
    }

    // 4. Flush to persist (FTS index usually built on flushed data)
    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // 5. Query with CONTAINS
    {
        let sql = "MATCH (a:Article) WHERE a.body CONTAINS 'programming' RETURN a.title";
        let query = uni_query::parse_cypher(sql)?;
        let plan = planner.plan(query)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("a.title").unwrap().as_str().unwrap(),
            "Rust Lang"
        );
    }

    // 6. Query with STARTS WITH
    {
        let sql = "MATCH (a:Article) WHERE a.body STARTS WITH 'Graph' RETURN a.title";
        let query = uni_query::parse_cypher(sql)?;
        let plan = planner.plan(query)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("a.title").unwrap().as_str().unwrap(),
            "Databases"
        );
    }

    // 7. Query with ENDS WITH
    {
        let sql = "MATCH (a:Article) WHERE a.body ENDS WITH 'science.' RETURN a.title";
        let query = uni_query::parse_cypher(sql)?;
        let plan = planner.plan(query)?;
        let results = executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].get("a.title").unwrap().as_str().unwrap(),
            "Python"
        );
    }

    Ok(())
}
