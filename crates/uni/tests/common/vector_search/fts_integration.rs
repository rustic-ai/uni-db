// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
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
    let writer = Arc::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    );

    // 2. Insert Data
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema_manager.schema());

    let inserts = vec![
        r#"CREATE (:Article { title: "Rust Lang", body: "Rust is a systems programming language." })"#,
        r#"CREATE (:Article { title: "Python", body: "Python is great for data science." })"#,
        r#"CREATE (:Article { title: "Databases", body: "Graph databases are versatile." })"#,
    ];

    for q in inserts {
        let query = uni_cypher::parse(q)?;
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
        let query = uni_cypher::parse(ddl)?;
        let plan = planner.plan(query)?;
        executor
            .execute(plan, &prop_manager, &HashMap::new())
            .await?;
    }

    // 4. Flush to persist (FTS index usually built on flushed data)
    {
        let w: &uni_store::Writer = writer.as_ref();
        w.flush_to_l1(None).await?;
    }

    // 5. Query with CONTAINS
    {
        let sql = "MATCH (a:Article) WHERE a.body CONTAINS 'programming' RETURN a.title";
        let query = uni_cypher::parse(sql)?;
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
        let query = uni_cypher::parse(sql)?;
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
        let query = uni_cypher::parse(sql)?;
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

/// CREATE FULLTEXT INDEX on existing (flushed) data should auto-build the
/// physical tantivy index — no manual `rebuild_indexes()` required.
#[tokio::test]
async fn test_fts_auto_build_on_create_index() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dir = tempdir()?;
    let path = dir.path();

    let schema_manager =
        uni_db::core::schema::SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "title", DataType::String, false)?;
    schema_manager.add_property("Doc", "content", DataType::String, false)?;
    schema_manager.save().await?;

    let db = uni_db::Uni::open(path.to_str().unwrap()).build().await?;

    // Insert and flush so data lives in Lance
    let tx = db.session().tx().await?;
    tx.execute(r#"CREATE (:Doc { title: "Rust Guide", content: "Memory safety without garbage collection." })"#).await?;
    tx.execute(r#"CREATE (:Doc { title: "Go Manual", content: "Concurrency with goroutines and channels." })"#).await?;
    tx.commit().await?;
    db.flush().await?;

    // Create FTS index — should auto-build physical index (no rebuild_indexes needed)
    let tx = db.session().tx().await?;
    tx.execute("CREATE FULLTEXT INDEX doc_content_fts FOR (d:Doc) ON EACH [d.content]")
        .await?;
    tx.commit().await?;

    // Query via FTS procedure — should find results without manual rebuild
    let results = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'content', 'memory safety', 10) \
             YIELD node \
             RETURN node.title AS title",
        )
        .await?;
    assert_eq!(
        results.len(),
        1,
        "FTS should find 'Rust Guide' without manual rebuild_indexes; got {} results",
        results.len()
    );
    let title: String = results.rows()[0].get("title")?;
    assert_eq!(title, "Rust Guide");

    Ok(())
}

/// A CREATE FULLTEXT INDEX with an English stemmer + stop-word removal must
/// honor the analyzer: `run` matches the inflections `running`/`runs` (Snowball
/// folds them to the `run` stem) but not the unrelated `walking`, and the stop
/// word `the` matches nothing (it is dropped at index + query time).
///
/// Note: `ran` is an irregular form the algorithmic stemmer does not fold, so it
/// is intentionally absent from the corpus.
#[tokio::test]
async fn test_fts_analyzer_stemming_and_stopwords() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dir = tempdir()?;
    let path = dir.path();

    let schema_manager =
        uni_db::core::schema::SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "title", DataType::String, false)?;
    schema_manager.add_property("Doc", "content", DataType::String, false)?;
    schema_manager.save().await?;

    let db = uni_db::Uni::open(path.to_str().unwrap()).build().await?;

    let tx = db.session().tx().await?;
    tx.execute(r#"CREATE (:Doc { title: "A", content: "running fast every morning" })"#)
        .await?;
    tx.execute(r#"CREATE (:Doc { title: "B", content: "she runs a marathon" })"#)
        .await?;
    tx.execute(r#"CREATE (:Doc { title: "C", content: "walking to the store" })"#)
        .await?;
    tx.execute(r#"CREATE (:Doc { title: "D", content: "the quick brown fox" })"#)
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // English analyzer with stemming + built-in stop words.
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE FULLTEXT INDEX doc_content_fts FOR (d:Doc) ON EACH [d.content] \
         OPTIONS { analyzer: 'standard', language: 'english', stemmer: true, stopwords: true }",
    )
    .await?;
    tx.commit().await?;

    // Stemming: 'run' should match running (A) and runs (B) but not walking (C).
    let results = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'content', 'run', 10) \
             YIELD node RETURN node.title AS title ORDER BY title",
        )
        .await?;
    let titles: Vec<String> = results
        .rows()
        .iter()
        .map(|r| r.get::<String>("title").unwrap())
        .collect();
    assert_eq!(
        titles,
        vec!["A".to_string(), "B".to_string()],
        "English stemmer should match running/runs for query 'run'; got {titles:?}"
    );

    // Stop word: 'the' is removed, so it matches nothing.
    let results = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'content', 'the', 10) \
             YIELD node RETURN node.title AS title",
        )
        .await?;
    assert_eq!(
        results.len(),
        0,
        "stop word 'the' should be dropped and match nothing; got {} rows",
        results.len()
    );

    Ok(())
}

/// A full-text index built with a custom analyzer must keep honoring that
/// analyzer after the database is reopened (the tokenizer config is persisted
/// with the index definition).
#[tokio::test]
async fn test_fts_analyzer_persists_across_reopen() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dir = tempdir()?;
    let path = dir.path();
    let path_str = path.to_str().unwrap().to_string();

    {
        let schema_manager =
            uni_db::core::schema::SchemaManager::load(&path.join("schema.json")).await?;
        schema_manager.add_label("Doc")?;
        schema_manager.add_property("Doc", "title", DataType::String, false)?;
        schema_manager.add_property("Doc", "content", DataType::String, false)?;
        schema_manager.save().await?;

        let db = uni_db::Uni::open(&path_str).build().await?;
        let tx = db.session().tx().await?;
        tx.execute(r#"CREATE (:Doc { title: "A", content: "jumping over fences" })"#)
            .await?;
        tx.commit().await?;
        db.flush().await?;

        let tx = db.session().tx().await?;
        tx.execute(
            "CREATE FULLTEXT INDEX doc_content_fts FOR (d:Doc) ON EACH [d.content] \
             OPTIONS { analyzer: 'standard', language: 'english', stemmer: true }",
        )
        .await?;
        tx.commit().await?;
    }

    // Reopen the database.
    let db = uni_db::Uni::open(&path_str).build().await?;

    // Stemmed match still works: 'jump' matches 'jumping'.
    let results = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'content', 'jump', 10) \
             YIELD node RETURN node.title AS title",
        )
        .await?;
    assert_eq!(
        results.len(),
        1,
        "stemming analyzer should persist across reopen; got {} rows",
        results.len()
    );
    let title: String = results.rows()[0].get("title")?;
    assert_eq!(title, "A");

    Ok(())
}

/// FTS queries must see unflushed L0 writes and respect L0 tombstones.
#[tokio::test]
async fn test_fts_query_sees_l0_writes() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dir = tempdir()?;
    let path = dir.path();

    // 1. Schema setup
    let schema_manager =
        uni_db::core::schema::SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Article")?;
    schema_manager.add_property("Article", "title", DataType::String, false)?;
    schema_manager.add_property("Article", "body", DataType::String, false)?;
    schema_manager.save().await?;

    let db = uni_db::Uni::open(path.to_str().unwrap()).build().await?;

    // 2. Insert initial articles and flush to Lance
    let tx = db.session().tx().await?;
    tx.execute(r#"CREATE (:Article { title: "Alpha", body: "The quick brown fox jumps over the lazy dog." })"#).await?;
    tx.execute(r#"CREATE (:Article { title: "Beta", body: "Machine learning transforms modern data pipelines." })"#).await?;
    tx.commit().await?;
    db.flush().await?;

    // 3. Create FTS index and rebuild so tantivy picks up the flushed data
    let tx = db.session().tx().await?;
    tx.execute("CREATE FULLTEXT INDEX article_body_fts FOR (a:Article) ON EACH [a.body]")
        .await?;
    tx.commit().await?;
    db.indexes().rebuild("Article", false).await?;

    // Sanity: flushed data is findable via FTS
    let flushed = db
        .session()
        .query(
            "CALL uni.fts.query('Article', 'body', 'machine learning', 10) \
             YIELD node \
             RETURN node.title AS title",
        )
        .await?;
    assert_eq!(flushed.len(), 1, "Flushed article should be found via FTS");
    let title: String = flushed.rows()[0].get("title")?;
    assert_eq!(title, "Beta");

    // 4. Insert a NEW article — do NOT flush (stays in L0)
    let tx = db.session().tx().await?;
    tx.execute(r#"CREATE (:Article { title: "Gamma", body: "Quantum computing breakthroughs in machine learning." })"#).await?;
    tx.commit().await?;

    // 5. FTS query must find the L0-only article
    let l0_results = db
        .session()
        .query(
            "CALL uni.fts.query('Article', 'body', 'quantum computing', 10) \
             YIELD node \
             RETURN node.title AS title",
        )
        .await?;
    assert!(
        !l0_results.is_empty(),
        "L0-only article 'Gamma' should appear in FTS results, got 0 results",
    );
    let titles: Vec<String> = l0_results
        .rows()
        .iter()
        .map(|r| r.get("title").unwrap())
        .collect();
    assert!(
        titles.contains(&"Gamma".to_string()),
        "L0-only article 'Gamma' not found; got {:?}",
        titles
    );

    // 6. Delete a flushed article via Cypher (tombstone in L0, not flushed)
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Article) WHERE a.title = 'Beta' DELETE a")
        .await?;
    tx.commit().await?;

    // 7. FTS query for the deleted article's keywords should exclude it
    let after_delete = db
        .session()
        .query(
            "CALL uni.fts.query('Article', 'body', 'machine learning', 10) \
             YIELD node \
             RETURN node.title AS title",
        )
        .await?;
    let titles_after: Vec<String> = after_delete
        .rows()
        .iter()
        .map(|r| r.get("title").unwrap())
        .collect();
    assert!(
        !titles_after.contains(&"Beta".to_string()),
        "Deleted article 'Beta' should not appear in FTS results; got {:?}",
        titles_after
    );
    // The L0 article 'Gamma' should still appear (it also mentions "machine learning")
    assert!(
        titles_after.contains(&"Gamma".to_string()),
        "L0 article 'Gamma' should still appear after deleting Beta; got {:?}",
        titles_after
    );

    Ok(())
}

/// `uni.fts.query` must yield a `score` that *increases* with relevance, and
/// must honour `threshold` on that same scale.
///
/// Regression cover for the pair of defects where FTS results were pushed
/// through the L2 arm of `calculate_score`, producing `1/(1+bm25)` — strictly
/// *decreasing* in BM25. The documented `ORDER BY score DESC` therefore
/// returned the worst matches first, with no error to signal it. `threshold`
/// compounded it by filtering the raw BM25 value, a different range *and*
/// direction from the score the caller actually saw.
///
/// The assertions are about ordering and bounds rather than exact values: BM25
/// magnitudes depend on corpus statistics, but "a document that repeats the
/// term outranks one that mentions it once" is stable.
#[tokio::test]
async fn test_fts_query_score_increases_with_relevance() -> Result<()> {
    use uni_db::Uni;

    let tmp = tempdir()?;
    let db = Uni::open(tmp.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("Doc")
        .property("name", DataType::String)
        .property("body", DataType::String)
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE FULLTEXT INDEX doc_body_fts FOR (d:Doc) ON EACH [d.body]")
        .await?;
    tx.commit().await?;

    // `high` repeats the term, `low` mentions it once -> BM25(high) > BM25(low).
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Doc {name: 'high', body: 'graph graph graph databases'})")
        .await?;
    tx.execute("CREATE (:Doc {name: 'low', body: 'graph theory and unrelated filler text'})")
        .await?;
    tx.execute("CREATE (:Doc {name: 'none', body: 'completely unrelated content here'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;

    let ranked: Vec<(String, f64)> = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'body', 'graph', 10) \
             YIELD node, score \
             RETURN node.name AS name, score \
             ORDER BY score DESC",
        )
        .await?
        .rows()
        .iter()
        .map(|r| {
            (
                r.get::<String>("name").expect("name column"),
                r.get::<f64>("score").expect("score column"),
            )
        })
        .collect();

    assert!(
        ranked.len() >= 2,
        "expected both matching docs, got {ranked:?}"
    );
    assert_eq!(
        ranked[0].0, "high",
        "ORDER BY score DESC must rank the most relevant document first, got {ranked:?}"
    );
    assert!(
        ranked[0].1 > ranked[1].1,
        "score must increase with relevance: {ranked:?}"
    );
    for (name, score) in &ranked {
        assert!(
            (0.0..1.0).contains(score),
            "normalized BM25 score for {name} outside [0,1): {score}"
        );
    }

    // `threshold` filters on the same scale as the yielded score, so a bound
    // between the two scores keeps exactly the stronger match.
    let cutoff = (ranked[0].1 + ranked[1].1) / 2.0;
    let names: Vec<String> = db
        .session()
        .query(&format!(
            "CALL uni.fts.query('Doc', 'body', 'graph', 10, null, {cutoff}) \
             YIELD node, score RETURN node.name AS name"
        ))
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").expect("name column"))
        .collect();

    assert_eq!(
        names,
        vec!["high".to_string()],
        "threshold must keep scores >= the cutoff on the yielded scale, got {names:?}"
    );

    Ok(())
}

/// A filtered FTS query must apply its predicate before the top-k limit.
///
/// The excluded documents deliberately score higher than the matching
/// documents, so applying the filter after the FTS limit starves the result.
/// Regression for #194.
#[tokio::test]
async fn test_filtered_fts_query_prefilters_before_k() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let dir = tempdir()?;
    let path = dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Doc")?;
    schema_manager.add_property("Doc", "body", DataType::String, false)?;
    schema_manager.add_property("Doc", "tag", DataType::String, false)?;
    schema_manager.save().await?;

    let db = uni_db::Uni::open(path.to_str().unwrap()).build().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "UNWIND range(0, 49) AS i \
         CREATE (:Doc {tag: 'drop', body: 'gamma gamma gamma gamma gamma drop-' + toString(i)})",
    )
    .await?;
    tx.execute(
        "UNWIND range(0, 49) AS i \
         CREATE (:Doc {tag: 'keep', body: 'gamma filler filler filler filler keep-' + toString(i)})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE FULLTEXT INDEX doc_body_fts FOR (d:Doc) ON EACH [d.body]")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .query(
            "CALL uni.fts.query('Doc', 'body', 'gamma', 10, \"tag = 'keep'\", null, {}) \
             YIELD node \
             RETURN node.tag AS tag",
        )
        .await?;

    let tags: Vec<String> = result
        .rows()
        .iter()
        .map(|row| {
            row.get("tag")
                .map_err(|error| anyhow::anyhow!(error.to_string()))
        })
        .collect::<Result<_>>()?;
    assert_eq!(
        tags.len(),
        10,
        "filtered FTS should fill all requested slots"
    );
    assert!(
        tags.iter().all(|tag| tag == "keep"),
        "filtered FTS returned excluded rows: {tags:?}"
    );

    Ok(())
}
