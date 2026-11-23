// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A JSON full-text index is read (C24): `uni.fts.query` on the JSON column
//! searches the text of its string values, and a `CONTAINS` on the column or
//! a path in it is prefiltered by the index's text, with the predicate kept.
//!
//! Each stage runs with the index declared before the data and after it, and
//! with the rows unflushed and flushed.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(json_fts_derived)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{Uni, UniConfig, Value};

const INDEX: &str =
    "CREATE JSON FULLTEXT INDEX article_fts FOR (a:Article) ON doc OPTIONS {with_positions: true}";

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn open() -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    run(&db, "CREATE LABEL Article (id INT, doc JSON)").await?;
    Ok(db)
}

async fn insert(db: &Uni) -> Result<()> {
    insert_last(db).await?;
    insert_first(db).await
}

async fn insert_first(db: &Uni) -> Result<()> {
    run(
        db,
        "CREATE (:Article {id: 1, doc: {title: 'Graph databases', body: 'nodes and edges', \
                                         tags: ['storage', 'query']}}), \
                (:Article {id: 2, doc: {title: 'Vector search', body: 'graph of neighbours'}}), \
                (:Article {id: 3, doc: {title: 'Cooking', meta: {author: 'Graphene Smith'}}})",
    )
    .await
}

async fn insert_last(db: &Uni) -> Result<()> {
    run(
        db,
        "CREATE (:Article {id: 4, doc: {title: 'Untitled', count: 7}})",
    )
    .await
}

async fn ids(db: &Uni, query: &str, params: &[(&str, &str)]) -> Result<Vec<i64>> {
    let session = db.session();
    let mut builder = session.query_with(query);
    for (k, v) in params {
        builder = builder.param(*k, Value::String((*v).to_string()));
    }
    let result = builder.fetch_all().await?;
    let mut out: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

async fn search(db: &Uni, q: &str) -> Result<Vec<i64>> {
    ids(
        db,
        "CALL uni.fts.query('Article', 'doc', $q, 10) YIELD node RETURN node.id AS id",
        &[("q", q)],
    )
    .await
}

/// The `CONTAINS` queries compared with and without the index, each with
/// the ids it must return.
const CONTAINS: &[(&str, &[i64])] = &[
    (
        "MATCH (a:Article) WHERE a.doc.title CONTAINS 'Graph' RETURN a.id AS id",
        &[1],
    ),
    (
        "MATCH (a:Article) WHERE a.doc.body CONTAINS 'graph' RETURN a.id AS id",
        &[2],
    ),
    (
        "MATCH (a:Article) WHERE a.doc.meta.author CONTAINS 'raph' RETURN a.id AS id",
        &[3],
    ),
    (
        "MATCH (a:Article) WHERE a.doc.title CONTAINS $t RETURN a.id AS id",
        &[2],
    ),
    (
        "MATCH (a:Article) WHERE a.doc.title CONTAINS 'nothing' RETURN a.id AS id",
        &[],
    ),
];

async fn check(db: &Uni, stage: &str) -> Result<()> {
    assert_eq!(search(db, "graph").await?, vec![1, 2], "{stage}: graph");
    assert_eq!(search(db, "cooking OR smith").await?, vec![3], "{stage}");
    assert_eq!(search(db, "graph -vector").await?, vec![1], "{stage}");
    assert_eq!(search(db, "\"graph databases\"").await?, vec![1], "{stage}");
    assert_eq!(
        search(db, "storage").await?,
        vec![1],
        "{stage}: list values"
    );
    for (query, expected) in CONTAINS {
        let got = ids(db, query, &[("t", "Vector")]).await?;
        assert_eq!(&got, expected, "{stage}: {query}");
    }
    Ok(())
}

#[tokio::test]
async fn json_fts_derived_index_before_data() -> Result<()> {
    let db = open().await?;
    run(&db, INDEX).await?;
    // The first flush creates the table, with the text column, and builds
    // the deferred index. (Before any table exists, unflushed rows are scored
    // with Lance's bare tokenizer, which does not fold case.)
    insert_last(&db).await?;
    db.flush().await?;
    insert_first(&db).await?;
    check(&db, "declared first, unflushed").await?;
    db.flush().await?;
    check(&db, "declared first, flushed").await?;
    // An unflushed update moves a row in and out of the matches.
    run(
        &db,
        "MATCH (a:Article {id: 4}) SET a.doc = {title: 'Graph theory'}",
    )
    .await?;
    run(
        &db,
        "MATCH (a:Article {id: 2}) SET a.doc = {title: 'Vector search'}",
    )
    .await?;
    assert_eq!(
        search(&db, "graph").await?,
        vec![1, 4],
        "updated, unflushed"
    );
    db.flush().await?;
    assert_eq!(search(&db, "graph").await?, vec![1, 4], "updated, flushed");
    Ok(())
}

#[tokio::test]
async fn json_fts_derived_index_after_data() -> Result<()> {
    let db = open().await?;
    insert(&db).await?;
    db.flush().await?;
    run(&db, INDEX).await?;
    check(&db, "declared after a flush").await
}

/// The `CONTAINS` prefilter is reported by EXPLAIN and consulted by the scan.
#[tokio::test]
async fn json_fts_derived_contains_prefilter_is_used() -> Result<()> {
    let db = open().await?;
    run(&db, INDEX).await?;
    insert(&db).await?;
    db.flush().await?;
    let query = CONTAINS[0].0;
    let explain = db.session().query_with(query).explain().await?;
    let entry = explain
        .index_usage
        .iter()
        .find(|u| u.label_or_type == "Article" && u.property == "doc")
        .unwrap_or_else(|| panic!("EXPLAIN must report the JSON full-text index: {explain:#?}"));
    assert!(entry.used && entry.index_type == "JSON_FTS", "{entry:?}");
    let (rows, profile) = db.session().query_with(query).profile().await?;
    assert_eq!(rows.len(), 1);
    let scan = profile
        .runtime_stats
        .iter()
        .find(|s| s.operator == "GraphScanExec")
        .expect("no GraphScanExec in the profiled plan");
    assert!(
        scan.index_hits.is_some_and(|n| n >= 1),
        "the scan did not consult the JSON text: {:?}",
        profile.runtime_stats
    );
    Ok(())
}

/// `OPTIONS {paths: [...]}` indexes only the string values under those paths;
/// a `CONTAINS` outside them is still answered, without the prefilter.
#[tokio::test]
async fn json_fts_derived_paths() -> Result<()> {
    let db = open().await?;
    insert(&db).await?;
    db.flush().await?;
    run(
        &db,
        "CREATE JSON FULLTEXT INDEX article_fts FOR (a:Article) ON doc \
         OPTIONS {paths: ['title', 'meta.author']}",
    )
    .await?;
    assert_eq!(search(&db, "graph").await?, vec![1], "body is not indexed");
    assert_eq!(search(&db, "smith").await?, vec![3]);
    assert_eq!(search(&db, "storage").await?, Vec::<i64>::new());
    let got = ids(
        &db,
        "MATCH (a:Article) WHERE a.doc.body CONTAINS 'graph' RETURN a.id AS id",
        &[],
    )
    .await?;
    assert_eq!(got, vec![2]);
    let err = run(
        &db,
        "CREATE JSON FULLTEXT INDEX other FOR (a:Article) ON doc OPTIONS {path: 'title'}",
    )
    .await
    .expect_err("an unknown JSON FULLTEXT option must be refused");
    assert!(format!("{err:#}").contains("path"), "{err:#}");
    Ok(())
}
