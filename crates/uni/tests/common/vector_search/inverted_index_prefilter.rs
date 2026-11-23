// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An Online inverted index serves `ANY(x IN n.p WHERE x IN <list>)` as a
//! candidate-vid prefilter, with the predicate kept as a residual, and
//! `CREATE INVERTED INDEX` declares one (C11).
//!
//! Every query is answered by a database with the index and one without, and
//! the answers must match: flushed, unflushed, after updates and deletes that
//! have not been flushed, and with `max_terms_per_doc` truncating a document.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(inverted_index_prefilter)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{Uni, UniConfig, Value};

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn open(index_ddl: Option<&str>) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    run(&db, "CREATE LABEL Product (name STRING, tags LIST<STRING>)").await?;
    if let Some(ddl) = index_ddl {
        run(&db, ddl).await?;
    }
    Ok(db)
}

/// The queries compared, each with its parameters.
fn queries() -> Vec<(&'static str, Option<Vec<&'static str>>)> {
    vec![
        (
            "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['rust']) RETURN p.name AS n",
            None,
        ),
        (
            "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['web', 'ml']) RETURN p.name AS n",
            None,
        ),
        (
            "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN $terms) RETURN p.name AS n",
            Some(vec!["db", "python"]),
        ),
        // Case differs from the stored terms: the normalized index widens the
        // candidates, the residual keeps the answer exact.
        (
            "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['Rust']) RETURN p.name AS n",
            None,
        ),
        (
            "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['rust']) AND p.name <> 'A' \
             RETURN p.name AS n",
            None,
        ),
        (
            "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['nothing']) RETURN p.name AS n",
            None,
        ),
        // Past the third term of a capped document.
        (
            "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['t9']) RETURN p.name AS n",
            None,
        ),
    ]
}

async fn answer(db: &Uni, query: &str, terms: &Option<Vec<&str>>) -> Result<Vec<String>> {
    let session = db.session();
    let mut builder = session.query_with(query);
    if let Some(terms) = terms {
        builder = builder.param(
            "terms",
            Value::List(terms.iter().map(|t| Value::String(t.to_string())).collect()),
        );
    }
    let result = builder.fetch_all().await?;
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get::<String>("n"))
        .collect::<Result<_, _>>()?;
    names.sort();
    Ok(names)
}

async fn assert_same(with: &Uni, without: &Uni, stage: &str) -> Result<()> {
    for (query, terms) in queries() {
        let a = answer(with, query, &terms).await?;
        let b = answer(without, query, &terms).await?;
        assert_eq!(a, b, "{stage}: {query} differs with the index");
    }
    Ok(())
}

async fn both(dbs: &[&Uni], stmt: &str) -> Result<()> {
    for db in dbs {
        run(db, stmt).await?;
    }
    Ok(())
}

async fn flush(dbs: &[&Uni]) -> Result<()> {
    for db in dbs {
        db.flush().await?;
    }
    Ok(())
}

async fn compare(index_ddl: &str) -> Result<()> {
    let with = open(Some(index_ddl)).await?;
    let without = open(None).await?;
    let dbs = [&with, &without];
    both(
        &dbs,
        "CREATE (:Product {name: 'A', tags: ['rust', 'db']}), \
                (:Product {name: 'B', tags: ['rust', 'web']}), \
                (:Product {name: 'C', tags: ['python', 'ml']}), \
                (:Product {name: 'D', tags: ['t1', 't2', 't3', 't4', 't5', 't6', 't7', 't8', 't9']}), \
                (:Product {name: 'E', tags: []})",
    )
    .await?;
    assert_same(&with, &without, "unflushed").await?;
    flush(&dbs).await?;
    assert_same(&with, &without, "flushed").await?;

    // Unflushed writes over flushed rows: a term gained, a term lost, a
    // delete, and a new row.
    both(&dbs, "MATCH (p:Product {name: 'C'}) SET p.tags = ['rust']").await?;
    both(&dbs, "MATCH (p:Product {name: 'B'}) SET p.tags = ['go']").await?;
    both(&dbs, "MATCH (p:Product {name: 'A'}) DELETE p").await?;
    both(&dbs, "CREATE (:Product {name: 'F', tags: ['ml', 'Rust']})").await?;
    assert_same(&with, &without, "updated, unflushed").await?;
    flush(&dbs).await?;
    assert_same(&with, &without, "updated, flushed").await
}

#[tokio::test]
async fn inverted_index_prefilter_matches_unindexed_answers() -> Result<()> {
    compare("CREATE INVERTED INDEX product_tags FOR (p:Product) ON (p.tags)").await
}

/// `max_terms_per_doc` caps what the index records for a document; a document
/// over the cap is still found by every one of its terms.
#[tokio::test]
async fn inverted_index_prefilter_with_options() -> Result<()> {
    compare(
        "CREATE INVERTED INDEX product_tags FOR (p:Product) ON (p.tags) \
         OPTIONS {normalize: false, max_terms_per_doc: 3}",
    )
    .await
}

#[tokio::test]
async fn inverted_index_ddl_records_options() -> Result<()> {
    let db = open(Some(
        "CREATE INVERTED INDEX product_tags FOR (p:Product) ON (p.tags) \
         OPTIONS {normalize: false, max_terms_per_doc: 3}",
    ))
    .await?;
    let schema = db.schema_manager().schema();
    let cfg = schema
        .indexes
        .iter()
        .find_map(|idx| match idx {
            uni_db::core::schema::IndexDefinition::Inverted(cfg) => Some(cfg.clone()),
            _ => None,
        })
        .expect("CREATE INVERTED INDEX must declare an inverted index");
    assert_eq!(cfg.name, "product_tags");
    assert_eq!(cfg.property, "tags");
    assert!(!cfg.normalize);
    assert_eq!(cfg.max_terms_per_doc, 3);

    // IF NOT EXISTS leaves the existing index as it is.
    run(
        &db,
        "CREATE INVERTED INDEX product_tags IF NOT EXISTS FOR (p:Product) ON (p.tags)",
    )
    .await?;
    let schema = db.schema_manager().schema();
    assert!(schema.indexes.iter().any(|idx| matches!(
        idx,
        uni_db::core::schema::IndexDefinition::Inverted(cfg) if !cfg.normalize
    )));

    // Unknown options are refused, for INVERTED and FULLTEXT alike.
    let err = run(
        &db,
        "CREATE INVERTED INDEX other FOR (p:Product) ON (p.tags) OPTIONS {normalise: true}",
    )
    .await
    .expect_err("an unknown INVERTED option must be refused");
    assert!(format!("{err:#}").contains("normalise"), "{err:#}");
    let err = run(
        &db,
        "CREATE FULLTEXT INDEX name_fts FOR (p:Product) ON EACH [p.name] \
         OPTIONS {normalize: true, max_terms_per_doc: 5}",
    )
    .await
    .expect_err("inverted-index options on a FULLTEXT index must be refused");
    let text = format!("{err:#}");
    assert!(
        text.contains("normalize") && text.contains("FULLTEXT"),
        "{text}"
    );
    Ok(())
}

/// EXPLAIN names the prefilter, and the scan reports consulting the index.
#[tokio::test]
async fn inverted_index_prefilter_is_visible() -> Result<()> {
    let db = open(Some(
        "CREATE INVERTED INDEX product_tags FOR (p:Product) ON (p.tags)",
    ))
    .await?;
    run(
        &db,
        "CREATE (:Product {name: 'A', tags: ['rust']}), (:Product {name: 'B', tags: ['go']})",
    )
    .await?;
    db.flush().await?;
    let query = "MATCH (p:Product) WHERE ANY(t IN p.tags WHERE t IN ['rust']) RETURN p.name AS n";
    let explain = db.session().query_with(query).explain().await?;
    let entry = explain
        .index_usage
        .iter()
        .find(|u| u.label_or_type == "Product" && u.property == "tags")
        .unwrap_or_else(|| panic!("EXPLAIN must report the inverted index: {explain:#?}"));
    assert!(entry.used, "{entry:?}");
    assert_eq!(entry.index_type, "INVERTED", "{entry:?}");

    let (rows, profile) = db.session().query_with(query).profile().await?;
    assert_eq!(rows.len(), 1);
    let scan = profile
        .runtime_stats
        .iter()
        .find(|s| s.operator == "GraphScanExec")
        .expect("no GraphScanExec in the profiled plan");
    assert!(
        scan.index_hits.is_some_and(|n| n >= 1),
        "the scan did not consult the inverted index: {:?}",
        profile.runtime_stats
    );
    Ok(())
}
