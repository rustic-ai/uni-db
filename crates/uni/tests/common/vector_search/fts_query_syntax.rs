// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.fts.query` query syntax: plain terms (OR), `"phrases"`, `-term` and
//! `NOT term`, `AND` / `OR` and parentheses, with the same answer for flushed
//! rows, unflushed rows, rows written since the index was built, and
//! schemaless labels (C12).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(fts_query_syntax)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

const DOCS: [(i64, &str); 5] = [
    (1, "quick brown fox"),
    (2, "quick brown cat"),
    (3, "brown fox jumps"),
    (4, "lazy cat sleeps"),
    (5, "fox brown quick"),
];

/// Each query with the ids it must return.
const CASES: &[(&str, &[i64])] = &[
    ("quick fox", &[1, 2, 3, 5]),
    ("quick OR lazy", &[1, 2, 4, 5]),
    ("quick AND fox", &[1, 5]),
    ("\"quick brown\"", &[1, 2]),
    ("\"brown fox\"", &[1, 3]),
    ("fox -quick", &[3]),
    ("fox NOT quick", &[3]),
    ("brown AND NOT cat", &[1, 3, 5]),
    ("(quick OR lazy) AND (fox OR cat)", &[1, 2, 4, 5]),
    ("\"quick brown\" -cat", &[1]),
    ("quick AND (fox OR cat) -brown", &[]),
    ("", &[]),
    ("   ", &[]),
];

/// Queries that must be refused, each with a fragment of the error.
const MALFORMED: &[(&str, &str)] = &[
    ("\"quick brown", "quote"),
    ("(quick OR fox", "parenthes"),
    ("quick)", "parenthes"),
    ("quick AND", "AND"),
    ("NOT", "NOT"),
    ("-quick", "negat"),
    ("NOT quick -fox", "negat"),
    ("quick AND (-fox -cat)", "negat"),
];

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn ids(db: &Uni, label: &str, query: &str) -> Result<Vec<i64>> {
    let result = db
        .session()
        .query_with(&format!(
            "CALL uni.fts.query('{label}', 'body', $q, 100) YIELD node RETURN node.id AS id"
        ))
        .param("q", Value::String(query.to_string()))
        .fetch_all()
        .await?;
    let mut out: Vec<i64> = result
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

async fn insert(db: &Uni, label: &str, docs: &[(i64, &str)]) -> Result<()> {
    let rows: Vec<String> = docs
        .iter()
        .map(|(id, body)| format!("(:{label} {{id: {id}, body: '{body}'}})"))
        .collect();
    run(db, &format!("CREATE {}", rows.join(", "))).await
}

async fn open(declared: bool, indexed: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    if declared {
        db.schema()
            .label("Doc")
            .property("id", DataType::Int)
            .property("body", DataType::String)
            .apply()
            .await?;
    }
    if indexed {
        run(
            &db,
            "CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.body]",
        )
        .await?;
    }
    Ok(db)
}

async fn check_all(db: &Uni, label: &str, stage: &str) -> Result<()> {
    for (query, expected) in CASES {
        let got = ids(db, label, query)
            .await
            .map_err(|e| anyhow::anyhow!("{stage}: query {query:?} failed: {e:#}"))?;
        assert_eq!(&got, expected, "{stage}: query {query:?}");
    }
    for (query, fragment) in MALFORMED {
        let err = ids(db, label, query).await.expect_err(&format!(
            "{stage}: malformed query {query:?} must be refused"
        ));
        let text = format!("{err:#}");
        assert!(
            text.contains(fragment),
            "{stage}: error for {query:?} should mention {fragment:?}: {text}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn fts_query_syntax_indexed_flushed() -> Result<()> {
    let db = open(true, true).await?;
    insert(&db, "Doc", &DOCS).await?;
    check_all(&db, "Doc", "unflushed").await?;
    db.flush().await?;
    check_all(&db, "Doc", "flushed").await
}

/// Rows the index covers, flushed rows it does not, and unflushed rows.
#[tokio::test]
async fn fts_query_syntax_indexed_mixed() -> Result<()> {
    let db = open(true, true).await?;
    insert(&db, "Doc", &DOCS[..2]).await?;
    db.flush().await?;
    insert(&db, "Doc", &DOCS[2..3]).await?;
    db.flush().await?;
    insert(&db, "Doc", &DOCS[3..]).await?;
    check_all(&db, "Doc", "mixed").await
}

#[tokio::test]
async fn fts_query_syntax_without_index() -> Result<()> {
    let db = open(true, false).await?;
    insert(&db, "Doc", &DOCS[..3]).await?;
    db.flush().await?;
    insert(&db, "Doc", &DOCS[3..]).await?;
    check_all(&db, "Doc", "no index, mixed").await?;
    db.flush().await?;
    check_all(&db, "Doc", "no index, flushed").await
}

#[tokio::test]
async fn fts_query_syntax_schemaless() -> Result<()> {
    let db = open(false, false).await?;
    insert(&db, "Loose", &DOCS[..3]).await?;
    db.flush().await?;
    insert(&db, "Loose", &DOCS[3..]).await?;
    check_all(&db, "Loose", "schemaless, mixed").await?;
    db.flush().await?;
    check_all(&db, "Loose", "schemaless, flushed").await
}

/// A phrase needs position postings; an index built without them (an n-gram
/// index) refuses a phrase with an error that says so.
#[tokio::test]
async fn fts_query_syntax_phrase_needs_positions() -> Result<()> {
    let db = open(true, false).await?;
    run(
        &db,
        "CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.body] \
         OPTIONS {tokenizer: 'ngram', ngram_min: 3, ngram_max: 3}",
    )
    .await?;
    insert(&db, "Doc", &DOCS).await?;
    db.flush().await?;
    let err = ids(&db, "Doc", "\"quick brown\"")
        .await
        .expect_err("a phrase on an index without positions must be refused");
    let text = format!("{err:#}");
    assert!(text.contains("position"), "{text}");
    // Terms still work on it.
    assert!(!ids(&db, "Doc", "quick").await?.is_empty());
    Ok(())
}
