// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T1: a row's content UID never includes a derived column.
//!
//! A JSON full-text index fills `__jfts_<column>` at batch build. The text is
//! stored in the row, so a path that rebuilds a row from its stored columns
//! (compaction, a SET on a flushed vertex) used to carry it into the property
//! map the UID is hashed from: the stored `_uid` of an unchanged row changed
//! at compaction, and differed from a fresh write of the same content.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(uid_derived_columns)'

// Rust guideline compliant

use anyhow::Result;
use arrow_array::{Array, FixedSizeBinaryArray, Int64Array};
use std::collections::BTreeMap;
use uni_db::store::backend::table_names::vertex_table_name;
use uni_db::store::backend::types::ScanRequest;
use uni_db::store::storage::compaction::Compactor;
use uni_db::{Uni, UniConfig};

async fn run(db: &Uni, stmt: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn open() -> Result<Uni> {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    let db = Uni::in_memory().config(config).build().await?;
    run(&db, "CREATE LABEL Article (id INT, n INT, doc JSON)").await?;
    run(
        &db,
        "CREATE JSON FULLTEXT INDEX article_fts FOR (a:Article) ON doc",
    )
    .await?;
    Ok(db)
}

/// The stored `_uid` of every live row, by `id`, taking each vertex's newest
/// row.
async fn stored_uids(db: &Uni) -> Result<BTreeMap<i64, Vec<u8>>> {
    let batches = db
        .storage()
        .head_backend()
        .scan(ScanRequest::all(vertex_table_name("Article")))
        .await?;
    let mut newest: BTreeMap<i64, (u64, Vec<u8>)> = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned())
            .expect("id column");
        let uids = batch
            .column_by_name("_uid")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>().cloned())
            .expect("_uid column");
        let versions = batch
            .column_by_name("_version")
            .and_then(|c| {
                c.as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .cloned()
            })
            .expect("_version column");
        for i in 0..batch.num_rows() {
            if ids.is_null(i) {
                continue;
            }
            let entry = (versions.value(i), uids.value(i).to_vec());
            newest
                .entry(ids.value(i))
                .and_modify(|e| {
                    if entry.0 >= e.0 {
                        *e = entry.clone();
                    }
                })
                .or_insert(entry);
        }
    }
    Ok(newest.into_iter().map(|(id, (_, uid))| (id, uid)).collect())
}

const ROWS: &str = "CREATE (:Article {id: 1, n: 0, doc: {title: 'Graph databases'}}), \
                           (:Article {id: 2, n: 0, doc: {title: 'Vector search'}})";

/// The UIDs a fresh write of `stmt` stores.
async fn fresh_uids(stmt: &str) -> Result<BTreeMap<i64, Vec<u8>>> {
    let db = open().await?;
    run(&db, stmt).await?;
    db.flush().await?;
    stored_uids(&db).await
}

#[tokio::test]
async fn uid_derived_columns_unchanged_by_compaction() -> Result<()> {
    let db = open().await?;
    run(&db, ROWS).await?;
    db.flush().await?;
    let before = stored_uids(&db).await?;
    assert_eq!(before, fresh_uids(ROWS).await?, "guard: same writes");
    // A second version of row 2 makes compaction merge something.
    run(&db, "MATCH (a:Article {id: 2}) SET a.n = 0").await?;
    db.flush().await?;
    let report = Compactor::new(db.storage())
        .compact_vertices("Article")
        .await?;
    assert!(report.ran, "guard: the label was compacted");
    assert_eq!(
        stored_uids(&db).await?,
        before,
        "compaction changed the content UID of unchanged rows"
    );
    Ok(())
}

#[tokio::test]
async fn uid_derived_columns_unchanged_by_set_of_another_property() -> Result<()> {
    let db = open().await?;
    run(&db, ROWS).await?;
    db.flush().await?;
    run(&db, "MATCH (a:Article {id: 1}) SET a.n = 5").await?;
    db.flush().await?;
    let expected = fresh_uids(
        "CREATE (:Article {id: 1, n: 5, doc: {title: 'Graph databases'}}), \
                (:Article {id: 2, n: 0, doc: {title: 'Vector search'}})",
    )
    .await?;
    assert_eq!(
        stored_uids(&db).await?,
        expected,
        "a SET on a flushed row must store the UID a fresh write computes"
    );
    // The derived text is still served after the rewrite.
    let rows = db
        .session()
        .query("CALL uni.fts.query('Article', 'doc', 'graph', 10) YIELD node RETURN node.id AS id")
        .await?;
    assert_eq!(rows.len(), 1);
    Ok(())
}
