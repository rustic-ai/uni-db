// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A BTIC index is keyed by its interval's raw bounds, and one recorded
//! before BTIC had a key (a plain BTree on the packed column, `Online`, that
//! no predicate used) is upgraded at the next writable open.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(btic_lifecycle)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::core::schema::{IndexDefinition, IndexStatus, KeyKind};
use uni_db::{Uni, UniConfig};

const QUERY: &str = "MATCH (e:Span) WHERE btic_overlaps(e.v, btic('1985-06')) RETURN e.id AS id";

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

fn key_kind(db: &Uni) -> Option<KeyKind> {
    match db.storage().schema_manager().get_index("idx_v") {
        Some(IndexDefinition::Scalar(cfg)) => cfg.key.map(|k| k.kind),
        _ => None,
    }
}

async fn ids(db: &Uni) -> Result<Vec<i64>> {
    let rows = db.session().query(QUERY).await?;
    let mut out: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

async fn used(db: &Uni) -> Result<bool> {
    let explain = db.session().query_with(QUERY).explain().await?;
    Ok(explain
        .index_usage
        .iter()
        .any(|u| u.property == "v" && u.used))
}

async fn lance_indexes(db: &Uni) -> Result<Vec<String>> {
    Ok(db
        .storage()
        .head_backend()
        .list_indexes("vertices_Span")
        .await?
        .into_iter()
        .map(|i| i.name)
        .collect())
}

const ROWS: &str = "CREATE (:Span {id: 1, v: btic('1985')}), (:Span {id: 2, v: btic('1990')}), \
                    (:Span {id: 3, v: btic('/')}), (:Span {id: 4})";

#[tokio::test]
async fn btic_lifecycle_new_index_is_keyed_by_raw_bounds() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    run(&db, "CREATE LABEL Span (id INT, v BTIC)").await?;
    run(&db, ROWS).await?;
    db.flush().await?;
    run(&db, "CREATE INDEX idx_v FOR (e:Span) ON (e.v)").await?;
    assert_eq!(key_kind(&db), Some(KeyKind::BticBounds));
    let indexes = lance_indexes(&db).await?;
    assert!(
        indexes.iter().any(|n| n == "idx_v__lo") && indexes.iter().any(|n| n == "idx_v__hi"),
        "{indexes:?}"
    );
    assert!(!indexes.iter().any(|n| n == "idx_v"), "{indexes:?}");
    assert_eq!(ids(&db).await?, vec![1, 3]);
    assert!(used(&db).await?);
    // The key columns are hidden.
    let rows = db
        .session()
        .query("MATCH (e:Span {id: 3}) RETURN keys(e) AS k")
        .await?;
    let keys = format!("{:?}", rows.rows()[0].value("k"));
    assert!(!keys.contains("__key_"), "{keys}");
    Ok(())
}

/// A plain BTree on the packed BTIC column, `Online` (what a declaration
/// built before BTIC had a key), is upgraded by the next writable open.
#[tokio::test]
async fn btic_lifecycle_plain_index_is_upgraded_at_open() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    {
        let db = Uni::open(&path).config(config()).build().await?;
        // Declared while `v` is undeclared, so it is recorded without a key.
        run(&db, "CREATE LABEL Span (id INT)").await?;
        run(&db, "CREATE (:Span {id: 0})").await?;
        db.flush().await?;
        run(&db, "CREATE INDEX idx_v FOR (e:Span) ON (e.v)").await?;
        run(&db, "ALTER LABEL Span ADD PROPERTY v BTIC").await?;
        assert_eq!(key_kind(&db), None, "the scenario needs an unkeyed index");
        run(&db, ROWS).await?;
        db.flush().await?;
        // What an older binary built for it.
        db.storage()
            .head_backend()
            .create_scalar_index(
                "vertices_Span",
                &["v"],
                uni_db::store::backend::types::ScalarIndexType::BTree,
                Some("idx_v"),
            )
            .await?;
        db.storage().publish_cut().await?;
        db.storage()
            .schema_manager()
            .update_index_metadata("idx_v", |m| m.status = IndexStatus::Online)?;
        db.storage().schema_manager().save().await?;
        assert!(lance_indexes(&db).await?.iter().any(|n| n == "idx_v"));
        assert!(!used(&db).await?, "a plain BTIC index serves no predicate");
        assert_eq!(ids(&db).await?, vec![1, 3]);
        db.shutdown().await?;
    }
    let db = Uni::open(&path).config(config()).build().await?;
    assert_eq!(key_kind(&db), Some(KeyKind::BticBounds));
    let indexes = lance_indexes(&db).await?;
    assert!(
        indexes.iter().any(|n| n == "idx_v__lo")
            && indexes.iter().any(|n| n == "idx_v__hi")
            && indexes.iter().any(|n| n == "_vid_idx"),
        "{indexes:?}"
    );
    assert!(
        !indexes.iter().any(|n| n == "idx_v"),
        "the packed-column BTree is gone: {indexes:?}"
    );
    assert_eq!(ids(&db).await?, vec![1, 3]);
    assert!(used(&db).await?);
    Ok(())
}
