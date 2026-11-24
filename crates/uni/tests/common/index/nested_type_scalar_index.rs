// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A scalar index on a property stored as a nested Arrow column (DATETIME,
//! TIME, points, lists, maps, vectors) cannot be built: Lance's BTree and
//! Bitmap refuse nested fields (#356). A new one is refused where it is
//! declared; one already in a catalog is recorded `Failed`, is not retried at
//! every flush, and a rebuild does not report it `Online`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(nested_type_scalar_index)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni};

async fn run(db: &Uni, stmt: &str) -> uni_db::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(())
}

async fn status(db: &Uni, label: &str, index: &str) -> Result<Option<String>> {
    Ok(db
        .get_label_info(label)
        .await?
        .and_then(|info| info.indexes.into_iter().find(|i| i.name == index))
        .map(|i| i.status))
}

#[tokio::test]
async fn nested_type_scalar_index_is_refused_in_cypher_ddl() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(
        &db,
        "CREATE LABEL Message (id INT, ts DATETIME, at TIME, tags LIST<INT>)",
    )
    .await?;
    for prop in ["ts", "at", "tags"] {
        let err = run(
            &db,
            &format!("CREATE INDEX idx_{prop} FOR (m:Message) ON (m.{prop})"),
        )
        .await
        .expect_err("a scalar index on a nested column must be refused");
        let text = err.to_string();
        assert!(
            text.contains(prop) && text.contains("cannot"),
            "{prop}: {text}"
        );
        assert_eq!(status(&db, "Message", &format!("idx_{prop}")).await?, None);
    }
    // A plain column still takes one.
    run(&db, "CREATE INDEX idx_id FOR (m:Message) ON (m.id)").await?;
    Ok(())
}

#[tokio::test]
async fn nested_type_scalar_index_is_refused_by_the_procedure() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, "CREATE LABEL Message (id INT, ts DATETIME)").await?;
    let err = run(
        &db,
        "CALL uni.schema.createIndex('Message', 'ts', {type: 'BTREE'})",
    )
    .await
    .expect_err("the procedure must refuse it too");
    assert!(err.to_string().contains("ts"), "{err}");
    Ok(())
}

#[tokio::test]
async fn nested_type_scalar_index_is_refused_by_the_schema_builder() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db
        .schema()
        .label("Message")
        .property("ts", DataType::DateTime)
        .index("ts", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await
        .expect_err("the builder must refuse it too");
    assert!(err.to_string().contains("ts"), "{err}");

    // A TIMESTAMP column is a plain Arrow timestamp, and indexes.
    db.schema()
        .label("Event")
        .property("at", DataType::Timestamp)
        .index("at", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;
    run(
        &db,
        "CREATE (:Event {at: datetime('2026-01-01T00:00:00Z')})",
    )
    .await?;
    db.flush().await?;
    assert_eq!(
        status(&db, "Event", "idx_Event_at").await?,
        Some("ONLINE".into())
    );
    Ok(())
}

/// An index already in the catalog (here declared while the property was
/// untyped, then typed DATETIME) is recorded `Failed` at its first build, is
/// not rebuilt at every later flush, and stays `Failed` through a rebuild.
#[tokio::test]
async fn nested_type_scalar_index_in_a_catalog_is_failed_not_retried() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    run(&db, "CREATE LABEL Message (id INT)").await?;
    run(&db, "CREATE INDEX idx_ts FOR (m:Message) ON (m.ts)").await?;
    run(&db, "ALTER LABEL Message ADD PROPERTY ts DATETIME").await?;
    run(
        &db,
        "CREATE (:Message {id: 1, ts: datetime('2026-01-01T00:00:00Z')})",
    )
    .await?;
    db.flush().await?;
    assert_eq!(
        status(&db, "Message", "idx_ts").await?,
        Some("FAILED".into())
    );

    let version = db.storage().schema_manager().schema().schema_version;
    for i in 2..5 {
        run(
            &db,
            &format!("CREATE (:Message {{id: {i}, ts: datetime('2026-01-01T00:00:00Z')}})"),
        )
        .await?;
        db.flush().await?;
    }
    assert_eq!(
        db.storage().schema_manager().schema().schema_version,
        version,
        "a failed index must not be rebuilt (and the catalog rewritten) at every flush"
    );

    db.indexes().rebuild("Message", false).await?;
    assert_eq!(
        status(&db, "Message", "idx_ts").await?,
        Some("FAILED".into()),
        "a rebuild must not report an index it could not build as online"
    );
    // The background rebuild marks the label's built indexes Online when it
    // finishes; it used to mark this one Online too.
    db.indexes().rebuild("Message", true).await?;
    for _ in 0..200 {
        let tasks = db.indexes().rebuild_status().await?;
        if !tasks.is_empty()
            && tasks
                .iter()
                .all(|t| t.status == uni_store::storage::IndexRebuildStatus::Completed)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(
        status(&db, "Message", "idx_ts").await?,
        Some("FAILED".into()),
        "a background rebuild must not report it online either"
    );
    // Queries on the property still answer, by scanning.
    let rows = db
        .session()
        .query("MATCH (m:Message) WHERE m.ts > datetime('2025-01-01T00:00:00Z') RETURN m.id AS id")
        .await?;
    assert_eq!(rows.len(), 4);
    Ok(())
}
