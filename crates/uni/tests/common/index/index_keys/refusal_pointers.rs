// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T14: a scalar index on a type no scalar index serves is refused with a
//! pointer to the index kind that fits; DURATION and whole-JSON BTrees
//! already in a catalog keep building and opening; a POINT index recorded
//! before points had a key is upgraded at the next writable open.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(refusal_pointers)'

// Rust guideline compliant

use super::harness::run;
use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig};

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

async fn status(db: &Uni, label: &str, index: &str) -> Result<Option<String>> {
    Ok(db
        .get_label_info(label)
        .await?
        .and_then(|info| info.indexes.into_iter().find(|i| i.name == index))
        .map(|i| i.status))
}

/// The property, its declared type, and what the refusal must point to.
const REFUSED: &[(&str, &str, &str)] = &[
    ("tags", "LIST<STRING>", "CREATE INVERTED INDEX"),
    ("nums", "LIST<INT>", "queries on it scan"),
    ("emb", "VECTOR(4)", "CREATE VECTOR INDEX"),
    ("took", "DURATION", "not ordered"),
    ("doc", "JSON", "CREATE JSON FULLTEXT INDEX"),
    ("attrs", "MAP<STRING, INT>", "ON (n.<property>.<key>)"),
];

#[tokio::test]
async fn refusal_pointers_name_the_index_kind_in_cypher_ddl() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let columns: Vec<String> = REFUSED
        .iter()
        .map(|(p, ty, _)| format!("{p} {ty}"))
        .collect();
    run(
        &db,
        &format!("CREATE LABEL Doc (id INT, {})", columns.join(", ")),
    )
    .await?;
    for (prop, _, pointer) in REFUSED {
        let err = run(
            &db,
            &format!("CREATE INDEX idx_{prop} FOR (d:Doc) ON (d.{prop})"),
        )
        .await
        .expect_err("refused");
        let text = format!("{err:#}");
        assert!(
            text.contains(&format!("Doc.{prop}")) && text.contains(pointer),
            "{prop}: {text}"
        );
        assert_eq!(status(&db, "Doc", &format!("idx_{prop}")).await?, None);
    }
    // The pointer is right: an inverted index on the string list builds.
    run(
        &db,
        "CREATE INVERTED INDEX idx_tags_inv FOR (d:Doc) ON d.tags",
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn refusal_pointers_from_the_procedure_and_the_builder() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    run(&db, "CREATE LABEL Doc (id INT, took DURATION, doc JSON)").await?;
    let err = run(
        &db,
        "CALL uni.schema.createIndex('Doc', 'took', {type: 'BTREE'})",
    )
    .await
    .expect_err("refused");
    assert!(format!("{err:#}").contains("not ordered"), "{err:#}");
    let err = db
        .schema()
        .label("Item")
        .property("emb", DataType::Vector { dimensions: 4 })
        .index("emb", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await
        .expect_err("refused");
    assert!(
        format!("{err:#}").contains("CREATE VECTOR INDEX"),
        "{err:#}"
    );
    let err = db
        .schema()
        .label("Item2")
        .property("doc", DataType::CypherValue)
        .index("doc", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await
        .expect_err("refused");
    assert!(
        format!("{err:#}").contains("CREATE JSON FULLTEXT INDEX"),
        "{err:#}"
    );
    Ok(())
}

/// A DURATION or JSON BTree recorded before the refusal (here declared
/// while the property was undeclared, then typed) builds `Online`, keeps
/// opening, rebuilds, and the queries on it answer.
#[tokio::test]
async fn refusal_pointers_existing_duration_and_json_indexes_still_open() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    let query =
        "MATCH (d:Doc) WHERE d.took = duration('P1D') AND d.doc IS NOT NULL RETURN d.id AS id";
    {
        let db = Uni::open(&path).config(config()).build().await?;
        run(&db, "CREATE LABEL Doc (id INT)").await?;
        run(&db, "CREATE INDEX idx_took FOR (d:Doc) ON (d.took)").await?;
        run(&db, "CREATE INDEX idx_doc FOR (d:Doc) ON (d.doc)").await?;
        run(&db, "ALTER LABEL Doc ADD PROPERTY took DURATION").await?;
        run(&db, "ALTER LABEL Doc ADD PROPERTY doc JSON").await?;
        run(
            &db,
            "CREATE (:Doc {id: 1, took: duration('P1D'), doc: {a: 1}}), \
                    (:Doc {id: 2, took: duration('PT1H'), doc: {a: 2}})",
        )
        .await?;
        db.flush().await?;
        for idx in ["idx_took", "idx_doc"] {
            assert_eq!(
                status(&db, "Doc", idx).await?,
                Some("ONLINE".into()),
                "{idx}"
            );
        }
        assert_eq!(db.session().query(query).await?.len(), 1);
        db.shutdown().await?;
    }
    let db = Uni::open(&path).config(config()).build().await?;
    for idx in ["idx_took", "idx_doc"] {
        assert_eq!(
            status(&db, "Doc", idx).await?,
            Some("ONLINE".into()),
            "{idx}"
        );
        assert!(matches!(
            db.storage().schema_manager().get_index(idx),
            Some(uni_db::core::schema::IndexDefinition::Scalar(cfg)) if cfg.key.is_none()
        ));
    }
    run(
        &db,
        "CREATE (:Doc {id: 3, took: duration('P1D'), doc: {a: 1}})",
    )
    .await?;
    db.flush().await?;
    db.indexes().rebuild("Doc", false).await?;
    for idx in ["idx_took", "idx_doc"] {
        assert_eq!(
            status(&db, "Doc", idx).await?,
            Some("ONLINE".into()),
            "{idx}"
        );
    }
    let rows = db.session().query(query).await?;
    assert_eq!(rows.len(), 2);
    // A new one is still refused.
    let err = run(&db, "CREATE INDEX idx_took2 FOR (d:Doc) ON (d.took)")
        .await
        .expect_err("refused");
    assert!(format!("{err:#}").contains("not ordered"), "{err:#}");
    Ok(())
}

/// A POINT index recorded before points had a key (refused by #356 and
/// recorded `Failed`) is keyed by the next writable open, and serves.
#[tokio::test]
async fn refusal_pointers_point_index_from_before_keys_is_upgraded() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    let query = "MATCH (p:Spot) WHERE distance(p.loc, point({x: 0.0, y: 0.0})) < 2.0 \
                 RETURN p.id AS id";
    {
        let db = Uni::open(&path).config(config()).build().await?;
        run(&db, "CREATE LABEL Spot (id INT)").await?;
        run(&db, "CREATE (:Spot {id: 0})").await?;
        db.flush().await?;
        run(&db, "CREATE INDEX idx_loc FOR (p:Spot) ON (p.loc)").await?;
        run(&db, "ALTER LABEL Spot ADD PROPERTY loc POINT").await?;
        db.storage()
            .schema_manager()
            .update_index_metadata("idx_loc", |m| {
                m.status = uni_db::core::schema::IndexStatus::Failed;
            })?;
        db.storage().schema_manager().save().await?;
        run(&db, "MATCH (p:Spot {id: 0}) DELETE p").await?;
        run(
            &db,
            "CREATE (:Spot {id: 1, loc: point({x: 1.0, y: 1.0})}), \
                    (:Spot {id: 2, loc: point({x: 5.0, y: 5.0})})",
        )
        .await?;
        db.flush().await?;
        db.shutdown().await?;
    }
    let db = Uni::open(&path).config(config()).build().await?;
    assert_eq!(status(&db, "Spot", "idx_loc").await?, Some("ONLINE".into()));
    let rows = db.session().query(query).await?;
    assert_eq!(rows.len(), 1);
    let explain = db.session().query_with(query).explain().await?;
    assert!(
        explain
            .index_usage
            .iter()
            .any(|u| u.property == "loc" && u.used),
        "{explain:?}"
    );
    Ok(())
}
