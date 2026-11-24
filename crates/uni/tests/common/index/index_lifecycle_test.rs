// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for index lifecycle management (IndexStatus, metadata, auto-rebuild).

use anyhow::Result;
use std::collections::HashMap;
use uni_db::Uni;
use uni_db::UniConfig;
use uni_db::core::schema::{IndexDefinition, IndexStatus, ScalarIndexConfig, ScalarIndexType};
use uni_db::unival;

#[tokio::test]
async fn test_list_indexes_with_metadata() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Add a label and index
    db.schema_manager().add_label("Person")?;
    db.schema_manager().add_property(
        "Person",
        "name",
        uni_db::core::schema::DataType::String,
        false,
    )?;

    let idx = IndexDefinition::Scalar(ScalarIndexConfig {
        name: "idx_person_name".to_string(),
        label: "Person".to_string(),
        properties: vec!["name".to_string()],
        index_type: ScalarIndexType::BTree,
        where_clause: None,
        metadata: Default::default(),
        key: None,
    });
    db.schema_manager().add_index(idx)?;

    // list_indexes returns indexes for a specific label
    let indexes = db.indexes().list(Some("Person"));
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].name(), "idx_person_name");
    assert_eq!(indexes[0].metadata().status, IndexStatus::Online);
    assert!(indexes[0].metadata().last_built_at.is_none());

    // list_all_indexes returns all
    let all = db.indexes().list(None);
    assert_eq!(all.len(), 1);

    // No indexes for a non-existent label
    let empty = db.indexes().list(Some("NoSuchLabel"));
    assert!(empty.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_update_index_metadata_persists() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema_manager().add_label("Product")?;
    let idx = IndexDefinition::Scalar(ScalarIndexConfig {
        name: "idx_product_sku".to_string(),
        label: "Product".to_string(),
        properties: vec!["sku".to_string()],
        index_type: ScalarIndexType::BTree,
        where_clause: None,
        metadata: Default::default(),
        key: None,
    });
    db.schema_manager().add_index(idx)?;

    // Update metadata
    let now = chrono::Utc::now();
    db.schema_manager()
        .update_index_metadata("idx_product_sku", |m| {
            m.status = IndexStatus::Stale;
            m.last_built_at = Some(now);
            m.row_count_at_build = Some(500);
        })?;

    // Verify through list_indexes
    let indexes = db.indexes().list(Some("Product"));
    assert_eq!(indexes[0].metadata().status, IndexStatus::Stale);
    assert_eq!(indexes[0].metadata().row_count_at_build, Some(500));

    // Save and reload
    db.schema_manager().save().await?;
    let indexes2 = db.indexes().list(Some("Product"));
    assert_eq!(indexes2[0].metadata().status, IndexStatus::Stale);

    Ok(())
}

#[tokio::test]
async fn test_bulk_sync_sets_metadata() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Setup schema with a scalar index
    db.schema_manager().add_label("Item")?;
    db.schema_manager().add_property(
        "Item",
        "name",
        uni_db::core::schema::DataType::String,
        false,
    )?;
    db.schema_manager().save().await?;

    let idx = IndexDefinition::Scalar(ScalarIndexConfig {
        name: "idx_item_name".to_string(),
        label: "Item".to_string(),
        properties: vec!["name".to_string()],
        index_type: ScalarIndexType::BTree,
        where_clause: None,
        metadata: Default::default(),
        key: None,
    });
    db.schema_manager().add_index(idx)?;
    db.schema_manager().save().await?;

    // Bulk load some data with sync index rebuild
    let s = db.session();
    let tx = s.tx().await?;
    let mut bulk = tx
        .bulk_writer()
        .defer_scalar_indexes(true)
        .async_indexes(false)
        .build()?;

    let mut vertices = Vec::new();
    for i in 0..10 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), unival!(format!("item_{}", i)));
        vertices.push(props);
    }
    bulk.insert_vertices("Item", vertices).await?;
    let stats = bulk.commit().await?;
    assert_eq!(stats.vertices_inserted, 10);
    assert_eq!(stats.indexes_rebuilt, 1);

    // Verify metadata was updated on our original index
    let indexes = db.indexes().list(Some("Item"));
    assert!(!indexes.is_empty());
    let our_idx = indexes
        .iter()
        .find(|i| i.name() == "idx_item_name")
        .expect("idx_item_name should exist");
    assert_eq!(our_idx.metadata().status, IndexStatus::Online);
    assert!(our_idx.metadata().last_built_at.is_some());
    // row_count_at_build should be set (may be 10 or more depending on table format)
    assert!(our_idx.metadata().row_count_at_build.is_some());

    Ok(())
}

#[tokio::test]
async fn test_auto_rebuild_config_default_disabled() {
    let config = UniConfig::default();
    assert!(!config.index_rebuild.auto_rebuild_enabled);
    assert_eq!(config.index_rebuild.growth_trigger_ratio, 0.5);
    assert!(config.index_rebuild.max_index_age.is_none());
}

// ---------------------------------------------------------------------------
// `status: Online` must mean "physically built", not "declared".
//
// `IndexMetadata::default()` sets `status: Online`, so every index was born
// claiming to be built. When the physical build failed, `IndexManager` swallowed
// the backend error into a `warn!` and registered the definition anyway — so a
// scalar index over a column that does not exist reported itself `Online`, and
// six call sites gate real behaviour on exactly that: `pushdown.rs:166,237,279`
// and `SchemaManager`'s vector / sparse / fulltext property lookups.
//
// The build tolerance is deliberate and is kept: `CREATE INDEX` before a flush,
// or on a degenerate column, must not fail a DDL statement. What these tests pin
// is that the *outcome* is now recorded.
// ---------------------------------------------------------------------------

/// A `Person` label with 200 flushed rows and one typed `age` column.
async fn seeded_db() -> Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Person")
        .property("name", uni_db::DataType::String)
        .property_nullable("age", uni_db::DataType::Int)
        .apply()
        .await?;

    let rows: Vec<HashMap<String, uni_db::Value>> = (0..200)
        .map(|i| {
            let mut p = HashMap::new();
            p.insert("name".to_string(), unival!(format!("p{i}")));
            p.insert("age".to_string(), unival!(i64::from(i % 60)));
            p
        })
        .collect();
    let tx = db.session().tx().await?;
    tx.bulk_insert_vertices("Person", rows).await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

fn status_of(db: &Uni, name: &str) -> IndexStatus {
    db.indexes()
        .list(Some("Person"))
        .into_iter()
        .find(|i| i.name() == name)
        .unwrap_or_else(|| panic!("index '{name}' was not registered at all"))
        .metadata()
        .status
        .clone()
}

/// **The regression.** Lance rejects an index on a column that does not exist —
/// `CreateIndex: column 'x' does not exist` — and that rejection must reach the
/// index's status.
///
/// `apply()` still succeeds: the tolerance is the point, and escalating it to a
/// hard error would be a separate, wider behaviour change.
#[tokio::test]
async fn a_failed_physical_build_is_not_reported_online() -> Result<()> {
    let db = seeded_db().await?;

    let applied = db
        .schema()
        .label("Person")
        .index(
            "no_such_column",
            uni_db::api::schema::IndexType::Scalar(uni_db::api::schema::ScalarType::BTree),
        )
        .apply()
        .await;
    assert!(
        applied.is_ok(),
        "apply() should still tolerate a failed physical build: {applied:?}"
    );

    let status = status_of(&db, "idx_Person_no_such_column");
    assert_ne!(
        status,
        IndexStatus::Online,
        "an index whose physical build was rejected by the backend still reports \
         Online. Every `status == Online` gate — pushdown.rs and the three \
         SchemaManager property lookups — will treat this index as usable."
    );
    Ok(())
}

/// The other half, and the one that makes the test above mean something.
///
/// A fix that marked *every* index non-Online would satisfy the regression test
/// while silently disabling index pushdown across the board. So a real build on
/// a real column must still be `Online` — and must now carry the `last_built_at`
/// stamp that distinguishes it from the old default.
#[tokio::test]
async fn a_successful_physical_build_is_online_and_stamped() -> Result<()> {
    let db = seeded_db().await?;

    db.schema()
        .label("Person")
        .index(
            "age",
            uni_db::api::schema::IndexType::Scalar(uni_db::api::schema::ScalarType::BTree),
        )
        .apply()
        .await?;

    let idx = db
        .indexes()
        .list(Some("Person"))
        .into_iter()
        .find(|i| i.name() == "idx_Person_age")
        .expect("index not registered");

    assert_eq!(
        idx.metadata().status,
        IndexStatus::Online,
        "a scalar index that genuinely built on a typed column is not Online — \
         index pushdown is now disabled for every index in the database"
    );
    assert!(
        idx.metadata().last_built_at.is_some(),
        "a built index carries no build timestamp, so Online is still \
         indistinguishable from the default"
    );
    Ok(())
}

/// **A known gap, pinned so it stays visible.**
///
/// An index declared before the label has ever been flushed has no artifact
/// behind it, and still reports `Online`. That is not right, but demoting it is
/// not safe today and the reason is worth keeping next to the assertion:
/// `pushdown.rs:166` gates the hash-index point lookup on `status == Online`,
/// and that optimization needs no physical artifact — `ScalarIndexType::Hash`
/// has no Lance counterpart. The `issue_57_match_label_hash_index` suite
/// exercises exactly this shape: an index declared before any flush, never
/// physically built (auto-rebuild is off by default), whose pushdown must still
/// fire. Demoting this case turns those five tests red by disabling a working
/// optimization.
///
/// Closing it means separating "declared" from "materialized" at the six
/// `Online` gates, which is a wider change than the fix this file tests.
///
/// So this asserts today's behaviour deliberately. If it ever fails, the
/// separation has happened and this test should become the stronger `assert_ne!`
/// it wants to be.
#[tokio::test]
async fn an_index_declared_before_any_flush_still_reports_online() -> Result<()> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Person")
        .property("name", uni_db::DataType::String)
        .property_nullable("age", uni_db::DataType::Int)
        .index(
            "age",
            uni_db::api::schema::IndexType::Scalar(uni_db::api::schema::ScalarType::BTree),
        )
        .apply()
        .await?;

    assert_eq!(
        status_of(&db, "idx_Person_age"),
        IndexStatus::Online,
        "an index declared before any flush no longer reports Online. If that \
         was deliberate, the `Online` gates have been split into declared-vs-\
         materialized and this test should now assert the opposite."
    );
    assert!(
        db.indexes()
            .list(Some("Person"))
            .into_iter()
            .find(|i| i.name() == "idx_Person_age")
            .expect("index not registered")
            .metadata()
            .last_built_at
            .is_none(),
        "an index that was never built carries a build timestamp"
    );
    Ok(())
}

/// A scalar index declared before the label has any data must gain a physical
/// artifact at the first flush.
///
/// This is the resolution of the `NotAttempted` deferral. Before
/// `IndexManager::ensure_declared_indexes`, nothing ever built such an
/// index: `create_scalar_index` recorded `NotAttempted` because the Lance table
/// did not exist yet, and the flush path built only the `_vid`/`_uid`/`ext_id`
/// defaults. The declaration survived in the registry with no artifact behind
/// it, and only an explicit `db.indexes().rebuild()` fixed it — so a user who
/// declared their indexes up front, which is the natural order, silently got
/// none.
///
/// `last_built_at` is the discriminator rather than `status`, because
/// `NotAttempted` maps to `Online` (see `BuildOutcome`'s docs) and so status
/// alone cannot tell a built index from a declared one. Asserting it is `None`
/// *before* the flush is what makes the post-flush assertion mean something:
/// without it, an index that had somehow been built eagerly would pass the test
/// while proving nothing about the flush.
#[tokio::test]
async fn an_index_declared_before_any_data_is_built_at_the_first_flush() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.schema()
        .label("Person")
        .property("name", uni_db::core::schema::DataType::String)
        .done()
        .apply()
        .await?;

    // Declared with no rows and no table — the deferral case.
    db.schema()
        .label("Person")
        .index(
            "name",
            uni_db::api::schema::IndexType::Scalar(uni_db::api::schema::ScalarType::BTree),
        )
        .apply()
        .await?;

    let before = db
        .indexes()
        .list(Some("Person"))
        .into_iter()
        .find(|i| i.name() == "idx_Person_name")
        .expect("index not registered");
    assert!(
        before.metadata().last_built_at.is_none(),
        "precondition: the index must NOT be built yet, or this test proves nothing \
         about the flush"
    );

    let s = db.session();
    let tx = s.tx().await?;
    tx.query("CREATE (:Person {name: 'a'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let after = db
        .indexes()
        .list(Some("Person"))
        .into_iter()
        .find(|i| i.name() == "idx_Person_name")
        .expect("index disappeared from the registry across a flush");
    assert!(
        after.metadata().last_built_at.is_some(),
        "a scalar index declared before the table existed was never physically \
         built: the flush resolved no deferral, so the declaration is decorative \
         and every query on this column takes a full scan"
    );
    assert_eq!(after.metadata().status, IndexStatus::Online);

    Ok(())
}

/// The flush-time build is idempotent: a second flush must not rebuild.
///
/// `ensure_declared_indexes` runs on every flush of every label, so a
/// missing skip-if-present check would rebuild every declared index on every
/// flush — quadratic work that would show up as a flush-latency regression
/// rather than a failure.
#[tokio::test]
async fn the_flush_time_index_build_does_not_repeat() -> Result<()> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Person")
        .property("name", uni_db::core::schema::DataType::String)
        .done()
        .apply()
        .await?;
    db.schema()
        .label("Person")
        .index(
            "name",
            uni_db::api::schema::IndexType::Scalar(uni_db::api::schema::ScalarType::BTree),
        )
        .apply()
        .await?;

    let s = db.session();
    let tx = s.tx().await?;
    tx.query("CREATE (:Person {name: 'a'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let first = db
        .indexes()
        .list(Some("Person"))
        .into_iter()
        .find(|i| i.name() == "idx_Person_name")
        .expect("index not registered")
        .metadata()
        .last_built_at;
    assert!(first.is_some(), "precondition: first flush must build it");

    let tx = s.tx().await?;
    tx.query("CREATE (:Person {name: 'b'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let second = db
        .indexes()
        .list(Some("Person"))
        .into_iter()
        .find(|i| i.name() == "idx_Person_name")
        .expect("index not registered")
        .metadata()
        .last_built_at;

    assert_eq!(
        first, second,
        "the second flush rebuilt an index that was already present — the \
         skip-if-present check is not working, so every flush pays for every \
         declared index"
    );

    Ok(())
}

/// `row_count_at_build` must be populated by the paths that actually build
/// indexes, not only by the rebuild manager.
///
/// It has exactly one consumer: the growth trigger in
/// `IndexRebuildManager::labels_needing_rebuild`, gated on `Some(..)`. Every
/// `IndexManager` path used to leave it `None` and defer to the rebuild
/// manager, but `apply()`'s synchronous rebuild and the flush-time build both
/// go through `IndexManager` and never reach that manager — so the field stayed
/// empty and the trigger silently never fired. Growth is the only size-based
/// trigger enabled by default (`growth_trigger_ratio: 0.5`, `max_index_age:
/// None`), so an index built this way could grow without bound unnoticed.
///
/// `last_built_at` is the control: it was already being set on this path, so if
/// it is missing the index never built and a missing row count would prove
/// nothing.
#[tokio::test]
async fn index_build_records_the_row_count_it_indexed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = dir.path().join("store");
    let db = Uni::open(store.to_string_lossy()).build().await?;

    db.schema()
        .label("Foo")
        .property("name", uni_db::DataType::String)
        .index("name", uni_db::IndexType::Scalar(uni_db::ScalarType::BTree))
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    for i in 0..200 {
        tx.execute_with("CREATE (:Foo {name: $n})")
            .param("n", format!("n{i}"))
            .run()
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;

    let meta = db
        .schema_manager()
        .schema()
        .indexes
        .first()
        .map(|i| i.metadata().clone())
        .expect("one index");

    assert!(
        meta.last_built_at.is_some(),
        "control: the index never built, so a missing row count would say nothing"
    );
    assert_eq!(
        meta.row_count_at_build,
        Some(200),
        "a built index must record the number of rows it indexed, or the growth \
         trigger that reads this field can never fire for it"
    );

    db.shutdown().await?;
    Ok(())
}

/// The `last_built_at` of the index named `name` on `label`.
fn last_built(db: &Uni, label: &str, name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    db.indexes()
        .list(Some(label))
        .into_iter()
        .find(|i| i.name() == name)
        .unwrap_or_else(|| panic!("index '{name}' not registered"))
        .metadata()
        .last_built_at
}

/// A full-text index declared before the label has any data must gain a
/// physical artifact at the first flush, and not be rebuilt at later ones.
///
/// The flush resolved only the scalar deferral; full-text indexes were left
/// out of `ensure_declared_indexes` on the belief that they are "updated
/// incrementally". That is true of uni's own
/// inverted index, not of the Lance index behind `CREATE FULLTEXT INDEX`: it
/// was recorded `Online` with no artifact and searched by a flat scan, with a
/// tokenizer that does not lowercase, until something rebuilt it.
#[tokio::test]
async fn a_fulltext_index_declared_before_any_data_is_built_at_the_first_flush() -> Result<()> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property("body", uni_db::core::schema::DataType::String)
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.body]")
        .await?;
    tx.commit().await?;
    assert!(
        last_built(&db, "Doc", "doc_fts").is_none(),
        "precondition: the index must NOT be built yet, or this test proves nothing \
         about the flush"
    );

    let tx = s.tx().await?;
    tx.query("CREATE (:Doc {body: 'graph database'})").await?;
    tx.commit().await?;
    db.flush().await?;
    let first = last_built(&db, "Doc", "doc_fts");
    assert!(
        first.is_some(),
        "a full-text index declared before the table existed was never built"
    );

    // The index's tokenizer lowercases; the flat scan used without one does not.
    let hits = s
        .query("CALL uni.fts.query('Doc', 'body', 'GRAPH', 10) YIELD node RETURN node.body")
        .await?;
    assert_eq!(
        hits.rows().len(),
        1,
        "an upper-case query must match through the index"
    );

    let tx = s.tx().await?;
    tx.query("CREATE (:Doc {body: 'more text'})").await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(
        first,
        last_built(&db, "Doc", "doc_fts"),
        "the second flush rebuilt a full-text index that was already present"
    );
    Ok(())
}

/// `SHOW INDEXES` and `uni.schema.indexes()` name every index kind: inverted,
/// JSON full-text and sparse indexes were listed as `UNKNOWN`.
#[tokio::test]
async fn show_indexes_names_every_index_kind() -> Result<()> {
    use uni_db::DataType;
    use uni_db::core::schema::JsonFtsIndexConfig;

    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Doc")
        .property_nullable("tags", DataType::List(Box::new(DataType::String)))
        .property_nullable("emb", DataType::SparseVector { dimensions: 100 })
        .apply()
        .await?;
    db.session()
        .run("CALL uni.schema.createIndex('Doc', 'tags', {type: 'inverted', name: 'doc_tags'})")
        .await?;
    db.session()
        .run("CALL uni.schema.createIndex('Doc', 'emb', {type: 'sparse', name: 'doc_emb'})")
        .await?;
    db.schema_manager()
        .add_index(IndexDefinition::JsonFullText(JsonFtsIndexConfig {
            name: "doc_json".to_string(),
            label: "Doc".to_string(),
            column: "_doc".to_string(),
            paths: vec![],
            with_positions: false,
            metadata: Default::default(),
        }))?;

    let shown = db.session().query("SHOW INDEXES").await?;
    let mut rows: Vec<(String, String)> = shown
        .rows()
        .iter()
        .map(|r| Ok((r.get::<String>("name")?, r.get::<String>("type")?)))
        .collect::<Result<_>>()?;
    rows.sort();
    let want = vec![
        ("doc_emb".to_string(), "SPARSE".to_string()),
        ("doc_json".to_string(), "JSON_FTS".to_string()),
        ("doc_tags".to_string(), "INVERTED".to_string()),
    ];
    assert_eq!(rows, want, "SHOW INDEXES");

    let listed = db
        .session()
        .query("CALL uni.schema.indexes() YIELD name, type RETURN name, type ORDER BY name")
        .await?;
    let rows: Vec<(String, String)> = listed
        .rows()
        .iter()
        .map(|r| Ok((r.get::<String>("name")?, r.get::<String>("type")?)))
        .collect::<Result<_>>()?;
    assert_eq!(rows, want, "uni.schema.indexes()");
    Ok(())
}

/// `DROP INDEX IF EXISTS` on a missing index succeeds; without `IF EXISTS` it fails.
#[tokio::test]
async fn drop_index_if_exists_tolerates_a_missing_index() -> Result<()> {
    let db = Uni::temporary().build().await?;
    db.session()
        .run("DROP INDEX IF EXISTS no_such_index")
        .await?;
    assert!(db.session().run("DROP INDEX no_such_index").await.is_err());

    db.session()
        .run("CREATE LABEL Person (name STRING)")
        .await?;
    db.session()
        .run("CREATE INDEX person_name FOR (p:Person) ON (p.name)")
        .await?;
    db.session().run("DROP INDEX IF EXISTS person_name").await?;
    assert!(db.indexes().list(Some("Person")).is_empty());
    Ok(())
}
