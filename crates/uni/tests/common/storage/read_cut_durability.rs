// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! The manifest is the commit point (Phase D of the object-store proposal).
//!
//! Every manifest records the Lance version of each table (its read cut), and
//! reads see those versions rather than each table's newest. So a table
//! version no manifest names -- a crashed flush's partial commits, or an
//! append a stale writer landed after losing the store -- is invisible, and a
//! writable open rolls it back.
//!
//! A "foreign" commit here is a raw Lance append made through a second
//! backend over the same directory: what a stalled writer that resumes after
//! a takeover would do.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, UInt64Array};
use uni_db::{DataType, Uni, UniConfig};
use uni_store::backend::lance::LanceDbBackend;
use uni_store::backend::{ScanRequest, StorageBackend, WriteMode};

const PERSON: &str = "vertices_Person";

fn quiet() -> UniConfig {
    UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: 1_000_000,
        ..Default::default()
    }
}

fn fresh_reads() -> UniConfig {
    UniConfig {
        read_cut_refresh: Some(Duration::ZERO),
        ..quiet()
    }
}

async fn open(path: &Path, config: UniConfig) -> Result<Uni> {
    Ok(Uni::open(path.to_str().unwrap())
        .config(config)
        .build()
        .await?)
}

async fn open_read_only(path: &Path, config: UniConfig) -> Result<Uni> {
    Ok(Uni::open(path.to_str().unwrap())
        .config(config)
        .read_only()
        .build()
        .await?)
}

async fn create(db: &Uni, cypher: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

/// A store with `Person {name}` and Alice flushed.
async fn seeded(path: &Path) -> Result<Uni> {
    let db = open(path, quiet()).await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    create(&db, "CREATE (:Person {name: 'Alice'})").await?;
    db.flush().await?;
    Ok(db)
}

async fn names(db: &Uni) -> Result<Vec<String>> {
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;
    rows.rows()
        .iter()
        .map(|r| Ok(r.get::<String>("name")?))
        .collect()
}

/// A backend over the store's tables that knows nothing of manifests.
async fn raw(path: &Path) -> Result<LanceDbBackend> {
    let tables: PathBuf = path.join("storage");
    LanceDbBackend::connect(tables.to_str().unwrap(), None).await
}

/// Every `name` stored in `table`'s newest version, superseded rows included.
async fn stored_names(path: &Path, table: &str) -> Result<Vec<String>> {
    let backend = raw(path).await?;
    let batches = backend.scan(ScanRequest::all(table)).await?;
    let mut out = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column")
            .clone();
        for i in 0..col.len() {
            if col.is_valid(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Appends, behind every manifest's back, a newer row for each stored Person
/// renamed to `name`: a stale writer's late append.
async fn foreign_rename(path: &Path, name: &str) -> Result<()> {
    let backend = raw(path).await?;
    let batches = backend.scan(ScanRequest::all(PERSON)).await?;
    let mut out = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let rows = batch.num_rows();
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, column)| match field.name().as_str() {
                "name" => Arc::new(StringArray::from(vec![name; rows])) as ArrayRef,
                "_version" => Arc::new(UInt64Array::from(vec![1_000_000u64; rows])) as ArrayRef,
                _ => Arc::clone(column),
            })
            .collect();
        out.push(RecordBatch::try_new(schema, columns)?);
    }
    backend.write(PERSON, out, WriteMode::Append).await?;
    Ok(())
}

async fn head_version(path: &Path, table: &str) -> Result<u64> {
    Ok(raw(path)
        .await?
        .get_table_version(table)
        .await?
        .expect("table exists"))
}

#[tokio::test]
async fn a_flush_records_the_version_of_every_table_it_wrote() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    let manifest = db
        .storage()
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .expect("a manifest");
    assert_eq!(
        manifest.tables.get(PERSON).copied(),
        Some(head_version(dir.path(), PERSON).await?),
        "the manifest names the label table's version: {:?}",
        manifest.tables
    );
    for table in ["vertices", PERSON] {
        assert!(manifest.tables.contains_key(table), "{table} missing");
    }
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn a_foreign_append_is_invisible_to_the_writer_and_to_readers() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    foreign_rename(dir.path(), "Mallory").await?;

    assert_eq!(names(&db).await?, ["Alice"], "the writer's own reads");
    let reader = open_read_only(dir.path(), quiet()).await?;
    assert_eq!(names(&reader).await?, ["Alice"], "a read-only handle");
    reader.shutdown().await?;
    db.shutdown().await?;
    Ok(())
}

/// A writer that meets a table version it did not write stops writing rather
/// than build on it; its commits stay in the WAL, and the next open discards
/// the foreign version and replays them.
#[tokio::test]
async fn a_writer_meeting_a_foreign_commit_stops_and_the_reopen_discards_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    foreign_rename(dir.path(), "Mallory").await?;

    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    let err = db
        .flush()
        .await
        .expect_err("the flush meets the foreign version");
    assert!(
        matches!(err, uni_db::UniError::WriterFenced { .. }),
        "{err:?}"
    );
    assert_eq!(names(&db).await?, ["Alice", "Bob"], "commits stay readable");
    drop(db);

    let db = open(dir.path(), quiet()).await?;
    assert_eq!(names(&db).await?, ["Alice", "Bob"]);
    db.flush().await?;
    assert_eq!(
        stored_names(dir.path(), PERSON).await?,
        ["Alice", "Bob"],
        "the foreign row was rolled back, never published"
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn a_writable_open_rolls_back_versions_no_manifest_names() -> Result<()> {
    let dir = tempfile::tempdir()?;
    seeded(dir.path()).await?.shutdown().await?;
    foreign_rename(dir.path(), "Mallory").await?;

    let reader = open_read_only(dir.path(), quiet()).await?;
    assert_eq!(names(&reader).await?, ["Alice"]);
    reader.shutdown().await?;

    let db = open(dir.path(), quiet()).await?;
    assert_eq!(names(&db).await?, ["Alice"]);
    assert_eq!(stored_names(dir.path(), PERSON).await?, ["Alice"]);
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn a_read_only_handle_sees_a_flush_once_it_refreshes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    let eager = open_read_only(dir.path(), fresh_reads()).await?;
    let lazy = open_read_only(
        dir.path(),
        UniConfig {
            read_cut_refresh: Some(Duration::from_secs(3600)),
            ..quiet()
        },
    )
    .await?;
    assert_eq!(names(&lazy).await?, ["Alice"]);

    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    assert_eq!(names(&eager).await?, ["Alice", "Bob"]);
    assert_eq!(
        names(&lazy).await?,
        ["Alice"],
        "a handle within its refresh interval keeps its cut"
    );
    eager.shutdown().await?;
    lazy.shutdown().await?;
    db.shutdown().await?;
    Ok(())
}

/// A manifest an older binary wrote names no tables: reads see every table's
/// newest version, and the next flush records them.
#[tokio::test]
async fn a_manifest_without_a_cut_reads_newest_versions_and_is_adopted() -> Result<()> {
    let dir = tempfile::tempdir()?;
    seeded(dir.path()).await?.shutdown().await?;
    let catalog = dir.path().join("storage").join("catalog");
    let latest = std::fs::read_to_string(catalog.join("latest"))?;
    let body = catalog
        .join("manifests")
        .join(format!("{}.json", latest.trim()));
    let mut manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&body)?)?;
    manifest.as_object_mut().unwrap().remove("tables");
    std::fs::write(&body, serde_json::to_string(&manifest)?)?;

    let db = open(dir.path(), quiet()).await?;
    assert_eq!(names(&db).await?, ["Alice"]);
    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    let manifest = db
        .storage()
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .expect("a manifest");
    assert!(
        manifest.tables.contains_key(PERSON),
        "{:?}",
        manifest.tables
    );
    db.shutdown().await?;
    let db = open(dir.path(), quiet()).await?;
    assert_eq!(names(&db).await?, ["Alice", "Bob"]);
    db.shutdown().await?;
    Ok(())
}

/// Version cleanup after compaction must keep the version readers read.
#[tokio::test]
async fn compaction_with_no_retention_keeps_what_readers_read() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut config = quiet();
    config.compaction.version_retention = Duration::ZERO;
    let db = open(dir.path(), config.clone()).await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    for name in ["A", "B", "C", "D"] {
        create(&db, &format!("CREATE (:Person {{name: '{name}'}})")).await?;
        db.flush().await?;
    }
    db.compaction().compact("Person").await?;
    db.compaction().wait().await?;
    db.storage().compact().await?;
    assert_eq!(names(&db).await?, ["A", "B", "C", "D"]);
    let reader = open_read_only(dir.path(), quiet()).await?;
    assert_eq!(names(&reader).await?, ["A", "B", "C", "D"]);
    reader.shutdown().await?;
    db.shutdown().await?;
    let db = open(dir.path(), config).await?;
    assert_eq!(names(&db).await?, ["A", "B", "C", "D"]);
    db.shutdown().await?;
    Ok(())
}

/// A property declared on a populated label is readable at once: the
/// widened table version is published before the catalog names the column.
#[tokio::test]
async fn a_property_added_to_a_flushed_label_reads_at_once() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    db.schema()
        .label("Person")
        .property_nullable("age", DataType::Int64)
        .apply()
        .await?;
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name, p.age AS age")
        .await?;
    assert_eq!(rows.len(), 1);
    let reader = open_read_only(dir.path(), quiet()).await?;
    let rows = reader
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name, p.age AS age")
        .await?;
    assert_eq!(rows.len(), 1);
    reader.shutdown().await?;
    db.shutdown().await?;
    Ok(())
}

/// A bulk load commits each table on its own; another handle sees the load
/// once its manifest is published, whole.
#[tokio::test]
async fn a_bulk_load_reaches_another_handle_whole() -> Result<()> {
    use std::collections::HashMap;
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    let reader = open_read_only(dir.path(), fresh_reads()).await?;
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices(
        "Person",
        ["Bob", "Carol"]
            .iter()
            .map(|n| HashMap::from([("name".to_string(), uni_db::Value::String((*n).to_string()))]))
            .collect::<Vec<_>>(),
    )
    .await?;
    assert_eq!(names(&reader).await?, ["Alice"], "staged, not visible");
    bulk.commit().await?;
    tx.commit().await?;
    assert_eq!(names(&reader).await?, ["Alice", "Bob", "Carol"]);
    reader.shutdown().await?;
    db.shutdown().await?;
    let db = open(dir.path(), quiet()).await?;
    assert_eq!(
        names(&db).await?,
        ["Alice", "Bob", "Carol"],
        "kept by the reopen"
    );
    db.shutdown().await?;
    Ok(())
}

/// Index datasets are under the cut too: a flush names the UID index's
/// version, and a writable open rolls back a commit to it no manifest names.
#[tokio::test]
async fn the_uid_index_is_under_the_cut() -> Result<()> {
    use uni_db::core::id::{UniId, Vid};
    const UID: &str = "indexes/uni_id_to_vid/Person/index.lance";
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    let manifest = db
        .storage()
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .expect("a manifest");
    let named = *manifest.tables.get(UID).expect("the UID index is named");
    db.shutdown().await?;

    let tables = dir.path().join("storage");
    let foreign = uni_store::storage::index::UidIndex::new(tables.to_str().unwrap(), "Person");
    foreign
        .write_mapping(&[(UniId::from_bytes([7u8; 32]), Vid::new(999))])
        .await?;
    assert_eq!(
        foreign.get_vid(&UniId::from_bytes([7u8; 32])).await?,
        Some(Vid::new(999)),
        "landed at the newest version"
    );
    let _ = named;

    let db = open(dir.path(), quiet()).await?;
    let held = db
        .storage()
        .uid_index("Person")?
        .get_vid(&UniId::from_bytes([7u8; 32]))
        .await?;
    assert_eq!(held, None, "the foreign mapping was rolled back");
    db.shutdown().await?;
    Ok(())
}

/// A flush paused between its table commits shows a reader nothing of it.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_in_progress_is_invisible_until_its_manifest() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Arc::new(seeded(dir.path()).await?);
    db.schema()
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    let reader = open_read_only(dir.path(), fresh_reads()).await?;
    create(
        &db,
        "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:KNOWS]->(:Person {name: 'Bob'})",
    )
    .await?;

    fail::cfg("flush::before-label-tables", "pause").unwrap();
    let flushing = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.flush().await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let edges = reader
        .session()
        .query("MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")
        .await?;
    let people = names(&reader).await;
    fail::cfg("flush::before-label-tables", "off").unwrap();
    flushing.await??;

    assert_eq!(edges.rows()[0].get::<i64>("c")?, 0, "edges mid-flush");
    assert_eq!(people?, ["Alice"], "people mid-flush");
    assert_eq!(names(&reader).await?, ["Alice", "Bob"]);
    reader.shutdown().await?;
    Arc::into_inner(db).expect("sole owner").shutdown().await?;
    Ok(())
}

/// Child entry point for the crash tests; does nothing when run directly.
#[cfg(all(unix, feature = "failpoints"))]
#[tokio::test]
#[ignore = "crash-harness child; driven by its parent test"]
async fn read_cut_abort_child() -> Result<()> {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return Ok(());
    };
    let db = seeded(&path).await?;
    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    match scenario.as_str() {
        "before-manifest" => crate::crash_harness::abort_at("flush::before-manifest"),
        other => crate::crash_harness::unknown_scenario("read_cut_abort_child", other),
    }
    db.flush().await?;
    Ok(())
}

/// A crash after a flush's table commits and before its manifest: the open
/// rolls the tables back and the WAL writes Bob once.
#[cfg(all(unix, feature = "failpoints"))]
#[tokio::test]
async fn a_crash_before_the_manifest_leaves_no_partial_flush() -> Result<()> {
    let dir = tempfile::tempdir()?;
    crate::crash_harness::run_child_async(
        "storage::read_cut_durability::read_cut_abort_child",
        "before-manifest",
        dir.path(),
    )
    .await;
    assert_eq!(stored_names(dir.path(), PERSON).await?, ["Alice", "Bob"]);

    let db = open(dir.path(), quiet()).await?;
    assert_eq!(
        stored_names(dir.path(), PERSON).await?,
        ["Alice"],
        "the open rolled the unpublished commit back"
    );
    assert_eq!(names(&db).await?, ["Alice", "Bob"], "replayed from the WAL");
    db.flush().await?;
    assert_eq!(stored_names(dir.path(), PERSON).await?, ["Alice", "Bob"]);
    db.shutdown().await?;
    Ok(())
}
