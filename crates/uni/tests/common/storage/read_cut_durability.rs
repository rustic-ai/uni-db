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
    if scenario == "index-built-in-flush" {
        // A published flush first: a crash in a store's very first flush
        // leaves tables and no manifest, which the open refuses (#72).
        let db = seeded(&path).await?;
        db.schema()
            .label("Doc")
            .property("content", DataType::String)
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE FULLTEXT INDEX doc_fts FOR (d:Doc) ON EACH [d.content]")
            .await?;
        tx.commit().await?;
        create(&db, "CREATE (:Doc {content: 'jumping over fences'})").await?;
        crate::crash_harness::abort_at("flush::before-manifest");
        db.flush().await?;
        return Ok(());
    }
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

/// A bulk load names its own table versions in its manifest even while a
/// flush that wrote the same tables is still open: the load is not in the
/// WAL, so a manifest that left it out would let the next open roll it back.
#[tokio::test]
async fn a_bulk_load_is_named_by_its_manifest_while_a_flush_is_open() -> Result<()> {
    use std::collections::HashMap;
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    // Another flush, mid-way: it has committed the main vertex table.
    let cut = std::sync::Arc::clone(db.storage().read_cut());
    let open = cut.open_unit("open-flush");
    let busy = head_version(dir.path(), "vertices").await?;
    uni_store::storage::read_cut::in_unit("open-flush".to_string(), async {
        cut.record("vertices", busy);
    })
    .await;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices(
        "Person",
        vec![HashMap::from([(
            "name".to_string(),
            uni_db::Value::String("Bob".to_string()),
        )])],
    )
    .await?;
    bulk.commit().await?;
    tx.commit().await?;
    let manifest = db
        .storage()
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .expect("a manifest");
    drop(open);
    for table in ["vertices", PERSON] {
        assert_eq!(
            manifest.tables.get(table).copied(),
            Some(head_version(dir.path(), table).await?),
            "the load's manifest names {table} at the version holding the load"
        );
    }
    db.shutdown().await?;
    Ok(())
}

/// Restore publishes exactly one manifest: no successor naming rewound tables
/// under the pre-restore manifest appears while it runs.
#[tokio::test]
async fn a_restore_publishes_one_manifest() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    let snapshot = db.create_snapshot("one").await?;
    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    let storage = db.storage();
    let sm = storage.snapshot_manager();
    let before: std::collections::HashSet<String> =
        sm.list_snapshots().await?.into_iter().collect();
    let pre_restore = sm.latest_snapshot_id().await?.expect("a latest manifest");
    db.restore_snapshot(&snapshot).await?;
    // The restore's own manifest succeeds the pre-restore one; another
    // successor saved while the restore ran would name rewound tables under
    // the old contents.
    let mut successors = Vec::new();
    for id in sm.list_snapshots().await? {
        if !before.contains(&id)
            && sm.load_snapshot(&id).await?.parent_snapshot.as_deref() == Some(pre_restore.as_str())
        {
            successors.push(id);
        }
    }
    assert_eq!(
        successors.len(),
        1,
        "successors of the pre-restore manifest: {successors:?}"
    );
    assert_eq!(names(&db).await?, ["Alice"]);
    db.shutdown().await?;
    Ok(())
}

/// A restore whose rewind fails part way fences the handle: some tables are
/// rewound and others not, and only a reopen completes the restore.
#[cfg(feature = "failpoints")]
#[tokio::test]
async fn a_failed_restore_rewind_fences_the_handle() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    let snapshot = db.create_snapshot("one").await?;
    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    fail::cfg("restore::table-fail", "1*return").unwrap();
    let restored = db.restore_snapshot(&snapshot).await;
    fail::cfg("restore::table-fail", "off").unwrap();
    assert!(restored.is_err(), "the injected rollback failure surfaces");
    let write = async {
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Person {name: 'Carol'})").await?;
        tx.commit().await?;
        db.flush().await
    }
    .await;
    let err = format!("{:#}", write.expect_err("a write after the failed restore"));
    assert!(
        err.contains("restore of snapshot"),
        "refused by the fence: {err}"
    );
    let _ = db.shutdown().await;

    let db = Uni::open(dir.path().to_str().unwrap())
        .config(quiet())
        .build()
        .await?;
    assert_eq!(
        names(&db).await?,
        ["Alice"],
        "the reopen completes the restore"
    );
    db.shutdown().await?;
    Ok(())
}

/// A read-only handle's Locy evaluation reads the latest published flush.
#[tokio::test]
async fn a_read_only_locy_evaluation_sees_a_new_flush() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    let reader = open_read_only(dir.path(), quiet()).await?;
    let people = |r: &Uni| {
        let session = r.session();
        async move {
            let out = session
                .locy("CREATE RULE people AS MATCH (p:Person) YIELD KEY p")
                .await?;
            Ok::<_, anyhow::Error>(
                out.derived_rules()
                    .await
                    .unwrap()
                    .get("people")
                    .map_or(0, Vec::len),
            )
        }
    };
    assert_eq!(people(&reader).await?, 1);
    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    assert_eq!(people(&reader).await?, 2);
    reader.shutdown().await?;
    db.shutdown().await?;
    Ok(())
}

/// A flush whose future is dropped between its stream and its finalize
/// leaves no open unit behind: a later publish (a compaction's) does not
/// wait for it forever.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_flush_does_not_wedge_later_publishes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = seeded(dir.path()).await?;
    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    fail::cfg("flush::after-stream-stall", "1*return").unwrap();
    let cancelled = tokio::time::timeout(Duration::from_millis(500), db.flush()).await;
    assert!(
        cancelled.is_err(),
        "premise: the flush stalled and was dropped"
    );
    fail::cfg("flush::after-stream-stall", "off").unwrap();
    tokio::time::timeout(Duration::from_secs(20), db.compaction().compact("Person"))
        .await
        .expect("the compaction's publish does not wait for the dropped flush")?;
    tokio::time::timeout(Duration::from_secs(20), db.flush())
        .await
        .expect("a later flush completes")?;
    assert_eq!(names(&db).await?, ["Alice", "Bob"]);
    tokio::time::timeout(Duration::from_secs(20), db.shutdown())
        .await
        .expect("shutdown completes")?;
    Ok(())
}

/// Of two async flushes in flight, the second reads what the first wrote
/// though the first has not published yet: a relabel in the second keeps
/// the vertex the first created.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relabel_in_an_overlapping_flush_is_kept() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = UniConfig {
        async_flush_enabled: true,
        max_pending_flushes: 2,
        auto_flush_threshold: 1,
        auto_flush_min_mutations: 1,
        auto_flush_interval: None,
        ..Default::default()
    };
    let db = open(dir.path(), config).await?;
    db.schema()
        .label("A")
        .property("name", DataType::String)
        .done()
        .label("B")
        .property("name", DataType::String)
        .apply()
        .await?;
    // The first flush writes its tables, then waits before its finalize
    // (holding no lock); the second streams meanwhile.
    let head = db.storage().head_backend_arc();
    let versions = || {
        let head = Arc::clone(&head);
        async move {
            let mut all = std::collections::BTreeMap::new();
            for table in head.table_names().await? {
                all.insert(table.clone(), head.get_table_version(&table).await?);
            }
            Ok::<_, anyhow::Error>(all)
        }
    };
    fail::cfg("flush::before-async-finalize", "1*pause").unwrap();
    create(&db, "CREATE (:A {name: 'n'})").await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let first_streamed = versions().await;
    // A relabel alone counts no L0 mutation; the created row makes this
    // commit trigger the second flush.
    create(
        &db,
        "MATCH (n:A {name: 'n'}) SET n:B CREATE (:A {name: 'pad'})",
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let second_streamed = versions().await;
    fail::cfg("flush::before-async-finalize", "off").unwrap();
    // Still paused before the first finalize when read: the second flush
    // streamed meanwhile (it committed tables).
    assert_ne!(
        first_streamed?, second_streamed?,
        "premise: the second flush streamed while the first was open"
    );
    db.flush().await?;
    let rows = db
        .session()
        .query("MATCH (n:B) RETURN n.name AS name")
        .await?;
    assert_eq!(rows.len(), 1, "the relabel survived the flushes");
    db.shutdown().await?;
    let db = open(dir.path(), quiet()).await?;
    let rows = db
        .session()
        .query("MATCH (n:B) RETURN n.name AS name")
        .await?;
    assert_eq!(rows.len(), 1, "and the reopen");
    db.shutdown().await?;
    Ok(())
}

/// An index built inside a flush that crashed before its manifest: the open
/// rolls its table back, and the index is rebuilt rather than left recorded
/// built over a version that is gone.
#[cfg(all(unix, feature = "failpoints"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_built_by_a_crashed_flush_is_rebuilt() -> Result<()> {
    let dir = tempfile::tempdir()?;
    crate::crash_harness::run_child_async(
        "storage::read_cut_durability::read_cut_abort_child",
        "index-built-in-flush",
        dir.path(),
    )
    .await;
    let db = open(dir.path(), quiet()).await?;
    db.flush().await?;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        // An error is expected while the rebuild runs; the last one is
        // reported if it never answers.
        let answer = db
            .session()
            .query("CALL uni.fts.query('Doc', 'content', 'fences', 10) YIELD node RETURN node")
            .await
            .map(|r| r.len());
        if matches!(answer, Ok(1)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the full-text index never answered after the reopen: {answer:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    db.shutdown().await?;
    Ok(())
}

/// A flush while BACKUP copies, after it read the manifest it names: the
/// backup still holds the flush's commits (the WAL is not truncated while it
/// copies, and is copied before the tables), though opening it rolls the
/// tables back to that manifest.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flush_during_backup_loses_no_commit() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("src");
    let dest = dir.path().join("dest");
    let db = Arc::new(seeded(&src).await?);
    fail::cfg("backup::after-latest", "1*pause").unwrap();
    let backup = {
        let db = Arc::clone(&db);
        let to = format!("BACKUP TO '{}'", dest.display());
        tokio::spawn(async move { db.session().run(&to).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    // After the manifest the backup names: Bob reaches the tables, and the
    // flush would truncate the WAL past him.
    let paused_on = db.storage().snapshot_manager().latest_snapshot_id().await?;
    create(&db, "CREATE (:Person {name: 'Bob'})").await?;
    db.flush().await?;
    let after_flush = db.storage().snapshot_manager().latest_snapshot_id().await?;
    assert_ne!(paused_on, after_flush, "premise: the flush published");
    fail::cfg("backup::after-latest", "off").unwrap();
    let rows = backup.await??;
    // The backup read `latest` before the flush (it paused there): its
    // manifest lacks Bob, whom only the copied WAL holds.
    assert_eq!(
        rows.rows()[0].get::<String>("snapshot_id")?,
        paused_on.expect("the seed published a manifest"),
        "premise: the backup named the manifest from before the flush"
    );
    Arc::into_inner(db).expect("sole owner").shutdown().await?;

    let restored = open(&dest, quiet()).await?;
    assert_eq!(names(&restored).await?, ["Alice", "Bob"]);
    restored.shutdown().await?;
    Ok(())
}
