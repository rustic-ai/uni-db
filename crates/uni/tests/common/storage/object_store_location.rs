// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Where a store's files go, for every kind of location (issues #349, #350).
//!
//! These run without an object-store server: a `file://` URI resolves through
//! the same prefixed-store code path as `s3://bucket/prefix`, so a directory
//! stands in for a bucket and its subdirectories for prefixes. The S3 versions
//! are in `hybrid_localstack_e2e`.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use uni_common::{CloudStorageConfig, UniConfig};
use uni_db::{DataType, Uni};

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

/// A `file://` store at `bucket/rel` that treats `bucket` as its bucket
/// root, so a component handed the bucket-root handle would write inside
/// `bucket` (and be caught by the stray-file checks) rather than at `/`.
fn bucketed(bucket: &Path, rel: &str) -> uni_db::UniBuilder {
    Uni::open(file_uri(&bucket.join(rel))).storage_option(
        uni_db::store::location::FILE_BUCKET_ROOT_OPTION,
        bucket.display().to_string(),
    )
}

/// Every file under `root`, relative to it, with `/` separators.
fn files_under(root: &Path) -> BTreeSet<String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path.strip_prefix(root).unwrap();
                out.insert(rel.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(root, root, &mut out);
    out
}

/// The shape of a store's tree: top-level entries, catalog files, and the
/// table directories under `storage/`. Data file names (UUIDs) are left out.
fn layout_shape(root: &Path) -> BTreeSet<String> {
    files_under(root)
        .into_iter()
        .filter_map(|f| {
            let parts: Vec<&str> = f.split('/').collect();
            match parts.as_slice() {
                [one] => Some((*one).to_string()),
                ["catalog", name] if name.ends_with(".json") => Some(f.clone()),
                ["wal", ..] => Some("wal/".to_string()),
                ["storage", t, ..] if t.ends_with(".lance") => Some(format!("storage/{t}")),
                ["storage", "catalog", "latest"] => Some(f.clone()),
                ["storage", "catalog", "manifests", ..] => {
                    Some("storage/catalog/manifests/".to_string())
                }
                _ => None,
            }
        })
        .collect()
}

async fn people(db: &Uni) -> Result<Vec<String>> {
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;
    rows.rows()
        .iter()
        .map(|r| Ok(r.get::<String>("name")?))
        .collect()
}

async fn person_store(uri: &str, names: &[&str]) -> Result<Uni> {
    let db = Uni::open(uri).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    for n in names {
        tx.execute(&format!("CREATE (:Person {{name: '{n}'}})"))
            .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

fn read_json(path: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap_or_else(|e| panic!("{path:?}: {e}")))
        .unwrap()
}

// ---- E1: two stores under one root are independent ----------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_prefixes_in_one_root_are_independent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let open_a = || bucketed(dir.path(), "a");
    let open_b = || bucketed(dir.path(), "b");

    let a = open_a().build().await?;
    a.schema()
        .label("OnlyInA")
        .property("n", DataType::Int64)
        .apply()
        .await?;
    let tx = a.session().tx().await?;
    tx.execute("CREATE (:OnlyInA {n: 1})").await?;
    tx.commit().await?;
    a.flush().await?;
    let fork = a.session().fork("fa").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:OnlyInA {n: 2})").await?;
    tx.commit().await?;
    fork.flush().await?;
    drop(fork);

    let b = open_b().build().await?;
    let b_labels: Vec<String> = b.schema().current().labels.keys().cloned().collect();
    assert!(b_labels.is_empty(), "b inherited a's schema: {b_labels:?}");
    b.schema()
        .label("OnlyInB")
        .property("n", DataType::Int64)
        .apply()
        .await?;
    let tx = b.session().tx().await?;
    tx.execute("CREATE (:OnlyInB {n: 9})").await?;
    tx.commit().await?;
    b.flush().await?;
    assert!(b.list_forks().await.is_empty(), "b sees a's forks");
    drop((a, b));

    // Nothing either store wrote is outside its own directory.
    let stray: Vec<String> = files_under(dir.path())
        .into_iter()
        .filter(|f| !f.starts_with("a/") && !f.starts_with("b/"))
        .collect();
    assert!(stray.is_empty(), "files outside the prefixes: {stray:?}");

    // Each reopens with only its own schema and data.
    let a = open_a().build().await?;
    let labels: Vec<String> = a.schema().current().labels.keys().cloned().collect();
    assert_eq!(labels, ["OnlyInA"]);
    let n = a
        .session()
        .query("MATCH (x:OnlyInA) RETURN count(x) AS c")
        .await?;
    assert_eq!(n.rows()[0].get::<i64>("c")?, 1);
    assert_eq!(a.list_forks().await.len(), 1);
    // The fork reads its own rows back, and a nested fork stays under `a/`.
    let fork = a.session().fork("fa").await?;
    let n = fork.query("MATCH (x:OnlyInA) RETURN count(x) AS c").await?;
    assert_eq!(n.rows()[0].get::<i64>("c")?, 2);
    let child = fork.fork("fa2").await?;
    let tx = child.tx().await?;
    tx.execute("CREATE (:OnlyInA {n: 3})").await?;
    tx.commit().await?;
    child.flush().await?;
    drop((child, fork, a));
    let stray: Vec<String> = files_under(dir.path())
        .into_iter()
        .filter(|f| !f.starts_with("a/") && !f.starts_with("b/"))
        .collect();
    assert!(
        stray.is_empty(),
        "files outside the prefixes after forks: {stray:?}"
    );
    let b = open_b().build().await?;
    let labels: Vec<String> = b.schema().current().labels.keys().cloned().collect();
    assert_eq!(labels, ["OnlyInB"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_store_uses_the_standard_layout() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("s");
    let db = person_store(&file_uri(&root), &["Alice"]).await?;
    drop(db);

    assert_eq!(
        read_json(&root.join("catalog/layout.json"))["layout"],
        serde_json::json!("standard")
    );
    assert!(root.join("catalog/schema.json").is_file());
    assert!(root.join("id_allocator.json").is_file());
    assert!(root.join("storage/catalog/latest").is_file());
    assert!(root.join("storage/vertices.lance").is_dir());
    assert!(root.join("wal").is_dir());
    Ok(())
}

// ---- E1b: plain paths and file:// URIs lay a store out identically --------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plain_path_and_file_uri_produce_the_same_layout() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let plain = dir.path().join("plain");
    let uri = dir.path().join("uri");
    drop(person_store(plain.to_str().unwrap(), &["Alice", "Bob"]).await?);
    drop(person_store(&file_uri(&uri), &["Alice", "Bob"]).await?);
    assert_eq!(layout_shape(&plain), layout_shape(&uri));
    assert!(layout_shape(&plain).contains("catalog/layout.json"));
    Ok(())
}

// ---- E2: reopen; a trailing slash names the same store ---------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_file_uri_store_reopens_with_or_without_a_trailing_slash() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("s");
    drop(person_store(&file_uri(&root), &["Alice"]).await?);

    let db = Uni::open(format!("{}/", file_uri(&root))).build().await?;
    assert_eq!(people(&db).await?, ["Alice"]);
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;
    db.flush().await?;
    drop(db);

    let db = Uni::open(file_uri(&root)).build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob"]);
    // No `//` directory crept in from the trailing slash.
    assert!(
        files_under(&root).iter().all(|f| !f.contains("//")),
        "{:?}",
        files_under(&root)
    );
    Ok(())
}

// ---- E3: restore under a prefix rewinds data and indexes -------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_under_a_prefix_rewinds_data_and_indexes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let bucket = dir.path().join("bucket");
    let open = || bucketed(&bucket, "p");
    let db = open().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE CONSTRAINT person_name ON (p:Person) ASSERT p.name IS UNIQUE")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE INDEX person_name_idx FOR (p:Person) ON (p.name)")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let snapshot = db.create_snapshot("two").await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.execute("MATCH (b:Person {name: 'Bob'}) DETACH DELETE b")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(people(&db).await?, ["Alice", "Carol"]);

    db.restore_snapshot(&snapshot).await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob"]);
    db.shutdown().await?;

    let db = open().build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob"]);
    let by_name = |name: &'static str| {
        let db = &db;
        async move {
            let rows = db
                .session()
                .query(&format!(
                    "MATCH (p:Person) WHERE p.name = '{name}' RETURN count(p) AS c"
                ))
                .await?;
            anyhow::Ok(rows.rows()[0].get::<i64>("c")?)
        }
    };
    assert_eq!(by_name("Carol").await?, 0, "Carol survived the restore");
    assert_eq!(by_name("Bob").await?, 1, "Bob was not restored");
    // The uniqueness state was rewound with the data: Bob is taken again,
    // Carol is free.
    let tx = db.session().tx().await?;
    let dup = tx.execute("CREATE (:Person {name: 'Bob'})").await;
    let dup_rejected = match dup {
        Err(_) => true,
        Ok(_) => tx.commit().await.is_err(),
    };
    assert!(dup_rejected, "a second Bob was accepted after the restore");
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob", "Carol"]);

    let files = files_under(dir.path());
    let stray: Vec<&String> = files
        .iter()
        .filter(|f| !f.starts_with("bucket/p/"))
        .collect();
    assert!(stray.is_empty(), "{stray:?}");
    assert!(
        files
            .iter()
            .any(|f| f.starts_with("bucket/p/storage/indexes/")),
        "no index under the prefix: {files:?}"
    );
    Ok(())
}

// ---- E4: flat (pre-standard) remote stores keep working --------------------

/// Turns a standard store into the flat shape a ≤ 5.3.0 remote store has:
/// tables, indexes and snapshot catalog directly under the root.
fn flatten(root: &Path) {
    let storage = root.join("storage");
    for entry in std::fs::read_dir(&storage).unwrap().flatten() {
        let name = entry.file_name();
        if name == "catalog" {
            continue;
        }
        std::fs::rename(entry.path(), root.join(&name)).unwrap();
    }
    let catalog = storage.join("catalog");
    for entry in std::fs::read_dir(&catalog).unwrap().flatten() {
        std::fs::rename(entry.path(), root.join("catalog").join(entry.file_name())).unwrap();
    }
    std::fs::remove_dir_all(&storage).unwrap();
    let _ = std::fs::remove_file(root.join("catalog/layout.json"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flat_store_opens_in_place() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("flat");
    let uri = file_uri(&root);
    drop(person_store(&uri, &["Alice", "Bob"]).await?);
    flatten(&root);
    assert!(root.join("catalog/latest").is_file());
    assert!(root.join("vertices.lance").is_dir());

    // A read-only open reads it and writes no marker.
    let ro = Uni::open(&uri).read_only().build().await?;
    assert_eq!(people(&ro).await?, ["Alice", "Bob"]);
    drop(ro);
    assert!(!root.join("catalog/layout.json").exists());

    let db = Uni::open(&uri).build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob"]);
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.commit().await?;
    db.flush().await?;
    drop(db);
    assert_eq!(
        read_json(&root.join("catalog/layout.json"))["layout"],
        serde_json::json!("flat")
    );
    // Still flat: nothing was moved under `storage/`.
    assert!(!root.join("storage").exists());

    let db = Uni::open(&uri).build().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob", "Carol"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_prefixed_store_from_before_the_fix_is_refused_until_its_catalog_is_copied() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let bucket = dir.path().join("bucket");
    let root = bucket.join("a");
    let uri = file_uri(&root);
    drop(person_store(&uri, &["Alice"]).await?);
    flatten(&root);
    // ≤ 5.3.0 wrote the metadata at the bucket root, outside the prefix.
    std::fs::rename(root.join("catalog"), bucket.join("catalog"))?;
    std::fs::rename(root.join("wal"), bucket.join("wal"))?;
    std::fs::rename(
        root.join("id_allocator.json"),
        bucket.join("id_allocator.json"),
    )?;

    let open = || {
        Uni::open(&uri).storage_option(
            uni_db::store::location::FILE_BUCKET_ROOT_OPTION,
            bucket.display().to_string(),
        )
    };
    let err = match open().build().await {
        Ok(_) => panic!("a legacy prefixed store opened"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("catalog/latest") && err.contains("catalog/schema.json"),
        "{err}"
    );
    // Nothing was written while refusing.
    assert!(!root.join("catalog").exists());

    // The documented recovery: copy the bucket-root metadata under the prefix.
    copy_dir(&bucket.join("catalog"), &root.join("catalog"));
    copy_dir(&bucket.join("wal"), &root.join("wal"));
    std::fs::copy(
        bucket.join("id_allocator.json"),
        root.join("id_allocator.json"),
    )?;
    let db = open().build().await?;
    assert_eq!(people(&db).await?, ["Alice"]);
    Ok(())
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap().flatten() {
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_existing_local_store_is_marked_standard_and_otherwise_untouched() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db");
    drop(person_store(path.to_str().unwrap(), &["Alice"]).await?);
    let _ = std::fs::remove_file(path.join("catalog/layout.json"));
    let before: BTreeSet<String> = files_under(&path);

    let db = Uni::open(path.to_str().unwrap()).build().await?;
    assert_eq!(people(&db).await?, ["Alice"]);
    drop(db);
    assert_eq!(
        read_json(&path.join("catalog/layout.json"))["layout"],
        serde_json::json!("standard")
    );
    let after = files_under(&path);
    let removed: Vec<_> = before.difference(&after).collect();
    assert!(removed.is_empty(), "files moved or deleted: {removed:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_opens_write_nothing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("s");
    let uri = file_uri(&root);
    drop(person_store(&uri, &["Alice"]).await?);
    std::fs::remove_file(root.join("catalog/layout.json"))?;
    let before = files_under(&root);
    let ro = Uni::open(&uri).read_only().build().await?;
    assert_eq!(people(&ro).await?, ["Alice"]);
    drop(ro);
    assert_eq!(files_under(&root), before, "a read-only open wrote files");
    Ok(())
}

// ---- E5: losing the ID allocator file never reuses an ID -------------------

fn strip_id_marks_from_manifests(storage_catalog: &Path) {
    for entry in std::fs::read_dir(storage_catalog.join("manifests"))
        .unwrap()
        .flatten()
    {
        let mut v = read_json(&entry.path());
        let obj = v.as_object_mut().unwrap();
        obj.remove("next_vid");
        obj.remove("next_eid");
        std::fs::write(entry.path(), serde_json::to_vec(&v).unwrap()).unwrap();
    }
}

async fn allocator_loss_keeps_ids_unique(uri: &str, root: &Path, strip: bool) -> Result<()> {
    let db = person_store(uri, &["Alice", "Bob"]).await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;
    db.shutdown().await?;

    std::fs::remove_file(root.join("id_allocator.json"))?;
    if strip {
        strip_id_marks_from_manifests(&root.join("storage/catalog"));
    }

    let db = Uni::open(uri).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Carol'})").await?;
    tx.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(people(&db).await?, ["Alice", "Bob", "Carol"]);
    let edges = db
        .session()
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b ORDER BY a")
        .await?;
    let pairs: Vec<(String, String)> = edges
        .rows()
        .iter()
        .map(|r| Ok((r.get::<String>("a")?, r.get::<String>("b")?)))
        .collect::<Result<_>>()?;
    assert_eq!(
        pairs,
        [
            ("Alice".to_string(), "Bob".to_string()),
            ("Bob".to_string(), "Carol".to_string())
        ]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_allocator_file_does_not_reuse_vids() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("db");
    allocator_loss_keeps_ids_unique(root.to_str().unwrap(), &root, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_allocator_file_of_a_file_uri_store_does_not_reuse_vids() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("db");
    allocator_loss_keeps_ids_unique(&file_uri(&root), &root, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_allocator_file_of_an_old_store_does_not_reuse_vids() -> Result<()> {
    // Manifests written before they recorded the ID marks: the floor comes
    // from the tables themselves.
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("db");
    allocator_loss_keeps_ids_unique(root.to_str().unwrap(), &root, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unflushed_ids_past_a_batch_survive_losing_the_allocator_file() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("db");
    let path = root.to_str().unwrap().to_string();
    let config = UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: usize::MAX,
        ..Default::default()
    };
    let db = Uni::open(&path).config(config.clone()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    // More vertices than one allocator batch (1000), never flushed: they
    // live in the WAL only.
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(1, 1500) AS i CREATE (:Person {name: 'p' + toString(i)})")
        .await?;
    tx.commit().await?;
    drop(db);
    assert!(
        !root.join("storage/catalog/latest").exists(),
        "the writes were flushed; this test needs them in the WAL only"
    );
    std::fs::remove_file(root.join("id_allocator.json"))?;

    // Reopen: replay restores the ids. Close again without allocating
    // anything, so the counter file is never rewritten and only the manifest
    // the closing flush saves remembers how far ids went.
    let db = Uni::open(&path).config(config.clone()).build().await?;
    db.shutdown().await?;
    let _ = std::fs::remove_file(root.join("id_allocator.json"));

    let db = Uni::open(&path).config(config).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'new'})").await?;
    tx.commit().await?;
    let n = db
        .session()
        .query("MATCH (p:Person) RETURN count(DISTINCT p.name) AS c, count(p) AS n")
        .await?;
    assert_eq!(n.rows()[0].get::<i64>("c")?, 1501);
    assert_eq!(n.rows()[0].get::<i64>("n")?, 1501);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_a_hybrid_stores_local_directory_reuses_no_id() -> Result<()> {
    for strip in [false, true] {
        let dir = tempfile::tempdir()?;
        let bucket = dir.path().join("bucket");
        let open = |local: &Path| {
            Uni::open(local.to_str().unwrap())
                .hybrid(&file_uri(&bucket.join("g")))
                .storage_option(
                    uni_db::store::location::FILE_BUCKET_ROOT_OPTION,
                    bucket.display().to_string(),
                )
                .build()
        };
        let first = dir.path().join("local1");
        let db = open(&first).await?;
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .edge_type("KNOWS", &["Person"], &["Person"])
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        db.shutdown().await?;
        assert!(first.join("id_allocator.json").exists());
        assert!(!bucket.join("g/id_allocator.json").exists());
        std::fs::remove_dir_all(&first)?;
        if strip {
            strip_id_marks_from_manifests(&bucket.join("g/storage/catalog"));
        }

        let db = open(&dir.path().join("local2")).await?;
        let tx = db.session().tx().await?;
        tx.execute("MATCH (b:Person {name: 'Bob'}) CREATE (b)-[:KNOWS]->(:Person {name: 'Carol'})")
            .await?;
        tx.commit().await?;
        db.flush().await?;
        assert_eq!(
            people(&db).await?,
            ["Alice", "Bob", "Carol"],
            "strip={strip}"
        );
        let edges = db
            .session()
            .query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b ORDER BY a",
            )
            .await?;
        let pairs: Vec<(String, String)> = edges
            .rows()
            .iter()
            .map(|r| Ok((r.get::<String>("a")?, r.get::<String>("b")?)))
            .collect::<Result<_>>()?;
        assert_eq!(
            pairs,
            [
                ("Alice".to_string(), "Bob".to_string()),
                ("Bob".to_string(), "Carol".to_string())
            ],
            "strip={strip}"
        );
    }
    Ok(())
}

// ---- E6: a remote-only store honors the configured endpoint (#350) ---------

fn fail_fast() -> UniConfig {
    let mut config = UniConfig::default();
    config.object_store.max_retries = 0;
    config.object_store.connect_timeout = Duration::from_secs(2);
    config
}

fn assert_used_endpoint(result: Result<Uni, uni_db::UniError>, endpoint: &str) {
    let err = match result {
        Ok(_) => panic!("opened against a closed port"),
        Err(e) => format!("{e:#}"),
    };
    assert!(err.contains(endpoint), "{err}");
    assert!(!err.contains("169.254.169.254"), "went to IMDS: {err}");
}

#[tokio::test]
async fn remote_only_open_uses_storage_options() {
    let result = Uni::open("s3://bucket/p")
        .config(fail_fast())
        .storage_option("aws_endpoint", "http://127.0.0.1:1")
        .storage_option("aws_access_key_id", "test")
        .storage_option("aws_secret_access_key", "test")
        .build()
        .await;
    assert_used_endpoint(result, "127.0.0.1:1");
}

#[tokio::test]
async fn remote_only_open_uses_a_typed_cloud_config() {
    let cfg = CloudStorageConfig::S3 {
        bucket: "bucket".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("http://127.0.0.1:1".into()),
        access_key_id: Some("test".into()),
        secret_access_key: Some("test".into()),
        session_token: None,
        virtual_hosted_style: false,
    };
    let result = Uni::open("s3://bucket/p")
        .config(fail_fast())
        .cloud_storage(cfg)
        .build()
        .await;
    assert_used_endpoint(result, "127.0.0.1:1");
}

#[tokio::test]
async fn hybrid_open_without_a_typed_config_uses_storage_options() {
    let dir = tempfile::tempdir().unwrap();
    let result = Uni::open(dir.path().to_str().unwrap())
        .config(fail_fast())
        .hybrid("s3://bucket/p")
        .storage_option("aws_endpoint", "http://127.0.0.1:1")
        .storage_option("aws_access_key_id", "test")
        .storage_option("aws_secret_access_key", "test")
        .build()
        .await;
    assert_used_endpoint(result, "127.0.0.1:1");
}

// ---- E7: invalid locations fail before touching anything ------------------

#[tokio::test]
async fn a_bucket_mismatch_fails_the_open() {
    let cfg = CloudStorageConfig::S3 {
        bucket: "other".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("http://127.0.0.1:1".into()),
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
        virtual_hosted_style: false,
    };
    let err = match Uni::open("s3://bucket/p").cloud_storage(cfg).build().await {
        Ok(_) => panic!("opened with a mismatched bucket"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains("names bucket 'bucket'") && err.contains("names bucket 'other'"),
        "{err}"
    );
}

#[tokio::test]
async fn an_unsupported_scheme_fails_the_open() {
    for uri in ["memory://x", "ftp://host/x"] {
        let err = match Uni::open(uri).build().await {
            Ok(_) => panic!("{uri} opened"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("Unsupported"), "{uri}: {err}");
    }
}

#[tokio::test]
async fn deprecated_remote_storage_still_configures_hybrid_mode() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = CloudStorageConfig::S3 {
        bucket: "bucket".into(),
        region: Some("us-east-1".into()),
        endpoint: Some("http://127.0.0.1:1".into()),
        access_key_id: Some("test".into()),
        secret_access_key: Some("test".into()),
        session_token: None,
        virtual_hosted_style: false,
    };
    #[allow(deprecated)]
    let builder = Uni::open(dir.path().to_str().unwrap()).remote_storage("s3://bucket/p", cfg);
    assert_used_endpoint(builder.config(fail_fast()).build().await, "127.0.0.1:1");
}
