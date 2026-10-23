// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `restore_snapshot` puts the database back to the snapshot's state.
//!
//! After a restore every read — in the same handle and after a reopen — sees
//! the snapshot's vertices, edges and property values and nothing written
//! after it, whether those later writes were flushed to storage or still sat
//! in L0 (and the WAL) when the restore ran. Writes made after the restore
//! land on top of the restored state and survive a reopen.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(snapshot_restore_rewinds)'

// Rust guideline compliant

use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

/// The sorted `(name, age)` of every `Person`.
async fn people(db: &Uni) -> Result<Vec<(String, Option<i64>)>> {
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name")
        .await?;
    rows.rows()
        .iter()
        .map(|r| {
            let age = match r.value("age") {
                Some(Value::Int(i)) => Some(*i),
                _ => None,
            };
            Ok((r.get::<String>("name")?, age))
        })
        .collect()
}

/// The sorted `src->dst` names of every `KNOWS` edge, read by traversal.
async fn knows(db: &Uni) -> Result<Vec<String>> {
    let rows = db
        .session()
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             RETURN a.name + '->' + b.name AS e ORDER BY e",
        )
        .await?;
    rows.rows()
        .iter()
        .map(|r| Ok(r.get::<String>("e")?))
        .collect()
}

/// A single integer the query returns.
async fn int(db: &Uni, cypher: &str) -> Result<i64> {
    let rows = db.session().query(cypher).await?;
    Ok(rows.rows()[0].get::<i64>("c")?)
}

/// Every read the snapshot state must answer, asserted together.
async fn assert_snapshot_state(db: &Uni, when: &str) -> Result<()> {
    assert_eq!(
        people(db).await?,
        vec![
            ("Alice".to_string(), Some(30)),
            ("Bob".to_string(), Some(40)),
            ("Carol".to_string(), Some(50)),
        ],
        "{when}: vertices and property values"
    );
    assert_eq!(
        knows(db).await?,
        vec!["Alice->Bob".to_string(), "Bob->Carol".to_string()],
        "{when}: edges by traversal"
    );
    assert_eq!(
        int(db, "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c").await?,
        2,
        "{when}: edge count"
    );
    assert_eq!(
        int(db, "MATCH (n) RETURN count(n) AS c").await?,
        3,
        "{when}: vertex count"
    );
    assert_eq!(
        int(
            db,
            "MATCH (:Person {name: 'Bob'})<-[:KNOWS]-(a) RETURN count(a) AS c"
        )
        .await?,
        1,
        "{when}: incoming edges"
    );
    assert_eq!(
        int(db, "MATCH (:Person)-[r:KNOWS]->(:VIP) RETURN count(r) AS c").await?,
        1,
        "{when}: target label of a relabelled vertex"
    );
    assert_eq!(
        int(
            db,
            "MATCH (:Person {name: 'Bob'})-[:KNOWS]->(v) WHERE v:VIP RETURN count(v) AS c"
        )
        .await?,
        1,
        "{when}: label predicate on a traversal target"
    );
    assert_eq!(
        int(
            db,
            "MATCH (:Person {name: 'Bob'})-[:KNOWS*1..2]->(v:VIP) RETURN count(v) AS c"
        )
        .await?,
        1,
        "{when}: target label of a variable-length traversal"
    );
    Ok(())
}

/// The snapshot holds Alice(30)->Bob(40)->Carol(50). Afterwards: flushed
/// writes delete Bob, update Alice, add Dave and two edges; unflushed writes
/// update Carol, add Eve and one edge.
async fn diverged_store(path: &str) -> Result<(Uni, String)> {
    diverged_store_with(path, UniConfig::default()).await
}

/// [`diverged_store`] opened with `config`.
async fn diverged_store_with(path: &str, config: UniConfig) -> Result<(Uni, String)> {
    let db = Uni::open(path).config(config).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int64)
        .label("VIP")
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE CONSTRAINT person_name ON (p:Person) ASSERT p.name IS UNIQUE")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'Alice', age: 30})-[:KNOWS]->\
         (b:Person {name: 'Bob', age: 40})-[:KNOWS]->(c:Person:VIP {name: 'Carol', age: 50})",
    )
    .await?;
    tx.commit().await?;
    let snapshot = db.create_snapshot("three").await?;
    assert_snapshot_state(&db, "before divergence").await?;

    // Flushed after the snapshot.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (b:Person {name: 'Bob'}) DETACH DELETE b")
        .await?;
    tx.execute("MATCH (a:Person {name: 'Alice'}) SET a.age = 31")
        .await?;
    tx.execute(
        "MATCH (a:Person {name: 'Alice'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(c), (:Person {name: 'Dave', age: 60})-[:KNOWS]->(a)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // Unflushed (L0 + WAL) after the snapshot.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (c:Person {name: 'Carol'}) SET c.age = 99")
        .await?;
    tx.execute(
        "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS]->(:Person {name: 'Eve', age: 70})",
    )
    .await?;
    // Relabelled, so a traversal's target-label check that kept Carol's
    // later labels would drop `Bob->Carol` from a `:VIP` match after the
    // restore.
    tx.execute("MATCH (c:Person {name: 'Carol'}) REMOVE c:VIP")
        .await?;
    tx.commit().await?;

    // The divergence is real before the restore.
    assert_eq!(
        knows(&db).await?,
        vec![
            "Alice->Carol".to_string(),
            "Carol->Eve".to_string(),
            "Dave->Alice".to_string()
        ]
    );
    assert_eq!(
        int(
            &db,
            "MATCH (:Person)-[r:KNOWS]->(:VIP) RETURN count(r) AS c"
        )
        .await?,
        0
    );
    Ok((db, snapshot))
}

/// After a restore, the store answers with the snapshot's state, in the same
/// handle and after a reopen, and later writes build on it.
async fn restore_rewinds(via_procedure: bool) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let (db, snapshot) = diverged_store(path).await?;

    if via_procedure {
        let rows = db
            .session()
            .run(&format!("CALL uni.admin.snapshot.restore('{snapshot}')"))
            .await?;
        assert_eq!(
            rows.rows()[0].value("status"),
            Some(&Value::String("Restored".into()))
        );
    } else {
        db.restore_snapshot(&snapshot).await?;
    }
    assert_snapshot_state(&db, "same handle after restore").await?;

    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    assert_snapshot_state(&db, "after reopen").await?;

    // Writes after the restore build on the restored state and persist,
    // flushed or not.
    let tx = db.session().tx().await?;
    tx.execute(
        "MATCH (c:Person {name: 'Carol'}) CREATE (c)-[:KNOWS]->(:Person {name: 'Frank', age: 80})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Person {name: 'Alice'}) SET a.age = 32")
        .await?;
    tx.commit().await?;
    db.shutdown().await?;

    let db = Uni::open(path).build().await?;
    assert_eq!(
        people(&db).await?,
        vec![
            ("Alice".to_string(), Some(32)),
            ("Bob".to_string(), Some(40)),
            ("Carol".to_string(), Some(50)),
            ("Frank".to_string(), Some(80)),
        ],
        "writes after the restore"
    );
    assert_eq!(
        knows(&db).await?,
        vec![
            "Alice->Bob".to_string(),
            "Bob->Carol".to_string(),
            "Carol->Frank".to_string()
        ],
        "edges after the restore"
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_snapshot_rewinds_flushed_and_unflushed_writes() -> Result<()> {
    restore_rewinds(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_procedure_rewinds_flushed_and_unflushed_writes() -> Result<()> {
    restore_rewinds(true).await
}

/// Versions older than the compaction retention window are deleted by
/// compaction; a restorable snapshot pins its own, and compaction does not
/// fail over the pinned ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_after_compaction_past_retention() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let mut config = UniConfig::default();
    config.compaction.enabled = false;
    config.compaction.version_retention = Duration::ZERO;
    let (db, snapshot) = diverged_store_with(path, config.clone()).await?;
    db.flush().await?;
    db.session().run("CALL uni.admin.compact()").await?;

    db.restore_snapshot(&snapshot).await?;
    assert_snapshot_state(&db, "after compaction and restore").await?;
    // Compaction keeps working with the pinned versions in place. The scoped
    // entry points report a failed version cleanup, where the full pass only
    // logs it.
    db.session().run("CALL uni.admin.compact()").await?;
    db.compaction().compact("Person").await?;
    db.compaction().compact("KNOWS").await?;
    assert_snapshot_state(&db, "compacted after restore").await?;
    db.shutdown().await?;

    let db = Uni::open(path).config(config).build().await?;
    assert_snapshot_state(&db, "reopened").await?;
    db.shutdown().await?;
    Ok(())
}

/// Run `cypher` in its own committed transaction.
async fn exec(db: &Uni, cypher: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

/// The declared property names of `label`, sorted.
async fn property_names(db: &Uni, label: &str) -> Result<Vec<String>> {
    let info = db
        .get_label_info(label)
        .await?
        .ok_or_else(|| anyhow::anyhow!("label {label} is not in the catalog"))?;
    let mut names: Vec<String> = info.properties.into_iter().map(|p| p.name).collect();
    names.sort();
    Ok(names)
}

/// The catalog the snapshot of [`catalog_diverged_store`] holds, asserted
/// whole: the snapshot's data, its labels and edge types, `Person`'s
/// properties under their original names, its indexes and constraint, and
/// `Note.rank` as an `INT` that accepts and returns integers.
async fn assert_snapshot_catalog(
    db: &Uni,
    indexes: &[uni_db::core::schema::IndexDefinition],
    when: &str,
) -> Result<()> {
    assert_snapshot_state(db, when).await?;
    assert!(
        !db.label_exists("Pet").await?,
        "{when}: a later label is gone"
    );
    assert!(
        !db.edge_type_exists("OWNS").await?,
        "{when}: a later edge type is gone"
    );
    assert_eq!(
        property_names(db, "Person").await?,
        vec!["age".to_string(), "name".to_string()],
        "{when}: a later property is gone and a renamed one has its old name"
    );
    assert_eq!(
        db.indexes().list(Some("Person")),
        indexes,
        "{when}: the snapshot's index definitions"
    );
    let tx = db.session().tx().await?;
    let dup = tx.execute("CREATE (:Person {name: 'Bob'})").await;
    assert!(
        dup.is_err(),
        "{when}: the UNIQUE constraint dropped after the snapshot is back"
    );
    tx.rollback();
    assert_eq!(
        int(db, "MATCH (n:Note) RETURN count(n) AS c").await?,
        0,
        "{when}: the Note written after the snapshot is gone"
    );
    let info = db.get_label_info("Note").await?.expect("Note");
    let rank = info
        .properties
        .iter()
        .find(|p| p.name == "rank")
        .expect("rank");
    assert_eq!(
        rank.data_type,
        DataType::Int64.type_spec(),
        "{when}: the property retyped after the snapshot has its old type"
    );
    Ok(())
}

/// [`diverged_store`], plus catalog changes after the snapshot: a declared
/// and written property, a full-text index, a new label and edge type with
/// rows, a property rename, a dropped UNIQUE constraint, and `Note.rank`
/// (declared `INT` and never written at the snapshot) redeclared `STRING`
/// and written. Returns the store, the snapshot and its `Person` indexes.
async fn catalog_diverged_store(
    path: &str,
) -> Result<(Uni, String, Vec<uni_db::core::schema::IndexDefinition>)> {
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("Note")
        .property_nullable("rank", DataType::Int64)
        .apply()
        .await?;
    db.shutdown().await?;
    let (db, snapshot) = diverged_store(path).await?;
    let indexes = db.indexes().list(Some("Person"));

    db.schema()
        .label("Person")
        .property_nullable("email", DataType::String)
        .label("Pet")
        .property("age", DataType::String)
        .edge_type("OWNS", &["Person"], &["Pet"])
        .apply()
        .await?;
    exec(&db, "MATCH (a:Person {name: 'Alice'}) SET a.email = 'a@x'").await?;
    exec(
        &db,
        "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[:OWNS]->(:Pet {age: 'old'})",
    )
    .await?;
    exec(
        &db,
        "CREATE FULLTEXT INDEX person_fts FOR (p:Person) ON EACH [p.name]",
    )
    .await?;
    exec(&db, "ALTER LABEL Person RENAME PROPERTY age TO years").await?;
    exec(&db, "DROP CONSTRAINT person_name").await?;
    exec(&db, "ALTER LABEL Note DROP PROPERTY rank").await?;
    exec(&db, "ALTER LABEL Note ADD PROPERTY rank STRING").await?;
    exec(&db, "CREATE (:Note {rank: 'high'})").await?;
    db.flush().await?;

    // The divergence is real before the restore.
    assert!(db.label_exists("Pet").await?);
    assert!(db.edge_type_exists("OWNS").await?);
    assert_eq!(
        property_names(&db, "Person").await?,
        vec!["email".to_string(), "name".to_string(), "years".to_string()]
    );
    assert_ne!(db.indexes().list(Some("Person")), indexes);
    exec(&db, "CREATE (:Person {name: 'Bob'})").await?;
    exec(
        &db,
        "MATCH (p:Person {name: 'Bob'}) WHERE p.years IS NULL DETACH DELETE p",
    )
    .await?;
    Ok((db, snapshot, indexes))
}

/// The catalog is rewound with the data: everything declared, dropped,
/// renamed or retyped after the snapshot is as it was, in the same handle and
/// after a reopen, and the snapshot's declarations accept writes again.
async fn restore_rewinds_catalog(via_procedure: bool) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let (db, snapshot, indexes) = catalog_diverged_store(path).await?;

    if via_procedure {
        db.session()
            .run(&format!("CALL uni.admin.snapshot.restore('{snapshot}')"))
            .await?;
    } else {
        db.restore_snapshot(&snapshot).await?;
    }
    assert_snapshot_catalog(&db, &indexes, "same handle after restore").await?;
    db.shutdown().await?;

    let db = Uni::open(path).build().await?;
    assert_snapshot_catalog(&db, &indexes, "after reopen").await?;

    // The snapshot's `INT` rank takes integers, flushed and read back as
    // integers; a dropped label can be declared again with other types.
    exec(&db, "CREATE (:Note {rank: 3})").await?;
    db.schema()
        .label("Pet")
        .property("age", DataType::Int64)
        .apply()
        .await?;
    exec(&db, "CREATE (:Pet {age: 4})").await?;
    db.flush().await?;
    db.shutdown().await?;

    let db = Uni::open(path).build().await?;
    let rows = db
        .session()
        .query("MATCH (n:Note) RETURN n.rank AS r")
        .await?;
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(rows.rows()[0].value("r"), Some(&Value::Int(3)));
    let rows = db
        .session()
        .query("MATCH (p:Pet) RETURN p.age AS a")
        .await?;
    assert_eq!(rows.rows().len(), 1);
    assert_eq!(rows.rows()[0].value("a"), Some(&Value::Int(4)));
    assert_eq!(
        property_names(&db, "Person").await?,
        vec!["age".to_string(), "name".to_string()]
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_rewinds_the_catalog() -> Result<()> {
    restore_rewinds_catalog(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_procedure_rewinds_the_catalog() -> Result<()> {
    restore_rewinds_catalog(true).await
}

/// A restore point recorded before restore points held the catalog cannot
/// restore the catalog, so its snapshot is refused rather than restored
/// with today's catalog over yesterday's tables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_refuses_a_restore_point_without_a_catalog() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let (db, snapshot) = diverged_store(path).await?;
    let file = find_file(dir.path(), &format!("{snapshot}.json"), "restore_points")
        .expect("the snapshot's restore point");
    let mut point: serde_json::Value = serde_json::from_slice(&std::fs::read(&file)?)?;
    assert!(
        point["catalog"].is_object(),
        "a new restore point holds the catalog"
    );
    point.as_object_mut().unwrap().remove("catalog");
    std::fs::write(&file, serde_json::to_vec(&point)?)?;

    let diverged = knows(&db).await?;
    let err = db.restore_snapshot(&snapshot).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("before restore points included the catalog"),
        "{err}"
    );
    assert_eq!(knows(&db).await?, diverged, "the refusal changes nothing");
    // It can still be deleted, which releases its pins.
    db.delete_snapshot(&snapshot).await?;
    assert!(find_file(dir.path(), &format!("{snapshot}.json"), "restore_points").is_none());
    db.shutdown().await?;
    Ok(())
}

/// The first file named `name` under a directory named `parent`, searching
/// `root` recursively.
fn find_file(root: &std::path::Path, name: &str, parent: &str) -> Option<std::path::PathBuf> {
    files_under(root).into_iter().find(|p| {
        p.file_name().is_some_and(|n| n == name)
            && p.parent()
                .and_then(|d| d.file_name())
                .is_some_and(|d| d == parent)
    })
}

/// Every file under `root`, recursively.
fn files_under(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

/// `delete_snapshot` and `uni.admin.snapshot.delete`.
mod deletion {
    use super::*;
    use uni_db::UniError;

    /// How many Lance tag files pin `snapshot`'s versions.
    fn tags_of(root: &std::path::Path, snapshot: &str) -> usize {
        let name = format!("uni_snapshot_{snapshot}.json");
        files_under(root)
            .iter()
            .filter(|p| p.file_name().is_some_and(|n| n == name.as_str()))
            .count()
    }

    /// Whether the manifest of `version` of the dataset at `dataset` exists,
    /// under either of Lance's manifest naming schemes.
    fn version_exists(dataset: &std::path::Path, version: u64) -> bool {
        let dir = dataset.join("_versions");
        dir.join(format!("{version}.manifest")).exists()
            || dir
                .join(format!("{:020}.manifest", u64::MAX - version))
                .exists()
    }

    /// The directory of the Lance dataset of `table`.
    fn dataset_dir(root: &std::path::Path, table: &str) -> std::path::PathBuf {
        let name = format!("{table}.lance");
        files_under(root)
            .into_iter()
            .filter_map(|p| {
                p.ancestors()
                    .find(|a| a.file_name().is_some_and(|n| n == name.as_str()))
                    .map(std::path::Path::to_path_buf)
            })
            .next()
            .unwrap_or_else(|| panic!("no dataset for {table}"))
    }

    /// The table versions `snapshot`'s restore point records.
    fn recorded_versions(root: &std::path::Path, snapshot: &str) -> Vec<(String, u64)> {
        let file =
            find_file(root, &format!("{snapshot}.json"), "restore_points").expect("restore point");
        let point: serde_json::Value =
            serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
        point["tables"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(t, v)| (t.clone(), v.as_u64().unwrap()))
            .collect()
    }

    /// Deleting a snapshot removes its tags, so compaction's cleanup
    /// reclaims the versions only it pinned, while another snapshot keeps its
    /// own and still restores. The deleted one is gone from every surface.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn deleted_snapshot_versions_are_reclaimed_and_others_still_restore() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let path = root.to_str().unwrap();
        let mut config = UniConfig::default();
        config.compaction.enabled = false;
        config.compaction.version_retention = Duration::ZERO;
        let (db, first) = diverged_store_with(path, config.clone()).await?;
        db.flush().await?;
        let second = db.create_snapshot("diverged").await?;
        let second_state = (people(&db).await?, knows(&db).await?);
        exec(&db, "CREATE (:Person {name: 'Zed', age: 1})").await?;
        db.flush().await?;

        // Versions only the first snapshot pins: recorded by it, not by the
        // second, and not the table's current version.
        let pinned_by_first: Vec<(std::path::PathBuf, u64)> = {
            let second_versions = recorded_versions(root, &second);
            recorded_versions(root, &first)
                .into_iter()
                .filter(|(t, v)| !second_versions.contains(&(t.clone(), *v)))
                .map(|(t, v)| (dataset_dir(root, &t), v))
                .collect()
        };
        assert!(
            !pinned_by_first.is_empty(),
            "the snapshots differ in some table"
        );
        assert!(tags_of(root, &first) > 0);
        let second_tags = tags_of(root, &second);
        assert!(second_tags > 0);

        // Pinned, compaction keeps them.
        db.session().run("CALL uni.admin.compact()").await?;
        for (ds, v) in &pinned_by_first {
            assert!(version_exists(ds, *v), "{} v{v} is pinned", ds.display());
        }

        db.delete_snapshot(&first).await?;
        assert_eq!(tags_of(root, &first), 0, "every tag of the first is gone");
        assert_eq!(
            tags_of(root, &second),
            second_tags,
            "the second keeps its tags"
        );
        db.session().run("CALL uni.admin.compact()").await?;
        for (ds, v) in &pinned_by_first {
            assert!(
                !version_exists(ds, *v),
                "{} v{v} is reclaimed once unpinned",
                ds.display()
            );
        }

        // Gone from every surface.
        assert!(
            db.list_snapshots()
                .await?
                .iter()
                .all(|m| m.snapshot_id != first)
        );
        assert!(matches!(
            db.restore_snapshot(&first).await,
            Err(UniError::SnapshotNotFound { snapshot_id }) if snapshot_id == first
        ));
        assert!(matches!(
            db.delete_snapshot(&first).await,
            Err(UniError::SnapshotNotFound { snapshot_id }) if snapshot_id == first
        ));

        // The other still restores, now and after a reopen.
        db.restore_snapshot(&second).await?;
        assert_eq!((people(&db).await?, knows(&db).await?), second_state);
        db.shutdown().await?;
        let db = Uni::open(path).config(config).build().await?;
        assert_eq!((people(&db).await?, knows(&db).await?), second_state);
        db.shutdown().await?;
        Ok(())
    }

    /// The procedure deletes, reports `Deleted`, and keeps the typed
    /// not-found error; it refuses inside a transaction and on a read-only
    /// session. Deleting the snapshot `latest` names keeps the store
    /// openable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delete_procedure_and_refusals() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap();
        let (db, snapshot) = diverged_store(path).await?;
        let latest = db.create_snapshot("latest").await?;

        let err = db
            .session()
            .query(&format!("CALL uni.admin.snapshot.delete('{latest}')"))
            .await
            .unwrap_err();
        assert!(
            db.list_snapshots()
                .await?
                .iter()
                .any(|m| m.snapshot_id == latest),
            "a read-only query must not delete: {err}"
        );
        let tx = db.session().tx().await?;
        let err = tx
            .execute(&format!("CALL uni.admin.snapshot.delete('{latest}')"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("inside a transaction"), "{err}");
        tx.rollback();

        let rows = db
            .session()
            .run(&format!("CALL uni.admin.snapshot.delete('{latest}')"))
            .await?;
        assert_eq!(
            rows.rows()[0].value("status"),
            Some(&Value::String("Deleted".into()))
        );
        let err = db
            .session()
            .run("CALL uni.admin.snapshot.delete('no-such-snapshot')")
            .await
            .unwrap_err();
        assert!(
            matches!(&err, UniError::SnapshotNotFound { snapshot_id } if snapshot_id == "no-such-snapshot"),
            "{err:?}"
        );
        assert!(
            db.list_snapshots()
                .await?
                .iter()
                .all(|m| m.snapshot_id != latest),
            "deleted"
        );
        db.shutdown().await?;

        // `latest` named the deleted manifest; the store still opens with
        // its data, and the remaining snapshot restores.
        let db = Uni::open(path).build().await?;
        assert_eq!(people(&db).await?.len(), 4, "Alice, Carol, Dave, Eve");
        db.restore_snapshot(&snapshot).await?;
        assert_snapshot_state(&db, "restored after deleting latest").await?;
        db.shutdown().await?;
        Ok(())
    }
}

/// What a restore refuses, leaving the store untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_refusals() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let (db, snapshot) = diverged_store(path).await?;
    let diverged = knows(&db).await?;

    // A manifest an ordinary flush wrote has no restore point.
    db.flush().await?;
    let flushed = db
        .list_snapshots()
        .await?
        .into_iter()
        .max_by_key(|m| m.created_at)
        .map(|m| m.snapshot_id)
        .unwrap();
    let err = db.restore_snapshot(&flushed).await.unwrap_err();
    assert!(err.to_string().contains("no restore point"), "{err}");

    // An open transaction blocks it.
    let tx = db.session().tx().await?;
    let err = db.restore_snapshot(&snapshot).await.unwrap_err();
    assert!(err.to_string().contains("transactions are open"), "{err}");
    tx.rollback();

    assert_eq!(knows(&db).await?, diverged, "refusals change nothing");
    db.restore_snapshot(&snapshot).await?;
    assert_snapshot_state(&db, "restore after the refusals").await?;
    db.shutdown().await?;
    Ok(())
}

/// A restore refuses while the database has forks, since it rewinds primary
/// storage and the shared UID index but not fork branches.
mod forks {
    use super::*;
    use uni_db::UniError;

    /// The `forks` of a [`UniError::RestoreBlockedByForks`], or a panic.
    fn blocked_forks(err: &UniError) -> Vec<String> {
        match err {
            UniError::RestoreBlockedByForks { forks } => forks.clone(),
            other => panic!("expected RestoreBlockedByForks, got {other:?}: {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn restore_refused_while_forks_exist() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap();
        let config = UniConfig {
            disable_fork_sweeper: true,
            ..UniConfig::default()
        };
        let (db, snapshot) = diverged_store_with(path, config).await?;
        let diverged = knows(&db).await?;

        // A fork and a nested child, with no session left open on either.
        {
            let outer = db.session().fork("outer").await?;
            let inner = outer.fork("inner").await?;
            inner
                .tx()
                .await?
                .execute("CREATE (:Person {name: 'Forky', age: 1})")
                .await?;
        }
        let expected = vec!["inner".to_string(), "outer".to_string()];

        let err = db.restore_snapshot(&snapshot).await.unwrap_err();
        assert_eq!(blocked_forks(&err), expected, "{err}");
        assert!(err.to_string().contains("drop_fork_cascade"), "{err}");

        // The procedure refuses with the same typed error.
        let err = db
            .session()
            .run(&format!("CALL uni.admin.snapshot.restore('{snapshot}')"))
            .await
            .unwrap_err();
        assert_eq!(blocked_forks(&err), expected, "{err}");

        assert_eq!(knows(&db).await?, diverged, "refusals change nothing");

        // With the forks dropped, the restore runs.
        db.drop_fork_cascade("outer").await?;
        db.restore_snapshot(&snapshot).await?;
        assert_snapshot_state(&db, "restore after dropping the forks").await?;
        db.shutdown().await?;
        Ok(())
    }
}

/// Crash and race seams of the restore, driven through fail-rs. Named so
/// the failpoint lane's `resilience` filter selects them.
#[cfg(all(unix, feature = "failpoints"))]
mod resilience {
    use super::*;
    use uni_db::UniError;

    /// The child half of [`restore_interrupted_by_a_crash_completes_on_reopen`].
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "internal: child process entry point for the abort harness"]
    async fn restore_abort_child() {
        let Some((scenario, path)) = crate::crash_harness::child_env() else {
            return;
        };
        assert_eq!(scenario, "mid-rewind");
        let uri = path.to_string_lossy().into_owned();
        let (db, snapshot) = diverged_store(&uri).await.unwrap();
        crate::crash_harness::abort_at("restore::after-table");
        let _ = db.restore_snapshot(&snapshot).await;
        panic!("the restore returned; the seam was never reached");
    }

    /// A restore that crashes with some tables rewound and others not is
    /// completed when the store is next opened.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restore_interrupted_by_a_crash_completes_on_reopen() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let uri = dir.path().join("db");
        crate::crash_harness::run_child_async(
            "storage::snapshot_restore_rewinds::resilience::restore_abort_child",
            "mid-rewind",
            &uri,
        )
        .await;
        let db = Uni::open(uri.to_string_lossy().as_ref()).build().await?;
        assert_snapshot_state(&db, "reopened after the crash").await?;
        db.shutdown().await?;
        Ok(())
    }

    /// Holds the first thread to reach `seam` there until released; later
    /// ones pass. Returns (reached, release).
    fn hold_first_at(
        seam: &'static str,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let reached_tx = std::sync::Mutex::new(Some(reached_tx));
        let go_rx = std::sync::Mutex::new(go_rx);
        fail::cfg_callback(seam, move || {
            let first = reached_tx.lock().unwrap().take();
            if let Some(tx) = first {
                let _ = tx.send(());
                let _ = go_rx.lock().unwrap().recv();
            }
        })
        .unwrap();
        (reached_rx, go_tx)
    }

    /// A background index build under way when a restore starts finishes
    /// before the restore rewinds anything, and the restore then returns the
    /// catalog to the snapshot's. Without the pause, the build (holding a
    /// committed artifact over post-snapshot rows) recorded it after the
    /// rewind: the index declared after the snapshot came back `Online`
    /// over a table rolled back to before it, and the snapshot's index
    /// claimed a build over rows the restore had removed.
    ///
    /// fail-rs's registry is process-global; nextest runs each test in its
    /// own process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn index_build_racing_a_restore_cannot_outlive_the_rewind() -> Result<()> {
        const SEAM: &str = "index_manager::scalar-built";
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap();
        let mut config = UniConfig::default();
        config.index_rebuild.worker_check_interval = Duration::from_millis(50);
        let db = std::sync::Arc::new(Uni::open(path).config(config.clone()).build().await?);
        db.schema()
            .label("Person")
            .property("name", DataType::String)
            .property_nullable("age", DataType::Int64)
            .apply()
            .await?;
        exec(
            &db,
            "CREATE (:Person {name: 'Alice', age: 30}), (:Person {name: 'Bob', age: 40})",
        )
        .await?;
        db.flush().await?;
        exec(&db, "CREATE INDEX person_name FOR (p:Person) ON (p.name)").await?;
        let snapshot = db.create_snapshot("two").await?;
        let at_snapshot = db.indexes().list(Some("Person"));
        assert_eq!(at_snapshot.len(), 1);

        exec(
            &db,
            "CREATE (:Person {name: 'Carol', age: 50}), (:Person {name: 'Dave', age: 60})",
        )
        .await?;
        db.flush().await?;
        exec(&db, "CREATE INDEX person_age FOR (p:Person) ON (p.age)").await?;
        assert_eq!(db.indexes().list(Some("Person")).len(), 2);

        // The worker rebuilds both; the first physical build is held after
        // its artifact is committed and before the catalog records it.
        let (reached, go) = hold_first_at(SEAM);
        db.indexes().rebuild("Person", true).await?;
        tokio::task::spawn_blocking(move || reached.recv_timeout(Duration::from_secs(30)))
            .await??;

        let restorer = db.clone();
        let snap = snapshot.clone();
        let restore = tokio::spawn(async move { restorer.restore_snapshot(&snap).await });
        tokio::time::sleep(Duration::from_millis(500)).await;
        let waited = !restore.is_finished();
        go.send(())?;
        restore.await??;
        fail::remove(SEAM);
        assert!(waited, "the restore must wait for the build under way");

        // Let the worker go idle.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let busy = db.indexes().rebuild_status().await?.iter().any(|t| {
                matches!(
                    t.status,
                    uni_store::storage::IndexRebuildStatus::Pending
                        | uni_store::storage::IndexRebuildStatus::InProgress
                )
            });
            if !busy {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "rebuild never finished"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(
            db.indexes().list(Some("Person")),
            at_snapshot,
            "the snapshot's index definitions, build records included"
        );
        assert_eq!(
            int(
                &db,
                "MATCH (p:Person) WHERE p.name = 'Bob' RETURN count(p) AS c"
            )
            .await?,
            1
        );
        assert_eq!(int(&db, "MATCH (p:Person) RETURN count(p) AS c").await?, 2);
        let db = std::sync::Arc::try_unwrap(db).map_err(|_| anyhow::anyhow!("shared"))?;
        db.shutdown().await?;
        let db = Uni::open(path).config(config).build().await?;
        assert_eq!(
            db.indexes().list(Some("Person")),
            at_snapshot,
            "after reopen"
        );
        db.shutdown().await?;
        Ok(())
    }

    /// A snapshot deletion refuses while a restore runs, of that snapshot or
    /// another; once the restore is done it deletes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delete_refused_while_a_restore_runs() -> Result<()> {
        const SEAM: &str = "restore::after-table";
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap();
        let (db, first) = diverged_store(path).await?;
        let second = db.create_snapshot("second").await?;
        let db = std::sync::Arc::new(db);

        let (reached, go) = hold_first_at(SEAM);
        let restorer = db.clone();
        let snap = first.clone();
        let restore = tokio::spawn(async move { restorer.restore_snapshot(&snap).await });
        tokio::task::spawn_blocking(move || reached.recv_timeout(Duration::from_secs(30)))
            .await??;
        for id in [&first, &second] {
            // Refused at once, not queued behind the restore's locks.
            let err = tokio::time::timeout(Duration::from_secs(5), db.delete_snapshot(id))
                .await
                .expect("the deletion must refuse, not wait for the restore")
                .unwrap_err();
            assert!(err.to_string().contains("restore is in progress"), "{err}");
        }
        go.send(())?;
        restore.await??;
        fail::remove(SEAM);

        assert_snapshot_state(&db, "after the restore").await?;
        db.delete_snapshot(&second).await?;
        assert!(
            db.list_snapshots()
                .await?
                .iter()
                .all(|m| m.snapshot_id != second)
        );
        db.restore_snapshot(&first).await?;
        assert_snapshot_state(&db, "the remaining snapshot restores").await?;
        Ok(())
    }

    /// A catalog save that serialized the catalog before a restore swapped
    /// it, and reaches the store only after the restore saved the snapshot's
    /// catalog, must not leave the pre-restore catalog on disk. Without
    /// ordering between saves the late write won: the handle served the
    /// snapshot's catalog while the file held the pre-restore one, so a
    /// reopen brought back every label, property and index declared after
    /// the snapshot over storage rewound to before them.
    ///
    /// The save under test is a schema DDL's (`SchemaBuilder::apply`), held
    /// between serializing and writing. It runs on a named thread so the
    /// seam holds it and nothing else (background index builds save the
    /// catalog too). The restore gets up to 3 s to finish while the save is
    /// held: an unordered restore on this store finishes well inside that,
    /// while an ordered one waits for the held save and the window simply
    /// elapses.
    ///
    /// fail-rs's registry is process-global; nextest runs each test in its
    /// own process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn catalog_save_racing_a_restore_cannot_resurrect_the_old_catalog() -> Result<()> {
        const SEAM: &str = "schema::save::before-put";
        const DDL_THREAD: &str = "racing-ddl";
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap();
        let (db, snapshot, indexes) = catalog_diverged_store(path).await?;
        let db = std::sync::Arc::new(db);

        let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let reached_tx = std::sync::Mutex::new(Some(reached_tx));
        let go_rx = std::sync::Mutex::new(go_rx);
        fail::cfg_callback(SEAM, move || {
            if std::thread::current().name() != Some(DDL_THREAD) {
                return;
            }
            if let Some(tx) = reached_tx.lock().unwrap().take() {
                let _ = tx.send(());
                let _ = go_rx.lock().unwrap().recv();
            }
        })
        .unwrap();

        let ddl_db = db.clone();
        let runtime = tokio::runtime::Handle::current();
        let ddl = std::thread::Builder::new()
            .name(DDL_THREAD.to_string())
            .spawn(move || runtime.block_on(ddl_db.schema().label("Racer").apply()))?;
        tokio::task::spawn_blocking(move || reached_rx.recv_timeout(Duration::from_secs(30)))
            .await??;

        let restorer = db.clone();
        let snap = snapshot.clone();
        let restore = tokio::spawn(async move { restorer.restore_snapshot(&snap).await });
        let window = std::time::Instant::now() + Duration::from_secs(3);
        while !restore.is_finished() && std::time::Instant::now() < window {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        go_tx.send(())?;
        restore.await??;
        tokio::task::spawn_blocking(move || ddl.join())
            .await?
            .map_err(|_| anyhow::anyhow!("the DDL thread panicked"))??;
        fail::remove(SEAM);

        assert_snapshot_catalog(&db, &indexes, "same handle after the race").await?;
        let racer = db.label_exists("Racer").await?;
        let db = std::sync::Arc::try_unwrap(db).map_err(|_| anyhow::anyhow!("shared"))?;
        db.shutdown().await?;

        let db = Uni::open(path).build().await?;
        assert_snapshot_catalog(&db, &indexes, "reopened after the race").await?;
        assert_eq!(
            db.label_exists("Racer").await?,
            racer,
            "the reopened catalog is the one the handle served"
        );
        db.shutdown().await?;
        Ok(())
    }

    /// A fork whose fork point was captured before a restore, but which
    /// registered only after the restore checked the registry, is rolled
    /// back instead of being created from pre-restore dataset versions.
    ///
    /// fail-rs's registry is process-global; nextest runs each test in its
    /// own process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fork_created_across_a_restore_is_rolled_back() -> Result<()> {
        const SEAM: &str = "fork::create-before-begin";
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap();
        let config = UniConfig {
            disable_fork_sweeper: true,
            ..UniConfig::default()
        };
        let (db, snapshot) = diverged_store_with(path, config).await?;
        let db = std::sync::Arc::new(db);

        // Hold the fork at the seam until the restore has finished.
        let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let reached_tx = std::sync::Mutex::new(reached_tx);
        let go_rx = std::sync::Mutex::new(go_rx);
        fail::cfg_callback(SEAM, move || {
            let _ = reached_tx.lock().unwrap().send(());
            let _ = go_rx.lock().unwrap().recv();
        })
        .unwrap();
        let forker = db.clone();
        let fork = tokio::spawn(async move { forker.session().fork("racer").await.map(drop) });
        tokio::task::spawn_blocking(move || reached_rx.recv()).await??;

        db.restore_snapshot(&snapshot).await?;
        go_tx.send(())?;
        let outcome = fork.await?;
        fail::remove(SEAM);

        let err = outcome.expect_err("a fork cut before the restore must not be created");
        assert!(
            matches!(
                err,
                UniError::ForkLifecycle {
                    stage: "restore_race",
                    ..
                }
            ),
            "{err:?}: {err}"
        );
        assert!(
            matches!(
                db.fork_info("racer").await,
                Err(UniError::ForkNotFound { .. })
            ),
            "the rolled-back fork must not stay registered"
        );
        assert_snapshot_state(&db, "after the racing fork").await?;

        // A retry forks the restored state.
        let racer = db.session().fork("racer").await?;
        let rows = racer.query("MATCH (p:Person) RETURN count(p) AS c").await?;
        assert_eq!(
            rows.rows()[0].get::<i64>("c")?,
            3,
            "the fork sees the restored three people"
        );
        Ok(())
    }
}
