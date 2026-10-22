// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An older binary writing after an enforcing one voids the catalog's
//! endpoint-conformance records.
//!
//! The planner narrows an unlabelled endpoint of `()-[r:R]->()` to `R`'s
//! declared labels when `R` is `enforced` or `verified`. An older binary does
//! not check endpoints, so if it writes after those records were made it can
//! store an edge the narrowed plan silently drops. Every enforcing binary now
//! stamps its writer features on each catalog save, snapshot manifest and WAL
//! segment; an open that finds a replayed WAL segment, or a latest manifest,
//! without them downgrades every record to `unverified` and saves that.
//!
//! The older binary is simulated by writing exactly what it writes: a WAL
//! segment without the features (`WriteAheadLog::without_writer_features`),
//! or a flush's manifest without them (a copy of the latest manifest with a
//! new id, stripped of the field, made `latest`).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(endpoint_conformance_writer_features)'

// Rust guideline compliant

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::EndpointConformance;
use uni_db::{DataType, Uni, UniConfig, Value};
use uni_store::runtime::wal::{Mutation, WriteAheadLog};

use super::declared_endpoint_violation_reads::ints;

/// Opens `path` with no time-based background flush, so what is on disk
/// changes only when the test flushes or shuts down.
pub(crate) async fn open(path: &Path) -> Result<Uni> {
    let config = UniConfig {
        auto_flush_interval: None,
        ..Default::default()
    };
    Ok(Uni::open(path.to_string_lossy())
        .config(config)
        .build()
        .await?)
}

/// The conformance record of `edge_type`.
fn conformance(db: &Uni, edge_type: &str) -> Result<EndpointConformance> {
    Ok(db
        .schema()
        .current()
        .edge_types
        .get(edge_type)
        .context("edge type declared")?
        .endpoint_conformance
        .clone())
}

/// Every file under `root` named `name` whose parent directory is `parent`.
fn find(root: &Path, parent: &str, name: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().is_some_and(|n| n == name)
                && path
                    .parent()
                    .and_then(Path::file_name)
                    .is_some_and(|n| n == parent)
            {
                found.push(path);
            }
        }
    }
    found
}

/// The primary's `catalog/latest` pointer file (fork pointers live deeper,
/// under `catalog/forks/<id>/`).
fn latest_pointer(root: &Path) -> Result<PathBuf> {
    let mut pointers = find(root, "catalog", "latest");
    pointers.retain(|p| !p.to_string_lossy().contains("forks"));
    anyhow::ensure!(
        pointers.len() == 1,
        "one primary latest pointer: {pointers:?}"
    );
    Ok(pointers.remove(0))
}

/// The latest manifest's path and parsed body.
pub(crate) fn latest_manifest(root: &Path) -> Result<(PathBuf, serde_json::Value)> {
    let pointer = latest_pointer(root)?;
    let id = std::fs::read_to_string(&pointer)?.trim().to_string();
    let path = pointer
        .parent()
        .context("catalog dir")?
        .join("manifests")
        .join(format!("{id}.json"));
    let body = serde_json::from_slice(&std::fs::read(&path)?)?;
    Ok((path, body))
}

/// `true` when the latest manifest carries writer features.
fn latest_manifest_is_marked(root: &Path) -> Result<bool> {
    Ok(latest_manifest(root)?.1.get("writer_features").is_some())
}

/// Publish what an older binary's flush publishes: a new manifest cloned from
/// the latest one, without `writer_features`, made `latest`.
fn older_binary_flush(root: &Path) -> Result<()> {
    let (path, mut body) = latest_manifest(root)?;
    let parent_id = body["snapshot_id"].clone();
    let id = format!("older-{}", uuid::Uuid::new_v4());
    body["parent_snapshot"] = parent_id;
    body["snapshot_id"] = serde_json::json!(id);
    body.as_object_mut()
        .context("manifest object")?
        .remove("writer_features");
    let dir = path.parent().context("manifests dir")?;
    std::fs::write(
        dir.join(format!("{id}.json")),
        serde_json::to_vec_pretty(&body)?,
    )?;
    std::fs::write(latest_pointer(root)?, id)?;
    Ok(())
}

/// Strip `writer_features` from the latest manifest in place: the store's
/// last flush was by an older binary, before any enforcing binary opened it.
fn make_latest_manifest_legacy(root: &Path) -> Result<()> {
    let (path, mut body) = latest_manifest(root)?;
    body.as_object_mut()
        .context("manifest object")?
        .remove("writer_features");
    std::fs::write(path, serde_json::to_vec_pretty(&body)?)?;
    Ok(())
}

/// Rewrite `edge_type`'s declared source labels in `catalog/schema.json`,
/// leaving its conformance record as it is.
fn set_declared_sources(root: &Path, edge_type: &str, src: &[&str]) -> Result<()> {
    let path = find(root, "catalog", "schema.json")
        .into_iter()
        .next()
        .context("catalog/schema.json")?;
    let mut schema: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    schema["edge_types"][edge_type]["src_labels"] = serde_json::json!(src);
    std::fs::write(&path, serde_json::to_vec_pretty(&schema)?)?;
    Ok(())
}

/// `A -[R]-> F` declared, holding `(a:A {v:1})-[:R]->(f:F {v:2})` and an
/// unconnected `(s:S {v:3})`, flushed and shut down. Returns `(s, f, R id)`.
async fn enforced_store(path: &Path, sources: &[&str]) -> Result<(Vid, Vid, u32)> {
    let db = open(path).await?;
    db.schema()
        .label("A")
        .property("v", DataType::Int)
        .label("F")
        .property("v", DataType::Int)
        .label("S")
        .property("v", DataType::Int)
        .edge_type("R", sources, &["F"])
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:A {v: 1}), (f:F {v: 2}), (:S {v: 3}), (a)-[:R]->(f)")
        .await?;
    tx.commit().await?;
    assert_eq!(conformance(&db, "R")?, EndpointConformance::Enforced);
    let ids = db
        .session()
        .query("MATCH (s:S), (f:F) RETURN id(s), id(f)")
        .await?;
    let row = ids.rows().first().context("one row")?.clone();
    let vid = |i: usize| match &row.values()[i] {
        Value::Int(v) => Ok(Vid::from(u64::try_from(*v)?)),
        other => Err(anyhow::anyhow!("expected an id, got {other:?}")),
    };
    let (s, f) = (vid(0)?, vid(1)?);
    let r = db.schema().current().edge_types["R"].id;
    db.flush().await?;
    db.shutdown().await?;
    Ok((s, f, r))
}

/// Append one segment to the store's WAL holding `(s)-[:R]->(f)`, as a
/// binary with (`marked`) or without writer features writes it.
async fn append_edge_segment(path: &Path, s: Vid, f: Vid, r: u32, marked: bool) -> Result<()> {
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(path)?);
    let mut wal = WriteAheadLog::new(store, object_store::path::Path::from("wal"));
    if !marked {
        wal = wal.without_writer_features();
    }
    wal.initialize().await?;
    // Numbered above the manifest's mark, so recovery replays it.
    let mark = latest_manifest(path)?.1["wal_high_water_mark"]
        .as_u64()
        .context("wal mark")?;
    wal.ensure_lsn_above(mark)?;
    wal.append(Mutation::InsertEdge {
        src_vid: s,
        dst_vid: f,
        edge_type: r,
        eid: Eid::new(1 << 40),
        version: 1,
        properties: Default::default(),
        edge_type_name: Some("R".to_string()),
        created_at: None,
        updated_at: None,
    })?;
    wal.flush().await?;
    Ok(())
}

/// The sources of every `R` edge, by the unlabelled pattern the planner
/// narrows when `R` conforms.
const SOURCES: &str = "MATCH (x)-[:R]->(y) RETURN x.v";

/// A WAL segment written without writer features downgrades `R` on open,
/// persistently; the same segment from an enforcing binary does not.
#[tokio::test]
async fn endpoint_conformance_writer_features_unmarked_wal_segment_downgrades() -> Result<()> {
    for marked in [true, false] {
        let dir = tempfile::tempdir()?;
        let (s, f, r) = enforced_store(dir.path(), &["A"]).await?;
        append_edge_segment(dir.path(), s, f, r, marked).await?;
        let db = open(dir.path()).await?;
        if marked {
            assert_eq!(
                conformance(&db, "R")?,
                EndpointConformance::Enforced,
                "a segment carrying the features is trusted"
            );
            db.shutdown().await?;
            continue;
        }
        assert_eq!(conformance(&db, "R")?, EndpointConformance::Unverified);
        assert_eq!(
            ints(&db, SOURCES).await?,
            vec![1, 3],
            "the edge the older binary wrote is not narrowed away"
        );
        assert_eq!(
            ints(&db, "MATCH ()-[r:R]->() RETURN count(r)").await?,
            vec![2]
        );
        db.shutdown().await?;
        let db = open(dir.path()).await?;
        assert_eq!(
            conformance(&db, "R")?,
            EndpointConformance::Unverified,
            "the downgrade is persisted"
        );
        db.shutdown().await?;
    }
    Ok(())
}

/// A latest manifest written without writer features (an older binary's
/// flush) downgrades `R` on open; the enforcing binary's own manifest does
/// not.
#[tokio::test]
async fn endpoint_conformance_writer_features_unmarked_manifest_downgrades() -> Result<()> {
    for older_flushed in [false, true] {
        let dir = tempfile::tempdir()?;
        // The store holds `(s:S)-[:R]->(f)` under `A|S -> F`; the catalog is
        // then narrowed to `A -> F` with the `enforced` record kept -- the
        // state an older binary leaves when it writes an `S`-source edge into
        // an enforced `A -> F` type.
        let (s, f, _) = enforced_store(dir.path(), &["A", "S"]).await?;
        {
            let db = open(dir.path()).await?;
            let tx = db.session().tx().await?;
            tx.execute_with("MATCH (s), (f) WHERE id(s) = $s AND id(f) = $f CREATE (s)-[:R]->(f)")
                .param("s", Value::Int(i64::try_from(s.as_u64())?))
                .param("f", Value::Int(i64::try_from(f.as_u64())?))
                .run()
                .await?;
            tx.commit().await?;
            db.flush().await?;
            db.shutdown().await?;
        }
        assert!(latest_manifest_is_marked(dir.path())?, "this binary marks");
        set_declared_sources(dir.path(), "R", &["A"])?;
        if older_flushed {
            older_binary_flush(dir.path())?;
        }
        let db = open(dir.path()).await?;
        if !older_flushed {
            assert_eq!(
                conformance(&db, "R")?,
                EndpointConformance::Enforced,
                "a marked latest manifest is trusted"
            );
            db.shutdown().await?;
            continue;
        }
        assert_eq!(conformance(&db, "R")?, EndpointConformance::Unverified);
        assert_eq!(ints(&db, SOURCES).await?, vec![1, 3]);
        db.shutdown().await?;
        let db = open(dir.path()).await?;
        assert_eq!(
            conformance(&db, "R")?,
            EndpointConformance::Unverified,
            "the downgrade is persisted"
        );
        db.shutdown().await?;
    }
    Ok(())
}

/// A store whose last flush predates every enforcing binary: the unmarked
/// latest manifest is accepted as pre-existing, so a type declared after the
/// upgrade stays `enforced` across a reopen with no flush in between; an
/// older binary's flush after that still downgrades it.
#[tokio::test]
async fn endpoint_conformance_writer_features_preexisting_unmarked_manifest_is_accepted()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let db = open(dir.path()).await?;
        db.schema()
            .label("A")
            .property("v", DataType::Int)
            .label("F")
            .property("v", DataType::Int)
            .apply()
            .await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:A {v: 1}), (:F {v: 2})").await?;
        tx.commit().await?;
        db.flush().await?;
        db.shutdown().await?;
    }
    make_latest_manifest_legacy(dir.path())?;
    // The first enforcing open declares `R` and crashes before any flush
    // (a clean shutdown would publish a marked manifest).
    let crashed = tempfile::tempdir()?;
    {
        let db = open(dir.path()).await?;
        db.schema().edge_type("R", &["A"], &["F"]).apply().await?;
        assert_eq!(conformance(&db, "R")?, EndpointConformance::Enforced);
        copy_tree(dir.path(), crashed.path())?;
        db.shutdown().await?;
    }
    let dir = crashed;
    assert!(
        !latest_manifest_is_marked(dir.path())?,
        "nothing flushed: the legacy manifest is still latest"
    );
    {
        let db = open(dir.path()).await?;
        assert_eq!(
            conformance(&db, "R")?,
            EndpointConformance::Enforced,
            "the pre-existing manifest was accepted"
        );
        db.shutdown().await?;
    }
    older_binary_flush(dir.path())?;
    let db = open(dir.path()).await?;
    assert_eq!(conformance(&db, "R")?, EndpointConformance::Unverified);
    db.shutdown().await?;
    Ok(())
}

/// Recursive copy of `src` into `dst`, skipping the database lock file.
pub(crate) fn copy_tree(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &to)?;
        } else if !entry.file_name().to_string_lossy().ends_with(".lock") {
            std::fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}
