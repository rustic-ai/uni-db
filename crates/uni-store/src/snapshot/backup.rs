// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `BACKUP TO`: a copy of a store that opens as a store.
//!
//! The copy has the standard layout ([`StoreLayout::Standard`]) whatever the
//! source's, so `Uni::open(destination)` reads it:
//!
//! - the table root (tables, indexes, snapshot manifests and `latest`);
//! - the schema and Locy rules;
//! - the ID allocator, so writes to the restored store do not reuse ids;
//! - the WAL, which holds whatever was committed after the flush that precedes
//!   the copy.
//!
//! Forks are left out: a fork's Lance branches record its parent's URIs, so a
//! copied fork would read from, and write next to, the source.
//!
//! While the copy runs, the destination holds [`IN_PROGRESS_MARKER`], which
//! makes `Uni::open` refuse it; `catalog/latest` is written last, naming the
//! snapshot the backup reports. A copy that fails part way therefore never
//! opens as a store: delete it and back up again.
//!
//! The copy is of the store as of the manifest it names. Flushes keep
//! running while it copies, but none truncates the WAL until the copy is done
//! ([`StorageManager::hold_wal_truncation`]), and the WAL is copied before the
//! tables: opening the copy rolls every table back to that manifest's
//! versions and replays the copied WAL, which holds every commit since. The
//! commits after the WAL was copied are not in the backup.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::location::{StoreLayout, StoreLocation};
use crate::storage::manager::StorageManager;

/// What a backup copied.
#[derive(Debug, Clone)]
pub struct BackupReport {
    /// The snapshot the copy's `latest` points at.
    pub snapshot_id: String,
    /// Objects copied.
    pub objects: usize,
    /// Bytes copied.
    pub bytes: u64,
}

/// Present in a backup destination until the copy has finished.
pub use crate::location::IN_PROGRESS_MARKER;

/// Objects at or above this size are copied in parts rather than in memory.
const MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;

/// The size of each part of a multipart copy.
const PART_SIZE: usize = 8 * 1024 * 1024;

/// Parts of one multipart copy in flight at once.
const PARTS_IN_FLIGHT: usize = 8;

/// Catalog files under the store root that belong in a backup. The rest of
/// the root catalog is the fork registry, which a backup leaves out.
const ROOT_CATALOG_FILES: &[&str] = &["catalog/schema.json", "catalog/locy_rules.json"];

/// Whether a table-root object belongs to a fork rather than to the primary:
/// fork branches and their refs inside a dataset, and the fork catalog.
fn is_fork_object(path: &str) -> bool {
    path.contains("/tree/")
        || path.contains("/_refs/branches/")
        || path.contains("/_refs/tags/fork_")
        || path.starts_with("catalog/forks/")
        || path.starts_with("catalog/fork_")
        || path.starts_with("wal_forks/")
}

/// Whether an object under a flat store's root, which is also its table
/// root, is root metadata copied separately (or left out) rather than a
/// table-root object.
fn is_flat_root_metadata(path: &str) -> bool {
    path.starts_with("wal/")
        || path.starts_with("_system/")
        || path == "id_allocator.json"
        || path == crate::runtime::wal_lineage::WAL_ID_FILE
        || path.starts_with("wal_set_aside/")
        || path == "catalog/layout.json"
        || path == crate::runtime::wal_lineage::CLAIM_FILE
        || path == IN_PROGRESS_MARKER
        || path == "schema.json"
        || ROOT_CATALOG_FILES.contains(&path)
}

/// Copies the primary of the store `storage` manages to `dest`.
///
/// # Errors
///
/// `dest` already holds a store, the session is a fork, there is no
/// snapshot, or an object cannot be read or written.
pub async fn backup_store(storage: &StorageManager, dest: &StoreLocation) -> Result<BackupReport> {
    if storage.fork_scope().is_some() {
        bail!("BACKUP copies the primary store; run it from a primary session, not a fork");
    }
    // Before the manifest is read: every commit since it stays in the WAL
    // until the WAL is copied.
    let _wal_hold = storage.hold_wal_truncation();
    let snapshot = storage
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .ok_or_else(|| anyhow!("No snapshot to back up: the store has never been flushed"))?;
    // Test seam: the manifest is read, nothing is copied yet; a flush here
    // used to truncate the WAL the copy then missed.
    fail::fail_point!("backup::after-latest");

    let dest_meta = dest.meta_store();
    let dest_tables = dest.table_store(StoreLayout::Standard)?;
    refuse_existing_store(&dest_meta, &dest_tables, dest.root_uri()).await?;
    // From here until the end, the destination says it is unfinished.
    dest_meta
        .put(
            &Path::from(IN_PROGRESS_MARKER),
            serde_json::to_vec(&serde_json::json!({ "snapshot_id": snapshot.snapshot_id }))?.into(),
        )
        .await?;

    let mut report = BackupReport {
        snapshot_id: snapshot.snapshot_id.clone(),
        objects: 0,
        bytes: 0,
    };

    // The WAL first, with the ID allocator and the WAL's id: in the local
    // directory of a hybrid store, at the root otherwise. Copied before the
    // tables, so it holds every commit the tables copied after it may carry
    // above the manifest (which the open rolls back and replays).
    let mut wal_id_copied = false;
    if let Some(origin) = storage.origin() {
        let src_meta = origin.location.meta_store();
        {
            // The WAL and ID allocator: in the local directory of a hybrid
            // store, at the root otherwise.
            let src_aux = origin
                .local
                .clone()
                .unwrap_or_else(|| Arc::clone(&src_meta));
            copy_if_present(
                &src_aux,
                "id_allocator.json",
                "id_allocator.json",
                &dest_meta,
                &mut report,
            )
            .await?;
            // The WAL's id goes with it, and the copy's claim names it below,
            // so a hybrid host bringing another WAL to the copy is refused.
            wal_id_copied = copy_if_present(
                &src_aux,
                crate::runtime::wal_lineage::WAL_ID_FILE,
                crate::runtime::wal_lineage::WAL_ID_FILE,
                &dest_meta,
                &mut report,
            )
            .await?;
            let mut prefixes = vec!["wal"];
            // Plugin, scheduler and trigger sidecars live in the local
            // directory: a hybrid store's, or a local store's root.
            if origin.local.is_some() || origin.location.fs_root().is_some() {
                prefixes.push("_system");
            }
            for prefix in prefixes {
                let listed: Vec<object_store::ObjectMeta> = src_aux
                    .list(Some(&Path::from(prefix)))
                    .try_collect()
                    .await?;
                for meta in listed {
                    copy_object(&src_aux, &meta, &dest_meta, &meta.location, &mut report).await?;
                }
            }
        }
    }

    // Tables, indexes and the snapshot catalog, from the table root. The
    // `latest` pointer is written last, below.
    let src_tables = storage.store();
    let flat = storage
        .origin()
        .is_some_and(|o| o.layout == StoreLayout::Flat);
    let objects: Vec<object_store::ObjectMeta> = src_tables.list(None).try_collect().await?;
    for meta in objects {
        let path = meta.location.as_ref();
        if is_fork_object(path)
            || path == "compaction.lock"
            || path == crate::lease::LOCK_FILE
            || path == crate::lease::LEASE_PATH
            || path == "catalog/latest"
            || (flat && is_flat_root_metadata(path))
        {
            continue;
        }
        copy_object(
            &src_tables,
            &meta,
            &dest_tables,
            &meta.location,
            &mut report,
        )
        .await?;
    }

    match storage.origin() {
        Some(origin) => {
            let src_meta = origin.location.meta_store();
            for file in ROOT_CATALOG_FILES {
                copy_if_present(&src_meta, file, file, &dest_meta, &mut report).await?;
            }
            // A store from before the catalog directory keeps its schema at
            // the root; read-only opens never migrate it.
            if src_meta
                .head(&Path::from("catalog/schema.json"))
                .await
                .is_err()
            {
                copy_if_present(
                    &src_meta,
                    "schema.json",
                    "catalog/schema.json",
                    &dest_meta,
                    &mut report,
                )
                .await?;
            }
        }
        None => {
            // A manager built without a resolved location knows only its
            // table root; the schema comes from memory.
            let schema = serde_json::to_vec_pretty(&storage.schema_manager().schema())?;
            dest_meta
                .put(&Path::from("catalog/schema.json"), schema.into())
                .await?;
            report.objects += 1;
        }
    }

    // The copy follows the WAL it was given: a hybrid host that opens it
    // with another local WAL holding commits is refused (`StaleWal`).
    if wal_id_copied && let Some(wal_id) = crate::runtime::wal_lineage::load(&dest_meta).await? {
        crate::runtime::wal_lineage::claim_new(&dest_meta, &wal_id, "wal").await?;
    }

    // Last: the snapshot pointer, the layout, and then the copy is a store.
    dest_tables
        .put(
            &Path::from("catalog/latest"),
            snapshot.snapshot_id.clone().into_bytes().into(),
        )
        .await?;
    dest.write_layout_marker(StoreLayout::Standard).await?;
    dest_meta.delete(&Path::from(IN_PROGRESS_MARKER)).await?;
    Ok(report)
}

async fn refuse_existing_store(
    meta: &Arc<dyn ObjectStore>,
    tables: &Arc<dyn ObjectStore>,
    root: &str,
) -> Result<()> {
    for (store, prefix) in [(meta, Some("catalog")), (meta, Some("wal")), (tables, None)] {
        let listed = store
            .list_with_delimiter(prefix.map(Path::from).as_ref())
            .await
            .map(|r| !r.objects.is_empty() || !r.common_prefixes.is_empty());
        match listed {
            Ok(true) => bail!(
                "The backup destination {root} already holds a store (or part of one); back up \
                 to an empty location"
            ),
            Ok(false) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(anyhow!("List the backup destination {root}: {e}")),
        }
    }
    Ok(())
}

async fn copy_if_present(
    src: &Arc<dyn ObjectStore>,
    from: &str,
    to: &str,
    dst: &Arc<dyn ObjectStore>,
    report: &mut BackupReport,
) -> Result<bool> {
    let from = Path::from(from);
    match src.head(&from).await {
        // Copy from the path asked for: a prefixed local store's `head` can
        // report the location with its prefix still on.
        Ok(meta) => {
            let meta = object_store::ObjectMeta {
                location: from,
                ..meta
            };
            copy_object(src, &meta, dst, &Path::from(to), report).await?;
            Ok(true)
        }
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Copies one object, streaming it in parts when it is large.
async fn copy_object(
    src: &Arc<dyn ObjectStore>,
    meta: &object_store::ObjectMeta,
    dst: &Arc<dyn ObjectStore>,
    to: &Path,
    report: &mut BackupReport,
) -> Result<()> {
    let got = src.get(&meta.location).await?;
    if meta.size < MULTIPART_THRESHOLD {
        let bytes = got.bytes().await?;
        report.bytes += bytes.len() as u64;
        dst.put(to, bytes.into()).await?;
    } else {
        // Parts are put with a bounded number in flight, so a fast source
        // cannot buffer a whole large object in memory; any failure, the
        // final completion's included, aborts the upload so no orphaned parts
        // stay behind.
        let mut upload = dst.put_multipart(to).await?;
        let copied: Result<()> = async {
            let mut stream = got.into_stream();
            let mut part: Vec<u8> = Vec::with_capacity(PART_SIZE);
            let mut in_flight = futures::stream::FuturesUnordered::new();
            while let Some(chunk) = stream.try_next().await? {
                report.bytes += chunk.len() as u64;
                part.extend_from_slice(&chunk);
                // Every part but the last is exactly `PART_SIZE`: some
                // providers (R2) refuse uneven parts.
                while part.len() >= PART_SIZE {
                    let rest = part.split_off(PART_SIZE);
                    let payload =
                        object_store::PutPayload::from(std::mem::replace(&mut part, rest));
                    in_flight.push(upload.put_part(payload));
                    if in_flight.len() >= PARTS_IN_FLIGHT {
                        futures::StreamExt::next(&mut in_flight).await.transpose()?;
                    }
                }
            }
            if !part.is_empty() {
                in_flight.push(upload.put_part(object_store::PutPayload::from(part)));
            }
            while let Some(done) = futures::StreamExt::next(&mut in_flight).await {
                done?;
            }
            Ok(())
        }
        .await;
        let completed = match copied {
            Ok(()) => upload
                .complete()
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from),
            Err(e) => Err(e),
        };
        if let Err(e) = completed {
            let _ = upload.abort().await;
            return Err(e);
        }
    }
    report.objects += 1;
    Ok(())
}
