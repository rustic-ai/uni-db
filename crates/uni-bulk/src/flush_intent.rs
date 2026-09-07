// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Crash-atomicity for bulk loads.
//!
//! A bulk load writes many independent Lance datasets — per-label vertex tables,
//! the main vertices table, fwd/bwd edge delta tables, the main edges table —
//! each committed as its own Lance transaction. Lance has no cross-dataset
//! atomic commit, so a crash *between* two of those commits would otherwise
//! leave the tables permanently divergent (an entity in the per-label table but
//! not the main table), with no reconciliation on reopen.
//! [`crate::BulkWriter::abort`] only helps if it is actually reached, which a
//! crash skips.
//!
//! This module makes the *whole bulk load* atomic with a durable intent marker
//! plus reconciliation at database open:
//!
//! 1. Before the load commits any table it records that table's pre-load Lance
//!    version in a marker (`catalog/bulk_flush_intent.json`, phase `Active`),
//!    persisted durably *before* the table write.
//! 2. [`crate::BulkWriter::commit`] writes the marker as `Committed` (carrying the new
//!    snapshot id) *after* the manifest is durably saved but *before* the latest
//!    pointer is flipped, then deletes the marker once everything is finalized.
//! 3. On reopen [`recover_interrupted_bulk_load`] reconciles:
//!    * marker absent → nothing happened (or the load fully finalized);
//!    * `Active` → the load was interrupted before committing → **roll every
//!      touched table back** to its pre-load version (or drop tables created
//!      during the load), so the tables cannot be left divergent;
//!    * `Committed` → the load had committed (manifest written) but crashed
//!      before flipping the latest pointer / deleting the marker → **roll
//!      forward** by (idempotently) setting the latest snapshot.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path as ObjectStorePath;
use serde::{Deserialize, Serialize};
use uni_store::storage::manager::StorageManager;
use uni_store::store_utils::{delete_with_timeout, get_with_timeout, put_with_timeout};

const INTENT_TIMEOUT: Duration = Duration::from_secs(30);

fn intent_path() -> ObjectStorePath {
    ObjectStorePath::from("catalog/bulk_flush_intent.json")
}

/// Phase of an in-flight bulk load.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BulkIntentPhase {
    /// Tables are being written; the load has not declared intent to commit.
    /// Recovery rolls back.
    Active,
    /// The load committed (manifest durably saved); only the latest-pointer flip
    /// and/or marker delete may be missing. Recovery rolls forward.
    Committed,
}

/// Durable record of an in-flight bulk load's touched tables and phase.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BulkFlushIntent {
    pub phase: BulkIntentPhase,
    /// table name -> pre-load Lance version (`None` = table created during the
    /// load, so recovery drops it on rollback).
    pub tables: BTreeMap<String, Option<u64>>,
    /// Target snapshot id, set when transitioning to `Committed` (used to roll
    /// the latest pointer forward during recovery).
    pub snapshot_id: Option<String>,
}

/// Write/overwrite the marker in the `Active` phase with the current set of
/// touched tables. Must be called *before* the corresponding table writes.
pub(crate) async fn write_active(
    store: &Arc<dyn ObjectStore>,
    tables: &std::collections::HashMap<String, Option<u64>>,
) -> Result<()> {
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Active,
        tables: tables.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        snapshot_id: None,
    };
    write(store, &intent).await
}

/// Promote the marker to `Committed`, recording the target snapshot id. Must be
/// called *after* the snapshot manifest is durably saved and *before* the latest
/// pointer is flipped.
pub(crate) async fn write_committed(
    store: &Arc<dyn ObjectStore>,
    tables: &std::collections::HashMap<String, Option<u64>>,
    snapshot_id: &str,
) -> Result<()> {
    let intent = BulkFlushIntent {
        phase: BulkIntentPhase::Committed,
        tables: tables.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        snapshot_id: Some(snapshot_id.to_string()),
    };
    write(store, &intent).await
}

async fn write(store: &Arc<dyn ObjectStore>, intent: &BulkFlushIntent) -> Result<()> {
    let json = serde_json::to_vec(intent)?;
    put_with_timeout(store, &intent_path(), Bytes::from(json), INTENT_TIMEOUT).await?;
    Ok(())
}

/// Delete the marker (best-effort: a missing marker is not an error).
pub(crate) async fn clear(store: &Arc<dyn ObjectStore>) -> Result<()> {
    match delete_with_timeout(store, &intent_path(), INTENT_TIMEOUT).await {
        Ok(()) => Ok(()),
        Err(e) if is_not_found(&e) => Ok(()),
        Err(e) => Err(e),
    }
}

async fn read(store: &Arc<dyn ObjectStore>) -> Result<Option<BulkFlushIntent>> {
    match get_with_timeout(store, &intent_path(), INTENT_TIMEOUT).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            let intent: BulkFlushIntent = serde_json::from_slice(&bytes)?;
            Ok(Some(intent))
        }
        Err(e) if is_not_found(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// #233 Tier 1: this matched on the error's rendered STRING, so any genuine
/// store failure whose text happens to contain "not found" read as "no marker
/// present" — an interrupted bulk load would then never be reconciled and its
/// half-written tables would stay. `uni-store` already carries the typed
/// discrimination this needs.
fn is_not_found(e: &anyhow::Error) -> bool {
    uni_store::store_utils::is_not_found(e)
}

/// Reconcile an interrupted bulk load at database open. See the module docs for
/// the recovery policy. Idempotent and safe to call on every open (a no-op when
/// no marker is present).
pub async fn recover_interrupted_bulk_load(storage: &StorageManager) -> Result<()> {
    let store = storage.store();
    let Some(intent) = read(&store).await? else {
        return Ok(());
    };

    // Number of tables that failed to reconcile; when non-zero we must NOT clear
    // the intent marker, so a later reopen retries the reconciliation.
    let failures = match intent.phase {
        BulkIntentPhase::Committed => {
            // The load committed (manifest written); finish the latest-pointer
            // flip idempotently so the committed data becomes visible.
            if let Some(snapshot_id) = &intent.snapshot_id {
                storage
                    .snapshot_manager()
                    .set_latest_snapshot(snapshot_id)
                    .await?;
            }
            tracing::warn!(
                tables = intent.tables.len(),
                "Finalized a committed-but-unfinished bulk load on reopen"
            );
            0usize
        }
        BulkIntentPhase::Active => {
            // Interrupted before commit: roll every touched table back to its
            // pre-load version (drop tables created during the load) so the
            // per-label and main tables cannot be left divergent.
            let backend = storage.backend();
            let mut failures = 0usize;
            for (table, pre_version) in &intent.tables {
                let result = match pre_version {
                    Some(version) => backend.rollback_table(table, *version).await,
                    None => backend.drop_table(table).await,
                };
                if let Err(e) = result {
                    // A drop/rollback error is only a genuine reconciliation
                    // failure if the table is not already in its desired state.
                    // A table created during the load (pre_version `None`) that
                    // was never actually committed — the load faulted before
                    // writing it — is already absent, so its failed `drop_table`
                    // is a benign no-op, not divergence. A `Some` pre-version
                    // means the table existed before the load and must be
                    // restored, so a rollback error there is always genuine.
                    let benign =
                        pre_version.is_none() && !backend.table_exists(table).await.unwrap_or(true);
                    if benign {
                        tracing::debug!(table = %table, "bulk recovery: table already absent; nothing to roll back");
                    } else {
                        failures += 1;
                        tracing::warn!(table = %table, error = %e, "bulk recovery: table rollback failed");
                    }
                }
            }
            backend.clear_cache();
            tracing::warn!(
                tables = intent.tables.len(),
                failures,
                "Rolled back an interrupted bulk load on reopen"
            );
            failures
        }
    };

    // Only clear the marker once reconciliation fully succeeded. If any table
    // failed to roll back, retain the marker (mirroring the Committed branch's
    // fail-before-clear contract) and surface the failure so the next reopen
    // retries — otherwise a divergent table would be abandoned permanently.
    if failures > 0 {
        anyhow::bail!(
            "bulk recovery: {failures} table rollback(s) failed; intent marker retained for retry"
        );
    }

    clear(&store).await?;
    Ok(())
}
