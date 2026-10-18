// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Resilience of the bulk edge write against a concurrent compaction pass.
//!
//! Background compaction reads the edge delta tables with no coordination
//! with the writer that appends to them, folds what it finds into L2
//! adjacency, and relies on every delta EID already having its `main_edges`
//! row: that row is where the edge's properties live once the delta is gone,
//! and a debug build asserts it before clearing Delta L1. `Writer::flush_to_l1`
//! writes `main_edges` first for that reason. The bulk writer wrote both delta
//! directions first, so a pass that ran inside the window saw EIDs `main_edges`
//! did not hold yet — `runtime::compaction_lifecycle_test` tripped the
//! assertion in about 1 of 18 loaded runs, and in 5 of 5 once the window was
//! widened by 500 ms.
//!
//! The check is made at the instant a delta run is about to become visible,
//! which is the earliest moment a compaction pass could read it.

#![cfg(feature = "failpoints")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Result;
use uni_db::{Uni, UniConfig, Value};
use uni_store::backend::table_names;

// Rust guideline compliant

const SEAM: &str = "delta::before-write-run";
const V: usize = 50;
const E: usize = 200;

/// A bulk edge load has written every `main_edges` row before its first delta
/// run becomes readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_edges_reach_main_edges_before_any_delta_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    // No background pass: the test inspects the window itself, and a pass is
    // the thing that must never find it open.
    let mut cfg = UniConfig::default();
    cfg.compaction.enabled = false;
    let db = Arc::new(Uni::open(&path).config(cfg).build().await?);

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (k INT)").await?;
    tx.execute("CREATE EDGE TYPE E (w INT) FROM P TO P").await?;
    tx.commit().await?;

    // The first delta run parks here until the test has looked at storage;
    // later runs pass straight through.
    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let first = Arc::new(AtomicBool::new(true));
    {
        let first = Arc::clone(&first);
        fail::cfg_callback(SEAM, move || {
            if first.swap(false, Ordering::SeqCst) {
                reached_tx.send(()).ok();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(30))
                    .ok();
            }
        })
        .unwrap();
    }

    // The bulk writer's future is not `Send`, so the load runs on this task and
    // the inspection on a worker: the seam blocks the loading thread, not the
    // one that reads storage.
    let inspector = tokio::spawn({
        let db = Arc::clone(&db);
        async move {
            let reached = tokio::task::spawn_blocking(move || {
                reached_rx.recv_timeout(Duration::from_secs(30))
            })
            .await?;
            let main_rows = match reached {
                Ok(()) => {
                    let storage = db.storage();
                    let backend = storage.backend();
                    let main = table_names::main_edge_table_name();
                    let rows = if backend.table_exists(main).await? {
                        backend.count_rows(main, None).await?
                    } else {
                        0
                    };
                    Some(rows)
                }
                Err(_) => None,
            };
            release_tx.send(()).ok();
            anyhow::Ok(main_rows)
        }
    });

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let props: Vec<HashMap<String, Value>> = (0..V)
        .map(|i| HashMap::from([("k".to_string(), Value::Int(i as i64))]))
        .collect();
    let vids = bulk.insert_vertices("P", props).await?;
    let edges: Vec<uni_bulk::EdgeData> = (0..E)
        .map(|i| {
            uni_bulk::EdgeData::new(
                vids[i % V],
                vids[(i * 7 + 3) % V],
                HashMap::from([("w".to_string(), Value::Int(i as i64))]),
            )
        })
        .collect();
    bulk.insert_edges("E", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    fail::remove(SEAM);

    let main_rows = inspector
        .await??
        .ok_or_else(|| anyhow::anyhow!("no delta run was written; the test would prove nothing"))?;
    assert_eq!(
        main_rows, E,
        "the first delta run was about to become readable while main_edges held \
         {main_rows} of the {E} edges; a compaction pass in that window clears \
         deltas whose properties were never written"
    );
    Ok(())
}
