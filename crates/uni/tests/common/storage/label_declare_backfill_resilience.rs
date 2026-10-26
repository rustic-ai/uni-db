// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Declaring a label over flushed schemaless vertices, with a commit racing
//! the declaration and a crash before the label's backfill (#304).
//!
//! Until a label is declared, its vertices are flushed into the main vertex
//! table only. Declaring it copies them into the label's table; until then a
//! label-scoped read sees none of them. The label enters the live catalog
//! before that copy, and any commit in between saves the catalog
//! (`save_if_dirty`), so a crash there leaves the on-disk catalog declaring
//! the label with its rows still only in the main table. The declaration
//! leaves a marker first, and the next open finishes the copy.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration --features failpoints \
//!     --run-ignored all -E 'test(label_declare_backfill_resilience)'

// Rust guideline compliant

#![cfg(all(unix, feature = "failpoints"))]

use std::time::Duration;

use uni_db::{DataType, Uni};

use crate::crash_harness;

const ENTRY: &str = "storage::label_declare_backfill_resilience::label_declare_abort_child";
/// `SchemaBuilder::apply`: new labels are in the live catalog, not backfilled.
const SEAM: &str = "schema_builder::label-declared-before-backfill";
const DDL_THREAD: &str = "racing-declare-label";

/// Child: flush schemaless `Repository` vertices, start declaring the label,
/// hold the DDL at the seam, commit a write (which saves the dirty catalog),
/// then abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "child-process entry point; driven by run_child"]
async fn label_declare_abort_child() {
    let Some((scenario, path)) = crash_harness::child_env() else {
        return;
    };
    let race = match scenario.as_str() {
        "commit_during_declare" => true,
        "commit_after_declare" => false,
        other => crash_harness::unknown_scenario(ENTRY, other),
    };

    let db = std::sync::Arc::new(
        Uni::open(path.to_string_lossy())
            .build()
            .await
            .expect("open store"),
    );
    let tx = db.session().tx().await.expect("begin tx");
    tx.execute("CREATE (:Repository {uid: 'a'}), (:Repository {uid: 'b'})")
        .await
        .expect("create");
    tx.commit().await.expect("commit");
    db.flush().await.expect("flush");

    if race {
        let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
        let reached_tx = std::sync::Mutex::new(Some(reached_tx));
        fail::cfg_callback(SEAM, move || {
            if std::thread::current().name() != Some(DDL_THREAD) {
                return;
            }
            if let Some(tx) = reached_tx.lock().unwrap().take() {
                let _ = tx.send(());
                // Hold the DDL here until the process aborts.
                loop {
                    std::thread::park();
                }
            }
        })
        .expect("arm seam");

        let ddl_db = db.clone();
        let runtime = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name(DDL_THREAD.to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    ddl_db
                        .schema()
                        .label("Repository")
                        .property_nullable("uid", DataType::String)
                        .done()
                        .apply()
                        .await
                })
            })
            .expect("spawn ddl thread");
        tokio::task::spawn_blocking(move || reached_rx.recv_timeout(Duration::from_secs(30)))
            .await
            .expect("join")
            .expect("DDL never reached the seam");
    } else {
        db.schema()
            .label("Repository")
            .property_nullable("uid", DataType::String)
            .done()
            .apply()
            .await
            .expect("declare Repository");
    }

    // An unrelated commit while the live catalog declares `Repository`.
    let tx = db.session().tx().await.expect("begin tx");
    tx.execute("CREATE (:Other {uid: 'o'})")
        .await
        .expect("create");
    tx.commit().await.expect("commit");

    std::process::abort();
}

/// After the crash, `Repository` is declared and both vertices are visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_during_declare_then_crash_keeps_label_rows() {
    assert_rows_after("commit_during_declare").await;
}

/// Control: the same writes and crash, with the declaration finished first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_after_declare_then_crash_keeps_label_rows() {
    assert_rows_after("commit_after_declare").await;
}

async fn assert_rows_after(scenario: &str) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    crash_harness::run_child_async(ENTRY, scenario, &store).await;

    let db = Uni::open(store.to_string_lossy())
        .build()
        .await
        .expect("reopen after the crash");
    assert!(
        db.get_label_info("Repository")
            .await
            .expect("label info")
            .is_some(),
        "the racing commit persisted the declaration"
    );
    let rows = db
        .session()
        .query("MATCH (n:Repository) RETURN n.uid AS uid ORDER BY uid")
        .await
        .expect("reading Repository after reopen");
    let uids: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("uid").expect("uid"))
        .collect();
    assert_eq!(uids, vec!["a", "b"]);
}
