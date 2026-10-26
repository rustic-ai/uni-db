// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Adding a property to a populated label, with a commit racing the DDL and a
//! crash before the DDL saves the catalog.
//!
//! `SchemaBuilder::apply` recorded the new property in the live catalog, then
//! widened the Lance dataset, then saved the catalog. A transaction commit in
//! between persisted the dirty catalog through `save_if_dirty`, and if the
//! process died before the widening, the on-disk catalog named a column the
//! dataset lacked: every read of the label failed after reopen (`column types
//! must match schema types, expected Int64 but found LargeBinary`). Storage is
//! now widened before the property enters the live catalog, so whatever a
//! racing commit persists, the column is already there.
//!
//! The child holds the DDL after the property is declared and before the
//! save, commits, and aborts; reads of the label after reopen must succeed and
//! return the data.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration --features failpoints \
//!     --run-ignored all -E 'test(schema_ddl_widen_resilience)'

// Rust guideline compliant

#![cfg(all(unix, feature = "failpoints"))]

use std::time::Duration;

use uni_db::{DataType, Uni};

use crate::crash_harness;

const ENTRY: &str = "storage::schema_ddl_widen_resilience::ddl_widen_abort_child";
/// `SchemaBuilder::apply`: the property is declared, the catalog not saved.
const BUILDER_SEAM: &str = "schema_builder::declared-before-save";
/// Any DDL about to widen storage; `ALTER ... ADD PROPERTY` passes it.
const WIDEN_SEAM: &str = "storage::widen-declared-properties";
const DDL_THREAD: &str = "racing-add-property";

/// Child: populate and flush `Person`, start adding `age`, hold the DDL at the
/// seam, commit a write (which saves the dirty catalog), then abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "child-process entry point; driven by run_child"]
async fn ddl_widen_abort_child() {
    let Some((scenario, path)) = crash_harness::child_env() else {
        return;
    };
    // (race, through Cypher `ALTER` rather than the schema builder)
    let (race, alter) = match scenario.as_str() {
        "commit_during_add_property" => (true, false),
        "commit_during_alter_add_property" => (true, true),
        "commit_after_add_property" => (false, false),
        other => crash_harness::unknown_scenario(ENTRY, other),
    };

    let db = std::sync::Arc::new(
        Uni::open(path.to_string_lossy())
            .build()
            .await
            .expect("open store"),
    );
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .expect("register schema");
    let tx = db.session().tx().await.expect("begin tx");
    tx.execute("CREATE (:Person {name: 'ada'}), (:Person {name: 'bob'})")
        .await
        .expect("create");
    tx.commit().await.expect("commit");
    // The Person dataset must exist in Lance without `age`.
    db.flush().await.expect("flush");

    if !race {
        db.schema()
            .label("Person")
            .property_nullable("age", DataType::Int64)
            .done()
            .apply()
            .await
            .expect("add age");
        let tx = db.session().tx().await.expect("begin tx");
        tx.execute("CREATE (:Person {name: 'cy'})")
            .await
            .expect("create");
        tx.commit().await.expect("commit");
        std::process::abort();
    }

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let reached_tx = std::sync::Mutex::new(Some(reached_tx));
    let seam = if alter { WIDEN_SEAM } else { BUILDER_SEAM };
    fail::cfg_callback(seam, move || {
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
                if alter {
                    let tx = ddl_db.session().tx().await?;
                    tx.execute("ALTER LABEL Person ADD PROPERTY age INT64")
                        .await?;
                    tx.commit().await?;
                    Ok(())
                } else {
                    ddl_db
                        .schema()
                        .label("Person")
                        .property_nullable("age", DataType::Int64)
                        .done()
                        .apply()
                        .await
                }
            })
        })
        .expect("spawn ddl thread");
    tokio::task::spawn_blocking(move || reached_rx.recv_timeout(Duration::from_secs(30)))
        .await
        .expect("join")
        .expect("DDL never reached the seam");

    // An unrelated commit while the DDL is in flight.
    let tx = db.session().tx().await.expect("begin tx");
    tx.execute("CREATE (:Person {name: 'cy'})")
        .await
        .expect("create");
    if alter {
        // `ALTER ... ADD PROPERTY` holds the writer's declaration lock from
        // its value check to its catalog change (#304), and the seam is
        // inside that window: the commit cannot land there, and waits.
        match tx.commit().await {
            Err(uni_db::UniError::CommitTimeout { .. }) => {}
            other => panic!("a commit during the declaration must wait for it, got {other:?}"),
        }
    } else {
        tx.commit().await.expect("commit");
    }

    let catalog = std::fs::read_to_string(path.join("catalog/schema.json"))
        .or_else(|_| std::fs::read_to_string(path.join("storage/catalog/schema.json")))
        .unwrap_or_default();
    eprintln!(
        "on-disk catalog mentions `age` before the crash: {}",
        catalog.contains("\"age\"")
    );

    std::process::abort();
}

/// After the crash, `Person` must still be readable with all three rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_during_add_property_then_crash_keeps_label_readable() {
    assert_readable_after("commit_during_add_property").await;
}

/// The same race through Cypher `ALTER LABEL ... ADD PROPERTY`, held as it is
/// about to widen storage. That point is now inside the declaration lock
/// (#304), so the racing commit waits rather than landing; the label must
/// still be readable, without the commit's row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_during_alter_add_property_then_crash_keeps_label_readable() {
    assert_readable_after("commit_during_alter_add_property").await;
}

/// Control: the same writes and crash, with the DDL finished before the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_after_add_property_then_crash_keeps_label_readable() {
    assert_readable_after("commit_after_add_property").await;
}

async fn assert_readable_after(scenario: &str) {
    // The ALTER's racing commit is held off by the declaration lock.
    let expected: &[&str] = if scenario == "commit_during_alter_add_property" {
        &["ada", "bob"]
    } else {
        &["ada", "bob", "cy"]
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let store = dir.path().join("store");

    crash_harness::run_child_async(ENTRY, scenario, &store).await;

    let db = Uni::open(store.to_string_lossy())
        .build()
        .await
        .expect("reopen after the crash");
    let rows = db
        .session()
        .query("MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name")
        .await
        .expect("reading Person after reopen");
    let names: Vec<String> = rows
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").expect("name"))
        .collect();
    assert_eq!(names, expected);
}
