// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A bulk load's event delivery, part way through reading the load back when
//! the runtime it runs on is dropped, ends without a panic, and the next open
//! delivers the load's events.
//!
//! Lance 11 unwraps the `JoinError` of the fragment reads it spawns on the
//! ambient runtime (`filtered_read.rs`), so a scan polled once the runtime
//! that spawned its reads has shut down panics with `JoinError::Cancelled`
//! (pinned in `uni-store`). The delivery reads the load back from a detached
//! task that a dropped runtime can catch mid-poll; the read-back's scan now
//! runs on the IO runtime (`uni_bulk`'s `ScanFeed`, tested directly there).
//! This test holds the delivery between two batches of the read-back while
//! the runtime is dropped and pins the end-to-end contract; it does not by
//! itself force the panic's interleaving, which needs a fragment read
//! cancelled while the delivery's own poll is in flight.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration --features failpoints \
//!     -E 'test(bulk_events_runtime_drop_resilience)'

// Rust guideline compliant

#![cfg(feature = "failpoints")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use uni_db::{DataType, Uni, Value};

/// Loads made before the observed one, so the read-back scan spans more
/// fragments than Lance reads ahead.
const EARLIER_LOADS: usize = 40;

/// Rows of the observed load, one insert call each.
const OBSERVED_PARTS: usize = 40;

async fn count(db: &Uni, label: &str) -> i64 {
    db.session()
        .query(&format!("MATCH (n:{label}) RETURN count(n) AS n"))
        .await
        .expect("count")
        .rows()[0]
        .get::<i64>("n")
        .expect("n")
}

/// One bulk load of `parts` rows, each written as its own fragment (a load
/// larger than its writer's batch size spans fragments the same way).
async fn load(db: &Uni, x: i64, parts: usize) {
    let tx = db.session().tx().await.expect("tx");
    let mut bulk = tx.bulk_writer().batch_size(1).build().expect("bulk writer");
    for part in 0..parts {
        bulk.insert_vertices(
            "A",
            vec![HashMap::from([(
                "x".to_string(),
                Value::Int(x + part as i64),
            )])],
        )
        .await
        .expect("insert");
    }
    let _ = bulk.commit().await;
}

/// The delivery is held between two batches of its read-back scan while the
/// runtime is dropped, then released into the dead runtime: no Lance panic,
/// and the next open delivers the load's events.
#[test]
fn a_delivery_mid_scan_survives_its_runtime_being_dropped_resilience() {
    let lance_panics = Arc::new(AtomicUsize::new(0));
    {
        let lance_panics = Arc::clone(&lance_panics);
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if info.to_string().contains("JoinError::Cancelled") {
                lance_panics.fetch_add(1, Ordering::SeqCst);
            }
            previous(info);
        }));
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("store");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let db = Arc::new(rt.block_on(async {
        let db = Uni::open(root.to_string_lossy())
            .build()
            .await
            .expect("open");
        db.schema()
            .label("A")
            .property("x", DataType::Int64)
            .apply()
            .await
            .expect("schema");
        for x in 0..EARLIER_LOADS {
            load(&db, x as i64, 1).await;
        }
        let tx = db.session().tx().await.expect("tx");
        tx.execute(
            r#"CALL uni.plugin.declareTrigger(
                'test.bulk_runtime_drop', 'CREATE ON :A', 'CREATE (:AuditLog {msg: "bulk"})'
            )"#,
        )
        .await
        .expect("declare");
        tx.commit().await.expect("commit declaration");
        db
    }));

    let armed = Arc::new(AtomicBool::new(true));
    let (paused_tx, paused_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let paused_tx = parking_lot::Mutex::new(paused_tx);
    let release_rx = parking_lot::Mutex::new(release_rx);
    {
        let armed = Arc::clone(&armed);
        fail::cfg_callback("bulk::events::mid-scan", move || {
            if armed.swap(false, Ordering::SeqCst) {
                let _ = paused_tx.lock().send(());
                let _ = release_rx.lock().recv_timeout(Duration::from_secs(30));
            }
        })
        .expect("arm seam");
    }
    {
        let db = Arc::clone(&db);
        rt.spawn(async move { load(&db, -1000, OBSERVED_PARTS).await });
    }
    paused_rx
        .recv_timeout(Duration::from_secs(60))
        .expect("the delivery never reached the middle of its scan");

    // Dropping a multi-thread runtime waits for its workers, one of which is
    // held in the seam: drop it on another thread, let the shutdown cancel
    // the other tasks, then release the held worker into the dead runtime.
    let dropper = std::thread::spawn(move || drop(rt));
    std::thread::sleep(Duration::from_millis(500));
    release_tx.send(()).expect("release");
    dropper.join().expect("runtime drop");
    fail::remove("bulk::events::mid-scan");

    assert_eq!(
        lance_panics.load(Ordering::SeqCst),
        0,
        "a Lance scan panicked on its cancelled fragment reads"
    );
    drop(db);

    // The delivery was interrupted; the load's marker stays and the next
    // open delivers its events.
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let db = Uni::open(root.to_string_lossy())
            .build()
            .await
            .expect("reopen");
        assert_eq!(
            count(&db, "A").await,
            (EARLIER_LOADS + OBSERVED_PARTS) as i64
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while count(&db, "AuditLog").await == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            count(&db, "AuditLog").await >= 1,
            "the interrupted load's events were never delivered"
        );
    });
}
