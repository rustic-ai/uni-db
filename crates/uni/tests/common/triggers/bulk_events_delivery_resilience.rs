// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A visible bulk load's events are delivered whole, whatever happens to the
//! caller of `commit()` or to the process.
//!
//! Measured before the fix:
//!
//! - Dropping the `commit()` future while CDC held the load's chunks back cut
//!   the delivery short: the after-trigger missed the rest of the load, and
//!   CDC, handed an unfinished stream, halted every stream -- a later commit
//!   was never delivered.
//! - Dropping it while the load waited for its place in the announcement
//!   order announced nothing at all: no trigger fired, and CDC never heard of
//!   the load.
//! - A transaction commit abandoned after its commit point released its place
//!   silently: CDC delivered the next commit and checkpointed past the
//!   abandoned one, a hole.
//! - A load completed by recovery at open emitted nothing: a crash after the
//!   load became visible, before its events were delivered, lost them for
//!   good.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration --features failpoints \
//!     -E 'test(bulk_events_delivery_resilience)'

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use arrow_array::{Array, StringArray};
use parking_lot::{Condvar, Mutex};
use smol_str::SmolStr;
use uni_db::{DataType, Uni, Value};
use uni_plugin::traits::cdc::{CdcBatch, CdcLsn, CdcOutputProvider, CdcStartContext, CdcStream};
use uni_plugin::traits::trigger::{TriggerEventMask, TriggerPhase};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

use super::event_support::{RecordingTrigger, event_kinds, register_trigger};

const NODE_CREATE: u8 = 1;

async fn db() -> Result<Arc<Uni>> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("x", DataType::Int64)
        .label("B")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    Ok(Arc::new(db))
}

fn rows(n: usize) -> Vec<HashMap<String, Value>> {
    (0..n)
        .map(|i| HashMap::from([("x".to_string(), Value::Int(i as i64))]))
        .collect()
}

fn after_trigger() -> Arc<RecordingTrigger> {
    RecordingTrigger::new(
        TriggerPhase::AfterCommit,
        TriggerEventMask::NODE_CREATE,
        Some(vec![SmolStr::new("A")]),
    )
}

/// A gate a CDC `deliver` waits at while closed.
#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    closed: bool,
    /// A `deliver` has waited at the closed gate.
    entered: bool,
}

impl Gate {
    fn close(&self) {
        self.state.lock().closed = true;
    }

    fn open(&self) {
        self.state.lock().closed = false;
        self.changed.notify_all();
    }

    fn entered(&self) -> bool {
        self.state.lock().entered
    }

    /// Wait here while the gate is closed (bounded, so a regression cannot
    /// hang the suite).
    fn pass(&self) {
        let mut state = self.state.lock();
        let deadline = Instant::now() + Duration::from_secs(30);
        while state.closed && Instant::now() < deadline {
            state.entered = true;
            self.changed.wait_until(&mut state, deadline);
        }
    }
}

/// A CDC provider recording every batch, whose `deliver` waits at [`Gate`].
#[derive(Default)]
struct GatedCdc {
    delivered: Arc<Mutex<Vec<CdcBatch>>>,
    gate: Arc<Gate>,
}

impl std::fmt::Debug for GatedCdc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatedCdc").finish_non_exhaustive()
    }
}

struct GatedStream {
    delivered: Arc<Mutex<Vec<CdcBatch>>>,
    gate: Arc<Gate>,
    last: CdcLsn,
}

impl CdcStream for GatedStream {
    fn deliver(&mut self, batch: &CdcBatch) -> Result<(), FnError> {
        self.gate.pass();
        self.last = batch.lsn_end;
        self.delivered.lock().push(batch.clone());
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<CdcLsn, FnError> {
        Ok(self.last)
    }

    fn shutdown(&mut self) -> Result<(), FnError> {
        Ok(())
    }
}

impl CdcOutputProvider for GatedCdc {
    fn name(&self) -> &str {
        "gated-recorder"
    }

    fn start(&self, _ctx: CdcStartContext<'_>) -> Result<Box<dyn CdcStream>, FnError> {
        Ok(Box::new(GatedStream {
            delivered: Arc::clone(&self.delivered),
            gate: Arc::clone(&self.gate),
            last: CdcLsn(0),
        }))
    }
}

impl GatedCdc {
    /// `NODE_CREATE` rows delivered under label `A` (the loads' label; the
    /// other commits write `B`).
    fn node_creates(&self) -> usize {
        self.delivered
            .lock()
            .iter()
            .map(|b| {
                let labels = b
                    .mutations
                    .column_by_name("label")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned());
                event_kinds(&b.mutations)
                    .into_iter()
                    .enumerate()
                    .filter(|(i, k)| {
                        *k == NODE_CREATE && labels.as_ref().is_some_and(|l| l.value(*i) == "A")
                    })
                    .count()
            })
            .sum()
    }

    fn delivered_version(&self, version: u64) -> bool {
        self.delivered.lock().iter().any(|b| b.lsn_end.0 == version)
    }
}

/// Register a [`GatedCdc`] and commit once so the CDC runtime starts its
/// stream; returns it once that commit was delivered, with the record cleared.
async fn attach_gated_cdc(db: &Uni) -> Result<Arc<GatedCdc>> {
    let cdc = Arc::new(GatedCdc::default());
    let caps = CapabilitySet::from_iter_of([Capability::Cdc]);
    let mut r = PluginRegistrar::new(PluginId::new("gated-recorder"), &caps, db.plugin_registry());
    r.cdc_output(Arc::clone(&cdc) as Arc<dyn CdcOutputProvider>)?;
    r.commit_to_registry()?;
    let version = commit_b(db, -1).await?;
    wait_until("the warm-up commit reaches CDC", || {
        cdc.delivered_version(version)
    })
    .await;
    cdc.delivered.lock().clear();
    Ok(cdc)
}

/// Commit `(:B {x})`; returns its version.
async fn commit_b(db: &Uni, x: i64) -> Result<u64> {
    let tx = db.session().tx().await?;
    tx.execute(&format!("CREATE (:B {{x: {x}}})")).await?;
    Ok(tx.commit().await?.version)
}

/// Poll `cond` for up to 20 s; panics naming `what` if it never holds.
async fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// `commit()` dropped while CDC holds the load's chunks back: the delivery
/// runs to the end anyway. Every row reaches the after-trigger and CDC, and
/// CDC keeps delivering later commits (it did not halt).
///
/// The CDC `deliver` blocks on the load's first chunk; with
/// `MUTATION_STREAM_DEPTH` (4) chunks queued behind it, sending the sixth
/// waits, so the `commit()` future -- which used to run the delivery itself
/// -- is suspended mid-delivery when it is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_commit_mid_delivery_still_delivers_every_event_resilience() -> Result<()> {
    // Eight chunks of `BULK_EVENT_CHUNK_ROWS` (4096).
    const ROWS: usize = 30_000;
    let db = db().await?;
    let t = after_trigger();
    register_trigger(&db, "mid-delivery", t.clone())?;
    let cdc = attach_gated_cdc(&db).await?;

    cdc.gate.close();
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices("A", rows(ROWS)).await?;
    let commit = tokio::spawn(async move { bulk.commit().await });
    wait_until("CDC to block on the load's first chunk", || {
        cdc.gate.entered()
    })
    .await;
    // Let the delivery fill CDC's queue and wait on it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        t.rows() < ROWS,
        "premise: the delivery must still be in progress ({} rows fired)",
        t.rows()
    );
    commit.abort();
    let outcome = commit.await;
    assert!(
        outcome.as_ref().is_err_and(|e| e.is_cancelled()),
        "premise: commit() must be dropped mid-delivery, got {:?}",
        outcome.map(|r| r.map(|s| s.vertices_inserted))
    );
    cdc.gate.open();

    let deadline = Instant::now() + Duration::from_secs(20);
    while (t.rows() < ROWS || cdc.node_creates() < ROWS) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(t.rows(), ROWS, "after-trigger rows");
    assert_eq!(cdc.node_creates(), ROWS, "CDC NODE_CREATE rows");

    let later = commit_b(&db, 1).await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cdc.delivered_version(later) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        cdc.delivered_version(later),
        "CDC halted: a commit after the load was never delivered"
    );
    drop(tx);
    Ok(())
}

/// `commit()` dropped while the load waits for its place in the announcement
/// order (another commit holds the writer's commit lock): the load is still
/// announced and every event delivered, and CDC carries on.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_commit_awaiting_its_announcement_still_delivers_resilience() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;

    const ROWS: usize = 100;
    let db = db().await?;
    let t = after_trigger();
    register_trigger(&db, "awaiting-announcement", t.clone())?;
    let cdc = attach_gated_cdc(&db).await?;

    // When the load reaches its announcement, a racing commit takes the
    // commit lock and parks holding it; the load then waits for the lock.
    let load_armed = Arc::new(AtomicBool::new(true));
    let lock_armed = Arc::new(AtomicBool::new(false));
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (racer_tx, racer_rx) = mpsc::channel::<tokio::task::JoinHandle<Result<u64>>>();
    let held = Arc::new(AtomicBool::new(false));
    {
        let held = Arc::clone(&held);
        let lock_armed = Arc::clone(&lock_armed);
        let held_tx = Mutex::new(held_tx);
        let release_rx = Mutex::new(release_rx);
        fail::cfg_callback("commit::after-flush-lock", move || {
            if lock_armed.swap(false, Ordering::SeqCst) {
                held.store(true, Ordering::SeqCst);
                held_tx.lock().send(()).unwrap();
                let _ = release_rx.lock().recv_timeout(Duration::from_secs(30));
            }
        })
        .unwrap();
    }
    {
        let db = Arc::clone(&db);
        let load_armed = Arc::clone(&load_armed);
        let lock_armed = Arc::clone(&lock_armed);
        let held_rx = Mutex::new(held_rx);
        let racer_tx = Mutex::new(racer_tx);
        fail::cfg_callback("bulk::events::before-announce", move || {
            if load_armed.swap(false, Ordering::SeqCst) {
                lock_armed.store(true, Ordering::SeqCst);
                let db = Arc::clone(&db);
                let handle = tokio::runtime::Handle::current();
                // Spawned from outside the runtime: a task spawned on this
                // worker would sit in its unstealable LIFO slot while the
                // worker blocks below.
                let racer =
                    std::thread::spawn(move || handle.spawn(async move { commit_b(&db, 7).await }))
                        .join()
                        .unwrap();
                racer_tx.lock().send(racer).unwrap();
                let _ = held_rx.lock().recv_timeout(Duration::from_secs(30));
            }
        })
        .unwrap();
    }

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices("A", rows(ROWS)).await?;
    let commit = tokio::spawn(async move { bulk.commit().await });
    wait_until("the racing commit to hold the commit lock", || {
        held.load(Ordering::SeqCst)
    })
    .await;
    // Let the load reach the lock and wait on it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(t.rows(), 0, "premise: nothing announced yet");
    commit.abort();
    let outcome = commit.await;
    assert!(
        outcome.as_ref().is_err_and(|e| e.is_cancelled()),
        "premise: commit() must be dropped while it waits, got {:?}",
        outcome.map(|r| r.map(|s| s.vertices_inserted))
    );
    release_tx.send(())?;
    let racer = racer_rx.recv_timeout(Duration::from_secs(10))?;
    racer.await??;
    fail::remove("bulk::events::before-announce");
    fail::remove("commit::after-flush-lock");

    let deadline = Instant::now() + Duration::from_secs(20);
    while (t.rows() < ROWS || cdc.node_creates() < ROWS) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(t.rows(), ROWS, "after-trigger rows");
    assert_eq!(cdc.node_creates(), ROWS, "CDC NODE_CREATE rows");

    let later = commit_b(&db, 1).await?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cdc.delivered_version(later) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        cdc.delivered_version(later),
        "CDC halted: a commit after the load was never delivered"
    );
    drop(tx);
    Ok(())
}

/// A transaction commit that ends after its commit point without announcing
/// (as one dropped at the schema save after the commit does) is announced
/// with its mutations marked lost: watchers learn of it, and CDC halts rather
/// than deliver the next commit past a hole.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_abandoned_after_commit_point_halts_cdc_not_a_hole_resilience() -> Result<()> {
    let db = db().await?;
    let cdc = attach_gated_cdc(&db).await?;
    let mut watch = db.session().watch();

    fail::cfg("commit::abandon-after-commit-point", "1*return").unwrap();
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {x: 1})").await?;
    let abandoned = tx.commit().await;
    fail::remove("commit::abandon-after-commit-point");
    assert!(abandoned.is_err(), "premise: the seam ends the commit");
    let n = db
        .session()
        .query("MATCH (n:A) RETURN count(n) AS n")
        .await?;
    assert_eq!(
        n.rows()[0].get::<i64>("n")?,
        1,
        "premise: the commit point was passed"
    );

    let later = commit_b(&db, 2).await?;
    let first = tokio::time::timeout(Duration::from_secs(10), watch.next())
        .await?
        .expect("watch open");
    let second = tokio::time::timeout(Duration::from_secs(10), watch.next())
        .await?
        .expect("watch open");
    assert!(
        first.mutations_failed && first.version < later,
        "the abandoned commit must be announced, its mutations marked lost; got version {} \
         (mutations_failed {}) before the later commit's {later}",
        first.version,
        first.mutations_failed
    );
    assert_eq!(second.version, later);
    assert!(!second.mutations_failed);

    // CDC halted on the abandoned commit: the later one is not delivered.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !cdc.delivered_version(later),
        "CDC delivered the commit after an unannounced one: a silent hole"
    );
    Ok(())
}

/// Crash-recovery: the child loads under a declared trigger and dies once the
/// load is visible, before its events are delivered (or, reopening, part way
/// through the open's delivery). The next open delivers them.
#[cfg(all(unix, feature = "failpoints"))]
mod crash {
    use std::time::{Duration, Instant};

    use uni_db::{DataType, Uni};

    use super::rows;
    use crate::crash_harness;

    const ENTRY: &str = "triggers::bulk_events_delivery_resilience::crash::bulk_events_abort_child";
    const ROWS: usize = 50;
    /// The fork the fork scenario loads on.
    const FORK: &str = "loads";

    /// Declares `CREATE ON :A` with an action that writes an `:AuditLog`.
    const DECLARE: &str = r#"CALL uni.plugin.declareTrigger(
        'test.bulk_audit',
        'CREATE ON :A',
        'CREATE (:AuditLog {msg: "bulk"})'
    )"#;

    /// Child: see the scenarios below.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "child-process entry point; driven by run_child"]
    async fn bulk_events_abort_child() {
        let Some((scenario, path)) = crash_harness::child_env() else {
            return;
        };
        match scenario.as_str() {
            // Load under the declared trigger; die once the load is visible,
            // before it is announced.
            "load_then_crash" => {
                let db = Uni::open(path.to_string_lossy())
                    .build()
                    .await
                    .expect("open store");
                db.schema()
                    .label("A")
                    .property("x", DataType::Int64)
                    .apply()
                    .await
                    .expect("schema");
                let tx = db.session().tx().await.expect("tx");
                tx.execute(DECLARE).await.expect("declare trigger");
                tx.commit().await.expect("commit declaration");
                let tx = db.session().tx().await.expect("tx");
                let mut bulk = tx.bulk_writer().build().expect("bulk writer");
                bulk.insert_vertices("A", rows(ROWS)).await.expect("insert");
                crash_harness::abort_at("bulk::events::before-announce");
                let _ = bulk.commit().await;
            }
            // Reopen after `load_then_crash`; die as the open's delivery
            // reaches the load's first chunk, before any trigger fired.
            "reopen_then_crash" => {
                crash_harness::abort_at("bulk::events::before-chunk");
                let _db = Uni::open(path.to_string_lossy())
                    .build()
                    .await
                    .expect("reopen store");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            // A load on a fork; die once it is visible, before it is
            // announced.
            "fork_load_then_crash" => {
                let db = Uni::open(path.to_string_lossy())
                    .build()
                    .await
                    .expect("open store");
                db.schema()
                    .label("A")
                    .property("x", DataType::Int64)
                    .apply()
                    .await
                    .expect("schema");
                let fork = db.session().fork(FORK).await.expect("fork");
                let tx = fork.tx().await.expect("fork tx");
                let mut bulk = tx.bulk_writer().build().expect("bulk writer");
                bulk.insert_vertices("A", rows(ROWS)).await.expect("insert");
                crash_harness::abort_at("bulk::events::before-announce");
                let _ = bulk.commit().await;
            }
            other => crash_harness::unknown_scenario(ENTRY, other),
        }
    }

    async fn count(db: &Uni, label: &str) -> i64 {
        db.session()
            .query(&format!("MATCH (n:{label}) RETURN count(n) AS n"))
            .await
            .expect("count")
            .rows()[0]
            .get::<i64>("n")
            .expect("n")
    }

    /// The bulk-load marker files under the database root.
    fn markers(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        std::fs::read_dir(root.join("storage/catalog/bulk_flush_intents"))
            .or_else(|_| std::fs::read_dir(root.join("catalog/bulk_flush_intents")))
            .map(|d| d.filter_map(|e| e.ok().map(|e| e.path())).collect())
            .unwrap_or_default()
    }

    /// Wait for the declared trigger's action to have run, then for the
    /// load's marker to be finished.
    async fn assert_delivered(db: &Uni, root: &std::path::Path) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while count(db, "AuditLog").await == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            count(db, "AuditLog").await >= 1,
            "the declared trigger never fired for the recovered load"
        );
        while !markers(root).is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            markers(root).is_empty(),
            "the marker outlived the delivery: {:?}",
            markers(root)
        );
    }

    /// The load survives the crash and the open delivers its events.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovered_load_delivers_its_events_at_open_resilience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("store");
        crash_harness::run_child_async(ENTRY, "load_then_crash", &root).await;

        let db = Uni::open(root.to_string_lossy())
            .build()
            .await
            .expect("reopen");
        assert_eq!(count(&db, "A").await, ROWS as i64, "the load is applied");
        assert_delivered(&db, &root).await;
    }

    /// A crash during the open's delivery keeps the marker, and the open
    /// after that delivers the events.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn crash_during_the_opens_delivery_delivers_at_the_next_open_resilience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("store");
        crash_harness::run_child_async(ENTRY, "load_then_crash", &root).await;
        crash_harness::run_child_async(ENTRY, "reopen_then_crash", &root).await;
        assert_eq!(markers(&root).len(), 1, "premise: the marker survives");

        let db = Uni::open(root.to_string_lossy())
            .build()
            .await
            .expect("reopen");
        assert_delivered(&db, &root).await;
    }

    /// A fork's recovered load fires the database's triggers when the fork is
    /// opened, as an ordinary fork load does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovered_fork_load_fires_triggers_when_the_fork_opens_resilience() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("store");
        crash_harness::run_child_async(ENTRY, "fork_load_then_crash", &root).await;

        let db = Uni::open(root.to_string_lossy())
            .build()
            .await
            .expect("reopen");
        let t = super::after_trigger();
        super::register_trigger(&db, "fork-recovered", t.clone()).expect("register trigger");
        let fork = db.session().fork(FORK).await.expect("reopen fork");
        let deadline = Instant::now() + Duration::from_secs(20);
        while t.rows() < ROWS && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(t.rows(), ROWS, "the recovered fork load's NODE_CREATE rows");
        let n = fork
            .query("MATCH (n:A) RETURN count(n) AS n")
            .await
            .expect("fork count")
            .rows()[0]
            .get::<i64>("n")
            .expect("n");
        assert_eq!(n, ROWS as i64, "the fork load is applied");
    }
}
