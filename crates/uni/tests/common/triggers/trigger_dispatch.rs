#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5f acceptance — host-side dispatch of `TriggerPlugin`s.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use smol_str::SmolStr;
use uni_db::{DataType, Uni, UniError};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{
    AbiRange, Capability, CapabilitySet, Determinism, FnError, PluginId, PluginManifest,
    PluginRegistrar, ProvidedSurfaces, Scope, SideEffects as PluginSideEffects,
};

// ── Test trigger fixtures ──────────────────────────────────────────

/// Increments a shared counter by the number of rows in each batch.
struct CountingTrigger {
    sub: TriggerSubscription,
    counter: Arc<AtomicU64>,
}

impl CountingTrigger {
    fn new(
        phase: TriggerPhase,
        events: TriggerEventMask,
        labels: Option<Vec<SmolStr>>,
        fire_mode: FireMode,
        docs: &str,
    ) -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let sub = TriggerSubscription {
            phase,
            events,
            labels,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode,
            docs: docs.to_owned(),
        };
        (
            Self {
                sub,
                counter: counter.clone(),
            },
            counter,
        )
    }
}

impl TriggerPlugin for CountingTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        let n = events.events.num_rows() as u64;
        self.counter.fetch_add(n, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

/// Always returns `TriggerOutcome::Reject` — used for the
/// synchronous-before-commit abort test.
struct RejectingTrigger {
    sub: TriggerSubscription,
    reason: String,
}

impl RejectingTrigger {
    fn new(reason: &str) -> Self {
        let mut sub = TriggerSubscription {
            phase: TriggerPhase::BeforeCommit,
            events: TriggerEventMask::NODE_CREATE
                .union(TriggerEventMask::NODE_UPDATE)
                .union(TriggerEventMask::NODE_DELETE),
            labels: None,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: "RejectingTrigger".to_owned(),
        };
        sub.fire_mode = FireMode::Synchronous;
        Self {
            sub,
            reason: reason.to_owned(),
        }
    }
}

impl TriggerPlugin for RejectingTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        Ok(TriggerOutcome::Reject {
            reason: self.reason.clone(),
        })
    }
}

/// Sleeps for `delay`, then increments a counter. Used for the
/// `Async` fire-mode test.
struct DelayedAsyncTrigger {
    sub: TriggerSubscription,
    counter: Arc<AtomicU64>,
    delay: Duration,
}

impl DelayedAsyncTrigger {
    fn new(delay: Duration) -> (Self, Arc<AtomicU64>) {
        let counter = Arc::new(AtomicU64::new(0));
        let sub = TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events: TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
            labels: None,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Async,
            docs: "DelayedAsyncTrigger".to_owned(),
        };
        (
            Self {
                sub,
                counter: counter.clone(),
                delay,
            },
            counter,
        )
    }
}

impl TriggerPlugin for DelayedAsyncTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        // Synchronous sleep is fine — an `Async` fire runs on the blocking
        // pool, so blocking here stalls neither the writer nor the executor.
        std::thread::sleep(self.delay);
        let n = events.events.num_rows() as u64;
        self.counter.fetch_add(n, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

/// Panics whenever fired — used for the after-phase panic-catch test.
struct PanickingTrigger {
    sub: TriggerSubscription,
}

impl PanickingTrigger {
    fn new() -> Self {
        let sub = TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events: TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
            labels: None,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: "PanickingTrigger".to_owned(),
        };
        Self { sub }
    }
}

impl TriggerPlugin for PanickingTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        panic!("intentional panic from PanickingTrigger")
    }
}

// ── Test harness ───────────────────────────────────────────────────

fn register_trigger(
    uni: &Uni,
    plugin_id: &str,
    trigger: Arc<dyn TriggerPlugin>,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let manifest = PluginManifest {
        id: PluginId::new(plugin_id),
        version: "1.0.0".parse().expect("static version parses"),
        abi: AbiRange::parse("^1").expect("ABI range valid"),
        depends_on: vec![],
        capabilities: caps.clone(),
        determinism: Determinism::Pure,
        side_effects: PluginSideEffects::ReadOnly,
        scope: Scope::Instance,
        hash: None,
        signature: None,
        provides: ProvidedSurfaces::default(),
        docs: format!("Test trigger plugin '{plugin_id}'."),
        metadata: std::collections::BTreeMap::new(),
    };
    let _ = manifest;
    let mut r = PluginRegistrar::new(PluginId::new(plugin_id), &caps, registry);
    r.trigger(trigger)?;
    r.commit_to_registry()?;
    Ok(())
}

async fn db_with_audit_schema() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("_AuditMe")
        .property("x", DataType::Int64)
        .label("Other")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    Ok(db)
}

// ── Tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn trigger_fires_on_matching_label() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, counter) = CountingTrigger::new(
        TriggerPhase::AfterCommit,
        TriggerEventMask::NODE_CREATE
            .union(TriggerEventMask::NODE_UPDATE)
            .union(TriggerEventMask::NODE_DELETE),
        Some(vec![SmolStr::new("_AuditMe")]),
        FireMode::Synchronous,
        "match-audit",
    );
    register_trigger(&db, "test-match-label", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.execute("CREATE (:_AuditMe {x: 2})").await?;
    tx.commit().await?;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "trigger should fire once per matching-label row"
    );
    Ok(())
}

#[tokio::test]
async fn trigger_skips_non_matching_label() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, counter) = CountingTrigger::new(
        TriggerPhase::AfterCommit,
        TriggerEventMask::NODE_CREATE
            .union(TriggerEventMask::NODE_UPDATE)
            .union(TriggerEventMask::NODE_DELETE),
        Some(vec![SmolStr::new("_AuditMe")]),
        FireMode::Synchronous,
        "skip-other",
    );
    register_trigger(&db, "test-skip-label", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Other {x: 1})").await?;
    tx.commit().await?;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "label-filtered trigger must NOT fire on non-matching label"
    );
    Ok(())
}

#[tokio::test]
async fn synchronous_before_commit_reject_aborts_tx() -> Result<()> {
    let db = db_with_audit_schema().await?;
    register_trigger(
        &db,
        "test-reject",
        Arc::new(RejectingTrigger::new("policy violation")),
    )?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    let err = tx
        .commit()
        .await
        .expect_err("commit should be rejected by trigger");

    match err {
        UniError::TriggerRejected { reason, trigger } => {
            assert!(
                reason.contains("policy violation"),
                "reason should propagate: {reason}"
            );
            assert!(!trigger.is_empty(), "trigger name should be populated");
        }
        other => panic!("expected TriggerRejected, got {other:?}"),
    }

    // The mutation must not be visible — open a fresh read session and
    // confirm zero `:_AuditMe` nodes landed.
    let rows = db
        .session()
        .query("MATCH (n:_AuditMe) RETURN count(n) AS n")
        .await?;
    let n: i64 = rows.rows()[0].get("n")?;
    assert_eq!(n, 0, "rejected tx must not leave any mutations behind");
    Ok(())
}

#[tokio::test]
async fn async_fire_mode_does_not_block_commit() -> Result<()> {
    let db = db_with_audit_schema().await?;
    // Use a long trigger delay so the assertion does not depend on absolute
    // commit latency. The previous version asserted `commit < 120ms`, which
    // conflated "non-blocking" with "fast, uncontended machine" and flaked on
    // loaded CI runners (a trivial commit can take ~1s under full-core
    // nextest contention). The real invariant — commit does not wait for the
    // async trigger's work — is tested directly below: with a 3s delay, a
    // non-blocking commit returns long before the trigger's work lands, so the
    // counter is still zero; a blocking commit would have waited the full 3s
    // and observed counter == 1.
    let delay = Duration::from_secs(3);
    let (trigger, counter) = DelayedAsyncTrigger::new(delay);
    register_trigger(&db, "test-async", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;

    let before = Instant::now();
    tx.commit().await?;
    let elapsed = before.elapsed();

    // Machine-speed-independent proof that commit did not block on the async
    // trigger: its delayed work cannot have completed yet.
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "async trigger fired during commit — commit blocked on it (elapsed {elapsed:?})"
    );
    // Secondary sanity: commit returned before the trigger delay elapsed.
    assert!(
        elapsed < delay,
        "commit took {elapsed:?}, not clearly faster than the {delay:?} async trigger"
    );

    // The spawned task must still eventually fire — poll past the delay.
    let deadline = Instant::now() + delay + Duration::from_secs(2);
    while Instant::now() < deadline {
        if counter.load(Ordering::SeqCst) >= 1 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "async trigger never fired (counter = {})",
        counter.load(Ordering::SeqCst)
    );
}

/// An in-flight `Async` fire is a background task of the database: counted by
/// `background_task_count` and waited for by `shutdown_in_place`.
///
/// It was a detached `tokio::spawn`, so shutdown returned with the plugin
/// still running against a database that was gone, and nothing counted it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_fire_is_counted_and_awaited_by_shutdown() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, counter) = DelayedAsyncTrigger::new(Duration::from_secs(1));
    register_trigger(&db, "test-async-tracked", Arc::new(trigger))?;
    let baseline = db.background_task_count();

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;
    assert_eq!(
        db.background_task_count(),
        baseline + 1,
        "the in-flight async trigger fire is not counted"
    );

    db.shutdown_in_place().await?;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "shutdown returned while the async trigger was still firing"
    );
    assert_eq!(db.background_task_count(), 0);
    Ok(())
}

/// An `Async` fire started by a commit on a fork is a background task of the
/// database too: counted by the primary and waited for by its shutdown.
///
/// A fork's `UniInner` got its own `ShutdownHandle`, which nothing ever
/// signalled or awaited, so the fire was tracked by a handle no shutdown path
/// could reach and kept running after `shutdown_in_place` returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_fire_on_a_fork_is_counted_and_awaited_by_shutdown() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, counter) = DelayedAsyncTrigger::new(Duration::from_secs(1));
    register_trigger(&db, "test-async-fork", Arc::new(trigger))?;
    let baseline = db.background_task_count();

    let fork = db.session().fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;
    drop(fork);
    assert_eq!(
        db.background_task_count(),
        baseline + 1,
        "the in-flight async trigger fire on the fork is not counted"
    );

    db.shutdown_in_place().await?;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "shutdown returned while the fork's async trigger was still firing"
    );
    assert_eq!(db.background_task_count(), 0);
    Ok(())
}

/// The same fire is waited for by a bare `drop` of the database.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_fire_on_a_fork_is_awaited_by_drop() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, counter) = DelayedAsyncTrigger::new(Duration::from_secs(1));
    register_trigger(&db, "test-async-fork-drop", Arc::new(trigger))?;

    let fork = db.session().fork("f").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;
    drop(fork);
    drop(db);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "drop returned while the fork's async trigger was still firing"
    );
    Ok(())
}

#[tokio::test]
async fn after_phase_panic_is_caught() -> Result<()> {
    let db = db_with_audit_schema().await?;
    register_trigger(&db, "test-panic", Arc::new(PanickingTrigger::new()))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;

    // Commit should NOT propagate the trigger's panic. If panic-catch
    // is broken, this awaits forever / aborts the test runner.
    tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn event_kind_selector_filters_correctly() -> Result<()> {
    let db = db_with_audit_schema().await?;
    // Subscribe to NODE_DELETE only. L0 extraction reliably
    // distinguishes deletes (via `vertex_tombstones`) from writes,
    // and post-D1 the CREATE/UPDATE distinction is wired too via the
    // committed-state probe in `triggers::PreExistingProbe`.
    let (trigger, counter) = CountingTrigger::new(
        TriggerPhase::AfterCommit,
        TriggerEventMask::NODE_DELETE,
        None,
        FireMode::Synchronous,
        "delete-only",
    );
    register_trigger(&db, "test-delete-only", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "NODE_DELETE-only trigger must NOT fire on CREATE-only commits"
    );
    Ok(())
}

/// Records each fired event's kind discriminant + raw `old_value`
/// bytes so the CREATE-vs-UPDATE + old_value tests can introspect
/// the exact RecordBatch the dispatcher built.
struct RecordingTrigger {
    sub: TriggerSubscription,
    records: Arc<std::sync::Mutex<Vec<(u8, Option<Vec<u8>>)>>>,
}

impl RecordingTrigger {
    fn new(
        phase: TriggerPhase,
        events: TriggerEventMask,
        labels: Option<Vec<SmolStr>>,
        fire_mode: FireMode,
        docs: &str,
    ) -> (Self, Arc<std::sync::Mutex<Vec<(u8, Option<Vec<u8>>)>>>) {
        let records = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sub = TriggerSubscription {
            phase,
            events,
            labels,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode,
            docs: docs.to_owned(),
        };
        (
            Self {
                sub,
                records: records.clone(),
            },
            records,
        )
    }
}

impl TriggerPlugin for RecordingTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        use arrow_array::{Array, LargeBinaryArray, UInt8Array};
        let batch = events.events.as_ref();
        let kinds = batch
            .column_by_name("event_kind")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
            .expect("event_kind column");
        let olds = batch
            .column_by_name("old_value")
            .and_then(|c| c.as_any().downcast_ref::<LargeBinaryArray>())
            .expect("old_value column");
        let mut g = self.records.lock().expect("records mutex");
        for i in 0..batch.num_rows() {
            let k = kinds.value(i);
            let old = if olds.is_null(i) {
                None
            } else {
                Some(olds.value(i).to_vec())
            };
            g.push((k, old));
        }
        Ok(TriggerOutcome::Continue)
    }
}

/// `mask_to_discriminant` mapping — kept in sync with
/// `triggers::mask_to_discriminant`. Hard-coded here so the test
/// stays robust against future bit-position rearrangements (which
/// the unit test in `triggers.rs` already pins).
const NODE_CREATE_DISC: u8 = 1;
const NODE_UPDATE_DISC: u8 = 2;

#[tokio::test]
async fn create_emits_node_create_discriminant() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, records) = RecordingTrigger::new(
        TriggerPhase::AfterCommit,
        TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
        Some(vec![SmolStr::new("_AuditMe")]),
        FireMode::Synchronous,
        "record-create-vs-update",
    );
    register_trigger(&db, "test-create-discriminant", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    let g = records.lock().expect("records mutex");
    assert!(!g.is_empty(), "trigger should record at least one row");
    assert!(
        g.iter().all(|(k, _)| *k == NODE_CREATE_DISC),
        "fresh vertex write must emit NODE_CREATE, got {:?}",
        g.iter().map(|(k, _)| *k).collect::<Vec<_>>()
    );
    Ok(())
}

#[tokio::test]
async fn update_emits_node_update_and_populates_old_value_before_commit() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, records) = RecordingTrigger::new(
        TriggerPhase::BeforeCommit,
        TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
        Some(vec![SmolStr::new("_AuditMe")]),
        FireMode::Synchronous,
        "record-old-value",
    );
    register_trigger(&db, "test-old-value", Arc::new(trigger))?;

    // Tx 1: create the vertex; trigger sees CREATE with no pre-image.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    // Reset records — we only care about the update.
    records.lock().expect("records mutex").clear();

    // Tx 2: update the same vertex; trigger must see UPDATE with
    // populated old_value bytes.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:_AuditMe {x: 1}) SET n.x = 2").await?;
    tx.commit().await?;

    let g = records.lock().expect("records mutex");
    assert!(!g.is_empty(), "trigger should record at least one row");
    let (kind, old) = &g[0];
    assert_eq!(
        *kind, NODE_UPDATE_DISC,
        "second write to pre-existing vid must emit NODE_UPDATE"
    );
    let old_bytes = old
        .as_ref()
        .expect("BeforeCommit dispatch with a non-empty L0 probe must populate old_value bytes");
    let parsed: serde_json::Value =
        serde_json::from_slice(old_bytes).expect("old_value is JSON-serialized Properties");
    assert!(
        parsed.is_object(),
        "old_value should deserialize into a properties map"
    );
    Ok(())
}

/// Workstream F acceptance — `PreExistingProbe::extend_with_l1` ensures
/// that a vertex flushed to L1 in a previous commit is classified as
/// `NODE_UPDATE` (not `NODE_CREATE`) when subsequently mutated.
/// Without the L1 probe, the L0 chain misses the vertex (it's been
/// drained from L0 into L1) and the legacy probe path would
/// misclassify the next mutation as CREATE.
#[tokio::test]
async fn update_to_flushed_vertex_emits_node_update() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, records) = RecordingTrigger::new(
        TriggerPhase::AfterCommit,
        TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
        Some(vec![SmolStr::new("_AuditMe")]),
        FireMode::Synchronous,
        "record-after-flush",
    );
    register_trigger(&db, "test-l1-probe", Arc::new(trigger))?;

    // Tx 1: create the vertex.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    // Force a flush so the vertex moves from L0 into L1. Subsequent
    // commits will see L0 as empty for this vid and rely on the L1
    // probe to detect pre-existence.
    db.flush().await?;

    records.lock().expect("records mutex").clear();

    // Tx 2: update the same vertex — must emit NODE_UPDATE, not
    // NODE_CREATE, because the L1 probe detects pre-existence.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:_AuditMe {x: 1}) SET n.x = 99").await?;
    tx.commit().await?;

    let g = records.lock().expect("records mutex");
    assert!(!g.is_empty(), "trigger should record at least one row");
    assert!(
        g.iter().all(|(k, _)| *k == NODE_UPDATE_DISC),
        "post-flush update must emit NODE_UPDATE (L1 probe detected pre-existence); got {:?}",
        g.iter().map(|(k, _)| *k).collect::<Vec<_>>()
    );
    // M5f.2 acceptance — every UPDATE row must carry a populated
    // `old_value` (the L1 probe now fetches the full property image,
    // not just the vid). The serialized JSON must round-trip back to
    // a map containing the pre-tx `x: 1` value (the value before the
    // SET that set `x = 99`).
    for (_, old) in g.iter() {
        let bytes = old
            .as_ref()
            .expect("post-flush UPDATE must populate old_value via L1 pre-image probe");
        let parsed: serde_json::Value =
            serde_json::from_slice(bytes).expect("old_value should be JSON-serialized Properties");
        let obj = parsed
            .as_object()
            .expect("old_value should decode to a map");
        let x = obj
            .get("x")
            .expect("pre-image must include the `x` property");
        // The pre-image must reflect the tx-1 value (1), not the
        // tx-2 mutation (99).
        assert!(
            x.as_i64() == Some(1) || x.as_str() == Some("1"),
            "pre-image x should be 1 (pre-tx value), got {x:?}"
        );
    }
    Ok(())
}

/// The pre-image a trigger sees for a vertex is its user property set,
/// whether the vertex is still in L0 or was flushed to L1.
///
/// The L1 probe built the pre-image from every column of the per-label
/// table, so a flushed vertex's `old_value` carried the storage columns
/// (`overflow_json`, `_deleted`, `_uid`, `_created_at`, `_updated_at`)
/// beside its properties, and its undeclared properties sat nested inside
/// `overflow_json` instead of at top level. The L0 pre-image of the same
/// vertex is the reference; the two must decode to the same map.
#[tokio::test]
async fn update_pre_image_matches_between_l0_and_flushed_vertex() -> Result<()> {
    async fn pre_image(flush: bool) -> Result<serde_json::Value> {
        let db = db_with_audit_schema().await?;
        let (trigger, records) = RecordingTrigger::new(
            TriggerPhase::AfterCommit,
            TriggerEventMask::NODE_UPDATE,
            Some(vec![SmolStr::new("_AuditMe")]),
            FireMode::Synchronous,
            "record-pre-image",
        );
        register_trigger(&db, "test-pre-image", Arc::new(trigger))?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:_AuditMe {x: 1, u: 2, ext_id: 'e1'})")
            .await?;
        tx.commit().await?;
        if flush {
            db.flush().await?;
        }
        let tx = db.session().tx().await?;
        tx.execute("MATCH (n:_AuditMe {x: 1}) SET n.x = 99").await?;
        tx.commit().await?;
        let g = records.lock().expect("records mutex");
        assert_eq!(g.len(), 1, "one NODE_UPDATE expected, got {}", g.len());
        let bytes = g[0].1.as_ref().expect("an UPDATE carries a pre-image");
        Ok(serde_json::from_slice(bytes)?)
    }

    let unflushed = pre_image(false).await?;
    let flushed = pre_image(true).await?;
    assert_eq!(
        unflushed,
        serde_json::json!({"x": 1, "u": 2, "ext_id": "e1"}),
        "L0 pre-image reference"
    );
    assert_eq!(flushed, unflushed, "flushed pre-image differs from L0");
    Ok(())
}
