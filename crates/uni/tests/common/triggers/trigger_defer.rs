#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Workstream H acceptance — `TriggerOutcome::Defer` is enqueued into
//! the per-`Uni` [`DeferralQueue`] and re-fires on a subsequent tick
//! instead of being silently dropped.
//!
//! v1 limitations under test: in-memory only; re-deferral is capped;
//! persistent restart-survives backing is `TODO(M11-persist)`.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use smol_str::SmolStr;
use uni_db::{DataType, Uni};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerDeferral, TriggerEventMask, TriggerOutcome,
    TriggerPhase, TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

/// Trigger that defers the first N fires, then continues on attempt
/// N+1. The first-fire counter and final-fire counter are exposed so
/// the test can distinguish queue activity (re-fires) from initial
/// dispatch.
struct DeferringTrigger {
    sub: TriggerSubscription,
    initial_fires: Arc<AtomicU64>,
    final_fires: Arc<AtomicU64>,
    defers_remaining: Arc<AtomicU64>,
}

impl DeferringTrigger {
    fn new(defers_remaining: u64, phase: TriggerPhase) -> (Self, Arc<AtomicU64>, Arc<AtomicU64>) {
        let initial_fires = Arc::new(AtomicU64::new(0));
        let final_fires = Arc::new(AtomicU64::new(0));
        let defers_remaining = Arc::new(AtomicU64::new(defers_remaining));
        let sub = TriggerSubscription {
            phase,
            events: TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
            labels: Some(vec![SmolStr::new("_AuditMe")]),
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: "defer-then-continue".to_owned(),
        };
        (
            Self {
                sub,
                initial_fires: initial_fires.clone(),
                final_fires: final_fires.clone(),
                defers_remaining,
            },
            initial_fires,
            final_fires,
        )
    }
}

impl TriggerPlugin for DeferringTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        let remaining = self.defers_remaining.fetch_sub(1, Ordering::SeqCst);
        if remaining > 0 {
            self.initial_fires.fetch_add(1, Ordering::SeqCst);
            Ok(TriggerOutcome::Defer {
                until: TriggerDeferral::from_payload("v1-payload"),
            })
        } else {
            self.final_fires.fetch_add(1, Ordering::SeqCst);
            Ok(TriggerOutcome::Continue)
        }
    }
}

fn register_trigger(
    uni: &Uni,
    plugin_id: &str,
    trigger: Arc<dyn TriggerPlugin>,
) -> std::result::Result<(), uni_plugin::PluginError> {
    let registry = uni.plugin_registry();
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
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
        .apply()
        .await?;
    Ok(db)
}

async fn wait_for(predicate: impl Fn() -> bool, deadline: Duration) -> bool {
    let start = Instant::now();
    while Instant::now() - start < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    predicate()
}

#[tokio::test]
async fn defer_with_short_delay_fires_eventually() -> Result<()> {
    let db = db_with_audit_schema().await?;
    // Defer once, then continue — the queue tick (50ms) must re-fire
    // within the 2s deadline.
    let (trigger, initial, final_) = DeferringTrigger::new(1, TriggerPhase::BeforeCommit);
    register_trigger(&db, "test-defer-eventually", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    // First fire was a Defer (initial == 1, final == 0).
    assert!(
        initial.load(Ordering::SeqCst) >= 1,
        "initial fire should record the deferral"
    );

    // Wait up to 2s for the queue tick to re-fire the deferred item.
    let final_clone = Arc::clone(&final_);
    let fired = wait_for(
        move || final_clone.load(Ordering::SeqCst) >= 1,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        fired,
        "deferred trigger should re-fire via the queue tick (final = {})",
        final_.load(Ordering::SeqCst)
    );
    Ok(())
}

#[tokio::test]
async fn defer_retry_cap_drops_after_n() -> Result<()> {
    let db = db_with_audit_schema().await?;
    // Defer u64::MAX times — exceeds DEFER_MAX_ATTEMPTS. The queue
    // tick must drop the item after the cap with a warn (we can't
    // assert on the log, but we can assert the trigger stops getting
    // re-fired).
    let (trigger, initial, _final) = DeferringTrigger::new(u64::MAX, TriggerPhase::BeforeCommit);
    register_trigger(&db, "test-defer-cap", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    // Give the queue time to fire its cap-many retries (50ms tick *
    // 10 attempts = 500ms; double for safety margin).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let stable_initial = initial.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after_stable = initial.load(Ordering::SeqCst);

    assert_eq!(
        stable_initial, after_stable,
        "deferred trigger must stop re-firing after the cap (DEFER_MAX_ATTEMPTS); \
         got {stable_initial} then {after_stable}"
    );
    // The cap is DEFER_MAX_ATTEMPTS = 10, plus the initial fire that
    // saw the first Defer = 11. Allow some slack for tick alignment.
    assert!(
        stable_initial <= 20,
        "fire count should be bounded by the cap; got {stable_initial}"
    );
    Ok(())
}

// ── FU-5: TriggerDeferral::delay + on_deferred ──────────────────────

/// Trigger that defers exactly once with an explicit `delay`, then
/// continues. Exposes the fire-at-instant of each call so the test
/// can assert the elapsed gap.
struct DelayedDeferTrigger {
    sub: TriggerSubscription,
    fired_at: Arc<parking_lot::Mutex<Vec<Instant>>>,
    payloads_seen: Arc<parking_lot::Mutex<Vec<String>>>,
    defers_remaining: Arc<AtomicU64>,
    delay: Duration,
}

impl DelayedDeferTrigger {
    fn new(
        delay: Duration,
    ) -> (
        Self,
        Arc<parking_lot::Mutex<Vec<Instant>>>,
        Arc<parking_lot::Mutex<Vec<String>>>,
    ) {
        let fired_at = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let payloads_seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let sub = TriggerSubscription {
            phase: TriggerPhase::BeforeCommit,
            events: TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
            labels: Some(vec![SmolStr::new("_AuditMe")]),
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: "FU-5: delayed defer with payload".to_owned(),
        };
        (
            Self {
                sub,
                fired_at: fired_at.clone(),
                payloads_seen: payloads_seen.clone(),
                defers_remaining: Arc::new(AtomicU64::new(1)),
                delay,
            },
            fired_at,
            payloads_seen,
        )
    }
}

impl TriggerPlugin for DelayedDeferTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        self.fired_at.lock().push(Instant::now());
        if self.defers_remaining.fetch_sub(1, Ordering::SeqCst) > 0 {
            Ok(TriggerOutcome::Defer {
                until: TriggerDeferral::after("resume-marker", self.delay),
            })
        } else {
            Ok(TriggerOutcome::Continue)
        }
    }

    fn on_deferred(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationBatch,
        payload: &str,
    ) -> Result<TriggerOutcome, FnError> {
        self.payloads_seen.lock().push(payload.to_owned());
        // Delegate to fire() for the actual re-fire bookkeeping.
        self.fire(ctx, events)
    }
}

/// FU-5: `TriggerDeferral::after(payload, delay)` causes the deferral
/// queue to wait at least `delay` before re-invoking the trigger.
#[tokio::test]
async fn defer_with_explicit_delay_waits_at_least_that_long() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, fired_at, _payloads) = DelayedDeferTrigger::new(Duration::from_millis(400));
    register_trigger(&db, "test-defer-delayed", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    // Wait up to 2 s for the second fire.
    let fired_at_clone = Arc::clone(&fired_at);
    let two_fires = wait_for(
        move || fired_at_clone.lock().len() >= 2,
        Duration::from_secs(2),
    )
    .await;
    assert!(two_fires, "deferred trigger must re-fire within 2 s");

    let stamps = fired_at.lock().clone();
    let elapsed = stamps[1].duration_since(stamps[0]);
    assert!(
        elapsed >= Duration::from_millis(350),
        "re-fire must wait at least ~350 ms (allowing ~50 ms tick alignment); got {elapsed:?}"
    );
    Ok(())
}

/// FU-5: `TriggerPlugin::on_deferred` receives the `payload` set by
/// the first `fire()` call.
#[tokio::test]
async fn on_deferred_receives_payload_from_first_fire() -> Result<()> {
    let db = db_with_audit_schema().await?;
    let (trigger, fired_at, payloads_seen) = DelayedDeferTrigger::new(Duration::from_millis(50));
    register_trigger(&db, "test-defer-payload", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    let fired_at_clone = Arc::clone(&fired_at);
    let _ = wait_for(
        move || fired_at_clone.lock().len() >= 2,
        Duration::from_secs(2),
    )
    .await;

    let seen = payloads_seen.lock().clone();
    assert!(
        seen.iter().any(|p| p == "resume-marker"),
        "on_deferred should have observed the `resume-marker` payload from the first fire; got {seen:?}"
    );
    Ok(())
}

/// Defers its first fire; its `on_deferred` then blocks its thread for `hold`.
struct SlowOnDeferred {
    sub: TriggerSubscription,
    hold: Duration,
    started: Arc<AtomicU64>,
    finished: Arc<AtomicU64>,
}

impl TriggerPlugin for SlowOnDeferred {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        Ok(TriggerOutcome::Defer {
            until: TriggerDeferral::from_payload("slow"),
        })
    }

    fn on_deferred(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
        _payload: &str,
    ) -> Result<TriggerOutcome, FnError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(self.hold);
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

/// The deferral tick runs deferred fires off the executor.
///
/// A tick calls each due item's synchronous `on_deferred` and rewrites the
/// fsynced sidecar. Run inline in the tick task, a slow plugin held the
/// executor thread for its whole call, which on a `current_thread` runtime
/// (this test's) stalls every other task, the caller's own included. The
/// caller here keeps a 10 ms timer running while the slow fire is in flight
/// and records the longest gap between its wake-ups.
#[tokio::test]
async fn a_slow_deferred_fire_does_not_stall_a_current_thread_runtime() -> Result<()> {
    const HOLD: Duration = Duration::from_secs(2);
    let db = db_with_audit_schema().await?;
    let started = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(AtomicU64::new(0));
    let trigger = SlowOnDeferred {
        sub: TriggerSubscription {
            phase: TriggerPhase::BeforeCommit,
            events: TriggerEventMask::NODE_CREATE.union(TriggerEventMask::NODE_UPDATE),
            labels: Some(vec![SmolStr::new("_AuditMe")]),
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: "slow-on-deferred".to_owned(),
        },
        hold: HOLD,
        started: Arc::clone(&started),
        finished: Arc::clone(&finished),
    };
    register_trigger(&db, "test-slow-deferred", Arc::new(trigger))?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1})").await?;
    tx.commit().await?;

    let t = Instant::now();
    let mut worst_gap = Duration::ZERO;
    while finished.load(Ordering::SeqCst) == 0 {
        assert!(
            t.elapsed() < HOLD * 5,
            "the deferred fire never completed (started: {})",
            started.load(Ordering::SeqCst)
        );
        let before = Instant::now();
        tokio::time::sleep(Duration::from_millis(10)).await;
        worst_gap = worst_gap.max(before.elapsed());
    }
    assert!(
        worst_gap < HOLD / 2,
        "a deferred fire stalled the runtime for {worst_gap:?}: the deferral tick ran it \
         on the executor thread"
    );
    Ok(())
}
