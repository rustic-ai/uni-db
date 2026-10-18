// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// `FireMode::Async` after-commit triggers must run off the executor and be
// tracked by the shutdown handle (W1 follow-up).
//
// `TriggerRouter::dispatch_after` used to `runtime.spawn` an async block that
// called the plugin's synchronous `fire` inline. On a `current_thread` runtime
// that one call held the only executor thread for the whole fire, stalling
// every other task; and the task was detached, so it kept running after the
// database was dropped and shutdown could neither wait for it nor count it.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use uni_common::{Value, Vid};
use uni_store::runtime::l0::L0Buffer;

use uni_plugin::errors::FnError;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};

use uni_plugin_host::shutdown::ShutdownHandle;
use uni_plugin_host::triggers::{DeferralQueue, EcQueue, MutationEvents, TriggerRouter};

/// How long the test trigger's `fire` blocks its thread.
const SLOW: Duration = Duration::from_secs(2);

/// An `Async` after-commit trigger whose `fire` blocks for [`SLOW`].
struct SlowTrigger {
    sub: TriggerSubscription,
    started: Arc<AtomicUsize>,
    finished: Arc<AtomicUsize>,
}

impl TriggerPlugin for SlowTrigger {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(SLOW);
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

/// A router holding one [`SlowTrigger`], plus its start/finish counters.
fn slow_router(
    tracker: Option<ShutdownHandle>,
) -> (TriggerRouter, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let trigger = Arc::new(SlowTrigger {
        sub: TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events: TriggerEventMask::NODE_UPDATE,
            labels: None,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Async,
            docs: "slow".to_owned(),
        },
        started: Arc::clone(&started),
        finished: Arc::clone(&finished),
    });
    let registry = Arc::new(PluginRegistry::new());
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new("test"), &caps, &registry);
    r.trigger(trigger as Arc<dyn TriggerPlugin>).unwrap();
    r.commit_to_registry().unwrap();

    let queue = DeferralQueue::new();
    let ec_queue = EcQueue::new(Some(Arc::clone(&queue)), Duration::from_secs(1), 10_000);
    let mut router =
        TriggerRouter::from_registry_with_queue(&registry, Some(queue), ec_queue).unwrap();
    if let Some(tracker) = tracker {
        router = router.with_task_tracker(tracker);
    }
    (router, started, finished)
}

/// One vertex write, which the trigger's `NODE_UPDATE` mask matches.
fn one_vertex_events() -> MutationEvents {
    let mut l0 = L0Buffer::new(0, None);
    let mut props = HashMap::new();
    props.insert("p".to_owned(), Value::Int(1));
    l0.insert_vertex(Vid::new(7), props);
    MutationEvents::from_l0(&l0)
}

/// Waits (yielding) until `counter` reaches 1.
async fn wait_for(counter: &AtomicUsize, what: &str) {
    let t = Instant::now();
    while counter.load(Ordering::SeqCst) == 0 {
        assert!(
            t.elapsed() < Duration::from_secs(10),
            "{what} never happened"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// A slow `Async` fire must not stall unrelated tasks on a `current_thread`
/// runtime: an unrelated short timer loop finishes while the fire is still
/// in flight.
#[tokio::test]
async fn slow_async_trigger_does_not_stall_the_runtime() {
    let (router, started, finished) = slow_router(None);
    router.dispatch_after(
        TriggerContext::new("s1", 1),
        &one_vertex_events(),
        &tokio::runtime::Handle::current(),
    );

    let t0 = Instant::now();
    let unrelated = tokio::spawn(async {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    unrelated.await.unwrap();
    let elapsed = t0.elapsed();

    assert_eq!(
        started.load(Ordering::SeqCst),
        1,
        "control: the trigger must have started firing during the window"
    );
    assert!(
        elapsed < SLOW / 2,
        "an unrelated task took {elapsed:?} with a {SLOW:?} async trigger in flight: \
         the fire is blocking the runtime"
    );
    assert_eq!(
        finished.load(Ordering::SeqCst),
        0,
        "the unrelated task must finish while the fire is still in flight"
    );
    wait_for(&finished, "the fire finishing").await;
}

/// An `Async` fire is a tracked task: it is counted while running, and
/// shutdown waits for it instead of returning with it still in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_trigger_fire_is_tracked_and_joined_by_shutdown() {
    let shutdown = ShutdownHandle::new(Duration::from_secs(30));
    let (router, started, finished) = slow_router(Some(shutdown.clone()));
    router.dispatch_after(
        TriggerContext::new("s1", 1),
        &one_vertex_events(),
        &tokio::runtime::Handle::current(),
    );
    wait_for(&started, "the fire starting").await;
    assert_eq!(
        shutdown.live_task_count(),
        1,
        "the in-flight fire is not counted"
    );

    shutdown.shutdown_async().await.unwrap();
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "shutdown returned while the async trigger was still firing"
    );
    assert_eq!(shutdown.live_task_count(), 0);
}
