// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Concurrent `Defer` outcomes on the commit path, with sidecar persistence on.
//
// `DeferralQueue::push` runs inside `dispatch_before`, i.e. on the commit path,
// and used to rewrite and fsync the sidecar while holding the queue's mutex.
// Concurrent commits that defer then serialized on one another's fsyncs, and
// every other user of the queue (the deferral tick, `pending`) waited behind
// them. This drives that path from several threads, reports the latencies it
// saw, and checks the one thing that must not change: every deferral that
// `dispatch_before` acknowledged is in the sidecar afterwards.

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use uni_common::{Value, Vid};
use uni_store::runtime::l0::L0Buffer;

use uni_plugin::errors::FnError;
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerDeferral, TriggerEventMask, TriggerOutcome,
    TriggerPhase, TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar, PluginRegistry};

use uni_plugin_host::triggers::{DeferralQueue, EcQueue, MutationEvents, TriggerRouter};

/// Committing threads.
const THREADS: usize = 8;
/// Deferring dispatches per thread.
const PER_THREAD: usize = 25;

/// A before-commit trigger that defers every fire far into the future, so the
/// queue only grows while the test runs.
struct AlwaysDefer {
    sub: TriggerSubscription,
}

impl TriggerPlugin for AlwaysDefer {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        _events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        let mut until = TriggerDeferral::from_payload("p");
        until.delay = Some(Duration::from_secs(3600));
        Ok(TriggerOutcome::Defer { until })
    }
}

fn registry() -> Arc<PluginRegistry> {
    let registry = Arc::new(PluginRegistry::new());
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new("defer-probe"), &caps, &registry);
    r.trigger(Arc::new(AlwaysDefer {
        sub: TriggerSubscription {
            phase: TriggerPhase::BeforeCommit,
            events: TriggerEventMask::NODE_UPDATE,
            labels: None,
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: "always-defer".to_owned(),
        },
    }) as Arc<dyn TriggerPlugin>)
        .unwrap();
    r.commit_to_registry().unwrap();
    registry
}

/// Nearest-rank percentile of an ascending slice.
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

#[test]
fn concurrent_deferrals_are_all_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry();
    let queue = DeferralQueue::with_persistence(dir.path().to_path_buf());
    let ec_queue = EcQueue::new(Some(Arc::clone(&queue)), Duration::from_secs(1), 10_000);
    let router = Arc::new(
        TriggerRouter::from_registry_with_queue(&registry, Some(Arc::clone(&queue)), ec_queue)
            .unwrap(),
    );

    let mut l0 = L0Buffer::new(0, None);
    l0.insert_vertex(
        Vid::new(7),
        HashMap::from([("p".to_owned(), Value::Int(1))]),
    );
    let events = Arc::new(MutationEvents::from_l0(&l0));

    // A reader that only takes the queue lock, the way the deferral tick and
    // `pending` do; its worst wait is how long a push held the lock.
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let (queue, stop) = (Arc::clone(&queue), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut worst = Duration::ZERO;
            while !stop.load(Ordering::SeqCst) {
                let t = Instant::now();
                let _ = queue.pending();
                worst = worst.max(t.elapsed());
                std::thread::sleep(Duration::from_micros(200));
            }
            worst
        })
    };

    let wall = Instant::now();
    let workers: Vec<_> = (0..THREADS)
        .map(|t| {
            let (router, events) = (Arc::clone(&router), Arc::clone(&events));
            std::thread::spawn(move || {
                let session = format!("session-{t}");
                (0..PER_THREAD)
                    .map(|i| {
                        let start = Instant::now();
                        router
                            .dispatch_before(TriggerContext::new(&session, i as u64), &events)
                            .unwrap();
                        start.elapsed()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut latencies: Vec<Duration> = workers
        .into_iter()
        .flat_map(|w| w.join().unwrap())
        .collect();
    let wall = wall.elapsed();
    stop.store(true, Ordering::SeqCst);
    let reader_worst = reader.join().unwrap();

    latencies.sort();
    eprintln!(
        "deferral push: {THREADS}x{PER_THREAD} dispatches in {wall:?}; latency p50 {:?} \
         p99 {:?} max {:?}; worst queue-lock wait seen by a reader {reader_worst:?}",
        percentile(&latencies, 50.0),
        percentile(&latencies, 99.0),
        percentile(&latencies, 100.0),
    );

    let expected = THREADS * PER_THREAD;
    assert_eq!(queue.pending(), expected);
    // Every acknowledged deferral is on disk: a fresh queue over the same
    // directory restores all of them.
    let reloaded = DeferralQueue::with_persistence(dir.path().to_path_buf());
    assert_eq!(
        reloaded.load_from_sidecar(&registry),
        expected,
        "an acknowledged deferral is missing from the sidecar"
    );
}
