// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cost of the vertex pre-image probe (`PreExistingProbe::extend_with_l1`)
//! that runs at commit when a trigger (or a CDC output) is registered.
//!
//! The probe reads, from the per-label tables, every vertex a commit writes
//! that the L0 chain does not hold, so a trigger sees `NODE_UPDATE` /
//! `NODE_DELETE` with the pre-image of a vertex already flushed. "No user
//! trigger" still has the built-in node-only `:_AuditMe` trigger registered,
//! which watches one label. These tests run commits of `n` new vertices (on an
//! empty store and beside `n` flushed ones) and a `SET` over `n` flushed ones,
//! with no user trigger and with a node trigger on the written label, and print
//! commit wall time plus the probe's own time from its `uni::trigger_probe`
//! debug event (absent when it did not run).
//!
//! Ignored (slow); run with:
//!   cargo nextest run -p uni-db --test integration --run-ignored only \
//!     --no-capture -E 'test(trigger_vertex_probe_cost)'

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::{DataType, Uni, Value};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

/// Counts the node event rows it is fired with; no other work.
struct RowCounter {
    sub: TriggerSubscription,
    rows: Arc<AtomicU64>,
}

impl TriggerPlugin for RowCounter {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        self.rows
            .fetch_add(events.events.num_rows() as u64, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

/// Registers a counting node trigger on label `V` on `db`.
fn register_counter(db: &Uni) -> Result<Arc<AtomicU64>> {
    let rows = Arc::new(AtomicU64::new(0));
    let sub = TriggerSubscription {
        phase: TriggerPhase::AfterCommit,
        events: TriggerEventMask::NODE_CREATE
            .union(TriggerEventMask::NODE_UPDATE)
            .union(TriggerEventMask::NODE_DELETE),
        labels: Some(vec!["V".into()]),
        edge_types: None,
        properties: None,
        predicate_source: None,
        fire_mode: FireMode::Synchronous,
        docs: "vertex probe cost counter".to_owned(),
    };
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(
        PluginId::new("vertex-probe-cost"),
        &caps,
        db.plugin_registry(),
    );
    r.trigger(Arc::new(RowCounter {
        sub,
        rows: Arc::clone(&rows),
    }))?;
    r.commit_to_registry()?;
    Ok(rows)
}

/// Label `V {x}`, limits lifted for one-statement writes of up to 1M rows.
async fn fixture() -> Result<Uni> {
    let config = uni_db::UniConfig {
        query_timeout: Duration::from_secs(3600),
        commit_timeout: Duration::from_secs(3600),
        max_query_memory: usize::MAX / 4,
        max_transaction_memory: usize::MAX / 4,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("V")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    Ok(db)
}

/// Runs `cypher` in one transaction; returns `(execute, commit)` wall time.
async fn timed_tx(db: &Uni, cypher: &str, n: usize) -> Result<(Duration, Duration)> {
    let tx = db.session().tx().await?;
    let started = Instant::now();
    tx.execute_with(cypher)
        .param("n", Value::Int(i64::try_from(n)?))
        .run()
        .await?;
    let executed = started.elapsed();
    let started = Instant::now();
    tx.commit().await?;
    Ok((executed, started.elapsed()))
}

const CREATE: &str = "UNWIND range(1, $n) AS i CREATE (:V {x: i})";
const UPDATE: &str = "MATCH (v:V) SET v.x = v.x + 1";

/// Prints the probe's own timing (`uni::trigger_probe` debug events).
fn trace_probe() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "uni::trigger_probe=debug",
        ))
        .with_writer(std::io::stderr)
        .try_init();
}

/// One measurement: `n` vertices, a user trigger or not.
async fn measure(n: usize, trigger: bool) -> Result<()> {
    trace_probe();
    let db = fixture().await?;
    let rows = if trigger {
        Some(register_counter(&db)?)
    } else {
        None
    };
    let report = |arm: &str, exec: Duration, commit: Duration| {
        eprintln!(
            "VPROBE n={n} trigger={trigger} {arm}: execute {exec:?} commit {commit:?} rows {:?}",
            rows.as_ref().map(|r| r.swap(0, Ordering::SeqCst))
        );
    };
    let (exec, commit) = timed_tx(&db, CREATE, n).await?;
    report("create", exec, commit);
    db.flush().await?;
    let (exec, commit) = timed_tx(&db, UPDATE, n).await?;
    report("update-flushed", exec, commit);
    db.flush().await?;
    let (exec, commit) = timed_tx(&db, CREATE, n).await?;
    report("create-beside-flushed", exec, commit);
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "slow: commit-time vertex probe cost at 100k vertices"]
async fn trigger_vertex_probe_cost_100k() -> Result<()> {
    for trigger in [false, true, false, true] {
        measure(100_000, trigger).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "slow: commit-time vertex probe cost at 1M vertices"]
async fn trigger_vertex_probe_cost_1m() -> Result<()> {
    for trigger in [false, true] {
        measure(1_000_000, trigger).await?;
    }
    Ok(())
}
