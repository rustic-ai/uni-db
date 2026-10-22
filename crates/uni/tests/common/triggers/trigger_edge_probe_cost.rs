// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Cost of the edge pre-image probe that runs at commit when a trigger (or a
//! CDC output) is registered.
//!
//! The probe reads the committed state of every edge a commit writes or
//! deletes so a trigger sees `EDGE_UPDATE` / `EDGE_DELETE` with the
//! pre-image of an edge already flushed to L1. "Trigger off" still has the
//! built-in node-only `:_AuditMe` trigger registered. A created edge has no
//! committed state, so for a commit of `n` new edges the read is pure
//! overhead. These tests measure commit wall time with and without a
//! registered trigger, for `n` new edges and for a `SET` over `n` flushed
//! ones, and print the numbers, plus the probe's own time from its
//! `uni::trigger_probe` debug event (absent when it had nothing to read).
//!
//! Ignored (slow); run with:
//!   cargo nextest run -p uni-db --test integration --run-ignored only \
//!     --no-capture -E 'test(trigger_edge_probe_cost)'

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

/// Counts the edge event rows it is fired with; no other work.
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

/// Registers a counting edge trigger on `db`.
fn register_counter(db: &Uni) -> Result<Arc<AtomicU64>> {
    let rows = Arc::new(AtomicU64::new(0));
    let sub = TriggerSubscription {
        phase: TriggerPhase::AfterCommit,
        events: TriggerEventMask::EDGE_CREATE
            .union(TriggerEventMask::EDGE_UPDATE)
            .union(TriggerEventMask::EDGE_DELETE),
        labels: None,
        edge_types: None,
        properties: None,
        predicate_source: None,
        fire_mode: FireMode::Synchronous,
        docs: "probe cost counter".to_owned(),
    };
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new("probe-cost"), &caps, db.plugin_registry());
    r.trigger(Arc::new(RowCounter {
        sub,
        rows: Arc::clone(&rows),
    }))?;
    r.commit_to_registry()?;
    Ok(rows)
}

/// `(:P {name:'a'})` and `(:P {name:'b'})`, `R {w}` between `P`s.
async fn fixture() -> Result<Uni> {
    // One statement writes up to 1M edges: lift the per-statement and
    // per-transaction limits that would refuse it.
    let config = uni_db::UniConfig {
        query_timeout: Duration::from_secs(3600),
        commit_timeout: Duration::from_secs(3600),
        max_query_memory: usize::MAX / 4,
        max_transaction_memory: usize::MAX / 4,
        ..Default::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .edge_type("R", &["P"], &["P"])
        .property("w", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'a'}), (:P {name: 'b'})")
        .await?;
    tx.commit().await?;
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

const CREATE: &str = "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) \
                      UNWIND range(1, $n) AS i CREATE (a)-[:R {w: i}]->(b)";
const UPDATE: &str = "MATCH ()-[r:R]->() SET r.w = r.w + 1";

/// Prints the probe's own timing (`uni::trigger_probe` debug events) to
/// stderr: commit wall time on a loaded machine is too noisy to isolate it.
fn trace_probe() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "uni::trigger_probe=debug",
        ))
        .with_writer(std::io::stderr)
        .try_init();
}

/// One measurement: `n` edges, trigger on or off, create or update.
async fn measure(n: usize, trigger: bool) -> Result<()> {
    trace_probe();
    let db = fixture().await?;
    let rows = if trigger {
        Some(register_counter(&db)?)
    } else {
        None
    };
    let (exec, commit) = timed_tx(&db, CREATE, n).await?;
    eprintln!(
        "PROBE n={n} trigger={trigger} create: execute {exec:?} commit {commit:?} rows {:?}",
        rows.as_ref().map(|r| r.load(Ordering::SeqCst))
    );
    db.flush().await?;
    if let Some(r) = &rows {
        r.store(0, Ordering::SeqCst);
    }
    let (exec, commit) = timed_tx(&db, UPDATE, n).await?;
    eprintln!(
        "PROBE n={n} trigger={trigger} update-flushed: execute {exec:?} commit {commit:?} rows {:?}",
        rows.as_ref().map(|r| r.load(Ordering::SeqCst))
    );
    if let Some(r) = &rows {
        assert_eq!(
            r.load(Ordering::SeqCst),
            u64::try_from(n)?,
            "one event per edge"
        );
        r.store(0, Ordering::SeqCst);
    }
    // New edges beside `n` flushed ones: the probe would look each new id
    // up among the flushed edges.
    db.flush().await?;
    let (exec, commit) = timed_tx(&db, CREATE, n).await?;
    eprintln!(
        "PROBE n={n} trigger={trigger} create-beside-flushed: execute {exec:?} commit {commit:?} rows {:?}",
        rows.as_ref().map(|r| r.load(Ordering::SeqCst))
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "slow: commit-time probe cost at 100k edges"]
async fn trigger_edge_probe_cost_100k() -> Result<()> {
    for trigger in [false, true, false, true] {
        measure(100_000, trigger).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "slow: commit-time probe cost at 1M edges"]
async fn trigger_edge_probe_cost_1m() -> Result<()> {
    for trigger in [false, true] {
        measure(1_000_000, trigger).await?;
    }
    Ok(())
}
