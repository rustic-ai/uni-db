// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Label-only writes (`SET n:L`, `REMOVE n:L`) reach triggers and CDC.
//!
//! The commit's event log was built from vertex property writes, deletes and
//! edges only, so a relabel — which writes no property — produced no row at
//! all, and `LABEL_ADDED` / `LABEL_REMOVED` could be subscribed to but were
//! never emitted. Each changed label now yields one row of its kind, routed
//! under that label.

use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow_array::{Array, StringArray, UInt8Array};
use smol_str::SmolStr;
use uni_db::{DataType, Uni};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

const CREATE: u8 = 1;
const ADDED: u8 = 8;
const REMOVED: u8 = 9;

/// Records `(event_kind, label)` for every row it receives.
struct Recorder {
    sub: TriggerSubscription,
    seen: Arc<Mutex<Vec<(u8, String)>>>,
}

impl TriggerPlugin for Recorder {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        batch: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        let events = &batch.events;
        let kinds = events
            .column_by_name("event_kind")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
            .expect("event_kind column");
        let labels = events
            .column_by_name("label")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("label column");
        let mut seen = self.seen.lock().unwrap();
        for i in 0..events.num_rows() {
            seen.push((kinds.value(i), labels.value(i).to_owned()));
        }
        Ok(TriggerOutcome::Continue)
    }
}

fn record(db: &Uni, id: &str, labels: Option<&[&str]>) -> Result<Arc<Mutex<Vec<(u8, String)>>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let trigger = Recorder {
        sub: TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events: TriggerEventMask::NODE_CREATE
                .union(TriggerEventMask::NODE_UPDATE)
                .union(TriggerEventMask::LABEL_ADDED)
                .union(TriggerEventMask::LABEL_REMOVED),
            labels: labels.map(|ls| ls.iter().map(|l| SmolStr::new(*l)).collect()),
            edge_types: None,
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: "records label events".to_owned(),
        },
        seen: seen.clone(),
    };
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new(id), &caps, db.plugin_registry());
    r.trigger(Arc::new(trigger))?;
    r.commit_to_registry()?;
    Ok(seen)
}

fn take(seen: &Mutex<Vec<(u8, String)>>) -> Vec<(u8, String)> {
    let mut rows = std::mem::take(&mut *seen.lock().unwrap());
    rows.sort();
    rows
}

async fn run(db: &Uni, cypher: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

async fn db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("x", DataType::Int64)
        .label("B")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    Ok(db)
}

/// Each changed label yields one row of its kind under that label, flushed or
/// not; a label the vertex already has yields none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_events_report_each_changed_label() -> Result<()> {
    for flush in [false, true] {
        let db = db().await?;
        run(&db, "CREATE (:A {x: 1})").await?;
        if flush {
            db.flush().await?;
        }
        let all = record(&db, "label-all", None)?;

        run(&db, "MATCH (n:A) SET n:B").await?;
        assert_eq!(take(&all), [(ADDED, "B".to_owned())], "flush={flush}: SET");

        run(&db, "MATCH (n:A) SET n:B").await?;
        assert_eq!(take(&all), [], "flush={flush}: SET of a label it has");

        run(&db, "MATCH (n:A) REMOVE n:B").await?;
        assert_eq!(
            take(&all),
            [(REMOVED, "B".to_owned())],
            "flush={flush}: REMOVE"
        );

        run(&db, "MATCH (n:A) SET n:B REMOVE n:A").await?;
        assert_eq!(
            take(&all),
            [(ADDED, "B".to_owned()), (REMOVED, "A".to_owned())],
            "flush={flush}: both in one statement"
        );
    }
    Ok(())
}

/// Two relabels of one vertex in one transaction report the net change
/// against the labels it had when the transaction began.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_events_are_net_of_the_transaction() -> Result<()> {
    let db = db().await?;
    run(&db, "CREATE (:A {x: 1})").await?;
    let all = record(&db, "label-net", None)?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:A) SET n:B").await?;
    tx.execute("MATCH (n:A) REMOVE n:B").await?;
    tx.commit().await?;
    assert_eq!(take(&all), [], "added then removed: no net change");
    Ok(())
}

/// A trigger filtered to a label sees the vertex leave it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn label_removed_reaches_a_trigger_on_that_label() -> Result<()> {
    let db = db().await?;
    run(&db, "CREATE (:A:B {x: 1})").await?;
    let on_b = record(&db, "label-on-b", Some(&["B"]))?;

    run(&db, "MATCH (n:A) REMOVE n:B").await?;
    assert_eq!(take(&on_b), [(REMOVED, "B".to_owned())]);
    Ok(())
}

/// A vertex created in the transaction is reported by its NODE_CREATE rows,
/// under its final labels, and by no label rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_vertex_created_in_the_transaction_has_no_label_rows() -> Result<()> {
    let db = db().await?;
    let all = record(&db, "label-create", None)?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:A {x: 1})").await?;
    tx.execute("MATCH (n:A) SET n:B").await?;
    tx.commit().await?;
    assert_eq!(
        take(&all),
        [(CREATE, "A".to_owned()), (CREATE, "B".to_owned())]
    );
    Ok(())
}
