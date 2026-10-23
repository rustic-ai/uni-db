#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Trigger label / edge-type selectors written in Cypher form (`:Label`).
//!
//! The built-in audit trigger subscribed to `":_AuditMe"`, but the router
//! compared selectors to the bare label an event row carries (`_AuditMe`),
//! so it never fired. A declared trigger (`ON :Label`) already stripped the
//! colon; a `TriggerSubscription` built in code did not. The router now
//! normalizes every selector when it builds its routes, so `":Foo"` and
//! `"Foo"` name the same label, and the built-in ships the bare name.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use smol_str::SmolStr;
use uni_db::{DataType, Uni};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};
use uni_plugin_builtin::triggers::LabelAuditTrigger;

fn register_trigger(db: &Uni, id: &str, t: Arc<dyn TriggerPlugin>) -> Result<()> {
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new(id), &caps, db.plugin_registry());
    r.trigger(t)?;
    r.commit_to_registry()?;
    Ok(())
}

/// Polls until `done` holds or five seconds pass, then lets any straggling
/// async fire land so an over-count is caught too.
async fn settle(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// The shipped built-in subscribes to the bare label.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_selector_builtin_audit_ships_bare_label() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let shipped: Vec<String> = db
        .plugin_registry()
        .triggers()
        .iter()
        .filter_map(|t| t.subscription().labels.clone())
        .flatten()
        .map(|l| l.to_string())
        .collect();
    assert!(
        shipped.iter().any(|l| l == "_AuditMe"),
        "built-in audit trigger label: {shipped:?}"
    );
    assert!(
        shipped.iter().all(|l| !l.starts_with(':')),
        "a shipped selector is colon-prefixed: {shipped:?}"
    );
    Ok(())
}

/// An audit trigger built exactly as the built-in used to be (`":_AuditMe"`)
/// fires through the real registration path, like the bare-label control.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_selector_colon_prefixed_label_fires() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("_AuditMe")
        .property("x", DataType::Int64)
        .label("Other")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    let colon = Arc::new(LabelAuditTrigger::new(":_AuditMe"));
    let bare = Arc::new(LabelAuditTrigger::new("_AuditMe"));
    register_trigger(&db, "selector-colon", colon.clone())?;
    register_trigger(&db, "selector-bare", bare.clone())?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:_AuditMe {x: 1}), (:Other {x: 2})")
        .await?;
    tx.commit().await?;
    settle(|| bare.events_seen() >= 1 && colon.events_seen() >= 1).await;

    assert_eq!(
        (bare.events_seen(), colon.events_seen()),
        (1, 1),
        "(bare, colon-prefixed) audit counts for one _AuditMe create beside an Other create"
    );
    Ok(())
}

struct EdgeCounter {
    sub: TriggerSubscription,
    seen: Arc<AtomicU64>,
}

impl TriggerPlugin for EdgeCounter {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        self.seen
            .fetch_add(events.events.num_rows() as u64, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

fn edge_counter(edge_type: &str) -> (Arc<EdgeCounter>, Arc<AtomicU64>) {
    let seen = Arc::new(AtomicU64::new(0));
    let t = EdgeCounter {
        sub: TriggerSubscription {
            phase: TriggerPhase::AfterCommit,
            events: TriggerEventMask::EDGE_CREATE,
            labels: None,
            edge_types: Some(vec![SmolStr::new(edge_type)]),
            properties: None,
            predicate_source: None,
            fire_mode: FireMode::Synchronous,
            docs: format!("edge counter on {edge_type}"),
        },
        seen: seen.clone(),
    };
    (Arc::new(t), seen)
}

/// Edge-type selectors follow the same rule.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_selector_colon_prefixed_edge_type_fires() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("x", DataType::Int64)
        .edge_type("KNOWS", &["P"], &["P"])
        .edge_type("LIKES", &["P"], &["P"])
        .apply()
        .await?;
    let (colon, colon_seen) = edge_counter(":KNOWS");
    let (bare, bare_seen) = edge_counter("KNOWS");
    register_trigger(&db, "edge-colon", colon)?;
    register_trigger(&db, "edge-bare", bare)?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (a:P {x: 1}), (b:P {x: 2}), (a)-[:KNOWS]->(b), (a)-[:LIKES]->(b)")
        .await?;
    tx.commit().await?;

    assert_eq!(
        (
            bare_seen.load(Ordering::SeqCst),
            colon_seen.load(Ordering::SeqCst)
        ),
        (1, 1),
        "(bare, colon-prefixed) KNOWS-create counts"
    );
    Ok(())
}
