// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Trigger pre-images (`old_value` / `properties_old`) for edges.
//!
//! A trigger's view of an edge's pre-mutation state must not depend on
//! whether the edge still sits in L0 or was flushed to L1. Every scenario
//! here runs twice, once against an unflushed store (the L0-chain probe is
//! the reference) and once after `flush()`, and the two event logs must be
//! identical: same kind (`EDGE_UPDATE` / `EDGE_DELETE`, never
//! `EDGE_CREATE` for a pre-existing edge), same pre-image.
//!
//! Covered mutations: `SET`, `REMOVE`, `DELETE`, `DETACH DELETE` of an
//! endpoint and `MERGE ... ON MATCH SET`, on a declared edge type carrying
//! an undeclared property, and on a schemaless edge type.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use uni_db::{DataType, Uni};
use uni_plugin::traits::trigger::{
    FireMode, MutationBatch, TriggerContext, TriggerEventMask, TriggerOutcome, TriggerPhase,
    TriggerPlugin, TriggerSubscription,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar};

/// Discriminants of the edge event kinds (1-based bit positions of
/// `TriggerEventMask::EDGE_*`).
const EDGE_CREATE_DISC: u8 = 4;
const EDGE_UPDATE_DISC: u8 = 5;
const EDGE_DELETE_DISC: u8 = 6;

/// One recorded edge event: `(kind, edge type, pre-image as JSON)`.
type EdgeEvent = (u8, String, Option<serde_json::Value>);

/// Records every edge event row with its decoded `old_value`.
struct EdgeRecorder {
    sub: TriggerSubscription,
    rows: Arc<std::sync::Mutex<Vec<EdgeEvent>>>,
}

impl TriggerPlugin for EdgeRecorder {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        use arrow_array::{Array, LargeBinaryArray, StringArray, UInt8Array};
        let batch = events.events.as_ref();
        let kinds = batch
            .column_by_name("event_kind")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
            .expect("event_kind column");
        let labels = batch
            .column_by_name("label")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("label column");
        let olds = batch
            .column_by_name("old_value")
            .and_then(|c| c.as_any().downcast_ref::<LargeBinaryArray>())
            .expect("old_value column");
        let mut g = self.rows.lock().expect("rows mutex");
        for i in 0..batch.num_rows() {
            let old = (!olds.is_null(i)).then(|| {
                serde_json::from_slice(olds.value(i)).expect("old_value is JSON properties")
            });
            g.push((kinds.value(i), labels.value(i).to_owned(), old));
        }
        Ok(TriggerOutcome::Continue)
    }
}

/// Counts edge rows that pass an `old.<prop>` predicate, which reads the
/// `properties_old` bag rather than `old_value`.
struct OldPredicateCounter {
    sub: TriggerSubscription,
    counter: Arc<AtomicU64>,
}

impl TriggerPlugin for OldPredicateCounter {
    fn subscription(&self) -> &TriggerSubscription {
        &self.sub
    }

    fn fire(
        &self,
        _ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError> {
        self.counter
            .fetch_add(events.events.num_rows() as u64, Ordering::SeqCst);
        Ok(TriggerOutcome::Continue)
    }
}

fn edge_mask() -> TriggerEventMask {
    TriggerEventMask::EDGE_CREATE
        .union(TriggerEventMask::EDGE_UPDATE)
        .union(TriggerEventMask::EDGE_DELETE)
}

fn subscription(predicate: Option<&str>, docs: &str) -> TriggerSubscription {
    TriggerSubscription {
        phase: TriggerPhase::AfterCommit,
        events: edge_mask(),
        labels: None,
        edge_types: None,
        properties: None,
        predicate_source: predicate.map(str::to_owned),
        fire_mode: FireMode::Synchronous,
        docs: docs.to_owned(),
    }
}

fn register(db: &Uni, id: &str, trigger: Arc<dyn TriggerPlugin>) -> Result<()> {
    let caps = CapabilitySet::from_iter_of([Capability::Trigger]);
    let mut r = PluginRegistrar::new(PluginId::new(id), &caps, db.plugin_registry());
    r.trigger(trigger)?;
    r.commit_to_registry()?;
    Ok(())
}

/// `P` nodes; declared edge type `R` with `w` (declared) and `opt`
/// (declared, nullable); edge type `S` is never declared (schemaless).
async fn fixture_db() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("name", DataType::String)
        .edge_type("R", &["P"], &["P"])
        .property("w", DataType::Int64)
        .property_nullable("opt", DataType::String)
        .apply()
        .await?;
    Ok(db)
}

/// Seed `(a)-[:R {w:1, opt:'o', note:'n'}]->(b)` and
/// `(a)-[:S {k:7, tag:'t'}]->(b)`, optionally flush, then register an
/// edge recorder and run `mutation`. Returns the recorded edge events,
/// sorted so the comparison is order-independent.
async fn run(flush: bool, mutation: &str) -> Result<Vec<EdgeEvent>> {
    let db = fixture_db().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:P {name: 'a'}), (b:P {name: 'b'}), \
         (a)-[:R {w: 1, opt: 'o', note: 'n'}]->(b), \
         (a)-[:S {k: 7, tag: 't'}]->(b)",
    )
    .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }

    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    register(
        &db,
        "edge-pre-image-recorder",
        Arc::new(EdgeRecorder {
            sub: subscription(None, "edge pre-image recorder"),
            rows: Arc::clone(&rows),
        }),
    )?;

    let tx = db.session().tx().await?;
    tx.execute(mutation).await?;
    tx.commit().await?;

    let mut out = rows.lock().expect("rows mutex").clone();
    out.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
    Ok(out)
}

/// Runs `mutation` against an unflushed and a flushed store, asserts the
/// unflushed log equals `expected`, then that the flushed log equals it.
async fn assert_pre_images(mutation: &str, expected: Vec<EdgeEvent>) -> Result<()> {
    let unflushed = run(false, mutation).await?;
    assert_eq!(unflushed, expected, "L0 reference for `{mutation}`");
    let flushed = run(true, mutation).await?;
    assert_eq!(
        flushed, expected,
        "flushed edge pre-image differs from L0 for `{mutation}`"
    );
    Ok(())
}

fn r_pre_image() -> serde_json::Value {
    serde_json::json!({"w": 1, "opt": "o", "note": "n"})
}

fn s_pre_image() -> serde_json::Value {
    serde_json::json!({"k": 7, "tag": "t"})
}

#[tokio::test]
async fn set_on_edge_carries_pre_image() -> Result<()> {
    assert_pre_images(
        "MATCH ()-[r:R]->() SET r.w = 2",
        vec![(EDGE_UPDATE_DISC, "R".into(), Some(r_pre_image()))],
    )
    .await
}

#[tokio::test]
async fn set_undeclared_prop_on_edge_carries_pre_image() -> Result<()> {
    assert_pre_images(
        "MATCH ()-[r:R]->() SET r.note = 'changed'",
        vec![(EDGE_UPDATE_DISC, "R".into(), Some(r_pre_image()))],
    )
    .await
}

#[tokio::test]
async fn remove_on_edge_carries_pre_image() -> Result<()> {
    assert_pre_images(
        "MATCH ()-[r:R]->() REMOVE r.opt, r.note",
        vec![(EDGE_UPDATE_DISC, "R".into(), Some(r_pre_image()))],
    )
    .await
}

#[tokio::test]
async fn delete_edge_carries_pre_image() -> Result<()> {
    assert_pre_images(
        "MATCH ()-[r:R]->() DELETE r",
        vec![(EDGE_DELETE_DISC, "R".into(), Some(r_pre_image()))],
    )
    .await
}

#[tokio::test]
async fn detach_delete_endpoint_carries_edge_pre_images() -> Result<()> {
    assert_pre_images(
        "MATCH (a:P {name: 'a'}) DETACH DELETE a",
        vec![
            (EDGE_DELETE_DISC, "R".into(), Some(r_pre_image())),
            (EDGE_DELETE_DISC, "S".into(), Some(s_pre_image())),
        ],
    )
    .await
}

#[tokio::test]
async fn merge_on_match_set_carries_edge_pre_image() -> Result<()> {
    assert_pre_images(
        "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) \
         MERGE (a)-[r:R]->(b) ON MATCH SET r.w = 5",
        vec![(EDGE_UPDATE_DISC, "R".into(), Some(r_pre_image()))],
    )
    .await
}

#[tokio::test]
async fn schemaless_edge_set_and_delete_carry_pre_image() -> Result<()> {
    assert_pre_images(
        "MATCH ()-[s:S]->() SET s.k = 8, s.extra = 1",
        vec![(EDGE_UPDATE_DISC, "S".into(), Some(s_pre_image()))],
    )
    .await?;
    assert_pre_images(
        "MATCH ()-[s:S]->() DELETE s",
        vec![(EDGE_DELETE_DISC, "S".into(), Some(s_pre_image()))],
    )
    .await
}

/// A new edge stays a CREATE with no pre-image, flushed store or not —
/// the L1 lookup must not invent a pre-image for an EID it has never seen.
#[tokio::test]
async fn new_edge_beside_flushed_ones_is_create() -> Result<()> {
    assert_pre_images(
        "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) CREATE (b)-[:R {w: 9}]->(a)",
        vec![(EDGE_CREATE_DISC, "R".into(), None)],
    )
    .await
}

/// A pre-image built from L1 still yields to a newer committed L0 state:
/// the edge was updated after the flush in a separate transaction, so its
/// pre-image for the next update is that newer value, not the flushed one.
#[tokio::test]
async fn pre_image_prefers_newer_committed_l0_over_l1() -> Result<()> {
    let db = fixture_db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:R {w: 1, note: 'n'}]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:R]->() SET r.w = 2").await?;
    tx.commit().await?;

    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    register(
        &db,
        "edge-pre-image-newest",
        Arc::new(EdgeRecorder {
            sub: subscription(None, "newest pre-image"),
            rows: Arc::clone(&rows),
        }),
    )?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:R]->() SET r.w = 3").await?;
    tx.commit().await?;

    let g = rows.lock().expect("rows mutex").clone();
    assert_eq!(
        g,
        vec![(
            EDGE_UPDATE_DISC,
            "R".into(),
            Some(serde_json::json!({"w": 2, "note": "n"}))
        )]
    );
    Ok(())
}

/// `old.<prop>` predicates read `properties_old`, which is built from the
/// same probe; a flushed edge must satisfy `old.w = 1`.
#[tokio::test]
async fn old_predicate_sees_flushed_edge_pre_image() -> Result<()> {
    for flush in [false, true] {
        let db = fixture_db().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:P {name: 'a'})-[:R {w: 1}]->(:P {name: 'b'})")
            .await?;
        tx.commit().await?;
        if flush {
            db.flush().await?;
        }
        let counter = Arc::new(AtomicU64::new(0));
        register(
            &db,
            "edge-old-predicate",
            Arc::new(OldPredicateCounter {
                sub: subscription(Some("old.w = 1 AND n.w = 2"), "old predicate"),
                counter: Arc::clone(&counter),
            }),
        )?;
        let tx = db.session().tx().await?;
        tx.execute("MATCH ()-[r:R]->() SET r.w = 2").await?;
        tx.commit().await?;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "`old.w = 1 AND n.w = 2` must match once (flush = {flush})"
        );
    }
    Ok(())
}

/// The L1 pre-image lookup is batched: one commit touching 1000 flushed
/// edges must deliver 1000 pre-images and stay well inside a per-row-scan
/// budget. Timing is printed for the record; the assertion is on content.
#[tokio::test]
async fn thousand_flushed_edges_all_get_pre_images() -> Result<()> {
    let db = fixture_db().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:P {name: 'a'}), (b:P {name: 'b'}) \
         WITH a, b UNWIND range(1, 1000) AS i CREATE (a)-[:R {w: i}]->(b)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    register(
        &db,
        "edge-pre-image-bulk",
        Arc::new(EdgeRecorder {
            sub: subscription(None, "bulk pre-image"),
            rows: Arc::clone(&rows),
        }),
    )?;
    let started = std::time::Instant::now();
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:R]->() SET r.w = r.w + 10000")
        .await?;
    tx.commit().await?;
    eprintln!(
        "1000 flushed edge updates committed in {:?}",
        started.elapsed()
    );

    let g = rows.lock().expect("rows mutex").clone();
    assert_eq!(g.len(), 1000);
    let mut ws: Vec<i64> = g
        .iter()
        .map(|(kind, _, old)| {
            assert_eq!(*kind, EDGE_UPDATE_DISC);
            old.as_ref()
                .and_then(|o| o.get("w"))
                .and_then(serde_json::Value::as_i64)
                .expect("every flushed edge carries its pre-image `w`")
        })
        .collect();
    ws.sort_unstable();
    assert_eq!(ws, (1..=1000).collect::<Vec<_>>());
    Ok(())
}

/// The edge state a pre-image reports is the state a `SET` on that edge
/// merges into; an undeclared property of a flushed declared-type edge
/// must survive an update of a declared one.
#[tokio::test]
async fn set_on_flushed_edge_keeps_undeclared_props() -> Result<()> {
    let db = fixture_db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:R {w: 1, note: 'n'}]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:R]->() SET r.w = 2").await?;
    tx.commit().await?;
    let rows = db
        .session()
        .query("MATCH ()-[r:R]->() RETURN r.w AS w, r.note AS note")
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.rows()[0].get::<i64>("w")?, 2);
    assert_eq!(rows.rows()[0].get::<String>("note")?, "n");
    // A second flush persists the updated row; the untouched undeclared
    // property must still be there afterwards.
    db.flush().await?;
    let rows = db
        .session()
        .query("MATCH ()-[r:R]->() RETURN r.w AS w, r.note AS note")
        .await?;
    assert_eq!(rows.rows()[0].get::<i64>("w")?, 2);
    assert_eq!(rows.rows()[0].get::<String>("note")?, "n");
    Ok(())
}

/// Vertex counterpart of [`pre_image_prefers_newer_committed_l0_over_l1`]:
/// a flushed vertex updated in a later committed transaction reports its
/// whole committed property set as the next pre-image.
#[tokio::test]
async fn vertex_pre_image_after_flush_then_update_is_whole() -> Result<()> {
    let db = fixture_db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'a', extra: 5})").await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:P) SET n.extra = 6").await?;
    tx.commit().await?;

    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut sub = subscription(None, "vertex pre-image");
    sub.events = TriggerEventMask::NODE_UPDATE;
    register(
        &db,
        "vertex-pre-image-newest",
        Arc::new(EdgeRecorder {
            sub,
            rows: Arc::clone(&rows),
        }),
    )?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:P) SET n.extra = 7").await?;
    tx.commit().await?;
    let g = rows.lock().expect("rows mutex").clone();
    assert_eq!(g.len(), 1);
    assert_eq!(
        g[0].2,
        Some(serde_json::json!({"name": "a", "extra": 6})),
        "vertex pre-image"
    );
    Ok(())
}

/// Removed edge properties stay removed across flushes and later updates,
/// and the pre-image of the later update reflects the removal.
#[tokio::test]
async fn removed_edge_props_stay_removed_across_flushes() -> Result<()> {
    let db = fixture_db().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {name: 'a'})-[:R {w: 1, opt: 'o', note: 'n'}]->(:P {name: 'b'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:R]->() REMOVE r.opt, r.note")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let rows = Arc::new(std::sync::Mutex::new(Vec::new()));
    register(
        &db,
        "edge-pre-image-after-remove",
        Arc::new(EdgeRecorder {
            sub: subscription(None, "after remove"),
            rows: Arc::clone(&rows),
        }),
    )?;
    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:R]->() SET r.w = 5").await?;
    tx.commit().await?;
    assert_eq!(
        rows.lock().expect("rows mutex").clone(),
        vec![(
            EDGE_UPDATE_DISC,
            "R".into(),
            Some(serde_json::json!({"w": 1}))
        )]
    );
    db.flush().await?;
    let got = db
        .session()
        .query("MATCH ()-[r:R]->() RETURN r.w AS w, r.opt IS NULL AS no_opt, r.note IS NULL AS no_note")
        .await?;
    assert_eq!(got.len(), 1);
    assert_eq!(got.rows()[0].get::<i64>("w")?, 5);
    assert!(got.rows()[0].get::<bool>("no_opt")?, "opt resurrected");
    assert!(got.rows()[0].get::<bool>("no_note")?, "note resurrected");
    Ok(())
}
