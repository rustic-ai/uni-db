// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Crash consistency for the compaction path, in-process.
//!
//! Until this suite the compaction path had **zero** fault injection: no
//! `fail_point!` anywhere in `storage/compaction.rs`, `storage/manager.rs` or
//! `backend/lance.rs`, while the rest of the workspace carried 17 seams.
//! `background_compaction_test.rs` covers config, triggers and status only.
//!
//! Two seams are exercised here, both interrupted with an in-process panic:
//!
//! * `compaction::after-adj-replace-before-delta-clear` — the only genuine
//!   write-then-delete window in the path. L2 is overwritten with
//!   merge(L2, deltas) and the deltas that produced it are still present, so
//!   the next pass re-applies them onto an already-merged L2. That redo is
//!   believed safe because `apply_deltas_to_edges` is a per-op HashMap
//!   insert/remove — but idempotence-per-op is a property of the merge, not a
//!   protocol guarantee, and the redo is **not** replayed against the L2 it was
//!   computed from. These tests assert it rather than assuming it.
//! * `compaction::between-fwd-and-bwd` — one direction merged and cleared, the
//!   other untouched. Nothing in `compact_all` ties the two together.
//!
//! Durability across a real process death is covered separately by the abort
//! harness in `uni/tests/common/compaction_resilience.rs`; these tests cover
//! *ordering*, which needs no child process.

#![cfg(all(feature = "lance-backend", feature = "failpoints"))]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::SchemaManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::direction::Direction;
use uni_store::storage::manager::StorageManager;

// Rust guideline compliant

const ADJ_SEAM: &str = "compaction::after-adj-replace-before-delta-clear";
const DIR_SEAM: &str = "compaction::between-fwd-and-bwd";

struct Graph {
    writer: Arc<Writer>,
    storage: Arc<StorageManager>,
    schema_manager: Arc<SchemaManager>,
    edge_type_id: u32,
    path: String,
    _dir: TempDir,
}

/// `Person -[KNOWS]-> Person`, with both directions declared so
/// `compact_all` walks fwd then bwd for the same edge type.
async fn graph() -> Result<Graph> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    let edge_type_id = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string()],
        vec!["Person".to_string()],
    )?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(&path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage.clone(), schema_manager.clone(), 1).await?);

    Ok(Graph {
        writer,
        storage,
        schema_manager,
        edge_type_id,
        path,
        _dir: dir,
    })
}

async fn person(g: &Graph) -> Result<Vid> {
    let vid = g.writer.next_vid().await?;
    g.writer
        .insert_vertex_with_labels(vid, HashMap::new(), &["Person".to_string()], None)
        .await?;
    Ok(vid)
}

async fn knows(g: &Graph, src: Vid, dst: Vid) -> Result<Eid> {
    let eid = g.writer.next_eid(g.edge_type_id).await?;
    g.writer
        .insert_edge(src, dst, g.edge_type_id, eid, HashMap::new(), None, None)
        .await?;
    Ok(eid)
}

/// Neighbours as a freshly warmed read would see them: a new `StorageManager`
/// over the same directory, so the answer comes from L2 unioned with whatever
/// Delta L1 still holds — never from an in-process CSR that survived the crash.
async fn neighbors_after_reopen(g: &Graph, vid: Vid, direction: Direction) -> Result<Vec<Vid>> {
    let storage = Arc::new(StorageManager::new(&g.path, g.schema_manager.clone()).await?);
    let am = storage.adjacency_manager();
    am.warm(&storage, g.edge_type_id, direction, None).await?;
    // `get_neighbors` yields (neighbour, edge id); the edge set is what matters
    // here, and the eid is reallocated on re-insert.
    let mut out: Vec<Vid> = am
        .get_neighbors(vid, g.edge_type_id, direction)
        .into_iter()
        .map(|(neighbour, _eid)| neighbour)
        .collect();
    out.sort_by_key(|v| v.as_u64());
    Ok(out)
}

/// Drive an operation into a seam and require that it panicked there.
///
/// `assert!(res.is_err())` is the load-bearing half: without it an operation
/// that never reached the seam would leave a fully-compacted database, and
/// every assertion afterwards would pass while testing nothing.
async fn panic_at<F, Fut>(seam: &str, op: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    fail::cfg(seam, "panic").unwrap();
    let res = tokio::spawn(async move { op().await }).await;
    fail::remove(seam);
    assert!(
        res.is_err(),
        "{seam}: expected a panic at the seam, but the operation returned"
    );
}

/// The redo must be a no-op: re-applying deltas onto an already-merged L2 must
/// not double-insert a live edge or resurrect a deleted one.
#[tokio::test]
async fn adj_redo_after_replace_preserves_the_edge_set() -> Result<()> {
    let g = graph().await?;
    let a = person(&g).await?;
    let b = person(&g).await?;
    let c = person(&g).await?;
    let e_ab = knows(&g, a, b).await?;
    knows(&g, a, c).await?;
    g.writer.flush_to_l1(None).await?;

    // Delete one edge so the deltas carry both an Insert and a Delete.
    g.writer
        .delete_edge(e_ab, a, b, g.edge_type_id, None)
        .await?;
    g.writer.flush_to_l1(None).await?;

    let storage = g.storage.clone();
    panic_at(ADJ_SEAM, move || async move {
        let _ = Compactor::new(storage)
            .compact_adjacency("KNOWS", "Person", "fwd")
            .await;
    })
    .await;

    // L2 now holds the merged result while the deltas that produced it survive.
    assert_eq!(
        neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
        vec![c],
        "after the crash the union of L2 and the surviving deltas must still be correct"
    );

    // The redo, replayed against an L2 it was not computed from.
    Compactor::new(g.storage.clone())
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;

    assert_eq!(
        neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
        vec![c],
        "the redo re-applied the same deltas onto an already-merged L2 and changed the \
         edge set: per-op idempotence of apply_deltas_to_edges is not sufficient"
    );
    Ok(())
}

/// The sharpest case: a crash leaves a stale `Delete` in L1, and the same
/// endpoints are re-connected before the redo runs. If the redo applies the
/// stale delete after the new insert, the re-inserted edge is wiped.
#[tokio::test]
async fn adj_redo_does_not_wipe_an_edge_reinserted_after_the_crash() -> Result<()> {
    let g = graph().await?;
    let a = person(&g).await?;
    let b = person(&g).await?;
    let e_ab = knows(&g, a, b).await?;
    g.writer.flush_to_l1(None).await?;
    g.writer
        .delete_edge(e_ab, a, b, g.edge_type_id, None)
        .await?;
    g.writer.flush_to_l1(None).await?;

    let storage = g.storage.clone();
    panic_at(ADJ_SEAM, move || async move {
        let _ = Compactor::new(storage)
            .compact_adjacency("KNOWS", "Person", "fwd")
            .await;
    })
    .await;

    // Re-connect the same endpoints while the stale Delete is still in L1.
    knows(&g, a, b).await?;
    g.writer.flush_to_l1(None).await?;

    Compactor::new(g.storage.clone())
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;

    assert_eq!(
        neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
        vec![b],
        "the re-inserted edge was wiped by a stale Delete that the interrupted pass \
         had already merged but not yet cleared"
    );
    Ok(())
}

/// One direction merged and cleared, the other untouched. Both must still agree
/// about a deleted edge, and the next pass must converge them.
#[tokio::test]
async fn direction_skew_leaves_both_directions_agreeing() -> Result<()> {
    let g = graph().await?;
    let a = person(&g).await?;
    let b = person(&g).await?;
    let e_ab = knows(&g, a, b).await?;
    g.writer.flush_to_l1(None).await?;
    g.writer
        .delete_edge(e_ab, a, b, g.edge_type_id, None)
        .await?;
    g.writer.flush_to_l1(None).await?;

    let storage = g.storage.clone();
    panic_at(DIR_SEAM, move || async move {
        let _ = Compactor::new(storage).compact_all().await;
    })
    .await;

    // fwd is merged and its deltas are gone; bwd still has everything.
    assert_eq!(
        neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
        Vec::<Vid>::new(),
        "the deleted edge must be gone in the compacted direction"
    );
    assert_eq!(
        neighbors_after_reopen(&g, b, Direction::Incoming).await?,
        Vec::<Vid>::new(),
        "the deleted edge must also be gone in the direction whose compaction never \
         ran: the two directions disagree after a mid-compact_all crash"
    );

    // And the next pass converges them.
    Compactor::new(g.storage.clone()).compact_all().await?;
    assert_eq!(
        neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
        Vec::<Vid>::new()
    );
    assert_eq!(
        neighbors_after_reopen(&g, b, Direction::Incoming).await?,
        Vec::<Vid>::new(),
        "a completed compaction must leave both directions agreeing"
    );
    Ok(())
}

/// A failure to read `main_edges` during the pre-clear invariant check is an
/// error of the pass, not evidence that an EID is missing.
///
/// The check used `exists_by_eid(..).unwrap_or(false)`, so any read failure
/// tripped the `debug_assert!` with "EID … not found in main_edges", a panic
/// that blamed the data for an I/O fault, and in release builds the failure
/// went unreported. The read is failed here by injection; the pass must return
/// the error with its context, and the deltas must survive for the next pass.
///
/// The check exists only with debug assertions, so the test does too.
#[cfg(debug_assertions)]
#[tokio::test]
async fn main_edges_read_failure_in_the_invariant_check_is_an_error() -> Result<()> {
    const SEAM: &str = "main_edge::exists-by-eid";
    let g = graph().await?;
    let a = person(&g).await?;
    let b = person(&g).await?;
    knows(&g, a, b).await?;
    g.writer.flush_to_l1(None).await?;

    fail::cfg(SEAM, "return").unwrap();
    let storage = g.storage.clone();
    let joined = tokio::spawn(async move {
        Compactor::new(storage)
            .compact_adjacency("KNOWS", "Person", "fwd")
            .await
    })
    .await;
    fail::remove(SEAM);
    let res = joined.unwrap_or_else(|e| {
        panic!("the pass panicked on a main_edges read failure instead of returning it: {e}")
    });
    let err = match res {
        Ok(_) => panic!("the pass succeeded although main_edges could not be read"),
        Err(e) => e,
    };
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("could not check main_edges") && rendered.contains("injected"),
        "the error lost its context: {rendered}"
    );

    // Nothing was cleared, and the next pass converges.
    Compactor::new(g.storage.clone())
        .compact_adjacency("KNOWS", "Person", "fwd")
        .await?;
    assert_eq!(
        neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
        vec![b]
    );
    Ok(())
}

/// Probe, not a repro. `adjacency_table_name` ignores its `label` argument, so
/// an edge type with N `src_labels` calls `compact_adjacency` N times against
/// the *same* L2 and delta tables. Passes 2..N early-return on empty deltas, so
/// it is benign today — but the shape had no coverage.
#[tokio::test]
async fn compact_all_with_multi_src_label_edge_type() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_label("Robot")?;
    let edge_type_id = schema_manager.add_edge_type(
        "KNOWS",
        vec!["Person".to_string(), "Robot".to_string()],
        vec!["Person".to_string(), "Robot".to_string()],
    )?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(&path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage.clone(), schema_manager.clone(), 1).await?);

    let a = writer.next_vid().await?;
    let b = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(a, HashMap::new(), &["Person".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(b, HashMap::new(), &["Robot".to_string()], None)
        .await?;
    let eid = writer.next_eid(edge_type_id).await?;
    writer
        .insert_edge(a, b, edge_type_id, eid, HashMap::new(), None, None)
        .await?;
    writer.flush_to_l1(None).await?;

    Compactor::new(storage.clone()).compact_all().await?;

    let reopened = Arc::new(StorageManager::new(&path, schema_manager.clone()).await?);
    let am = reopened.adjacency_manager();
    am.warm(&reopened, edge_type_id, Direction::Outgoing, None)
        .await?;
    let neighbours: Vec<Vid> = am
        .get_neighbors(a, edge_type_id, Direction::Outgoing)
        .into_iter()
        .map(|(neighbour, _eid)| neighbour)
        .collect();
    assert_eq!(
        neighbours,
        vec![b],
        "an edge type with two src_labels compacts the same tables twice; the second \
         pass must not clear what the first merged"
    );
    Ok(())
}

/// Every point at which compaction honours cancellation.
const SAFE_POINTS: [&str; 5] = [
    "compaction::safe-point::before-label",
    "compaction::safe-point::before-vertex-replace",
    "compaction::safe-point::before-adjacency",
    "compaction::safe-point::before-adj-replace",
    "compaction::safe-point::before-delta-clear",
];

/// A pass cancelled at any safe point must leave a store that reads the same
/// edge set in both directions (from the deltas over the old L2, or from a
/// half-merged pass), and a later pass on a fresh handle must converge.
///
/// The cancel is issued from the safe point's own fail point, so it lands
/// exactly there; the test then requires the pass to report that very point,
/// which is what rules out a pass that never reached it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_compaction_leaves_store_consistent_at_every_safe_point() -> Result<()> {
    use uni_store::storage::compaction::CompactionCancelled;

    for seam in SAFE_POINTS {
        let g = graph().await?;
        // A declared, populated property, so the vertex pass reaches its
        // replace: a label with no declared properties is skipped outright.
        g.schema_manager.add_property(
            "Person",
            "name",
            uni_common::core::schema::DataType::String,
            true,
        )?;
        g.schema_manager.save().await?;
        let named = |name: &str| {
            HashMap::from([(
                "name".to_string(),
                uni_common::Value::String(name.to_string()),
            )])
        };
        let mut vids = Vec::new();
        for name in ["a", "b", "c"] {
            let vid = g.writer.next_vid().await?;
            g.writer
                .insert_vertex_with_labels(vid, named(name), &["Person".to_string()], None)
                .await?;
            vids.push(vid);
        }
        let (a, b, c) = (vids[0], vids[1], vids[2]);
        knows(&g, a, b).await?;
        let e_ac = knows(&g, a, c).await?;
        g.writer.flush_to_l1(None).await?;
        g.writer
            .delete_edge(e_ac, a, c, g.edge_type_id, None)
            .await?;
        g.writer.flush_to_l1(None).await?;

        let token = g.storage.compaction_cancel_token().clone();
        fail::cfg_callback(seam, move || token.cancel()).unwrap();
        let res = Compactor::new(g.storage.clone()).compact_all().await;
        fail::remove(seam);
        let err = match res {
            Ok(r) => panic!("{seam}: the pass completed although it was cancelled: {r:?}"),
            Err(e) => e,
        };
        let cancelled = err
            .downcast_ref::<CompactionCancelled>()
            .unwrap_or_else(|| panic!("{seam}: expected CompactionCancelled, got {err:#}"));
        assert_eq!(cancelled.at, seam, "the pass stopped somewhere else");

        let check = |label: &str| format!("{seam} ({label})");
        assert_eq!(
            neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
            vec![b],
            "{}",
            check("fwd after cancel")
        );
        assert_eq!(
            neighbors_after_reopen(&g, b, Direction::Incoming).await?,
            vec![a],
            "{}",
            check("bwd after cancel")
        );
        assert_eq!(
            neighbors_after_reopen(&g, c, Direction::Incoming).await?,
            Vec::<Vid>::new(),
            "{}",
            check("deleted edge resurrected after cancel")
        );

        // The cancelled token is permanent; a fresh handle finishes the work.
        let fresh = Arc::new(StorageManager::new(&g.path, g.schema_manager.clone()).await?);
        Compactor::new(fresh).compact_all().await?;
        assert_eq!(
            neighbors_after_reopen(&g, a, Direction::Outgoing).await?,
            vec![b],
            "{}",
            check("fwd after converging pass")
        );
        assert_eq!(
            neighbors_after_reopen(&g, b, Direction::Incoming).await?,
            vec![a],
            "{}",
            check("bwd after converging pass")
        );
        assert_eq!(
            neighbors_after_reopen(&g, c, Direction::Incoming).await?,
            Vec::<Vid>::new(),
            "{}",
            check("deleted edge resurrected after converging pass")
        );
    }
    Ok(())
}
