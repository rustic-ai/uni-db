// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Failing regression repros for four VERIFIED storage-race bugs in the flush
//! / L0-rotation machinery, driven deterministically with `fail::fail_point!`
//! seams.
//!
//! These tests are RED today (they assert the *buggy* observable). They turn
//! GREEN once the underlying races are fixed; the seams themselves are no-ops
//! without the `failpoints` feature. Each test owns its failpoint and runs in
//! its own process under nextest, so the global registry does not bleed.
//!
//! Covered:
//! - Bug #3 — a failed async rotate permanently wedges the flush finalizer
//!   (`flush::rotate-fail`).
//! - Bug #4 — a non-transactional delete races L0 rotation and is silently lost
//!   (`nontx::after-capture`).
//! - Bug #9A — a unique-constraint hole opens during the flush window
//!   (`flush::after-rotate-before-lance`).
//! - Bug #10 — a stale property-cache window after flush finalize yields a
//!   non-monotonic read (`flush::after-complete-before-cache-clear`).
//!
//! Run with: `cargo nextest run -p uni-store --features failpoints <test_name>`.

#![cfg(feature = "failpoints")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::Value;
use uni_common::config::UniConfig;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType, SchemaManager};
use uni_store::runtime::QueryContext;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

// Rust guideline compliant

/// Builds a writer with the `Counter`/`n` schema and an explicit [`UniConfig`].
async fn make_writer_with_config(config: UniConfig) -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Counter")?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer =
        Arc::new(Writer::new_with_config(storage, schema_manager, 1, config, None, None).await?);
    Ok((writer, dir))
}

/// Builds a writer carrying a UNIQUE constraint on `E.eid`.
async fn make_writer_unique(config: UniConfig) -> Result<(Arc<Writer>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("E")?;
    schema_manager.add_constraint(Constraint {
        name: "E_eid_unique".to_string(),
        constraint_type: ConstraintType::Unique {
            properties: vec!["eid".to_string()],
        },
        target: ConstraintTarget::Label("E".to_string()),
        enabled: true,
    })?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer =
        Arc::new(Writer::new_with_config(storage, schema_manager, 1, config, None, None).await?);
    Ok((writer, dir))
}

fn counter_props(n: i64) -> HashMap<String, Value> {
    let mut props = HashMap::new();
    props.insert("n".to_string(), Value::Int(n));
    props
}

fn eid_props(value: &str) -> HashMap<String, Value> {
    let mut props = HashMap::new();
    props.insert("eid".to_string(), Value::String(value.to_string()));
    props
}

/// Builds the same read context the production read path uses (current L0 plus
/// every `pending_flush` buffer). Reads with `ctx = None` short-circuit past all
/// L0 buffers, so faithful reproduction requires this context.
fn read_ctx(writer: &Writer) -> QueryContext {
    QueryContext::new_with_pending(
        writer.l0_manager.get_current(),
        None,
        writer.l0_manager.get_pending_flush(),
    )
}

// ── Bug #3 — failed async rotate wedges the flush finalizer ──────────────────

/// Regression for Bug #3: a single failed async rotate must not permanently
/// wedge the flush finalizer.
///
/// `commit_transaction_l0` consumes a rotate sequence number and bumps the
/// pending count BEFORE the fallible `flush_l0_rotate`; on the Err arm both are
/// leaked. The finalizer only finalizes strictly-consecutive seq numbers and
/// only decrements pending on finalize, so one rotate failure leaves a
/// permanent gap: later flushes can never finalize and the pending count never
/// drains.
///
/// RED today: after one injected rotate failure, a subsequent async flush never
/// drains — the assertion that it DOES drain fails. GREEN once the seq/pending
/// are rolled back on the Err path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_rotate_does_not_wedge_flush_finalizer() -> Result<()> {
    let config = UniConfig {
        async_flush_enabled: true,
        auto_flush_threshold: 1, // every commit triggers a flush
        auto_flush_min_mutations: 1,
        max_pending_flushes: 4,
        drop_fork_drain_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let (writer, _dir) = make_writer_with_config(config).await?;

    let coord = writer
        .flush_coordinator()
        .expect("async flush enabled => coordinator present")
        .clone();

    // First commit: rotate fails exactly once. The commit still returns Ok
    // (the rotate failure is logged as non-critical), but the seq/pending were
    // leaked by the buggy Err arm.
    fail::cfg("flush::rotate-fail", "return").unwrap();
    let tx0 = writer.create_transaction_l0();
    let v0 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v0, counter_props(0), &["Counter".to_string()], Some(&tx0))
        .await?;
    writer.commit_transaction_l0(tx0).await?; // Ok despite the skipped flush
    fail::cfg("flush::rotate-fail", "off").unwrap();

    // The leaked rotate left the pending count incremented even though no
    // stream/finalize was ever submitted.
    let pending_after_fail = coord.pending_flush_count();

    // Second commit: rotate now succeeds and a real async flush is submitted.
    // But its seq is one past the leaked-and-never-finalized seq, so the
    // strictly-consecutive finalizer can never advance to it.
    let tx1 = writer.create_transaction_l0();
    let v1 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, counter_props(1), &["Counter".to_string()], Some(&tx1))
        .await?;
    writer.commit_transaction_l0(tx1).await?;

    // Correct behavior: the second flush should finalize and the pipeline
    // should drain to zero. RED today — the finalizer is wedged behind the
    // leaked seq, so `drain` times out and the pending count stays >= 1.
    let drained = coord.drain(Duration::from_secs(2)).await;
    assert!(
        drained.is_ok() && coord.pending_flush_count() == 0,
        "flush finalizer wedged by a failed rotate (Bug #3): drain = {drained:?}, \
         pending_after_fail = {pending_after_fail}, pending_now = {}. \
         A correct rollback of the leaked seq/pending would let the later flush \
         finalize and drain to zero.",
        coord.pending_flush_count()
    );
    Ok(())
}

// ── Bug #4 — non-transactional delete lost across L0 rotation ────────────────

/// Regression for Bug #4: a non-transactional `delete_vertex` must not be lost
/// when a concurrent flush rotates the buffer it captured.
///
/// `delete_vertex` captures the current L0 Arc, then (with no flush lock held)
/// awaits a label lookup before writing the tombstone into the captured Arc. If
/// a flush rotates + completes that buffer in the gap, the tombstone lands in an
/// orphaned buffer that no read path consults — a silent lost delete.
///
/// RED today: after the racing flush completes and the delete's tombstone
/// write lands, the vertex is still present. GREEN once the write re-resolves
/// `get_current()` under `flush_lock`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nontx_delete_not_lost_across_rotation() -> Result<()> {
    let (writer, _dir) = make_writer_with_config(UniConfig::default()).await?;

    // Insert V and flush it to L1 so it is durable and the current L0 is empty.
    let v = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v, counter_props(7), &["Counter".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Task A: non-tx delete of V, paused right after it captured the current
    // L0 Arc but before it writes the tombstone.
    fail::cfg("nontx::after-capture", "pause").unwrap();
    let writer_a = writer.clone();
    let handle = tokio::spawn(async move {
        writer_a
            .delete_vertex(v, Some(vec!["Counter".to_string()]), None)
            .await
    });

    // Give A time to reach the pause seam.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Main task: insert a second vertex so the current buffer has a mutation,
    // then flush it to completion. This rotates + completes the very buffer A
    // captured (A captured it before this flush rotated it).
    let other = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(other, counter_props(0), &["Counter".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Release A so its tombstone write lands in the now-orphaned buffer.
    fail::cfg("nontx::after-capture", "off").unwrap();
    handle.await.expect("delete task join")?;

    // Correct behavior: V is deleted, so the read returns Null. RED today —
    // the tombstone landed in an orphaned buffer, so the read still sees V via
    // storage (n = 7).
    let pm = writer
        .property_manager
        .as_ref()
        .expect("writer has a property manager");
    let ctx = read_ctx(&writer);
    let observed = pm.get_vertex_prop_with_ctx(v, "n", Some(&ctx)).await?;
    assert_eq!(
        observed,
        Value::Null,
        "non-tx delete lost across L0 rotation (Bug #4): V still reads {observed:?} \
         instead of Null. The tombstone landed in a buffer that was rotated + \
         completed out from under the captured Arc."
    );
    Ok(())
}

// ── Bug #9A — unique-constraint hole during the flush window ─────────────────

/// Regression for Bug #9 (Mechanism A): a UNIQUE-constrained key must not become
/// insertable during the flush window.
///
/// `check_unique_constraint_multi` consults only the current buffer's index, the
/// tx L0, and durable Lance. After a flush rotates a key's buffer onto
/// `pending_flush` and installs a fresh empty current buffer, the key is
/// invisible to all three until it reaches Lance — so a duplicate slips through.
///
/// RED today: the second insert of the same unique key SUCCEEDS while the flush
/// is paused mid-window. GREEN once the check also consults `pending_flush`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unique_constraint_hole_during_flush_window() -> Result<()> {
    let (writer, _dir) = make_writer_unique(UniConfig::default()).await?;

    // Insert key K into the current L0.
    let k1 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(k1, eid_props("shared"), &["E".to_string()], None)
        .await?;

    // Start a flush that rotates K onto pending_flush, then PAUSES before the
    // rotated rows reach Lance (K is now in neither current nor Lance).
    fail::cfg("flush::after-rotate-before-lance", "pause").unwrap();
    let writer_f = writer.clone();
    let flush_handle = tokio::spawn(async move { writer_f.flush_to_l1(None).await });

    // Give the flush time to rotate and reach the pause seam.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // From the main task, insert K again. The constraint check sees an empty
    // current buffer, no tx L0, and K not yet in Lance.
    let k2 = writer.next_vid().await?;
    let second = writer
        .insert_vertex_with_labels(k2, eid_props("shared"), &["E".to_string()], None)
        .await;

    // Release the flush and let it finish.
    fail::cfg("flush::after-rotate-before-lance", "off").unwrap();
    flush_handle.await.expect("flush task join")?;

    // Correct behavior: the duplicate unique-key insert must be REJECTED. RED
    // today — the constraint check misses K (it is on pending_flush, not in
    // current, not yet in Lance), so the duplicate slips through.
    assert!(
        second.is_err(),
        "unique-constraint hole during the flush window (Bug #9A): a duplicate \
         insert of key 'shared' SUCCEEDED while K was mid-flush (on pending_flush, \
         not yet in Lance). A correct check would consult pending_flush and reject it."
    );
    Ok(())
}

// ── Bug #10 — stale property-cache window after flush finalize ───────────────

/// Regression for Bug #10: a read after a flush finalize must not return a stale
/// cached value (non-monotonic read).
///
/// `flush_finalize_body` does `complete_flush` (J), then WAL truncate (K), then
/// `clear_cache` (L), in that order. Property reads check the L0 chain, then the
/// cache, then storage. Between J and L, a value that lived only in the
/// now-removed pending buffer misses the L0 chain and hits a STALE cache entry.
///
/// RED today: after updating to NEW and flushing with a pause between J and L,
/// the read returns the OLD cached value. GREEN once the cache is cleared (or
/// the buffer stays visible) across that window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_property_cache_window_after_flush() -> Result<()> {
    let (writer, _dir) = make_writer_with_config(UniConfig::default()).await?;
    let pm = writer
        .property_manager
        .as_ref()
        .expect("writer has a property manager")
        .clone();

    // Insert V with prop = OLD and flush so OLD reaches Lance, current empty.
    let v = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v, counter_props(100), &["Counter".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Read once (no live L0 buffer holds V now) so the property cache is
    // populated with OLD: the L0 chain is empty, so this falls through to
    // storage and caches the value.
    {
        let ctx = read_ctx(&writer);
        assert_eq!(
            pm.get_vertex_prop_with_ctx(v, "n", Some(&ctx)).await?,
            Value::Int(100)
        );
    }

    // Update V to NEW into the (now-current) L0. A read now sees NEW via the L0
    // chain (it shadows the cached OLD).
    writer
        .insert_vertex_with_labels(v, counter_props(200), &["Counter".to_string()], None)
        .await?;
    {
        let ctx = read_ctx(&writer);
        assert_eq!(
            pm.get_vertex_prop_with_ctx(v, "n", Some(&ctx)).await?,
            Value::Int(200)
        );
    }

    // Flush the buffer holding NEW, pausing between complete_flush (J) and
    // clear_cache (L). During the pause: NEW's buffer is removed from the L0
    // chain but the cache still holds OLD.
    fail::cfg("flush::after-complete-before-cache-clear", "pause").unwrap();
    let writer_f = writer.clone();
    let flush_handle = tokio::spawn(async move { writer_f.flush_to_l1(None).await });

    // Give the flush time to reach the pause seam (it must get past J).
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Read during the window. The L0 chain no longer has NEW (buffer removed at
    // J), the cache still has OLD, so the read returns the STALE OLD value.
    let observed = {
        let ctx = read_ctx(&writer);
        pm.get_vertex_prop_with_ctx(v, "n", Some(&ctx)).await?
    };

    // Release the flush and let it finish.
    fail::cfg("flush::after-complete-before-cache-clear", "off").unwrap();
    flush_handle.await.expect("flush task join")?;

    // Correct behavior: the read is monotonic and returns NEW (200). RED today —
    // it goes backwards to the stale cached OLD (100).
    assert_eq!(
        observed,
        Value::Int(200),
        "stale property-cache window after flush finalize (Bug #10): a read in the \
         post-complete_flush / pre-clear_cache window returned {observed:?} (the \
         stale OLD value) instead of the monotonic NEW (200)."
    );
    Ok(())
}

// ── ext_id uniqueness during the flush window ────────────────────────────────

/// The global ext_id uniqueness check must consult rotated-but-unflushed
/// buffers (`pending_flush`) — the same window Bug #9A pinned for declared
/// UNIQUE constraints. `check_extid_globally_unique` walks
/// `[current, tx?, pending_flush…]` via each buffer's `extid_index`; this
/// test holds a flush paused mid-window and asserts a duplicate is rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extid_check_covers_flush_window() -> Result<()> {
    fn ext_props(value: &str) -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert("ext_id".to_string(), Value::String(value.to_string()));
        props
    }

    let (writer, _dir) = make_writer_with_config(UniConfig::default()).await?;

    let k1 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(k1, ext_props("shared"), &["Counter".to_string()], None)
        .await?;

    // Rotate K onto pending_flush, pausing before it reaches Lance.
    fail::cfg("flush::after-rotate-before-lance", "pause").unwrap();
    let writer_f = writer.clone();
    let flush_handle = tokio::spawn(async move { writer_f.flush_to_l1(None).await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let k2 = writer.next_vid().await?;
    let second = writer
        .insert_vertex_with_labels(k2, ext_props("shared"), &["Counter".to_string()], None)
        .await;

    fail::cfg("flush::after-rotate-before-lance", "off").unwrap();
    flush_handle.await.expect("flush task join")?;

    assert!(
        second.is_err(),
        "duplicate ext_id insert must be rejected while the owner is mid-flush \
         (on pending_flush, not yet in Lance)"
    );
    Ok(())
}

// ── Commit-time overlay checks during the flush window ───────────────────────
//
// An ASYNC flush rotation moves the current buffer onto `pending_flush`,
// installs an empty current, releases `flush_lock`, and streams to Lance in
// the background — so commits interleave with the window (inline flushes hold
// `flush_lock` throughout and cannot race commits). The COMMIT-TIME layer had
// the same exposure Bug #9A pinned for the per-insert unique check: the
// serializable-MERGE/ext_id re-probes, the CRDT carve-out merge, and the
// issue-#77 endpoint check all used to consult only the current buffer.
// These tests hold an async flush paused mid-window and pin each seam.

/// `UniConfig` with the async flush pipeline enabled (the only mode where a
/// commit can interleave with the post-rotate flush window).
fn async_flush_config() -> UniConfig {
    UniConfig {
        async_flush_enabled: true,
        ..Default::default()
    }
}

/// Two transactions insert the same ext_id before either commits; the first
/// committer's row is mid-flush when the second commits. The commit-time
/// re-probe must find the key in the pending buffer and abort.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extid_commit_probe_covers_flush_window() -> Result<()> {
    fn ext_props(value: &str) -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert("ext_id".to_string(), Value::String(value.to_string()));
        props
    }

    let (writer, _dir) = make_writer_with_config(async_flush_config()).await?;

    let tx_a = writer.create_transaction_l0();
    let tx_b = writer.create_transaction_l0();
    let va = writer.next_vid().await?;
    let vb = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(
            va,
            ext_props("shared"),
            &["Counter".to_string()],
            Some(&tx_a),
        )
        .await?;
    writer
        .insert_vertex_with_labels(
            vb,
            ext_props("shared"),
            &["Counter".to_string()],
            Some(&tx_b),
        )
        .await?;
    writer.commit_transaction_l0(tx_a).await?;

    // Rotate tx_a's row onto pending_flush (async: flush_lock released after
    // the rotate), pausing the stream before it reaches Lance.
    fail::cfg("flush::after-rotate-before-lance", "pause").unwrap();
    let ticket = writer.flush_to_l1_async(None).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let second = writer.commit_transaction_l0(tx_b).await;

    fail::cfg("flush::after-rotate-before-lance", "off").unwrap();
    ticket.await_finalize().await?;

    assert!(
        second.is_err(),
        "ext_id committed by tx_a must conflict even while its row is mid-flush \
         (commit-time re-probe must walk pending_flush)"
    );
    Ok(())
}

/// Concurrent CRDT counter increments across a flush window must MERGE (sum):
/// the committed counter state sits in a pending buffer while the second
/// increment commits into the fresh (empty) current buffer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crdt_increments_merge_across_flush_window() -> Result<()> {
    use uni_crdt::{Crdt, GCounter};
    use uni_store::runtime::QueryContext;
    use uni_store::runtime::l0_visibility::lookup_vertex_prop;

    fn gcounter_val(node: &str, count: u64) -> Value {
        let mut gc = GCounter::new();
        gc.increment(node, count);
        let json = serde_json::to_value(Crdt::GCounter(gc)).expect("serialize GCounter");
        Value::from(json)
    }

    let (writer, _dir) = make_writer_with_config(async_flush_config()).await?;
    let vid = writer.next_vid().await?;

    // Node "a" increments and commits.
    let tx1 = writer.create_transaction_l0();
    let mut p1 = HashMap::new();
    p1.insert("hits".to_string(), gcounter_val("a", 1));
    writer
        .insert_vertex_with_labels(vid, p1, &["Counter".to_string()], Some(&tx1))
        .await?;
    writer.commit_transaction_l0(tx1).await?;

    // Rotate node-a's counter state onto pending_flush, pause mid-window.
    fail::cfg("flush::after-rotate-before-lance", "pause").unwrap();
    let ticket = writer.flush_to_l1_async(None).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Node "b" increments the same counter and commits during the window.
    let tx2 = writer.create_transaction_l0();
    let mut p2 = HashMap::new();
    p2.insert("hits".to_string(), gcounter_val("b", 1));
    writer
        .insert_vertex_with_labels(vid, p2, &["Counter".to_string()], Some(&tx2))
        .await?;
    writer.commit_transaction_l0(tx2).await?;

    fail::cfg("flush::after-rotate-before-lance", "off").unwrap();
    ticket.await_finalize().await?;

    // Both increments must survive: a:1 + b:1 = 2. Without the commit-time
    // seed, node b's state shadows node a's mid-flush state at read time.
    let ctx = QueryContext::new_with_pending(
        writer.l0_manager.get_current(),
        None,
        writer.l0_manager.get_pending_flush(),
    );
    let merged = lookup_vertex_prop(vid, "hits", Some(&ctx)).expect("counter must be visible");
    let crdt: Crdt = serde_json::from_value(merged.into()).expect("decode merged CRDT");
    let Crdt::GCounter(gc) = crdt else {
        panic!("expected GCounter");
    };
    assert_eq!(
        gc.value(),
        2,
        "increments from both nodes must survive the flush window"
    );
    Ok(())
}

/// Issue #77 during the flush window: an edge to a vertex whose tombstone is
/// mid-flush (on pending_flush, current buffer empty) must be rejected at
/// commit, before the durable WAL flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edge_to_vertex_tombstoned_in_flush_window_rejected() -> Result<()> {
    let (writer, _dir) = make_writer_with_config(async_flush_config()).await?;
    let v1 = writer.next_vid().await?;
    let v2 = writer.next_vid().await?;
    writer
        .insert_vertex_with_labels(v1, counter_props(1), &["Counter".to_string()], None)
        .await?;
    writer
        .insert_vertex_with_labels(v2, counter_props(2), &["Counter".to_string()], None)
        .await?;

    // Delete v2 and pause its tombstone mid-flush.
    let tx_del = writer.create_transaction_l0();
    writer.delete_vertex(v2, None, Some(&tx_del)).await?;
    writer.commit_transaction_l0(tx_del).await?;
    fail::cfg("flush::after-rotate-before-lance", "pause").unwrap();
    let ticket = writer.flush_to_l1_async(None).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let etype = writer.schema_manager.get_or_assign_edge_type_id("REL");
    let eid = writer.next_eid(etype).await?;
    let tx_edge = writer.create_transaction_l0();
    writer
        .insert_edge(
            v1,
            v2,
            etype,
            eid,
            HashMap::new(),
            Some("REL".into()),
            Some(&tx_edge),
        )
        .await?;
    let result = writer.commit_transaction_l0(tx_edge).await;

    fail::cfg("flush::after-rotate-before-lance", "off").unwrap();
    ticket.await_finalize().await?;

    assert!(
        result.is_err(),
        "edge to a vertex whose tombstone is mid-flush must be rejected (issue #77)"
    );
    Ok(())
}
