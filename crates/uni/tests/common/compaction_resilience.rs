// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Compaction crash consistency under a real process abort.
//!
//! The in-process suite in `uni-store/tests/common/compaction_resilience.rs`
//! covers *ordering* at the three compaction seams. This one covers
//! *durability*: the child is killed with `SIGABRT` at the seam, so no `Drop`
//! impl runs, no shutdown flush happens, and the parent reopens whatever
//! actually reached disk.
//!
//! That distinction is the whole point of the abort harness. Every "crash" in
//! the suite before it was a panic-in-task followed by `drop(db)`, which still
//! runs the shutdown flush — validating graceful-close atomicity, a strictly
//! weaker property.
//!
//! Compaction is driven through `VACUUM`, which is the user-facing route to
//! semantic compaction (`flush_to_l1` then `Compactor::compact_all`). The
//! public `db.compaction().compact()` API reaches only the Lance tier and never
//! runs a semantic pass, so it cannot reach these seams.

#![cfg(all(unix, feature = "failpoints"))]

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_db::Uni;

// Rust guideline compliant

const CHILD: &str = "compaction_resilience::compaction_abort_child";

/// Deterministic flush, and no background compaction: the sweeper would
/// otherwise be a second, unsynchronised caller of the armed seam.
fn crash_config() -> UniConfig {
    let mut config = UniConfig {
        async_flush_enabled: false,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

/// Two people who know each other, plus a third vertex and an edge that are
/// deleted before the crash — so the pass being interrupted has both a
/// tombstone to drop and live rows to preserve.
async fn seed(db: &Uni) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    tx.execute("CREATE EDGE TYPE KNOWS FROM Person TO Person")
        .await?;
    tx.execute("CREATE (:Person {name: 'alice', nickname: 'al'})")
        .await?;
    tx.execute("CREATE (:Person {name: 'bob'})").await?;
    tx.execute("CREATE (:Person {name: 'carol'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = session.tx().await?;
    tx.execute(
        "MATCH (a:Person {name: 'alice'}), (b:Person {name: 'bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;
    tx.execute(
        "MATCH (a:Person {name: 'alice'}), (c:Person {name: 'carol'}) CREATE (a)-[:KNOWS]->(c)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // The tombstones: one edge and one vertex.
    let tx = session.tx().await?;
    tx.execute("MATCH (a:Person {name: 'alice'})-[r:KNOWS]->(c:Person {name: 'carol'}) DELETE r")
        .await?;
    tx.commit().await?;
    let tx = session.tx().await?;
    tx.execute("MATCH (c:Person {name: 'carol'}) DETACH DELETE c")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

/// The read invariants every scenario must satisfy after reopening.
///
/// Asserted both immediately after recovery and again after a completed
/// compaction, so it must not mutate the graph — the usability check is
/// deliberately separate for that reason.
async fn assert_read_invariants(db: &Uni) -> Result<()> {
    let session = db.session();

    let people = session
        .query("MATCH (p:Person) RETURN p.name AS name ORDER BY name")
        .await?;
    let names: Vec<String> = people
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap_or_default())
        .collect();
    assert_eq!(
        names,
        vec!["alice".to_string(), "bob".to_string()],
        "a vertex deleted before the crash must not be resurrected, and survivors must remain"
    );

    // The schemaless property is the canary for the reserved-column
    // reconstruction that `compact_vertices` performs: a crash mid-pass must
    // not leave a half-rebuilt row behind.
    let alice = session
        .query("MATCH (p:Person {name: 'alice'}) RETURN p.nickname AS nickname")
        .await?;
    assert_eq!(alice.len(), 1, "alice survives");
    assert_eq!(
        alice.rows()[0].get::<String>("nickname")?,
        "al",
        "a schemaless property must survive a crash during compaction"
    );

    let edges = session
        .query("MATCH (:Person)-[:KNOWS]->(t:Person) RETURN t.name AS name ORDER BY name")
        .await?;
    let targets: Vec<String> = edges
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap_or_default())
        .collect();
    assert_eq!(
        targets,
        vec!["bob".to_string()],
        "the deleted edge must stay deleted and the live one must survive"
    );

    // Both directions must agree — the point of the fwd/bwd seam.
    let reverse = session
        .query("MATCH (t:Person)<-[:KNOWS]-(s:Person) RETURN t.name AS name ORDER BY name")
        .await?;
    let reverse_targets: Vec<String> = reverse
        .rows()
        .iter()
        .map(|r| r.get::<String>("name").unwrap_or_default())
        .collect();
    assert_eq!(
        reverse_targets, targets,
        "the two traversal directions disagree after the crash"
    );

    Ok(())
}

/// A database that answers every read correctly but cannot accept a new commit
/// is still broken, and no read-only assertion detects that. Runs once, last,
/// because it mutates the graph the read invariants pin.
async fn assert_accepts_a_write(db: &Uni) -> Result<()> {
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'dave'})").await?;
    tx.commit().await?;
    let after = session
        .query("MATCH (p:Person {name: 'dave'}) RETURN p.name AS name")
        .await?;
    assert_eq!(after.len(), 1, "the recovered database must accept a write");
    Ok(())
}

/// Child-process entry point. Returns immediately in the parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal: child process entry point for the abort harness"]
async fn compaction_abort_child() {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return;
    };
    let uri = path.to_string_lossy().into_owned();

    // Seed and close cleanly, so the pre-crash state is genuinely durable and
    // the abort is the only ungraceful event in the run.
    {
        let db = Uni::open(&uri)
            .config(crash_config())
            .build()
            .await
            .unwrap();
        seed(&db).await.unwrap();
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(&uri)
        .config(crash_config())
        .build()
        .await
        .unwrap();
    let seam = match scenario.as_str() {
        "adj-mid" => "compaction::after-adj-replace-before-delta-clear",
        "dir-skew" => "compaction::between-fwd-and-bwd",
        "vtx-mid" => "compaction::after-vertex-replace",
        other => crate::crash_harness::unknown_scenario(CHILD, other),
    };
    crate::crash_harness::abort_at(seam);

    let session = db.session();
    let tx = session.tx().await.unwrap();
    let _ = tx.execute("VACUUM").await;
    let _ = tx.commit().await;

    panic!("the operation returned; the seam for '{scenario}' was never reached");
}

async fn assert_scenario_recovers(scenario: &str) -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_string_lossy().into_owned();

    crate::crash_harness::run_child_async(CHILD, scenario, dir.path()).await;

    let db = Uni::open(&uri).config(crash_config()).build().await?;
    assert_read_invariants(&db).await?;

    // A completed compaction after recovery must converge to the same answers.
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("VACUUM").await?;
    tx.commit().await?;
    assert_read_invariants(&db).await?;

    assert_accepts_a_write(&db).await?;
    db.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn crash_after_adj_replace_before_delta_clear_preserves_edge_set() -> Result<()> {
    assert_scenario_recovers("adj-mid").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn crash_between_fwd_and_bwd_leaves_directions_agreeing() -> Result<()> {
    assert_scenario_recovers("dir-skew").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn crash_after_vertex_replace_does_not_resurrect_deleted_vertex() -> Result<()> {
    assert_scenario_recovers("vtx-mid").await
}
