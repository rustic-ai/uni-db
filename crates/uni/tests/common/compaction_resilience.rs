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

/// `label-skew` seeds a two-label graph instead: one multi-label vertex that
/// survives and one that is deleted, so the interrupted pass has a tombstone to
/// drop from one anchor while the other anchor still holds it.
async fn multi_label_child(uri: &str) -> ! {
    {
        let db = Uni::open(uri).config(crash_config()).build().await.unwrap();
        let session = db.session();
        let tx = session.tx().await.unwrap();
        tx.execute("CREATE LABEL Person (name STRING)")
            .await
            .unwrap();
        tx.execute("CREATE LABEL Staff (badge STRING)")
            .await
            .unwrap();
        tx.execute("CREATE (:Person:Staff {name: 'alice', badge: 'a1', nickname: 'al'})")
            .await
            .unwrap();
        tx.execute("CREATE (:Person:Staff {name: 'carol', badge: 'c1'})")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();

        // Deliberately UNANCHORED. A label-anchored `DETACH DELETE` of a
        // multi-label vertex is correct in memory but resurrects through the
        // vertex's other labels after a flush — a pre-existing defect unrelated
        // to compaction, pinned by
        // `storage::multi_label_delete::a_label_anchored_delete_survives_a_flush`.
        // Using the anchored form here would make this test fail for a reason
        // that has nothing to do with the seam it exists to exercise.
        let tx = session.tx().await.unwrap();
        tx.execute("MATCH (c {name: 'carol'}) DETACH DELETE c")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();

        // Precondition: the delete must actually have taken effect through both
        // anchors before the crash, or the assertions afterwards prove nothing.
        for anchor in ["Person", "Staff"] {
            let rows = session
                .query(&format!("MATCH (n:{anchor}) RETURN n.name AS name"))
                .await
                .unwrap();
            assert_eq!(rows.len(), 1, "precondition: carol is deleted via {anchor}");
        }
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(uri).config(crash_config()).build().await.unwrap();
    crate::crash_harness::abort_at("compaction::between-labels");
    let session = db.session();
    let tx = session.tx().await.unwrap();
    let _ = tx.execute("VACUUM").await;
    let _ = tx.commit().await;
    panic!("the operation returned; the seam for 'label-skew' was never reached");
}

/// `lance-mid` needs a scalar index and enough fragments for `compact_files` to
/// have something to merge.
///
/// The first `compact` is allowed to COMPLETE before the seam is armed:
/// compaction runs one flush behind, so an armed first pass would abort on a
/// call that merged nothing and the crash would prove far less.
async fn indexed_child(uri: &str) -> ! {
    {
        let db = Uni::open(uri).config(crash_config()).build().await.unwrap();
        let session = db.session();
        let tx = session.tx().await.unwrap();
        tx.execute("CREATE LABEL Item (name STRING)").await.unwrap();
        tx.commit().await.unwrap();
        let tx = session.tx().await.unwrap();
        tx.execute("CREATE INDEX idx_item_name FOR (i:Item) ON (i.name)")
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // Separate flushes so each round becomes its own fragment.
        for round in 0..6 {
            let tx = session.tx().await.unwrap();
            for i in 0..10 {
                tx.execute(&format!("CREATE (:Item {{name: 'item-{}-{}'}})", round, i))
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
            db.flush().await.unwrap();
        }
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(uri).config(crash_config()).build().await.unwrap();
    // Unarmed: lets the one-flush-behind pass complete so the armed pass below
    // is one that actually merges.
    db.compaction().compact("Item").await.unwrap();

    crate::crash_harness::abort_at("compaction::after-compact-files-before-cleanup");
    let _ = db.compaction().compact("Item").await;
    panic!("the operation returned; the seam for 'lance-mid' was never reached");
}

/// Child-process entry point. Returns immediately in the parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal: child process entry point for the abort harness"]
async fn compaction_abort_child() {
    let Some((scenario, path)) = crate::crash_harness::child_env() else {
        return;
    };
    let uri = path.to_string_lossy().into_owned();

    // Scenarios needing a different graph dispatch BEFORE the default seed:
    // each of these drives its own seed, arms its own seam, and never returns.
    match scenario.as_str() {
        "label-skew" => multi_label_child(&uri).await,
        "lance-mid" => indexed_child(&uri).await,
        _ => {}
    }

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
        "label-skew" => "compaction::between-labels",
        "lance-mid" => "compaction::after-compact-files-before-cleanup",
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

/// Both label anchors must agree about a multi-label vertex, whichever label
/// the interrupted pass happened to reach — `schema.labels` is a map, so the
/// order is not defined and the assertion is deliberately symmetric.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn crash_between_labels_keeps_multi_label_vertex_consistent() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_string_lossy().into_owned();
    crate::crash_harness::run_child_async(CHILD, "label-skew", dir.path()).await;

    let db = Uni::open(&uri).config(crash_config()).build().await?;
    let session = db.session();

    for anchor in ["Person", "Staff"] {
        let rows = session
            .query(&format!(
                "MATCH (n:{anchor}) RETURN n.name AS name ORDER BY name"
            ))
            .await?;
        let names: Vec<String> = rows
            .rows()
            .iter()
            .map(|r| r.get::<String>("name").unwrap_or_default())
            .collect();
        assert_eq!(
            names,
            vec!["alice".to_string()],
            "anchor {anchor} disagrees after a crash between per-label compactions: a \
             tombstone dropped from one label's table must not make the vertex visible \
             through the other"
        );
    }

    // The survivor must report the same properties through both anchors,
    // including the schemaless one the reserved-column rebuild reconstructs.
    let via_person = session
        .query("MATCH (n:Person {name: 'alice'}) RETURN n.badge AS badge, n.nickname AS nickname")
        .await?;
    let via_staff = session
        .query("MATCH (n:Staff {badge: 'a1'}) RETURN n.badge AS badge, n.nickname AS nickname")
        .await?;
    assert_eq!(via_person.len(), 1, "alice is reachable through :Person");
    assert_eq!(via_staff.len(), 1, "alice is reachable through :Staff");
    assert_eq!(
        via_person.rows()[0].get::<String>("nickname")?,
        via_staff.rows()[0].get::<String>("nickname")?,
        "the two label anchors report different property maps for the same vertex"
    );

    // A completed pass must converge.
    let tx = session.tx().await?;
    tx.execute("VACUUM").await?;
    tx.commit().await?;
    for anchor in ["Person", "Staff"] {
        let rows = session
            .query(&format!("MATCH (n:{anchor}) RETURN n.name AS name"))
            .await?;
        assert_eq!(
            rows.len(),
            1,
            "anchor {anchor} after a completed compaction"
        );
    }

    db.shutdown().await?;
    Ok(())
}

/// A committed fragment rewrite whose index repair never ran. Nothing
/// re-triggers `optimize_indices` on its own — the next `compact_files` finds
/// an empty plan — so if reads depended on the repair they would stay wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "failpoint crash injection; run with --features failpoints"]
async fn crash_after_compact_files_leaves_indexes_usable() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_string_lossy().into_owned();
    crate::crash_harness::run_child_async(CHILD, "lance-mid", dir.path()).await;

    let db = Uni::open(&uri).config(crash_config()).build().await?;
    let session = db.session();

    // Full scan: the reference answer, no index involved.
    let all = session
        .query("MATCH (i:Item) RETURN i.name AS name")
        .await?;
    assert_eq!(
        all.len(),
        60,
        "no row may be lost by a crash between compact_files and its cleanup"
    );

    // Index-backed equality on the indexed column, for every seeded value. If
    // the rewrite left index entries pointing at pre-rewrite row addresses,
    // these lookups disagree with the scan above.
    for round in 0..6 {
        for i in 0..10 {
            let name = format!("item-{round}-{i}");
            let hit = session
                .query(&format!(
                    "MATCH (i:Item) WHERE i.name = '{name}' RETURN i.name AS name"
                ))
                .await?;
            assert_eq!(
                hit.len(),
                1,
                "index-backed lookup of {name} disagrees with the full scan after a crash \
                 that committed a fragment rewrite without re-optimizing indices"
            );
        }
    }

    // And the database converges: compaction reaches a fixpoint rather than
    // rediscovering work forever.
    let mut quiet = 0;
    for _ in 0..10 {
        let stats = db.compaction().compact("Item").await?;
        if stats.fragments_removed == 0 {
            quiet += 1;
            if quiet == 2 {
                break;
            }
        } else {
            quiet = 0;
        }
    }
    assert_eq!(
        quiet, 2,
        "compaction did not reach a fixpoint after the crash"
    );

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
