// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A bulk load through a forked session: invisible until commit, discarded by
//! abort, published on the fork alone, and never reconciled against primary.
//!
//! Measured 2026-10-18 before the fix:
//!
//! - `abort()` failed (`rollback_table on a forked backend is not supported`)
//!   and the aborted rows stayed: the fork counted 2 + 20 vertices after it.
//!   A fork's branched backend could not stage, so its loads kept the old
//!   protocol of writing each flush visibly and rolling back on abort, and a
//!   branch cannot be rolled back.
//! - An interrupted fork load destroyed **primary** data at the next open.
//!   Each visible flush wrote the database's crash marker with the versions
//!   the fork backend reports for a table, which are the primary table's, and
//!   reopen rolled primary back to them: a primary vertex committed and
//!   flushed after the fork load began was gone, and a primary table the fork
//!   load had seen absent was dropped with the primary rows written to it
//!   since.
//!
//! A fork now stages on its own branch of each table. Its crash marker and
//! receipt live in the fork's own catalog namespace (`catalog/forks/{id}/`),
//! never the database's, so a fork load is completed on the fork's branches
//! and never reconciled against primary (wave P1, B3).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(fork_bulk_load)'

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{EdgeData, Session, Uni, UniConfig, Value, Vid};

fn config() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        disable_fork_sweeper: true,
        ..Default::default()
    }
}

/// `n` rows `{k: from..from+n}`.
fn rows(from: i64, n: i64) -> Vec<HashMap<String, Value>> {
    (from..from + n)
        .map(|k| HashMap::from([("k".to_string(), Value::Int(k))]))
        .collect()
}

async fn count(session: &Session, q: &str) -> Result<i64> {
    let r = session.query(q).await?;
    match r.rows()[0].values()[0] {
        Value::Int(n) => Ok(n),
        ref other => anyhow::bail!("`{q}` returned {other:?}"),
    }
}

const NODES: &str = "MATCH (n:P) RETURN count(n)";
const EDGES: &str = "MATCH (:P)-[r:E]->(:P) RETURN count(r)";

/// `P {k}` and `E` declared on primary, `k: 1, 2` committed and flushed.
async fn primary(path: &str) -> Result<Uni> {
    let db = Uni::open(path).config(config()).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (k INT)").await?;
    tx.execute("CREATE LABEL Q (k INT)").await?;
    tx.execute("CREATE EDGE TYPE E (w INT) FROM P TO P").await?;
    tx.execute("CREATE (:P {k: 1}), (:P {k: 2})").await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// An aborted fork load leaves the fork and primary as they were, whether or
/// not the fork had written (and flushed) the table before.
#[tokio::test]
async fn fork_bulk_load_abort_discards_everything() -> Result<()> {
    for (fork_wrote, fork_flushed) in [(false, false), (true, false), (true, true)] {
        let dir = tempfile::tempdir()?;
        let db = primary(dir.path().to_str().unwrap()).await?;
        let s = db.session();
        let fork = s.fork("bulk_abort").await?;
        if fork_wrote {
            let tx = fork.tx().await?;
            tx.execute("CREATE (:P {k: 50})").await?;
            tx.commit().await?;
            if fork_flushed {
                fork.flush().await?;
            }
        }
        let before = count(&fork, NODES).await?;
        let ctx = format!("fork wrote {fork_wrote}, flushed {fork_flushed}");

        let tx = fork.tx().await?;
        let mut bulk = tx.bulk_writer().batch_size(5).build()?;
        let vids = bulk.insert_vertices("P", rows(100, 20)).await?;
        bulk.insert_edges(
            "E",
            vids.windows(2)
                .map(|w| EdgeData::new(w[0], w[1], HashMap::new()))
                .collect(),
        )
        .await?;
        // Batch size 5 flushed the load in several runs; none is visible.
        assert_eq!(count(&fork, NODES).await?, before, "{ctx}: mid-load");
        assert_eq!(count(&fork, EDGES).await?, 0, "{ctx}: mid-load edges");
        bulk.abort().await?;
        tx.rollback();

        assert_eq!(count(&fork, NODES).await?, before, "{ctx}: after abort");
        assert_eq!(count(&fork, EDGES).await?, 0, "{ctx}: edges after abort");
        fork.flush().await?;
        assert_eq!(count(&fork, NODES).await?, before, "{ctx}: abort, flush");
        assert_eq!(count(&s, NODES).await?, 2, "{ctx}: primary");
    }
    Ok(())
}

/// A committed fork load is visible on the fork, traversable, and absent
/// from primary.
#[tokio::test]
async fn fork_bulk_load_commit_publishes_on_the_fork_only() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = primary(dir.path().to_str().unwrap()).await?;
    let s = db.session();
    let fork = s.fork("bulk_commit").await?;
    let tx = fork.tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(5).build()?;
    let vids: Vec<Vid> = bulk.insert_vertices("P", rows(200, 10)).await?;
    bulk.insert_edges(
        "E",
        vids.windows(2)
            .map(|w| EdgeData::new(w[0], w[1], HashMap::from([("w".into(), Value::Int(1))])))
            .collect(),
    )
    .await?;
    let load_id = bulk.load_id().to_string();
    bulk.commit().await?;
    tx.commit().await?;
    // Measured on df777cb3e: `false` -- a fork load kept no receipt.
    assert!(
        fork.bulk_load_applied(&load_id).await?,
        "the fork's session reports its applied load"
    );
    assert!(
        !db.bulk_load_applied(&load_id).await?,
        "primary has no record of a fork's load"
    );
    for flushed in [false, true] {
        if flushed {
            fork.flush().await?;
        }
        assert_eq!(count(&fork, NODES).await?, 12, "fork (flushed {flushed})");
        assert_eq!(
            count(&fork, EDGES).await?,
            9,
            "fork edges (flushed {flushed})"
        );
        assert_eq!(
            count(&fork, "MATCH (:P {k: 200})-[:E*]->(m:P) RETURN count(m)").await?,
            9,
            "fork traversal (flushed {flushed})"
        );
        assert_eq!(count(&s, NODES).await?, 2, "primary (flushed {flushed})");
        assert_eq!(
            count(&s, EDGES).await?,
            0,
            "primary edges (flushed {flushed})"
        );
    }
    Ok(())
}

/// A fork load interrupted without commit or abort does not touch primary
/// at the next open, for a table primary had written and one it had not.
#[tokio::test]
async fn fork_bulk_load_interrupted_leaves_primary_intact_at_reopen() -> Result<()> {
    for label in ["P", "Q"] {
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_str().unwrap().to_string();
        {
            let db = primary(&path).await?;
            let s = db.session();
            let fork = s.fork("bulk_crash").await?;
            let tx = fork.tx().await?;
            let mut bulk = tx.bulk_writer().batch_size(5).build()?;
            bulk.insert_vertices(label, rows(100, 20)).await?;
            // The writer and its transaction go away with neither commit nor
            // abort -- what a crash leaves behind.
            drop(bulk);
            tx.rollback();
            drop(fork);
            // Primary writes both labels after the fork load began.
            let tx = s.tx().await?;
            tx.execute("CREATE (:P {k: 7}), (:Q {k: 8})").await?;
            tx.commit().await?;
            db.flush().await?;
        }
        let db = Uni::open(&path).config(config()).build().await?;
        let s = db.session();
        assert_eq!(
            count(&s, NODES).await?,
            3,
            "fork load into {label}: P after reopen"
        );
        assert_eq!(
            count(&s, "MATCH (n:Q) RETURN count(n)").await?,
            1,
            "fork load into {label}: Q after reopen"
        );
    }
    Ok(())
}

/// A fork load that fails part-way through publishing is completed on the
/// fork, in-process, and does not touch primary at the next open.
///
/// Measured on df777cb3e: the commit returned `Internal`, and the fork kept
/// the per-label table published and the main table not -- 22 vertices by
/// label, 0 label-less -- through reopen. The fork's commit record now drives
/// the same in-process completion as primary's.
#[tokio::test]
async fn fork_bulk_load_failed_publish_leaves_primary_intact_at_reopen() -> Result<()> {
    use std::sync::atomic::Ordering::SeqCst;

    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = primary(&path).await?;
        let s = db.session();
        let fork = s.fork("bulk_publish_crash").await?;
        let tx = fork.tx().await?;
        let mut bulk = tx.bulk_writer().batch_size(5).build()?;
        bulk.insert_vertices("P", rows(100, 20)).await?;
        // Fails after the per-label table is published, before `vertices`.
        let load_id = bulk.load_id().to_string();
        uni_db::api::bulk::FAIL_AFTER_PERLABEL_WRITE.store(true, SeqCst);
        let r = bulk.commit().await;
        uni_db::api::bulk::FAIL_AFTER_PERLABEL_WRITE.store(false, SeqCst);
        let stats = r?;
        assert!(
            stats
                .warnings
                .iter()
                .any(|w| w.contains("injected failure after per-label vertex commit")),
            "the failed first attempt is reported: {:?}",
            stats.warnings
        );
        tx.rollback();
        assert_eq!(count(&fork, NODES).await?, 22, "the fork's label scan");
        assert_eq!(
            count(&fork, "MATCH (n) WHERE n.k >= 100 RETURN count(n)").await?,
            20,
            "the fork's label-less scan sees the whole load too"
        );
        assert!(fork.bulk_load_applied(&load_id).await?);
        drop(fork);
        let tx = s.tx().await?;
        tx.execute("CREATE (:P {k: 7})").await?;
        tx.commit().await?;
        db.flush().await?;
    }
    let db = Uni::open(&path).config(config()).build().await?;
    assert_eq!(
        count(&db.session(), NODES).await?,
        3,
        "primary after reopen"
    );
    Ok(())
}

/// A fork's bulk commit must not write the fork's schema over primary's.
///
/// A fork's `SchemaManager` is `primary ⊕ overlay` but keeps primary's path, so
/// `SchemaManager::save` from a fork writes the merged view to primary's
/// `catalog/schema.json`. The bulk commit saved the schema whatever the
/// session, which published a fork-local label on primary at the next open.
#[tokio::test]
async fn fork_bulk_load_does_not_leak_fork_schema_to_primary() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = primary(&path).await?;
        let s = db.session();
        let fork = s.fork("bulk_schema").await?;
        fork.fork_schema().label("ForkOnly").apply().await?;
        let tx = fork.tx().await?;
        let mut bulk = tx.bulk_writer().batch_size(5).build()?;
        bulk.insert_vertices("ForkOnly", rows(0, 3)).await?;
        bulk.commit().await?;
        tx.commit().await?;
        assert_eq!(
            count(&fork, "MATCH (n:ForkOnly) RETURN count(n)").await?,
            3,
            "premise: the fork sees its load"
        );
        assert!(
            !db.label_exists("ForkOnly").await?,
            "primary must not know the fork-local label (live)"
        );
    }
    let db = Uni::open(&path).config(config()).build().await?;
    assert!(
        !db.label_exists("ForkOnly").await?,
        "primary must not know the fork-local label after reopen"
    );
    Ok(())
}
