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
//! A fork now stages on its own branch of each table and writes no crash
//! marker.
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
    bulk.commit().await?;
    tx.commit().await?;
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

/// A fork load that fails part-way through publishing does not touch primary
/// at the next open.
///
/// The publish step is where a staging load writes the crash marker, so this
/// is the case the fork's marker-free publish exists for. What it leaves on
/// the fork is pinned too: the per-label table is published and the main
/// table is not, the residual a fork has no reconciliation for (a fork's
/// branches are outside the database's single crash marker).
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
        uni_db::api::bulk::FAIL_AFTER_PERLABEL_WRITE.store(true, SeqCst);
        let r = bulk.commit().await;
        uni_db::api::bulk::FAIL_AFTER_PERLABEL_WRITE.store(false, SeqCst);
        assert!(r.is_err(), "the injected fault must fail the publish");
        tx.rollback();
        assert_eq!(
            count(&fork, NODES).await?,
            22,
            "residual: the fork's per-label table was published"
        );
        assert_eq!(
            count(&fork, "MATCH (n) WHERE n.k >= 100 RETURN count(n)").await?,
            0,
            "residual: the fork's main table was not"
        );
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
