// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A commit made after a reopen survives a crash before the next flush.
//!
//! A flush publishes a manifest whose `wal_high_water_mark` is the WAL LSN it
//! covers, then deletes every segment at or below it. A cleanly shut-down
//! store therefore had no segments, the reopened log numbered from LSN 1
//! again, and recovery -- which replays only segments above the mark -- skipped
//! every commit written after the reopen (manifest mark 3, new segment LSN 1:
//! the fourth row was lost).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(wal_lsn_after_reopen)'

// Rust guideline compliant

use anyhow::Result;
use uni_db::DataType;

use super::declared_endpoint_violation_reads::ints;
use super::endpoint_conformance_writer_features::{copy_tree, open};

/// A commit after a reopen survives a crash: its WAL segment's LSN is above
/// the latest manifest's WAL mark, so replay does not skip it.
#[tokio::test]
async fn wal_lsn_after_reopen_commit_survives_a_crash() -> Result<()> {
    let dir = tempfile::tempdir()?;
    {
        let db = open(dir.path()).await?;
        db.schema()
            .label("A")
            .property("v", DataType::Int)
            .apply()
            .await?;
        for v in 1..=3 {
            let tx = db.session().tx().await?;
            tx.execute(&format!("CREATE (:A {{v: {v}}})")).await?;
            tx.commit().await?;
            db.flush().await?;
        }
        db.shutdown().await?;
    }
    let crashed = tempfile::tempdir()?;
    {
        let db = open(dir.path()).await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE (:A {v: 4})").await?;
        tx.commit().await?;
        // The committed state as a crash leaves it: copied while open.
        copy_tree(dir.path(), crashed.path())?;
        db.shutdown().await?;
    }
    let db = open(crashed.path()).await?;
    assert_eq!(
        ints(&db, "MATCH (n:A) RETURN n.v").await?,
        vec![1, 2, 3, 4],
        "the commit after the reopen is recovered"
    );
    db.shutdown().await?;
    Ok(())
}
