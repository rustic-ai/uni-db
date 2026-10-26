// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #305 triage repro: a `BulkWriter` dropped without `commit()` /
//! `abort()` must not lose an earlier committed, unflushed transaction.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

fn rows(prefix: &str, n: usize) -> Vec<HashMap<String, Value>> {
    (0..n)
        .map(|i| {
            let mut p = HashMap::new();
            p.insert("sid".to_string(), Value::String(format!("{prefix}{i}")));
            p
        })
        .collect()
}

async fn count(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH (c:Concept) RETURN count(c) AS n")
        .await?;
    Ok(r.rows()[0].get::<i64>("n")?)
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// No bulk writer at all.
    Control,
    /// `flush()` after the committed tx, then a dropped writer.
    FlushThenDrop,
    /// No flush, then a dropped writer.
    NoFlushThenDrop,
    /// No flush, an aborted writer, then a dropped writer.
    NoFlushAbortThenDrop,
}

/// (after committed tx, mid-load, after drop, after reopen)
async fn run(mode: Mode) -> Result<(i64, Option<i64>, i64, i64)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).build().await?;
    db.schema()
        .label("Concept")
        .property("sid", DataType::String)
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.bulk_insert_vertices("Concept", rows("S", 10)).await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.bulk_insert_vertices("Concept", rows("K", 1000)).await?;
    tx.commit().await?;
    if matches!(mode, Mode::FlushThenDrop) {
        db.flush().await?;
    }
    let after_commit = count(&db).await?;

    if matches!(mode, Mode::NoFlushAbortThenDrop) {
        let tx = db.session().tx().await?;
        let mut w = tx.bulk_writer().batch_size(100).build()?;
        w.insert_vertices("Concept", rows("A", 500)).await?;
        w.abort().await?;
        tx.rollback();
    }

    let mut mid = None;
    if !matches!(mode, Mode::Control) {
        let tx = db.session().tx().await?;
        let mut w = tx.bulk_writer().batch_size(100).build()?;
        w.insert_vertices("Concept", rows("D", 500)).await?;
        mid = Some(count(&db).await?);
        drop(w);
        tx.rollback();
    }
    let after_drop = count(&db).await?;

    db.shutdown().await?;
    let db = Uni::open(&path).build().await?;
    let after_reopen = count(&db).await?;
    db.shutdown().await?;
    Ok((after_commit, mid, after_drop, after_reopen))
}

#[tokio::test]
async fn issue_305_dropped_bulk_writer_keeps_committed_rows() -> Result<()> {
    let mut failures = Vec::new();
    for mode in [
        Mode::Control,
        Mode::FlushThenDrop,
        Mode::NoFlushThenDrop,
        Mode::NoFlushAbortThenDrop,
    ] {
        let (c, mid, d, r) = run(mode).await?;
        eprintln!(
            "{mode:?}: after_commit={c} mid_load={mid:?} after_drop={d} after_reopen={r} \
             (expected 1010 throughout)"
        );
        if c != 1010 || mid.is_some_and(|m| m != 1010) || d != 1010 || r != 1010 {
            failures.push(format!(
                "{mode:?}: commit={c} mid={mid:?} drop={d} reopen={r}"
            ));
        }
    }
    assert!(failures.is_empty(), "issue #305: {failures:#?}");
    Ok(())
}
