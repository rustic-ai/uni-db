// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Repro suite for the residual async-flush visibility bug (Plan §14).
//
// These are regression tests now: the bug they isolate is fixed. They
// stay because the fix is a string match in `is_lance_conflict`, and
// nothing else would notice if a Lance upgrade reworded the error.
//
//   R1 — Single-stream async (max_pending=1). Always passed, including
//        while the bug was live: it cannot produce two concurrent
//        stream phases, which is what the bug required.
//
//   R2 — Concurrent streams, single table (max_pending=4). The repro.
//
//   R3 — Concurrent streams, multiple tables. Distinguishes per-table
//        cache staleness from a storage-wide bug.
//
// # The cause, since "flaky" was the wrong label for ~50% failures
//
// Two concurrent flush streams both created the same per-label UID
// index dataset. Lance failed the loser with `Dataset already exists`,
// which `is_lance_conflict` did not recognise despite its doc comment
// naming concurrent table creation as the case it existed for. The
// loser was treated as fatal, its rotated L0 was stranded on
// `pending_flush`, and `flush_to_l1` refused the barrier -- correctly.
//
// R2/R3 failed 6 of 12 runs each before the fix and 0 of 12 after.
// A ~50% failure that is *worse* in isolation than under parallel load
// is not contention, and should not be filed as flake.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use uni_common::config::UniConfig;
use uni_db::{DataType, Uni, Value};

async fn build_db_with_async(
    threshold: usize,
    max_pending: usize,
    labels: &[&str],
) -> Result<Arc<Uni>> {
    let config = UniConfig {
        auto_flush_threshold: threshold,
        auto_flush_interval: None,
        async_flush_enabled: true,
        max_pending_flushes: max_pending,
        ..Default::default()
    };
    let db = Arc::new(Uni::in_memory().config(config).build().await?);
    for label in labels {
        db.schema()
            .label(label)
            .property("name", DataType::String)
            .apply()
            .await?;
    }
    Ok(db)
}

/// Write `total` distinct vertices to `label`, in commits of `per_tx`.
/// Each vertex's `name` is unique across the entire write.
async fn write_vertices(
    db: &Arc<Uni>,
    label: &str,
    total: usize,
    per_tx: usize,
    name_prefix: &str,
) -> Result<()> {
    let session = db.session();
    let mut written = 0usize;
    let mut tx_idx = 0usize;
    while written < total {
        let tx = session.tx().await?;
        let batch_size = per_tx.min(total - written);
        let mut props = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let mut row = HashMap::new();
            row.insert(
                "name".to_string(),
                Value::String(format!("{}_{}_{}", name_prefix, tx_idx, i)),
            );
            props.push(row);
        }
        tx.bulk_insert_vertices(label, props).await?;
        tx.commit().await?;
        written += batch_size;
        tx_idx += 1;
    }
    Ok(())
}

/// Drain: repeatedly call `db.flush()` and sleep until the count
/// reported by `MATCH (n:Label) RETURN count(n)` stabilizes at the
/// expected value (or we time out).
async fn drain_and_count(db: &Arc<Uni>, label: &str, expected: usize) -> Result<i64> {
    let mut last_seen: i64 = -1;
    for _ in 0..40 {
        db.flush().await?;
        let result = db
            .session()
            .query(&format!("MATCH (n:{label}) RETURN count(n) AS c"))
            .await?;
        let c: i64 = result.rows()[0].get("c")?;
        if c as usize == expected {
            return Ok(c);
        }
        last_seen = c;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    Ok(last_seen)
}

/// R1 — Single stream serialized via `max_pending_flushes = 1`.
/// No concurrent stream phases. Must pass deterministically.
#[tokio::test]
async fn r1_single_stream_async_serialized() -> Result<()> {
    let db = build_db_with_async(1000, 1, &["Person"]).await?;
    write_vertices(&db, "Person", 10_000, 50, "p").await?;
    let observed = drain_and_count(&db, "Person", 10_000).await?;
    assert_eq!(
        observed, 10_000,
        "R1 expected 10000 vertices, observed {}",
        observed
    );
    Ok(())
}

/// R1_no_query_during_drain — write, drain WITHOUT querying during drain,
/// then query once at the end. If this passes but R1 fails, the bug is
/// triggered by the count queries mid-drain (queries somehow disturb
/// the visibility state).
#[tokio::test]
async fn r1_no_query_during_drain() -> Result<()> {
    let db = build_db_with_async(1000, 1, &["Person"]).await?;
    write_vertices(&db, "Person", 10_000, 50, "p").await?;
    for _ in 0..40 {
        db.flush().await?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let result = db
        .session()
        .query("MATCH (n:Person) RETURN count(n) AS c")
        .await?;
    let c: i64 = result.rows()[0].get("c")?;
    assert_eq!(c, 10_000, "R1_no_query expected 10000, observed {}", c);
    Ok(())
}

/// Diagnostic — same as R1 but at the end, count by tx_idx pattern to
/// identify WHICH batch is missing. Names are "p_{tx}_{i}" — by
/// scanning each tx_idx independently we can see if any specific tx
/// (= one async flush batch) is entirely missing.
#[tokio::test]
async fn r1_diag_which_batch_missing() -> Result<()> {
    let db = build_db_with_async(1000, 1, &["Person"]).await?;
    write_vertices(&db, "Person", 10_000, 50, "p").await?;
    for _ in 0..40 {
        db.flush().await?;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let total_r = db
        .session()
        .query("MATCH (n:Person) RETURN count(n) AS c")
        .await?;
    let total: i64 = total_r.rows()[0].get("c")?;
    eprintln!("REPRO-DIAG total={}", total);
    // 10000 / 50 = 200 transactions
    for tx in 0..200 {
        let q = format!(
            "MATCH (n:Person) WHERE n.name STARTS WITH 'p_{}_' RETURN count(n) AS c",
            tx
        );
        let r = db.session().query(&q).await?;
        let c: i64 = r.rows()[0].get("c")?;
        if c != 50 {
            eprintln!("REPRO-DIAG tx_idx={} count={} (expected 50)", tx, c);
        }
    }
    Ok(())
}

/// R0 — Same workload as R1 but with SYNC flush (async_flush_enabled=false).
/// Baseline: must pass deterministically. If R0 also flakes, the bug is
/// NOT in the async path but somewhere shared.
#[tokio::test]
async fn r0_baseline_sync_flush() -> Result<()> {
    let config = UniConfig {
        auto_flush_threshold: 1000,
        auto_flush_interval: None,
        async_flush_enabled: false,
        ..Default::default()
    };
    let db = Arc::new(Uni::in_memory().config(config).build().await?);
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    write_vertices(&db, "Person", 10_000, 50, "p").await?;
    let observed = drain_and_count(&db, "Person", 10_000).await?;
    assert_eq!(
        observed, 10_000,
        "R0 expected 10000 vertices, observed {}",
        observed
    );
    Ok(())
}

/// R2 — Concurrent streams on a single table. This is the bug repro.
/// 10 batches of 1000 vertices each → 10 async flushes; max_pending=4
/// permits up to 4 concurrent stream phases on `vertices_Person`.
#[tokio::test]
async fn r2_concurrent_streams_single_table() -> Result<()> {
    let db = build_db_with_async(1000, 4, &["Person"]).await?;
    write_vertices(&db, "Person", 10_000, 50, "p").await?;
    let observed = drain_and_count(&db, "Person", 10_000).await?;
    assert_eq!(
        observed, 10_000,
        "R2 expected 10000 vertices, observed {}",
        observed
    );
    Ok(())
}

/// R3 — Concurrent streams across multiple tables. Each label gets
/// its own writer/stream pipeline; if the bug is per-table cache
/// staleness, this should fail per-table independently.
#[tokio::test]
async fn r3_concurrent_streams_multiple_tables() -> Result<()> {
    let labels = &["Person", "Account", "Item"];
    let db = build_db_with_async(1000, 4, labels).await?;
    let per_label = 3_000usize;
    for label in labels {
        write_vertices(&db, label, per_label, 50, &label.to_lowercase()).await?;
    }
    for label in labels {
        let observed = drain_and_count(&db, label, per_label).await?;
        assert_eq!(
            observed, per_label as i64,
            "R3[{}] expected {}, observed {}",
            label, per_label, observed
        );
    }
    Ok(())
}
