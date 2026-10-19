// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `UniConfig::lance_{metadata,index}_cache_bytes` reach the Lance session.
//!
//! Every dataset a database opens reads through one shared Lance session whose
//! caches default to 1 GiB (metadata) and 6 GiB (index), and which a process
//! that flushes often fills over time. The caps are only worth having if Lance
//! actually evicts down to them, so these tests read the live cache occupancy
//! (`DatabaseMetrics::storage_*_cache_bytes`) after a workload that, uncapped,
//! fills both caches past the cap.

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig};

/// Cap small enough that the workload below overflows it uncapped.
const CAP: usize = 16 * 1024;

/// Flush-and-read rounds; each flush writes a new Lance version whose
/// manifest and file metadata the next read caches.
const ROUNDS: usize = 12;

/// Runs the workload on a fresh on-disk database and returns the cache
/// occupancy afterward as `(metadata_bytes, index_bytes)`.
async fn cache_bytes_after_workload(config: UniConfig) -> Result<(u64, u64)> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_string_lossy().to_string())
        .config(config)
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("n", DataType::Int64)
        .property("title", DataType::String)
        .index("n", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;
    let session = db.session();
    for round in 0..ROUNDS {
        let tx = session.tx().await?;
        tx.execute(&format!(
            "UNWIND range(1, 50) AS i CREATE (:Doc {{n: {round} * 100 + i, title: 'doc'}})"
        ))
        .await?;
        tx.commit().await?;
        db.flush().await?;
        // An indexed point lookup and a full scan: the first touches the
        // index cache, both touch the metadata cache.
        session
            .query(&format!(
                "MATCH (d:Doc) WHERE d.n = {} RETURN d.title",
                round * 100 + 7
            ))
            .await?;
        session.query("MATCH (d:Doc) RETURN count(d)").await?;
    }
    let metrics = db.metrics();
    eprintln!(
        "lance cache after workload: metadata={} B index={} B",
        metrics.storage_metadata_cache_bytes, metrics.storage_index_cache_bytes
    );
    Ok((
        metrics.storage_metadata_cache_bytes,
        metrics.storage_index_cache_bytes,
    ))
}

/// With no cap, the workload fills the metadata cache past [`CAP`]; with the
/// cap set, Lance keeps it at or under the cap.
///
/// The uncapped run is what makes the capped assertion discriminating: it
/// shows the figure is the live cache (a reading stuck at 0 would pass the
/// capped bound for free) and that this workload exceeds the cap.
#[tokio::test]
async fn metadata_cache_cap_bounds_the_lance_session() -> Result<()> {
    let (uncapped, _) = cache_bytes_after_workload(UniConfig::default()).await?;
    assert!(
        uncapped > CAP as u64,
        "uncapped metadata cache holds {uncapped} B; the workload must exceed the \
         {CAP} B cap for the capped run to mean anything"
    );

    let (capped, _) = cache_bytes_after_workload(UniConfig {
        lance_metadata_cache_bytes: Some(CAP),
        ..Default::default()
    })
    .await?;
    assert!(
        capped <= CAP as u64,
        "metadata cache holds {capped} B with lance_metadata_cache_bytes = {CAP} \
         (uncapped: {uncapped} B); the cap did not reach Lance"
    );
    Ok(())
}

/// The index-cache counterpart of
/// [`metadata_cache_cap_bounds_the_lance_session`].
#[tokio::test]
async fn index_cache_cap_bounds_the_lance_session() -> Result<()> {
    let (_, uncapped) = cache_bytes_after_workload(UniConfig::default()).await?;
    assert!(
        uncapped > CAP as u64,
        "uncapped index cache holds {uncapped} B; the workload must exceed the \
         {CAP} B cap for the capped run to mean anything"
    );

    let (_, capped) = cache_bytes_after_workload(UniConfig {
        lance_index_cache_bytes: Some(CAP),
        ..Default::default()
    })
    .await?;
    assert!(
        capped <= CAP as u64,
        "index cache holds {capped} B with lance_index_cache_bytes = {CAP} \
         (uncapped: {uncapped} B); the cap did not reach Lance"
    );
    Ok(())
}

/// A cap of zero disables a cache without breaking reads: the indexed lookup
/// still answers, from storage.
#[tokio::test]
async fn zero_caps_disable_caching_but_reads_still_answer() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = Uni::open(dir.path().to_string_lossy().to_string())
        .config(UniConfig {
            lance_metadata_cache_bytes: Some(0),
            lance_index_cache_bytes: Some(0),
            ..Default::default()
        })
        .build()
        .await?;
    db.schema()
        .label("Doc")
        .property("n", DataType::Int64)
        .index("n", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("UNWIND range(1, 100) AS i CREATE (:Doc {n: i})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let rows = db
        .session()
        .query("MATCH (d:Doc) WHERE d.n = 42 RETURN d.n AS n")
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.rows()[0].get::<i64>("n")?, 42);
    let metrics = db.metrics();
    assert_eq!(metrics.storage_metadata_cache_bytes, 0);
    assert_eq!(metrics.storage_index_cache_bytes, 0);
    Ok(())
}
