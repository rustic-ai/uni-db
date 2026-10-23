// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Beginning a transaction costs the same at 1k and 200k flushed vertices.
//!
//! Every transaction begin deep-copied the VID → labels index for its pinned
//! storage view, so begin was linear in the number of vertices: 67 ms at
//! 200k, 636× its cost at 1k. The index is now copy-on-write, sharded by VID,
//! and a begin clones a map of shard pointers.
//!
//! Timing under parallel test load: each sample is one begin + rollback, the
//! two sizes are sampled alternately so contention hits both alike, and each
//! is summarized by its minimum, which is what the begin itself costs once
//! scheduling noise is removed. A linear begin is two orders of magnitude
//! above the bound; a flat one is within noise of 1×.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(tx_begin_cost)'

use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::{DataType, Session, Uni, UniConfig};

async fn flushed_db(n: usize, ssi: bool) -> Result<Uni> {
    let cfg = UniConfig {
        ssi_enabled: ssi,
        ..Default::default()
    };
    let db = Uni::in_memory().config(cfg).build().await?;
    db.schema()
        .label("P")
        .property("i", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!("UNWIND range(1, {n}) AS i CREATE (:P {{i: i}})"))
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

async fn one_begin(s: &Session) -> Result<Duration> {
    let start = Instant::now();
    let tx = s.tx().await?;
    let elapsed = start.elapsed();
    tx.rollback();
    Ok(elapsed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tx_begin_cost_is_flat_in_flushed_vertex_count() -> Result<()> {
    for ssi in [true, false] {
        let small = flushed_db(1_000, ssi).await?;
        let big = flushed_db(200_000, ssi).await?;
        let (s_small, s_big) = (small.session(), big.session());
        let (mut min_small, mut min_big) = (Duration::MAX, Duration::MAX);
        for _ in 0..200 {
            min_small = min_small.min(one_begin(&s_small).await?);
            min_big = min_big.min(one_begin(&s_big).await?);
        }
        let ratio = min_big.as_secs_f64() / min_small.as_secs_f64();
        assert!(
            ratio < 3.0,
            "ssi={ssi}: begin at 200k vertices {min_big:?} vs 1k {min_small:?} ({ratio:.1}x)"
        );
    }
    Ok(())
}
