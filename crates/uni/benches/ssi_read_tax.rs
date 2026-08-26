// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Read-path tax of SSI: latency of point-read / scan / traversal.
//!
//! The OCC premise is that snapshot reads never lock and never copy on the read
//! path (a pinned generation is `Arc`-cloned at begin). This bench measures the
//! per-query read latency so the regression can be quantified by running it with
//! and without the feature:
//!
//! ```bash
//! cargo bench --bench ssi_read_tax
//! ```
//!
//! # The off arm
//!
//! An earlier revision of this header told you to pass `--features ssi`. **There
//! is no such feature** -- SSI/OCC is always compiled and toggled at runtime via
//! `UniConfig::ssi_enabled` (default `true`), so cargo rejects the flag and the
//! comparison this bench documents has never been runnable from it. Until this
//! bench sweeps both arms as cells the way `ssi_contention.rs` does, run the off
//! arm by building the database with
//! `Uni::in_memory().config(UniConfig { ssi_enabled: false, ..Default::default() })`.
//!
//! Acceptance target: < 3% reader regression with `ssi` enabled.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use uni_db::{DataType, Uni};

const N: usize = 2_000;

async fn seed() -> Arc<Uni> {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("N")
        .property("id", DataType::String)
        .property("v", DataType::Int)
        .done()
        .edge_type("R", &["N"], &["N"])
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for i in 0..N {
        tx.execute(&format!("CREATE (:N {{id: 'n{i}', v: {i}}})"))
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let tx = s.tx().await.unwrap();
    tx.execute("MATCH (a:N {id: 'n0'}), (b:N {id: 'n1'}) CREATE (a)-[:R]->(b)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    Arc::new(db)
}

/// Times `query` run `iters` times against `db`.
fn time_query(rt: &Runtime, db: &Arc<Uni>, iters: u64, query: &str) -> Duration {
    rt.block_on(async {
        let start = Instant::now();
        for _ in 0..iters {
            db.session().query(query).await.unwrap();
        }
        start.elapsed()
    })
}

fn bench_read_paths(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(seed());

    let mut group = c.benchmark_group("ssi_read_tax");

    for (name, q) in [
        ("point_read", "MATCH (n:N {id: 'n1234'}) RETURN n.v"),
        (
            "label_scan_filtered",
            "MATCH (n:N) WHERE n.v > 1990 RETURN n.id",
        ),
        (
            "one_hop_traversal",
            "MATCH (a:N {id: 'n0'})-[:R]->(b) RETURN b.id",
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| time_query(&rt, &db, iters, q));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_read_paths);
criterion_main!(benches);
