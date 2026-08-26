// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Commit-path tax of SSI: validation + write-set derivation + registry insert,
//! and the read-set recording cost on a read-heavy transaction.
//!
//! ```bash
//! cargo bench --bench ssi_commit_overhead
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
//! - `small_commit` — single-vertex RMW commit. The ssi-on delta is the
//!   validate + `WriteSet::from_l0` + registry-insert cost. Target: < 10%.
//! - `read_heavy_commit` — a transaction that scans many rows before a small
//!   write. The ssi-on delta additionally includes read-set recording over the
//!   scan (and the VidLookupJoin→HashJoin fallback on keyed reads). Expected to
//!   be the larger regression; this bench is where it gets characterized.

use std::sync::Arc;
use std::time::Instant;

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
    Arc::new(db)
}

fn bench_commit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let db = rt.block_on(seed());

    let mut group = c.benchmark_group("ssi_commit_overhead");

    // Small RMW commit: one read + one write + commit.
    group.bench_function("small_commit", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    tx.execute("MATCH (n:N {id: 'n1'}) SET n.v = n.v + 1")
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                }
                start.elapsed()
            })
        });
    });

    // Read-heavy commit: scan many rows (recorded into the read-set under ssi),
    // then a single small write.
    group.bench_function("read_heavy_commit", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let s = db.session();
                    let tx = s.tx().await.unwrap();
                    tx.query("MATCH (n:N) WHERE n.v >= 0 RETURN n.id")
                        .await
                        .unwrap();
                    tx.execute("MATCH (n:N {id: 'n1'}) SET n.v = n.v + 1")
                        .await
                        .unwrap();
                    tx.commit().await.unwrap();
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

criterion_group!(benches, bench_commit);
criterion_main!(benches);
