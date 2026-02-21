// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mutation benchmarks for the DataFusion engine.
//!
//! ```bash
//! cargo bench --bench mutation_benchmarks
//! ```

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use uni_db::Uni;

/// Create an in-memory Uni instance.
async fn make_db() -> Uni {
    Uni::in_memory().build().await.unwrap()
}

/// Seed `count` schemaless nodes with an `idx` property.
async fn seed_nodes(db: &Uni, count: usize) {
    for i in 0..count {
        db.execute(&format!("CREATE (n:BenchNode {{idx: {i}}})"))
            .await
            .unwrap();
    }
}

fn bench_create_100_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_create_100_nodes");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || rt.block_on(make_db()),
            |db| {
                rt.block_on(async {
                    for i in 0..100 {
                        db.execute(&format!("CREATE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_set_100_properties(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_set_100_props");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(make_db());
                rt.block_on(seed_nodes(&db, 100));
                db
            },
            |db| {
                rt.block_on(async {
                    db.execute("MATCH (n:BenchNode) SET n.updated = true")
                        .await
                        .unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_delete_100_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_delete_100_nodes");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(make_db());
                rt.block_on(seed_nodes(&db, 100));
                db
            },
            |db| {
                rt.block_on(async {
                    db.execute("MATCH (n:BenchNode) DETACH DELETE n")
                        .await
                        .unwrap();
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_create_then_match(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_create_then_match");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || rt.block_on(make_db()),
            |db| {
                rt.block_on(async {
                    for i in 0..50 {
                        db.execute(&format!("CREATE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                    let result = db
                        .query("MATCH (n:BenchNode) RETURN count(n) AS cnt")
                        .await
                        .unwrap();
                    assert_eq!(result.rows().len(), 1);
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_merge_50_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_merge_50_nodes");
    group.sample_size(10);

    group.bench_function("df_path", |b| {
        b.iter_batched(
            || rt.block_on(make_db()),
            |db| {
                rt.block_on(async {
                    // First pass: all creates
                    for i in 0..50 {
                        db.execute(&format!("MERGE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                    // Second pass: all matches
                    for i in 0..50 {
                        db.execute(&format!("MERGE (n:BenchNode {{idx: {i}}})"))
                            .await
                            .unwrap();
                    }
                })
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_create_100_nodes,
    bench_set_100_properties,
    bench_delete_100_nodes,
    bench_create_then_match,
    bench_merge_50_nodes,
);
criterion_main!(benches);
