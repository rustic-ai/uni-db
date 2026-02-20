// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mutation benchmarks comparing DataFusion path vs fallback path.
//!
//! Each benchmark runs with both `MutationPathConfig::all_enabled()` (DF) and
//! `all_disabled()` (fallback) for direct comparison.
//!
//! ```bash
//! cargo bench --bench mutation_benchmarks
//! ```

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;
use uni_db::Uni;
use uni_db::common::config::{MutationPathConfig, UniConfig};

/// Create an in-memory Uni instance with the given mutation path config.
async fn make_db(mutation_path: MutationPathConfig) -> Uni {
    let config = UniConfig {
        mutation_path,
        ..Default::default()
    };
    Uni::in_memory().config(config).build().await.unwrap()
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

    for (label, config) in [
        ("df_path", MutationPathConfig::all_enabled()),
        ("fallback", MutationPathConfig::all_disabled()),
    ] {
        group.bench_with_input(BenchmarkId::new("path", label), &config, |b, cfg| {
            b.iter_batched(
                || rt.block_on(make_db(cfg.clone())),
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
    }
    group.finish();
}

fn bench_set_100_properties(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_set_100_props");
    group.sample_size(10);

    for (label, config) in [
        ("df_path", MutationPathConfig::all_enabled()),
        ("fallback", MutationPathConfig::all_disabled()),
    ] {
        group.bench_with_input(BenchmarkId::new("path", label), &config, |b, cfg| {
            b.iter_batched(
                || {
                    let db = rt.block_on(make_db(cfg.clone()));
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
    }
    group.finish();
}

fn bench_delete_100_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_delete_100_nodes");
    group.sample_size(10);

    for (label, config) in [
        ("df_path", MutationPathConfig::all_enabled()),
        ("fallback", MutationPathConfig::all_disabled()),
    ] {
        group.bench_with_input(BenchmarkId::new("path", label), &config, |b, cfg| {
            b.iter_batched(
                || {
                    let db = rt.block_on(make_db(cfg.clone()));
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
    }
    group.finish();
}

fn bench_create_then_match(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_create_then_match");
    group.sample_size(10);

    for (label, config) in [
        ("df_path", MutationPathConfig::all_enabled()),
        ("fallback", MutationPathConfig::all_disabled()),
    ] {
        group.bench_with_input(BenchmarkId::new("path", label), &config, |b, cfg| {
            b.iter_batched(
                || rt.block_on(make_db(cfg.clone())),
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
    }
    group.finish();
}

fn bench_merge_50_nodes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("mutation_merge_50_nodes");
    group.sample_size(10);

    for (label, config) in [
        ("df_path", MutationPathConfig::all_enabled()),
        ("fallback", MutationPathConfig::all_disabled()),
    ] {
        group.bench_with_input(BenchmarkId::new("path", label), &config, |b, cfg| {
            b.iter_batched(
                || rt.block_on(make_db(cfg.clone())),
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
    }
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
