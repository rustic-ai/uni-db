// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Graph Algorithm Benchmarks
//!
//! Run with:
//! cargo bench --bench algo_benchmarks

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use uni_cypher::parse;
use uni_db::UniConfig;
use uni_db::core::id::Vid;
use uni_db::core::schema::SchemaManager;
use uni_db::query::executor::Executor;
use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[derive(Clone, Debug)]
struct AlgoBenchConfig {
    nodes: usize,
    edges_per_node: usize,
}

impl AlgoBenchConfig {
    fn from_env() -> Self {
        let nodes = env::var("BENCH_NODES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000); // Default 1000 nodes for algorithm benchmarks
        let edges_per_node = env::var("BENCH_EDGES_PER_NODE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        Self {
            nodes,
            edges_per_node,
        }
    }

    fn label(&self) -> String {
        format!("{}n_{}deg", self.nodes, self.edges_per_node)
    }
}

struct AlgoBenchContext {
    _dir: tempfile::TempDir,
    writer: Arc<RwLock<Writer>>,
    executor: Executor,
    prop_manager: PropertyManager,
    planner: QueryPlanner,
    schema_manager: Arc<SchemaManager>,
}

impl AlgoBenchContext {
    async fn new() -> Self {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let schema_path = path.join("schema.json");
        let storage_path = path.join("storage");

        let schema_manager = SchemaManager::load(&schema_path).await.unwrap();
        schema_manager.add_label("Node").unwrap();
        schema_manager
            .add_edge_type("LINK", vec!["Node".into()], vec!["Node".into()])
            .unwrap();
        schema_manager.save().await.unwrap();

        let schema_manager = Arc::new(schema_manager);
        let storage = Arc::new(
            StorageManager::new(storage_path.to_str().unwrap(), schema_manager.clone())
                .await
                .unwrap(),
        );

        // Initialize Writer
        let config = UniConfig {
            auto_flush_threshold: 100_000,
            ..Default::default()
        };
        let writer = Arc::new(RwLock::new(
            Writer::new_with_config(
                storage.clone(),
                schema_manager.clone(),
                0,
                config,
                None,
                None,
            )
            .await
            .unwrap(),
        ));

        let executor = Executor::new_with_writer(storage.clone(), writer.clone());
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 10000);
        let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

        Self {
            _dir: dir,
            writer,
            executor,
            prop_manager,
            planner,
            schema_manager,
        }
    }

    async fn populate_random_graph(&self, nodes: usize, edges_per_node: usize) {
        let mut w = self.writer.write().await;
        let _node_lbl = self.schema_manager.schema().labels["Node"].id;
        let link_type = self.schema_manager.schema().edge_types["LINK"].id;

        // Create nodes
        for i in 0..nodes {
            let vid = Vid::new(i as u64);
            w.insert_vertex_with_labels(vid, HashMap::new(), vec!["Node".to_string()])
                .await
                .unwrap();
        }

        // Create random edges
        let mut rng = rand::thread_rng();
        for i in 0..nodes {
            let src = Vid::new(i as u64);
            for _ in 0..edges_per_node {
                let target_idx = rng.gen_range(0..nodes);
                if target_idx != i {
                    let dst = Vid::new(target_idx as u64);
                    let eid = w.next_eid(link_type).await.unwrap();
                    w.insert_edge(src, dst, link_type, eid, HashMap::new())
                        .await
                        .unwrap();
                }
            }
        }
        w.flush_to_l1(None).await.unwrap();
    }
}

fn run_algo_benchmark(c: &mut Criterion, name: &str, query: &str) {
    let rt = Runtime::new().unwrap();
    let config = AlgoBenchConfig::from_env();
    let mut group = c.benchmark_group(name);
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("exec", config.label()),
        &config,
        |b, cfg| {
            b.iter_batched(
                || {
                    let ctx = rt.block_on(AlgoBenchContext::new());
                    rt.block_on(ctx.populate_random_graph(cfg.nodes, cfg.edges_per_node));
                    ctx
                },
                |ctx| {
                    rt.block_on(async {
                        let ast = parse(query).unwrap();
                        let plan = ctx.planner.plan(ast).unwrap();
                        let res = ctx
                            .executor
                            .execute(plan, &ctx.prop_manager, &HashMap::new())
                            .await
                            .unwrap();
                        assert!(!res.is_empty());
                    })
                },
                BatchSize::PerIteration, // Setup is costly, but we need fresh state if algo modifies (some might in future)
                                         // For read-only algos, we could reuse context but caches might be warm.
                                         // To benchmark 'cold' vs 'warm' is a different story.
                                         // Here we benchmark full execution including projection build if any.
            )
        },
    );
    group.finish();
}

fn bench_pagerank(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "pagerank",
        "CALL uni.algo.pageRank(['Node'], ['LINK']) YIELD nodeId, score RETURN nodeId, score",
    );
}

fn bench_wcc(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "wcc",
        "CALL uni.algo.wcc(['Node'], ['LINK']) YIELD nodeId, componentId RETURN nodeId, componentId",
    );
}

fn bench_louvain(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "louvain",
        "CALL uni.algo.louvain(['Node'], ['LINK']) YIELD nodeId, communityId RETURN nodeId, communityId",
    );
}

fn bench_label_propagation(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "label_propagation",
        "CALL uni.algo.labelPropagation(['Node'], ['LINK']) YIELD nodeId, communityId RETURN nodeId, communityId",
    );
}

fn bench_scc(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "scc",
        "CALL uni.algo.scc(['Node'], ['LINK']) YIELD nodeId, componentId RETURN nodeId, componentId",
    );
}

fn bench_betweenness(c: &mut Criterion) {
    // Sampling to keep it reasonably fast for larger graphs
    run_algo_benchmark(
        c,
        "betweenness",
        "CALL uni.algo.betweenness(['Node'], ['LINK'], true, 100) YIELD nodeId, score RETURN nodeId, score",
    );
}

fn bench_node_similarity(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "node_similarity",
        "CALL uni.algo.nodeSimilarity(['Node'], ['LINK']) YIELD node1, node2, similarity RETURN node1, node2, similarity",
    );
}

fn bench_closeness(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "closeness",
        "CALL uni.algo.closeness(['Node'], ['LINK']) YIELD nodeId, score RETURN nodeId, score",
    );
}

fn bench_triangle_count(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "triangle_count",
        "CALL uni.algo.triangleCount(['Node'], ['LINK']) YIELD nodeId, triangleCount RETURN nodeId, triangleCount",
    );
}

fn bench_kcore(c: &mut Criterion) {
    run_algo_benchmark(
        c,
        "kcore",
        "CALL uni.algo.kCore(['Node'], ['LINK']) YIELD nodeId, coreNumber RETURN nodeId, coreNumber",
    );
}

fn bench_random_walk(c: &mut Criterion) {
    // Starting from all nodes (empty list) with short walk
    run_algo_benchmark(
        c,
        "random_walk",
        "CALL uni.algo.randomWalk(['Node'], ['LINK'], 5, 1, []) YIELD path RETURN path",
    );
}

fn bench_apsp(c: &mut Criterion) {
    // APSP is very heavy O(V*(V+E)). We should probably reduce node count for this one specifically
    // or just run it and let it take time.
    // For now, let's skip it in default suite or run it if nodes are small.
    let config = AlgoBenchConfig::from_env();
    if config.nodes > 500 {
        return;
    }
    run_algo_benchmark(
        c,
        "apsp",
        "CALL uni.algo.allPairsShortestPath(['Node'], ['LINK']) YIELD sourceNodeId, targetNodeId, distance RETURN sourceNodeId, targetNodeId, distance",
    );
}

criterion_group!(
    benches,
    bench_pagerank,
    bench_wcc,
    bench_louvain,
    bench_label_propagation,
    bench_scc,
    bench_betweenness,
    bench_node_similarity,
    bench_closeness,
    bench_triangle_count,
    bench_kcore,
    bench_random_walk,
    bench_apsp
);
criterion_main!(benches);
