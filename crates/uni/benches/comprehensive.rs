// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Comprehensive benchmarks for Uni graph database using the Public API
//!
//! # Running with custom parameters
//!
//! Use environment variables to customize the benchmark:
//!
//! ```bash
//! # Run with specific node/edge counts
//! BENCH_NODES=50000 BENCH_EDGES=100000 cargo bench --bench comprehensive
//!
//! # Run only traversal benchmarks with 100K nodes
//! BENCH_NODES=100000 cargo bench --bench comprehensive -- traversal
//!
//! # Quick smoke test with small dataset
//! BENCH_NODES=100 BENCH_EDGES=100 cargo bench --bench comprehensive
//!
//! # Run index benchmarks
//! BENCH_NODES=10000 cargo bench --bench comprehensive -- index
//! ```

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use serde_json::json;
use std::collections::HashMap;
use std::env;
use tempfile::{TempDir, tempdir};
use tokio::runtime::Runtime;
use uni_db::{Uni, UniConfig};

/// Schema JSON for benchmark - defines Person label with properties and KNOWS edge type
const SCHEMA_JSON: &str = r#"{
    "schema_version": 1,
    "labels": {
        "Person": {
            "id": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "state": "Active"
        }
    },
    "edge_types": {
        "KNOWS": {
            "id": 1,
            "src_labels": ["Person"],
            "dst_labels": ["Person"],
            "state": "Active"
        }
    },
    "properties": {
        "Person": {
            "name": {
                "type": "String",
                "nullable": true,
                "added_in": 1,
                "state": "Active"
            },
            "age": {
                "type": "Int32",
                "nullable": true,
                "added_in": 1,
                "state": "Active"
            },
            "embedding": {
                "type": { "Vector": { "dimensions": 128 } },
                "nullable": true,
                "added_in": 1,
                "state": "Active"
            }
        }
    },
    "indexes": []
}"#;

/// Configuration for benchmark runs
#[derive(Clone, Debug)]
struct BenchConfig {
    nodes: usize,
    edges: usize,
}

impl BenchConfig {
    fn from_env() -> Self {
        let nodes = env::var("BENCH_NODES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        let edges = env::var("BENCH_EDGES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(nodes);
        Self { nodes, edges }
    }

    fn label(&self) -> String {
        format!("{}n_{}e", self.nodes, self.edges)
    }
}

struct BenchContext {
    _dir: TempDir,
    db: Uni,
}

impl BenchContext {
    async fn new() -> Self {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Write schema JSON to temp directory
        let schema_path = path.join("bench_schema.json");
        tokio::fs::write(&schema_path, SCHEMA_JSON).await.unwrap();

        let config = UniConfig {
            auto_flush_threshold: 100_000,
            ..Default::default()
        };

        let db = Uni::open(path.to_str().unwrap())
            .config(config)
            .build()
            .await
            .unwrap();

        // Load schema from JSON file
        db.load_schema(&schema_path).await.unwrap();

        Self { _dir: dir, db }
    }

    /// Fast bulk population using bulk insert APIs (test setup only).
    /// All benchmark queries still use Cypher.
    async fn populate(&self, num_vertices: usize, num_edges: usize) {
        // Build vertex properties
        let mut vertex_props = Vec::with_capacity(num_vertices);
        for i in 0..num_vertices {
            let embedding: Vec<f32> = (0..128).map(|x| (x + i) as f32).collect();
            let mut props: HashMap<String, serde_json::Value> = HashMap::new();
            props.insert("name".to_string(), json!(format!("Person_{}", i)));
            props.insert("age".to_string(), json!((i % 100) as i32));
            props.insert("embedding".to_string(), json!(embedding));
            vertex_props.push(props);
        }

        // Bulk insert vertices
        let vids = self
            .db
            .bulk_insert_vertices("Person", vertex_props)
            .await
            .unwrap();

        // Build edge data (src_vid, dst_vid, properties)
        let mut edges = Vec::with_capacity(num_edges);
        for i in 0..num_edges {
            let src_idx = i % num_vertices;
            let dst_idx = (i + 1) % num_vertices;
            edges.push((vids[src_idx], vids[dst_idx], HashMap::new()));
        }

        // Bulk insert edges
        self.db.bulk_insert_edges("KNOWS", edges).await.unwrap();
    }
}

fn bench_ingest_vertices(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();
    let mut group = c.benchmark_group("ingest");
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("vertices", config.label()),
        &config,
        |b, cfg| {
            b.iter_batched(
                || rt.block_on(BenchContext::new()),
                |ctx| {
                    rt.block_on(async {
                        let embedding: Vec<f32> = (0..128).map(|x| x as f32).collect();
                        let embedding_str = json!(embedding).to_string();
                        for i in 0..cfg.nodes {
                            let cypher = format!(
                                "CREATE (n:Person {{name: 'Bench_{}', age: 30, embedding: {}}})",
                                i, embedding_str
                            );
                            ctx.db.execute(&cypher).await.unwrap();
                        }
                    })
                },
                BatchSize::SmallInput,
            )
        },
    );
    group.finish();
}

fn bench_flush(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();
    let mut group = c.benchmark_group("flush");
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("l0_to_l1", config.label()),
        &config,
        |b, cfg| {
            b.iter_batched(
                || {
                    let ctx = rt.block_on(BenchContext::new());
                    rt.block_on(ctx.populate(cfg.nodes, cfg.edges / 2));
                    ctx
                },
                |ctx| rt.block_on(async { ctx.db.flush().await.unwrap() }),
                BatchSize::LargeInput,
            )
        },
    );
    group.finish();
}

fn bench_query_point_lookup(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, config.edges));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    let target_id = config.nodes / 2;

    // Diagnostic check
    rt.block_on(async {
        let count_result = ctx
            .db
            .query("MATCH (n:Person) RETURN count(n) as cnt")
            .await
            .unwrap();
        eprintln!(
            "DEBUG: Total Person count after flush: {:?}",
            count_result.rows.first()
        );

        let lookup = format!(
            "MATCH (n:Person) WHERE n.name = 'Person_{}' RETURN n.name, n.age",
            target_id
        );
        let result = ctx.db.query(&lookup).await.unwrap();
        eprintln!(
            "DEBUG: Person_{} lookup result: {} rows",
            target_id,
            result.rows.len()
        );
    });

    let mut group = c.benchmark_group("point_lookup");
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("by_name", config.label()),
        &target_id,
        |b, &tid| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = format!(
                        "MATCH (n:Person) WHERE n.name = 'Person_{}' RETURN n.age",
                        tid
                    );
                    let result = ctx.db.query(&cypher).await.unwrap();
                    assert_eq!(
                        result.rows.len(),
                        1,
                        "Expected 1 result for Person_{}, got {} results",
                        tid,
                        result.rows.len()
                    );
                })
            })
        },
    );
    group.finish();
}

fn bench_query_traversal(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, config.edges));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    let mut group = c.benchmark_group("traversal");
    group.sample_size(10);

    // 1-hop traversal
    group.bench_with_input(
        BenchmarkId::new("1_hop", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher =
                        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Person_0' RETURN b.name";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert!(!result.rows.is_empty());
                })
            })
        },
    );

    // 2-hop traversal
    group.bench_with_input(
        BenchmarkId::new("2_hop", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher =
                        "MATCH (a:Person)-[:KNOWS*2..2]->(b:Person) WHERE a.name = 'Person_0' RETURN b.name";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert!(!result.rows.is_empty());
                })
            })
        },
    );

    // 3-hop traversal
    group.bench_with_input(
        BenchmarkId::new("3_hop", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher =
                        "MATCH (a:Person)-[:KNOWS*3..3]->(b:Person) WHERE a.name = 'Person_0' RETURN b.name";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert!(!result.rows.is_empty());
                })
            })
        },
    );

    group.finish();
}

fn bench_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, 0));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    let mut group = c.benchmark_group("aggregation");
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("count", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = "MATCH (n:Person) RETURN count(n) as cnt";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert_eq!(result.rows.len(), 1);
                })
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("group_by_age", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = "MATCH (n:Person) RETURN n.age, count(n) as cnt";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert!(!result.rows.is_empty());
                })
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("avg_age", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = "MATCH (n:Person) RETURN avg(n.age) as avg_age";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert_eq!(result.rows.len(), 1);
                })
            })
        },
    );

    group.finish();
}

fn bench_vector_search(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, 0));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    let query_vec: Vec<f32> = (0..128).map(|x| x as f32).collect();
    let query_json = json!(query_vec).to_string();

    let mut group = c.benchmark_group("vector");
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("similarity_filter", config.label()),
        &query_json,
        |b, qj| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = format!(
                        "MATCH (n:Person) WHERE vector_similarity(n.embedding, {}) > 0.8 RETURN n.name LIMIT 5",
                        qj
                    );
                    let _result = ctx.db.query(&cypher).await.unwrap();
                })
            })
        },
    );
    group.finish();
}

fn bench_hybrid_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, config.edges));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    let query_vec: Vec<f32> = (0..128).map(|x| x as f32).collect();
    let query_json = json!(query_vec).to_string();

    let mut group = c.benchmark_group("hybrid");
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("vector_graph", config.label()),
        &query_json,
        |b, qj| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = format!(
                        "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE vector_similarity(a.embedding, {}) > 0.8 AND b.age >= 1 RETURN b.name",
                        qj
                    );
                    let result = ctx.db.query(&cypher).await.unwrap();
                    assert!(!result.rows.is_empty());
                })
            })
        },
    );
    group.finish();
}

fn bench_scalar_index(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, config.edges));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    // Create scalar index on age property using Cypher
    rt.block_on(async {
        ctx.db
            .execute("CREATE INDEX idx_person_age FOR (n:Person) ON (n.age)")
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("scalar_index");
    group.sample_size(10);

    // Equality query with BTree index
    group.bench_with_input(
        BenchmarkId::new("equality_indexed", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = "MATCH (n:Person) WHERE n.age = 25 RETURN n.name";
                    let _result = ctx.db.query(cypher).await.unwrap();
                })
            })
        },
    );

    // Range query with BTree index
    group.bench_with_input(
        BenchmarkId::new("range_indexed", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = "MATCH (n:Person) WHERE n.age >= 20 AND n.age <= 30 RETURN n.name";
                    let _result = ctx.db.query(cypher).await.unwrap();
                })
            })
        },
    );

    group.finish();
}

fn bench_vector_index(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, 0));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    // Create vector index using Cypher
    rt.block_on(async {
        ctx.db
            .execute("CREATE VECTOR INDEX idx_person_embedding FOR (n:Person) ON n.embedding OPTIONS {metric: 'cosine', index_type: 'hnsw'}")
            .await
            .unwrap();
    });

    let query_vec: Vec<f32> = (0..128).map(|x| (x as f32) / 128.0).collect();
    let query_json = json!(query_vec).to_string();

    let mut group = c.benchmark_group("vector_index");
    group.sample_size(10);

    // KNN via CALL uni.vector.query (uses Lance index)
    group.bench_with_input(
        BenchmarkId::new("knn_indexed", config.label()),
        &query_json,
        |b, qj| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = format!(
                        "CALL uni.vector.query('Person', 'embedding', {}, 10) YIELD node, distance RETURN node, distance",
                        qj
                    );
                    let result = ctx.db.query(&cypher).await.unwrap();
                    assert!(!result.rows.is_empty());
                })
            })
        },
    );

    group.finish();
}

fn bench_fulltext_index(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, 0));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    // Create fulltext index using Cypher (ON EACH [properties])
    rt.block_on(async {
        ctx.db
            .execute("CREATE FULLTEXT INDEX idx_person_name FOR (n:Person) ON EACH [n.name]")
            .await
            .unwrap();
    });

    let mut group = c.benchmark_group("fulltext_index");
    group.sample_size(10);

    // String match with FTS index
    group.bench_with_input(
        BenchmarkId::new("string_match_indexed", config.label()),
        &config,
        |b, cfg| {
            let search_term = format!("Person_{}", cfg.nodes / 2);
            b.iter(|| {
                rt.block_on(async {
                    let cypher = format!(
                        "MATCH (n:Person) WHERE n.name = '{}' RETURN n.age",
                        search_term
                    );
                    let result = ctx.db.query(&cypher).await.unwrap();
                    assert_eq!(result.rows.len(), 1);
                })
            })
        },
    );

    group.finish();
}

fn bench_index_creation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();
    let mut group = c.benchmark_group("index_creation");
    group.sample_size(10);

    // Scalar index creation benchmark
    group.bench_with_input(
        BenchmarkId::new("scalar_btree", config.label()),
        &config,
        |b, cfg| {
            b.iter_batched(
                || {
                    let ctx = rt.block_on(BenchContext::new());
                    rt.block_on(ctx.populate(cfg.nodes, 0));
                    rt.block_on(async { ctx.db.flush().await.unwrap() });
                    ctx
                },
                |ctx| {
                    rt.block_on(async {
                        ctx.db
                            .execute("CREATE INDEX idx_age FOR (n:Person) ON (n.age)")
                            .await
                            .unwrap();
                    })
                },
                BatchSize::LargeInput,
            )
        },
    );

    // Vector index creation benchmark
    group.bench_with_input(
        BenchmarkId::new("vector_hnsw", config.label()),
        &config,
        |b, cfg| {
            b.iter_batched(
                || {
                    let ctx = rt.block_on(BenchContext::new());
                    rt.block_on(ctx.populate(cfg.nodes, 0));
                    rt.block_on(async { ctx.db.flush().await.unwrap() });
                    ctx
                },
                |ctx| {
                    rt.block_on(async {
                        ctx.db
                            .execute("CREATE VECTOR INDEX idx_emb FOR (n:Person) ON n.embedding OPTIONS {metric: 'cosine', index_type: 'hnsw'}")
                            .await
                            .unwrap();
                    })
                },
                BatchSize::LargeInput,
            )
        },
    );

    // Fulltext index creation benchmark
    group.bench_with_input(
        BenchmarkId::new("fulltext", config.label()),
        &config,
        |b, cfg| {
            b.iter_batched(
                || {
                    let ctx = rt.block_on(BenchContext::new());
                    rt.block_on(ctx.populate(cfg.nodes, 0));
                    rt.block_on(async { ctx.db.flush().await.unwrap() });
                    ctx
                },
                |ctx| {
                    rt.block_on(async {
                        ctx.db
                            .execute(
                                "CREATE FULLTEXT INDEX idx_name FOR (n:Person) ON EACH [n.name]",
                            )
                            .await
                            .unwrap();
                    })
                },
                BatchSize::LargeInput,
            )
        },
    );

    group.finish();
}

fn bench_order_and_limit(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = BenchConfig::from_env();

    let ctx = rt.block_on(BenchContext::new());
    rt.block_on(ctx.populate(config.nodes, 0));
    rt.block_on(async { ctx.db.flush().await.unwrap() });

    let mut group = c.benchmark_group("order_limit");
    group.sample_size(10);

    group.bench_with_input(
        BenchmarkId::new("order_by_age", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age LIMIT 10";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert_eq!(result.rows.len(), 10);
                })
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("order_by_name_desc", config.label()),
        &config,
        |b, _cfg| {
            b.iter(|| {
                rt.block_on(async {
                    let cypher =
                        "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name DESC LIMIT 10";
                    let result = ctx.db.query(cypher).await.unwrap();
                    assert_eq!(result.rows.len(), 10);
                })
            })
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_ingest_vertices,
    bench_flush,
    bench_query_point_lookup,
    bench_query_traversal,
    bench_aggregation,
    bench_vector_search,
    bench_hybrid_query,
    bench_scalar_index,
    bench_vector_index,
    bench_fulltext_index,
    bench_index_creation,
    bench_order_and_limit
);
criterion_main!(benches);
