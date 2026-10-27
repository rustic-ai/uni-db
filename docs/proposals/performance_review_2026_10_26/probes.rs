use crate::perf::cpu_clock::{measure, process_cpu_time};
use anyhow::Result;
use std::time::Instant;
use uni_common::core::id::{Eid, Vid};
use uni_common::graph::simple_graph::SimpleGraph;
use uni_db::{DataType, Uni, UniConfig, Value};
use uni_store::runtime::l0::{L0Buffer, serialize_constraint_key};

async fn db() -> Result<Uni> {
    Ok(Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            auto_flush_threshold: usize::MAX,
            disable_fork_sweeper: true,
            disable_fork_index_builder: true,
            ..Default::default()
        })
        .build()
        .await?)
}
fn sample(case: &str, n: usize, round: usize, start: Instant, cpu: std::time::Duration) {
    eprintln!(
        "PERF case={case} n={n} round={round} wall_ms={:.3} cpu_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.,
        (process_cpu_time() - cpu).as_secs_f64() * 1000.
    );
}

#[test]
fn perf_review_graph_edge_removal() {
    for round in 0..3 {
        for n in [1000usize, 4000, 16000] {
            for star in [false, true] {
                let mut g = SimpleGraph::new();
                for i in 0..n as u64 {
                    let src = if star { 0 } else { 2 * i };
                    let dst = if star { i + 1 } else { 2 * i + 1 };
                    g.add_edge(Vid::new(src), Vid::new(dst), Eid::new(i), 1);
                }
                let start = Instant::now();
                let cpu = process_cpu_time();
                for i in 0..n as u64 {
                    assert!(g.remove_edge(Eid::new(i)).is_some());
                }
                sample(
                    if star {
                        "graph_star_remove_edges"
                    } else {
                        "graph_disjoint_remove_edges"
                    },
                    n,
                    round,
                    start,
                    cpu,
                );
                assert_eq!(g.edge_count(), 0);
            }
        }
    }
}

#[test]
fn perf_review_l0_constraint_deletion() {
    for round in 0..3 {
        for n in [1000usize, 4000, 16000] {
            for constrained in [false, true] {
                let mut l0 = L0Buffer::new(0, None);
                for i in 0..n as u64 {
                    l0.insert_vertex(Vid::new(i), Default::default());
                    if constrained {
                        let key =
                            serialize_constraint_key("Item", &[("i".into(), Value::Int(i as i64))]);
                        l0.insert_constraint_key(key, Vid::new(i));
                    }
                }
                let start = Instant::now();
                let cpu = process_cpu_time();
                for i in 0..n as u64 {
                    l0.delete_vertex(Vid::new(i)).unwrap();
                }
                sample(
                    if constrained {
                        "l0_delete_unique"
                    } else {
                        "l0_delete_no_keys"
                    },
                    n,
                    round,
                    start,
                    cpu,
                );
                assert!(l0.constraint_index.is_empty());
                assert_eq!(l0.vertex_tombstones.len(), n);
            }
        }
    }
}

#[tokio::test]
async fn perf_review_public_delete_unique() -> Result<()> {
    for round in 0..3 {
        for n in [1000usize, 4000, 16000] {
            for constrained in [false, true] {
                let db = db().await?;
                db.schema()
                    .label("Item")
                    .property("i", DataType::Int64)
                    .apply()
                    .await?;
                let s = db.session();
                if constrained {
                    let tx = s.tx().await?;
                    tx.execute("CREATE CONSTRAINT item_i ON (n:Item) ASSERT n.i IS UNIQUE")
                        .await?;
                    tx.commit().await?;
                }
                let tx = s.tx().await?;
                tx.bulk_insert_vertices(
                    "Item",
                    (0..n)
                        .map(|i| [("i".into(), Value::Int(i as i64))].into_iter().collect())
                        .collect(),
                )
                .await?;
                tx.commit().await?;
                let tx = s.tx().await?;
                let (result, t) = measure(tx.execute("MATCH (n:Item) DELETE n")).await;
                result?;
                eprintln!(
                    "PERF case=public_delete_execute unique={constrained} n={n} round={round} wall_ms={:.3} cpu_ms={:.3}",
                    t.wall.as_secs_f64() * 1000.,
                    t.cpu.as_secs_f64() * 1000.
                );
                let (result, t) = measure(tx.commit()).await;
                result?;
                eprintln!(
                    "PERF case=public_delete_commit unique={constrained} n={n} round={round} wall_ms={:.3} cpu_ms={:.3}",
                    t.wall.as_secs_f64() * 1000.,
                    t.cpu.as_secs_f64() * 1000.
                );
                assert_eq!(
                    s.query("MATCH (n:Item) RETURN count(n) AS c").await?.rows()[0]
                        .get::<i64>("c")?,
                    0
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn perf_review_public_delete_star() -> Result<()> {
    for round in 0..3 {
        for n in [1000usize, 4000, 16000] {
            for star in [false, true] {
                let db = db().await?;
                db.schema()
                    .label("Item")
                    .property("i", DataType::Int64)
                    .done()
                    .edge_type("R", &["Item"], &["Item"])
                    .apply()
                    .await?;
                let s = db.session();
                let tx = s.tx().await?;
                let count = if star { n + 1 } else { n * 2 };
                let vids = tx
                    .bulk_insert_vertices(
                        "Item",
                        (0..count)
                            .map(|i| [("i".into(), Value::Int(i as i64))].into_iter().collect())
                            .collect(),
                    )
                    .await?;
                let edges = (0..n)
                    .map(|i| {
                        let (a, b) = if star { (0, i + 1) } else { (2 * i, 2 * i + 1) };
                        (vids[a], vids[b], Default::default())
                    })
                    .collect();
                tx.bulk_insert_edges("R", edges).await?;
                tx.commit().await?;
                let tx = s.tx().await?;
                let (result, t) = measure(tx.execute("MATCH ()-[r:R]->() DELETE r")).await;
                result?;
                eprintln!(
                    "PERF case=public_delete_edges_execute star={star} n={n} round={round} wall_ms={:.3} cpu_ms={:.3}",
                    t.wall.as_secs_f64() * 1000.,
                    t.cpu.as_secs_f64() * 1000.
                );
                let (result, t) = measure(tx.commit()).await;
                result?;
                eprintln!(
                    "PERF case=public_delete_edges_commit star={star} n={n} round={round} wall_ms={:.3} cpu_ms={:.3}",
                    t.wall.as_secs_f64() * 1000.,
                    t.cpu.as_secs_f64() * 1000.
                );
                assert_eq!(
                    s.query("MATCH ()-[r:R]->() RETURN count(r) AS c")
                        .await?
                        .rows()[0]
                        .get::<i64>("c")?,
                    0
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn perf_review_recursive_cte() -> Result<()> {
    for n in [100usize, 300, 900] {
        let db = db().await?;
        db.schema()
            .label("Item")
            .property("i", DataType::Int64)
            .done()
            .edge_type("NEXT", &["Item"], &["Item"])
            .apply()
            .await?;
        let s = db.session();
        let tx = s.tx().await?;
        let vids = tx
            .bulk_insert_vertices(
                "Item",
                (0..n)
                    .map(|i| [("i".into(), Value::Int(i as i64))].into_iter().collect())
                    .collect(),
            )
            .await?;
        tx.bulk_insert_edges(
            "NEXT",
            vids.windows(2)
                .map(|v| (v[0], v[1], Default::default()))
                .collect(),
        )
        .await?;
        tx.commit().await?;
        let cte = "WITH RECURSIVE reach AS (MATCH (root:Item {i:0}) RETURN root UNION MATCH (a:Item)-[:NEXT]->(b:Item) WHERE a IN reach RETURN b) MATCH (n:Item) WHERE n IN reach RETURN count(n) AS c";
        let vlp = format!(
            "MATCH (root:Item {{i:0}})-[:NEXT*0..{}]->(n:Item) RETURN count(DISTINCT n) AS c",
            n - 1
        );
        let vlp = vlp.as_str();
        for round in 0..3 {
            let cases = if round % 2 == 0 {
                [("cte", cte), ("vlp", vlp)]
            } else {
                [("vlp", vlp), ("cte", cte)]
            };
            for (name, q) in cases {
                let (result, t) = measure(s.query(q)).await;
                let r = result?;
                assert_eq!(r.rows()[0].get::<i64>("c")?, n as i64);
                eprintln!(
                    "PERF case={name} n={n} round={round} wall_ms={:.3} cpu_ms={:.3} rows_scanned={} l0_reads={}",
                    t.wall.as_secs_f64() * 1000.,
                    t.cpu.as_secs_f64() * 1000.,
                    r.metrics().rows_scanned,
                    r.metrics().l0_reads
                );
            }
        }
    }
    Ok(())
}
