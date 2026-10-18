// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Measurement probe for the in-memory load
//! `UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) CREATE (a)-[:T]->(b)`
//! that grows super-linearly with N. `#[ignore]`d: a measurement, not a guard.
//!
//! `L0_LOAD_SIZES=1000,2000` and `L0_LOAD_VARIANTS=base,noidx,...` select runs.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::{Uni, UniConfig, Value};

fn env_list(key: &str, default: &str) -> Vec<String> {
    std::env::var(key)
        .unwrap_or_else(|_| default.to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_interval: None,
        auto_flush_threshold: usize::MAX / 2,
        query_timeout: Duration::from_secs(3000),
        ..Default::default()
    }
}

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

/// `n_edges` edges over `n_edges / 2 + 2` vertices, same shape as the #301 probe.
fn fixture(n_edges: usize) -> (Vec<String>, Vec<(String, String)>) {
    let len = n_edges / 2 + 2;
    let vs: Vec<String> = (0..len).map(|i| format!("n{i}")).collect();
    let mut es = Vec::new();
    for i in 0..n_edges {
        let (a, b) = (i % len, (i * 7 + 3) % len);
        if a != b {
            es.push((vs[a].clone(), vs[b].clone()));
        }
    }
    (vs, es)
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

const EDGE_Q: &str =
    "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) CREATE (a)-[:T]->(b)";
const MATCH_Q: &str =
    "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) RETURN count(*) AS c";

fn edge_rows(es: &[(String, String)]) -> Vec<Value> {
    es.iter()
        .map(|(a, b)| {
            let ia: i64 = a[1..].parse().unwrap();
            Value::Map(HashMap::from([
                ("a".to_string(), s(a)),
                ("b".to_string(), s(b)),
                ("i".to_string(), Value::Int(ia)),
                ("j".to_string(), Value::Int(b[1..].parse().unwrap())),
            ]))
        })
        .collect()
}

async fn run_variant(variant: &str, n: usize) -> Result<()> {
    let index = !variant.contains("noidx");
    let flushed = variant.contains("flushed");
    let (vs, es) = fixture(n);
    let db = Uni::in_memory().config(cfg()).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING, ix INT)").await?;
    tx.execute("CREATE EDGE TYPE T () FROM E TO E").await?;
    if index {
        tx.execute("CREATE INDEX idx_e_uid FOR (e:E) ON (e.uid)")
            .await?;
    }
    tx.commit().await?;

    let rows: Vec<Value> = vs
        .iter()
        .enumerate()
        .map(|(i, u)| {
            Value::Map(HashMap::from([
                ("uid".to_string(), s(u)),
                ("ix".to_string(), Value::Int(i as i64)),
            ]))
        })
        .collect();
    let t = Instant::now();
    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $rows AS r CREATE (:E {uid: r.uid, ix: r.ix})")
        .param("rows", Value::List(rows))
        .run()
        .await?;
    tx.commit().await?;
    let v_ms = ms(t);
    if flushed {
        db.flush().await?;
    }

    let mut extra = String::new();
    let t = Instant::now();
    if let Some(q) = variant.strip_prefix("q_") {
        // Read-only alternative query shapes, profiled.
        let q = match q {
            "one" => "UNWIND $rows AS r MATCH (a:E {uid: r.a}) RETURN count(*) AS c",
            "where" => "UNWIND $rows AS r MATCH (a:E) WHERE a.uid = r.a RETURN count(*) AS c",
            "two" => MATCH_Q,
            "noidx_two" => MATCH_Q,
            "tostring" => {
                "UNWIND $rows AS r WITH toString(r.a) AS ua MATCH (a:E {uid: ua}) RETURN count(*) AS c"
            }
            "int" => "UNWIND $rows AS r MATCH (a:E {ix: r.i}) RETURN count(*) AS c",
            "strlist" => "UNWIND $uids AS u MATCH (a:E {uid: u}) RETURN count(*) AS c",
            "nomatch" => "UNWIND $rows AS r RETURN count(*) AS c",
            _ => anyhow::bail!("unknown q"),
        };
        let tx = db.session().tx().await?;
        let uids: Vec<Value> = es.iter().map(|(a, _)| s(a)).collect();
        let (r, p) = tx
            .query_with(q)
            .param("rows", Value::List(edge_rows(&es)))
            .param("uids", Value::List(uids))
            .profile()
            .await?;
        let m = r.metrics();
        let ops: Vec<String> = p
            .runtime_stats
            .iter()
            .map(|o| {
                let name: String = o.operator.chars().take(60).collect();
                format!(
                    "[{}={}r ih{:?} {}ms]",
                    name, o.actual_rows, o.index_hits, o.time_ms
                )
            })
            .collect();
        extra = format!(
            "count={:?} rows_scanned={} index_scans={} l0={} storage={} ops={}",
            r.rows().first().map(|x| x.values().to_vec()),
            m.rows_scanned,
            m.index_scans,
            m.l0_reads,
            m.storage_reads,
            ops.join(" ")
        );
        tx.rollback();
    } else if variant.starts_with("single") {
        // One profiled scalar-parameter statement.
        let tx = db.session().tx().await?;
        let (a, b) = &es[es.len() / 2];
        let (r, p) = tx
            .execute_with("MATCH (a:E {uid: $s}), (b:E {uid: $d}) CREATE (a)-[:T]->(b)")
            .param("s", s(a))
            .param("d", s(b))
            .profile()
            .await?;
        let m = r.metrics();
        let ops: Vec<String> = p
            .runtime_stats
            .iter()
            .map(|o| {
                let name: String = o.operator.chars().take(40).collect();
                format!(
                    "[{}={}r ih{:?} {:.2}ms]",
                    name, o.actual_rows, o.index_hits, o.time_ms
                )
            })
            .collect();
        extra = format!(
            "exec_ms={} rows_scanned={} index_scans={} l0={} storage={} ops={}",
            p.total_time_ms,
            m.rows_scanned,
            m.index_scans,
            m.l0_reads,
            m.storage_reads,
            ops.join(" ")
        );
        tx.commit().await?;
    } else if variant.starts_with("loop") {
        // One statement per edge, scalar parameters, one transaction.
        let tx = db.session().tx().await?;
        let mut first = 0.0;
        let mut last = 0.0;
        for (i, (a, b)) in es.iter().enumerate() {
            let tc = Instant::now();
            tx.execute_with("MATCH (a:E {uid: $s}), (b:E {uid: $d}) CREATE (a)-[:T]->(b)")
                .param("s", s(a))
                .param("d", s(b))
                .run()
                .await?;
            if i < 100 {
                first += ms(tc);
            }
            if i + 100 >= es.len() {
                last += ms(tc);
            }
        }
        tx.commit().await?;
        extra = format!("first100_ms={first:.1} last100_ms={last:.1}");
    } else if variant.contains("matchonly") {
        // Read-only: lookups against committed vertices, no writes.
        let tx = db.session().tx().await?;
        let r = tx
            .query_with(MATCH_Q)
            .param("rows", Value::List(edge_rows(&es)))
            .fetch_all()
            .await?;
        extra = format!("count={:?}", r.rows().first().map(|x| x.values().to_vec()));
        tx.rollback();
    } else if variant.contains("profile") {
        let tx = db.session().tx().await?;
        let (r, p) = tx
            .execute_with(EDGE_Q)
            .param("rows", Value::List(edge_rows(&es)))
            .profile()
            .await?;
        let m = r.metrics();
        let ops: Vec<String> = p
            .runtime_stats
            .iter()
            .map(|o| {
                let name: String = o.operator.chars().take(40).collect();
                format!(
                    "[{}={}r ih{:?} {}ms]",
                    name, o.actual_rows, o.index_hits, o.time_ms
                )
            })
            .collect();
        extra = format!(
            "rows_scanned={} index_scans={} l0={} storage={} ops={}",
            m.rows_scanned,
            m.index_scans,
            m.l0_reads,
            m.storage_reads,
            ops.join(" ")
        );
        tx.commit().await?;
    } else if let Some(c) = variant.strip_prefix("chunk") {
        // `chunkK[_tx]`: K rows per statement; `_tx` keeps one transaction,
        // otherwise auto-commit per chunk.
        let k: usize = c.split('_').next().unwrap().parse()?;
        let one_tx = variant.ends_with("_tx");
        let mut per = Vec::new();
        if one_tx {
            let tx = db.session().tx().await?;
            for ch in es.chunks(k) {
                let tc = Instant::now();
                tx.execute_with(EDGE_Q)
                    .param("rows", Value::List(edge_rows(ch)))
                    .run()
                    .await?;
                per.push(ms(tc));
            }
            tx.commit().await?;
        } else {
            for ch in es.chunks(k) {
                let tc = Instant::now();
                let tx = db.session().tx().await?;
                tx.execute_with(EDGE_Q)
                    .param("rows", Value::List(edge_rows(ch)))
                    .run()
                    .await?;
                tx.commit().await?;
                per.push(ms(tc));
            }
        }
        let first = per.first().copied().unwrap_or(0.0);
        let last = per.last().copied().unwrap_or(0.0);
        extra = format!("chunks={} first_ms={first:.1} last_ms={last:.1}", per.len());
    } else {
        let q = if variant.contains("intkey") {
            "UNWIND $rows AS r MATCH (a:E {ix: r.i}), (b:E {ix: r.j}) CREATE (a)-[:T]->(b)"
        } else {
            EDGE_Q
        };
        let tx = db.session().tx().await?;
        tx.execute_with(q)
            .param("rows", Value::List(edge_rows(&es)))
            .run()
            .await?;
        tx.commit().await?;
    }
    let e_ms = ms(t);

    let r = db
        .session()
        .query("MATCH (:E)-[t:T]->(:E) RETURN count(t) AS c")
        .await?;
    eprintln!(
        "W3|{variant}|N={n}|V={}|vertex_ms={v_ms:.1}|edge_ms={e_ms:.1}|us_per_edge={:.1}|edges={:?}|{extra}",
        vs.len(),
        e_ms * 1000.0 / es.len() as f64,
        r.rows().first().map(|x| x.values().to_vec())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement probe; run with --run-ignored all --no-capture"]
async fn l0_load_probe() -> Result<()> {
    if std::env::var("L0_LOAD_TRACE").is_ok() {
        use tracing_subscriber::layer::SubscriberExt;
        let filter = tracing_subscriber::filter::Targets::new()
            .with_target("uni_query::cross_join_in_pushdown", tracing::Level::DEBUG);
        let sub = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(filter);
        let _ = tracing::subscriber::set_global_default(sub);
    }
    let sizes: Vec<usize> = env_list("L0_LOAD_SIZES", "1000,2000,4000")
        .iter()
        .map(|x| x.parse().unwrap())
        .collect();
    let variants = env_list("L0_LOAD_VARIANTS", "base");
    for v in &variants {
        for &n in &sizes {
            run_variant(v, n).await?;
        }
    }
    Ok(())
}
