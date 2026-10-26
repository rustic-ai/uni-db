// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #312 probe: a large, unreferenced property of a matched node is
//! carried into every joined row.
//!
//! Measurement, not a pass/fail regression test. For each `(n_rows, prop_kb)`
//! cell it finds, by bisection over `TxQueryBuilder::max_memory`, the smallest
//! query memory pool the statement completes under -- the same accounting that
//! raises the issue's `Query exceeded memory limit` error -- and the wall time
//! at the default limit. The differential arms are:
//!
//! * `create`  -- the issue's statement (`... CREATE (a)-[:CONTAINS]->(b)`).
//! * `read`    -- identical MATCHes, `RETURN count(*)`: `a` is never read.
//! * `read_uid`-- identical MATCHes, `RETURN a.uid`: one small property read.
//! * `create_prematch` -- the issue's "match `a` once first" variant.
//!
//! `prop_kb = 0` is the control: same rows, same statement, empty `manifest`.
//!
//! Run: `cargo nextest run -p uni-db --test integration --run-ignored all \
//!       -E 'test(issue_312)' --no-capture`

use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};

const CREATE_Q: &str = "UNWIND $edges AS e \
     MATCH (a:PackageRevision) WHERE id(a) = e.src \
     MATCH (b:ElementVersion) WHERE id(b) = e.dst \
     CREATE (a)-[:CONTAINS]->(b) RETURN count(*) AS c";

const READ_Q: &str = "UNWIND $edges AS e \
     MATCH (a:PackageRevision) WHERE id(a) = e.src \
     MATCH (b:ElementVersion) WHERE id(b) = e.dst \
     RETURN count(*) AS c";

const READ_UID_Q: &str = "UNWIND $edges AS e \
     MATCH (a:PackageRevision) WHERE id(a) = e.src \
     MATCH (b:ElementVersion) WHERE id(b) = e.dst \
     RETURN a.uid AS u, id(b) AS c";

const CREATE_PREMATCH_Q: &str = "MATCH (p:PackageRevision) WHERE id(p) = $src \
     WITH id(p) AS src UNWIND $dsts AS d \
     MATCH (a:PackageRevision) WHERE id(a) = src \
     MATCH (b:ElementVersion) WHERE id(b) = d \
     CREATE (a)-[:CONTAINS]->(b) RETURN count(*) AS c";

const TRAVERSE_Q: &str = "MATCH (p:PackageRevision)-[:CONTAINS]->(v:ElementVersion) \
     RETURN v.n AS n";

struct Fixture {
    db: Uni,
    src: i64,
    dsts: Vec<i64>,
}

async fn fixture(n_rows: usize, prop_kb: usize, flush: bool) -> Result<Fixture> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("PackageRevision")
        .property("uid", DataType::String)
        .property("package", DataType::String)
        .property("manifest", DataType::String)
        .label("ElementVersion")
        .property("n", DataType::Int64)
        .edge_type("CONTAINS", &["PackageRevision"], &["ElementVersion"])
        .apply()
        .await?;

    let manifest = "x".repeat(prop_kb * 1024);
    let tx = db.session().tx().await?;
    tx.query_with("CREATE (:PackageRevision {uid: 'p1', package: 'pkg', manifest: $m})")
        .param("m", Value::String(manifest))
        .execute()
        .await?;
    tx.query_with("UNWIND range(1, $n) AS i CREATE (:ElementVersion {n: i})")
        .param("n", Value::Int(n_rows as i64))
        .execute()
        .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }

    let s = db.session();
    let src = s
        .query("MATCH (a:PackageRevision) RETURN id(a) AS v")
        .await?
        .rows()[0]
        .get::<i64>("v")?;
    let dsts: Vec<i64> = s
        .query("MATCH (b:ElementVersion) RETURN id(b) AS v ORDER BY v")
        .await?
        .rows()
        .iter()
        .map(|r| r.get::<i64>("v").unwrap())
        .collect();
    assert_eq!(dsts.len(), n_rows);
    Ok(Fixture { db, src, dsts })
}

fn params(f: &Fixture) -> Vec<(&'static str, Value)> {
    let edges: Vec<Value> = f
        .dsts
        .iter()
        .map(|d| {
            Value::Map(HashMap::from([
                ("src".to_string(), Value::Int(f.src)),
                ("dst".to_string(), Value::Int(*d)),
            ]))
        })
        .collect();
    vec![
        ("edges", Value::List(edges)),
        ("src", Value::Int(f.src)),
        (
            "dsts",
            Value::List(f.dsts.iter().map(|d| Value::Int(*d)).collect()),
        ),
    ]
}

/// Run `q` in a fresh transaction that is rolled back, so every probe sees the
/// same committed state. `Ok(rows)` on success, `Err(msg)` on failure.
async fn run_once(f: &Fixture, q: &str, limit: Option<usize>) -> Result<usize, String> {
    let tx = f.db.session().tx().await.map_err(|e| e.to_string())?;
    let mut b = tx.query_with(q);
    for (k, v) in params(f) {
        b = b.param(k, v);
    }
    if let Some(l) = limit {
        b = b.max_memory(l);
    }
    let out = b
        .fetch_all()
        .await
        .map(|r| r.rows().len())
        .map_err(|e| e.to_string());
    tx.rollback();
    out
}

/// Smallest pool limit (bytes, within 8%) under which `q` completes, found by
/// bisection on a log scale. Also returns the error of the largest failing
/// probe, which names the operator that hit the limit.
async fn min_pool_bytes(f: &Fixture, q: &str) -> (usize, String) {
    let mut hi: f64 = (3u64 << 30) as f64;
    if let Err(e) = run_once(f, q, Some(hi as usize)).await {
        return (usize::MAX, e);
    }
    let mut lo: f64 = 1024.0;
    let mut last_err = match run_once(f, q, Some(lo as usize)).await {
        Ok(_) => return (lo as usize, String::new()),
        Err(e) => e,
    };
    while hi / lo > 1.08 {
        let mid = (hi * lo).sqrt();
        match run_once(f, q, Some(mid as usize)).await {
            Ok(_) => hi = mid,
            Err(e) => {
                last_err = e;
                lo = mid;
            }
        }
    }
    (hi as usize, last_err)
}

fn mb(b: usize) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "issue #312 measurement probe; prints a scaling table"]
async fn issue_312_scaling_table() -> Result<()> {
    println!(
        "{:>6} {:>6} {:>5} {:>10} {:>11} {:>12}  {}",
        "n", "propKB", "flush", "arm", "minPoolMB", "per-row KB", "operator at limit"
    );
    for &flush in &[false, true] {
        for &prop_kb in &[0usize, 200] {
            for &n in &[100usize, 200, 400] {
                let f = fixture(n, prop_kb, flush).await?;
                for (arm, q) in [
                    ("create", CREATE_Q),
                    ("read", READ_Q),
                    ("read_uid", READ_UID_Q),
                ] {
                    let (pool, err) = min_pool_bytes(&f, q).await;
                    let op = err
                        .split("for ")
                        .nth(1)
                        .and_then(|t| t.split(" with").next())
                        .unwrap_or("-")
                        .to_string();
                    println!(
                        "{:>6} {:>6} {:>5} {:>10} {:>11.2} {:>12.1}  {}",
                        n,
                        prop_kb,
                        flush,
                        arm,
                        mb(pool),
                        pool as f64 / 1024.0 / n as f64,
                        op
                    );
                }
            }
        }
    }
    Ok(())
}

/// Wall time only (no bisection), default 1 GiB limit, wider N range.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "issue #312 measurement probe; prints a timing table"]
async fn issue_312_timing_table() -> Result<()> {
    println!("{:>6} {:>6} {:>16} {:>10}", "n", "propKB", "arm", "ms");
    for &prop_kb in &[0usize, 50, 200] {
        for &n in &[50usize, 100, 200, 400, 800] {
            let f = fixture(n, prop_kb, false).await?;
            for (arm, q) in [
                ("create", CREATE_Q),
                ("read", READ_Q),
                ("read_uid", READ_UID_Q),
                ("create_prematch", CREATE_PREMATCH_Q),
            ] {
                let t0 = Instant::now();
                let r = run_once(&f, q, None).await;
                let ms = t0.elapsed().as_millis();
                let status = match &r {
                    Ok(_) => String::new(),
                    Err(e) => format!(" ERR: {}", &e[..e.len().min(200)]),
                };
                println!("{:>6} {:>6} {:>16} {:>10}{}", n, prop_kb, arm, ms, status);
            }
        }
    }
    Ok(())
}

/// The issue's "read traversals presumably pay the same cost" claim.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "issue #312 measurement probe; prints a scaling table"]
async fn issue_312_traverse_table() -> Result<()> {
    println!(
        "{:>6} {:>6} {:>5} {:>11}",
        "n", "propKB", "flush", "minPoolMB"
    );
    for &flush in &[false, true] {
        for &prop_kb in &[0usize, 200] {
            for &n in &[200usize, 800] {
                let f = fixture(n, prop_kb, flush).await?;
                // Materialize the edges once, committed.
                let tx = f.db.session().tx().await?;
                let mut b = tx.query_with(CREATE_Q);
                for (k, v) in params(&f) {
                    b = b.param(k, v);
                }
                b.max_memory(8 << 30).fetch_all().await?;
                tx.commit().await?;
                if flush {
                    f.db.flush().await?;
                }
                let (pool, _) = min_pool_bytes(&f, TRAVERSE_Q).await;
                println!("{:>6} {:>6} {:>5} {:>11.2}", n, prop_kb, flush, mb(pool));
            }
        }
    }
    Ok(())
}

/// Per-operator memory for the issue's statement, from `PROFILE`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "issue #312 measurement probe; prints a profile"]
async fn issue_312_profile() -> Result<()> {
    for &prop_kb in &[0usize, 200] {
        let f = fixture(400, prop_kb, false).await?;
        for (arm, q) in [("create", CREATE_Q), ("read", READ_Q)] {
            let tx = f.db.session().tx().await?;
            let mut b = tx.query_with(q);
            for (k, v) in params(&f) {
                b = b.param(k, v);
            }
            let (_, prof) = b.max_memory(8 << 30).profile().await?;
            tx.rollback();
            println!(
                "=== propKB={prop_kb} arm={arm} total_ms={}",
                prof.total_time_ms
            );
            for s in &prof.runtime_stats {
                println!(
                    "  {:<40} rows={:>6} mem={:>10.2}MB t={:.1}ms",
                    s.operator,
                    s.actual_rows,
                    mb(s.memory_bytes),
                    s.time_ms
                );
            }
            if prop_kb == 200 {
                println!("{}", prof.explain.plan_text);
            }
        }
    }
    Ok(())
}

/// Which reservation fails at each limit: names the successive charges the
/// issue's statement makes at n=400, 200 KB.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "issue #312 measurement probe; prints the reservation ladder"]
async fn issue_312_charge_ladder() -> Result<()> {
    let f = fixture(400, 200, false).await?;
    for limit_mb in [100usize, 200, 320, 400, 640, 700, 800, 900, 1000] {
        let r = run_once(&f, CREATE_Q, Some(limit_mb << 20)).await;
        match r {
            Ok(_) => println!("limit {limit_mb:>5} MB: OK"),
            Err(e) => println!("limit {limit_mb:>5} MB: {}", &e[..e.len().min(260)]),
        }
    }
    Ok(())
}
