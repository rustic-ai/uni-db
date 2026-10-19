// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `x IN fs` where `fs` is a column whose rows repeat in runs — the LDBC IC5
//! shape, `WITH collect(f) AS fs UNWIND … WHERE x IN fs`.
//!
//! The list arrives once per row, but consecutive rows carry the same list.
//! The membership test hashes each run's list once and probes it per row
//! instead of scanning the list per row. The answers are checked against
//! `any(y IN fs WHERE y = x)`, which has the same three-valued result through
//! a different mechanism, including `NULL` elements and lists holding `NULL`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(in_list_column_runs)'

// Rust guideline compliant

use std::time::Instant;

use anyhow::Result;
use uni_db::{Uni, Value};

/// `groups` lists of `len` integers (group `g` holds `g * len ..`), each
/// repeated over `rows` probes, half of which hit.
fn runs_query(groups: usize, len: usize, rows: usize) -> String {
    format!(
        "UNWIND range(0, {g}) AS g \
         WITH g, [i IN range(0, {l}) | g * {len} + i] AS fs \
         UNWIND range(0, {r}) AS j \
         WITH fs, g * {len} + (j * 7) % ({len} * 2) AS x \
         WHERE x IN fs RETURN count(*) AS c",
        g = groups - 1,
        l = len - 1,
        r = rows - 1,
    )
}

fn expected(groups: usize, len: usize, rows: usize) -> i64 {
    let per_group = (0..rows).filter(|j| (j * 7) % (len * 2) < len).count();
    (groups * per_group) as i64
}

/// Three-valued answers over runs of lists with and without `NULL`, against
/// `any(...)`, with integer, float, string, node and `NULL` elements.
#[tokio::test]
async fn column_list_runs_match_any() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("UNWIND range(0, 5) AS i CREATE (:N {id: i})")
        .await?;
    tx.commit().await?;
    let cases = [
        // Integer lists, a run each, some holding NULL, one NULL list.
        "UNWIND [[1, 2, 3], [4, null], null, [], [2.0, 'a']] AS fs \
         UNWIND [1, 2, 4, 5, null, 2.0, 'a', 9007199254740993] AS x \
         RETURN fs, x, x IN fs AS got, any(y IN fs WHERE y = x) AS want",
        // A list as long as several probes, repeated across a run of rows.
        "UNWIND range(0, 3) AS g WITH g, [i IN range(0, 63) | g * 64 + i] AS fs \
         UNWIND range(0, 40) AS j WITH fs, g * 64 + j * 3 AS x \
         RETURN fs, x, x IN fs AS got, any(y IN fs WHERE y = x) AS want",
        // Collected nodes, the IC5 form, probed with nodes and their ids.
        "MATCH (n:N) WITH n.id % 2 AS p, collect(n) AS fs \
         MATCH (m:N) RETURN p, m.id, m IN fs AS got, any(y IN fs WHERE y = m) AS want",
        "MATCH (n:N) WITH n.id % 2 AS p, collect(n) AS fs \
         MATCH (m:N) RETURN p, m.id, id(m) IN [f IN fs | id(f)] AS got, \
         any(y IN fs WHERE id(y) = id(m)) AS want",
    ];
    for q in cases {
        let res = session.query(q).await?;
        assert!(res.rows().len() > 8, "{q}: fixture too small");
        let (mut t, mut f, mut n) = (0, 0, 0);
        for row in res.rows() {
            let got = row.value("got").cloned().unwrap_or(Value::Null);
            let want = row.value("want").cloned().unwrap_or(Value::Null);
            assert_eq!(got, want, "{q}\nrow: {:?}", row.values());
            match got {
                Value::Bool(true) => t += 1,
                Value::Bool(false) => f += 1,
                _ => n += 1,
            }
        }
        assert!(t > 0 && f > 0, "{q}: {t} true, {f} false, {n} null");
    }
    db.shutdown().await?;
    Ok(())
}

/// The run-shaped query answers correctly at a size where a per-row scan
/// would be visible, and reports its time.
#[tokio::test]
async fn column_list_runs_count_is_exact() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let (groups, len, rows) = (4, 512, 256);
    let res = session.query(&runs_query(groups, len, rows)).await?;
    assert_eq!(res.rows()[0].get::<i64>("c")?, expected(groups, len, rows));
    db.shutdown().await?;
    Ok(())
}

/// Wall time of the run-shaped query as the list grows with the rows fixed.
/// A per-row scan grows with the list; a per-run hash grows only with the
/// cost of building each run's index once.
#[tokio::test]
#[ignore = "measurement probe; run with --run-ignored all --no-capture"]
async fn column_list_runs_timing_probe() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    for len in [256usize, 1024, 4096] {
        let (groups, rows) = (8, 1024);
        let q = runs_query(groups, len, rows);
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t = Instant::now();
            let res = session.query(&q).await?;
            best = best.min(t.elapsed().as_secs_f64() * 1e3);
            assert_eq!(res.rows()[0].get::<i64>("c")?, expected(groups, len, rows));
        }
        // Control: the same rows, lists carried along, no membership test.
        let control = q.replace(
            "WHERE x IN fs RETURN count(*)",
            "WHERE x >= 0 RETURN count(fs)",
        );
        let mut control_best = f64::MAX;
        for _ in 0..3 {
            let t = Instant::now();
            session.query(&control).await?;
            control_best = control_best.min(t.elapsed().as_secs_f64() * 1e3);
        }
        eprintln!(
            "len={len} groups={groups} rows/group={rows}: best of 3 {best:.1} ms \
             (control without IN {control_best:.1} ms)"
        );
    }
    db.shutdown().await?;
    Ok(())
}
