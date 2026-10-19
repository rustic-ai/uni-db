// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Measurement probe for the 2026-10-17 perf-class audit (predicted anchoring,
//! filter and recursion bottlenecks). Every candidate runs at two scales on a
//! bulk-loaded, flushed store, under `profile()`, and prints the operator
//! counters (`rows_scanned`, per-operator `actual_rows`, `index_scans`,
//! per-iteration `delta_facts`) next to a control formulation that needs only
//! O(answer) work. Results are recorded in
//! `docs/proposals/perf_class_measurements_2026_10_17.md`.
//!
//! `#[ignore]`d: these are measurements, not guards; they assert nothing
//! about cost. Run with
//! `cargo nextest run -p uni-db --run-ignored all -E 'test(perf_class_probe)' --no-capture`.
//! Scales can be overridden with `PERF_PROBE_SCALES=2000,8000`; `PERF_PROBE_SKIP_A5=1`
//! skips the A5 wall-time section of `perf_class_probe_timing`.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{Uni, UniConfig, Value};

fn scales(default: &[usize]) -> Vec<usize> {
    std::env::var("PERF_PROBE_SCALES")
        .ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| default.to_vec())
}

fn cfg() -> UniConfig {
    UniConfig {
        query_timeout: Duration::from_secs(300),
        ..Default::default()
    }
}

fn short_op(name: &str) -> String {
    name.split(['(', ':', ' '])
        .next()
        .unwrap_or(name)
        .to_string()
}

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

/// Vertices as (label, props) and edges as (type, src idx, dst idx, props).
struct Fixture {
    ddl: Vec<String>,
    vertices: Vec<(&'static str, HashMap<String, Value>)>,
    edges: Vec<(&'static str, usize, usize, HashMap<String, Value>)>,
}

/// Bulk-load, flush, reopen: every row is in L1.
async fn build(f: &Fixture, dir: &std::path::Path) -> Result<Uni> {
    let path = dir.to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).config(cfg()).build().await?;
        let tx = db.session().tx().await?;
        for d in &f.ddl {
            tx.execute(d).await?;
        }
        tx.commit().await?;
        let tx = db.session().tx().await?;
        let mut bulk = tx.bulk_writer().build()?;
        // Insert per label, preserving global indices.
        let mut vids = vec![None; f.vertices.len()];
        let mut by_label: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (i, (l, _)) in f.vertices.iter().enumerate() {
            by_label.entry(l).or_default().push(i);
        }
        for (label, idxs) in by_label {
            let props: Vec<HashMap<String, Value>> =
                idxs.iter().map(|&i| f.vertices[i].1.clone()).collect();
            let got = bulk.insert_vertices(label, props).await?;
            for (k, &i) in idxs.iter().enumerate() {
                vids[i] = Some(got[k]);
            }
        }
        let mut by_type: BTreeMap<&str, Vec<uni_bulk::EdgeData>> = BTreeMap::new();
        for (t, a, b, p) in &f.edges {
            by_type.entry(t).or_default().push(uni_bulk::EdgeData::new(
                vids[*a].unwrap(),
                vids[*b].unwrap(),
                p.clone(),
            ));
        }
        for (t, es) in by_type {
            bulk.insert_edges(t, es).await?;
        }
        bulk.commit().await?;
        tx.commit().await?;
        db.flush().await?;
        // Stop background compaction before the path is reopened.
        db.shutdown().await?;
    }
    Ok(Uni::open(&path).config(cfg()).build().await?)
}

// ---------------------------------------------------------------------------
// Cypher measurement
// ---------------------------------------------------------------------------

async fn cy(db: &Uni, tag: &str, n: usize, q: &str, params: &[(&str, Value)]) -> Result<()> {
    let session = db.session();
    let mut b = session.query_with(q);
    for (k, v) in params {
        b = b.param(*k, v.clone());
    }
    let t = Instant::now();
    let res = b.profile().await;
    let wall = t.elapsed().as_secs_f64() * 1000.0;
    let (r, p) = match res {
        Ok(x) => x,
        Err(e) => {
            eprintln!("CY|{tag}|N={n}|ERROR {e}");
            return Ok(());
        }
    };
    let m = r.metrics();
    let first = r
        .rows()
        .first()
        .map(|row| format!("{:?}", row.values()))
        .unwrap_or_default();
    let mut max_op = (String::new(), 0usize);
    let ops: Vec<String> = p
        .runtime_stats
        .iter()
        .map(|o| {
            if o.actual_rows > max_op.1 {
                max_op = (short_op(&o.operator), o.actual_rows);
            }
            match o.index_hits {
                Some(h) => format!("{}={}r(ih{h})", short_op(&o.operator), o.actual_rows),
                None => format!("{}={}r", short_op(&o.operator), o.actual_rows),
            }
        })
        .collect();
    eprintln!(
        "CY|{tag}|N={n}|rows={}|first={first}|rows_scanned={}|index_scans={}|scans_reported={}|l0={}|storage={}|max_op={}={}|wall_ms={wall:.1}|ops: {}",
        r.len(),
        m.rows_scanned,
        m.index_scans,
        m.scans_reported,
        m.l0_reads,
        m.storage_reads,
        max_op.0,
        max_op.1,
        ops.join(" ")
    );
    Ok(())
}

/// Generic graph: `N` nodes `:N {id, uid, name, x}` (uid, x indexed; name =
/// uid of the next node, unindexed), `N/8` nodes `:S {uid, name}`, and `2N`
/// `:R {w}` edges `i -> i+1`, `i -> 7i+3`.
fn graph_fixture(n: usize) -> Fixture {
    let mut vertices = Vec::new();
    for i in 0..n {
        vertices.push((
            "N",
            HashMap::from([
                ("id".to_string(), Value::Int(i as i64)),
                ("uid".to_string(), s(&format!("u{i}"))),
                ("name".to_string(), s(&format!("u{}", (i + 1) % n))),
                ("x".to_string(), Value::Int(i as i64)),
            ]),
        ));
    }
    for j in 0..n / 8 {
        vertices.push((
            "S",
            HashMap::from([
                ("uid".to_string(), s(&format!("s{j}"))),
                ("name".to_string(), s(&format!("u{}", j * 8))),
            ]),
        ));
    }
    let mut edges = Vec::new();
    for i in 0..n {
        edges.push((
            "R",
            i,
            (i + 1) % n,
            HashMap::from([("w".to_string(), Value::Int((i % 10) as i64))]),
        ));
        edges.push((
            "R",
            i,
            (i * 7 + 3) % n,
            HashMap::from([("w".to_string(), Value::Int(((i * 3) % 10) as i64))]),
        ));
    }
    Fixture {
        ddl: vec![
            "CREATE LABEL N (id INT, uid STRING, name STRING, x INT)".into(),
            "CREATE LABEL S (uid STRING, name STRING)".into(),
            "CREATE EDGE TYPE R (w INT) FROM N TO N".into(),
            "CREATE INDEX idx_n_uid FOR (n:N) ON (n.uid)".into(),
            "CREATE INDEX idx_n_x FOR (n:N) ON (n.x)".into(),
        ],
        vertices,
        edges,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf-class measurement probe; run with --run-ignored all --no-capture"]
async fn perf_class_probe_cypher() -> Result<()> {
    for n in scales(&[2000, 8000]) {
        let dir = tempfile::tempdir()?;
        let t = Instant::now();
        let db = build(&graph_fixture(n), dir.path()).await?;
        eprintln!(
            "\n##### cypher N={n} load {:.1}s",
            t.elapsed().as_secs_f64()
        );
        let q = |tag: &'static str, text: &'static str| (tag, text);
        let probes = [
            // Controls / A9 / F4
            q(
                "C0 indexed eq",
                "MATCH (n:N) WHERE n.uid = 'u42' RETURN n.id",
            ),
            q(
                "A9a literal-left",
                "MATCH (n:N) WHERE 'u42' = n.uid RETURN n.id",
            ),
            q(
                "C0x indexed int eq",
                "MATCH (n:N) WHERE n.x = 42 RETURN n.id",
            ),
            // F1
            q("F1 range <", "MATCH (n:N) WHERE n.x < 10 RETURN count(*)"),
            q(
                "F1 range between",
                "MATCH (n:N) WHERE n.x >= 100 AND n.x < 110 RETURN count(*)",
            ),
            q(
                "F1 starts-with",
                "MATCH (n:N) WHERE n.uid STARTS WITH 'u42' RETURN count(*)",
            ),
            q(
                "F1 in-list",
                "MATCH (n:N) WHERE n.uid IN ['u1','u2','u3'] RETURN count(*)",
            ),
            q(
                "F1 is-null",
                "MATCH (n:N) WHERE n.uid IS NULL RETURN count(*)",
            ),
            q(
                "F1 unindexed eq",
                "MATCH (n:N) WHERE n.name = 'u43' RETURN count(*)",
            ),
            q("F1 <>", "MATCH (n:N) WHERE n.x <> 5 RETURN count(*)"),
            // A6
            q(
                "A6 ctl back-anchored",
                "MATCH (a:N)-[:R]->(b:N {uid: 'u42'}) RETURN count(*)",
            ),
            q(
                "A6 named back-anchored",
                "MATCH p = (a:N)-[:R]->(b:N {uid: 'u42'}) RETURN count(p)",
            ),
            q(
                "A6 named fwd-anchored",
                "MATCH p = (a:N {uid: 'u42'})-[:R]->(b:N) RETURN count(p)",
            ),
            q(
                "A6 ctl fwd-anchored",
                "MATCH (a:N {uid: 'u42'})-[:R]->(b:N) RETURN count(*)",
            ),
            // A7
            q(
                "A7 anchored disjoint",
                "MATCH (a:N {uid: 'u42'}), (b:N) WHERE b.uid = a.name RETURN b.id",
            ),
            q("A7 ctl", "MATCH (b:N {uid: 'u43'}) RETURN b.id"),
            q(
                "A7 anchored disjoint 2-MATCH",
                "MATCH (a:N {uid: 'u42'}) MATCH (b:N) WHERE b.uid = a.name RETURN b.id",
            ),
            q(
                "A7 unanchored S-N",
                "MATCH (a:S), (b:N) WHERE a.name = b.uid RETURN count(*)",
            ),
            // A5
            q(
                "A5 ctl VLP",
                "MATCH (a:N {uid: 'u0'})-[:R*1..3]->(b) RETURN count(*)",
            ),
            q(
                "A5 VLP propmap",
                "MATCH (a:N {uid: 'u0'})-[:R*1..3 {w: 1}]->(b) RETURN count(*)",
            ),
            q(
                "A5 ctl 1-hop",
                "MATCH (a:N {uid: 'u0'})-[:R]->(b) RETURN count(*)",
            ),
            q(
                "A5 1-hop propmap",
                "MATCH (a:N {uid: 'u0'})-[:R {w: 1}]->(b) RETURN count(*)",
            ),
            // F2
            q(
                "F2 ctl anchored",
                "MATCH (a:N {uid: 'u42'})-[r:R]->(b) RETURN count(*)",
            ),
            q(
                "F2 anchored r.w>5",
                "MATCH (a:N {uid: 'u42'})-[r:R]->(b) WHERE r.w > 5 RETURN count(*)",
            ),
            q(
                "F2 ctl unanchored",
                "MATCH (a:N)-[r:R]->(b) RETURN count(*)",
            ),
            q(
                "F2 unanchored r.w=7",
                "MATCH (a:N)-[r:R]->(b) WHERE r.w = 7 RETURN count(*)",
            ),
            // F3
            q(
                "F3 ctl",
                "MATCH (a:N {uid: 'u42'})-[:R]->(b:N) RETURN count(*)",
            ),
            q(
                "F3 target filter",
                "MATCH (a:N {uid: 'u42'})-[:R]->(b:N) WHERE b.x = 43 RETURN count(*)",
            ),
            // A8
            q(
                "A8 ctl bound target",
                "MATCH p = shortestPath((a:N {uid: 'u0'})-[:R*]->(b:N {uid: 'u19'})) RETURN length(p)",
            ),
            q(
                "A8 filtered targets x<20",
                "MATCH p = shortestPath((a:N {uid: 'u0'})-[:R*]->(b:N)) WHERE b.x < 20 RETURN count(p)",
            ),
        ];
        for (tag, text) in probes {
            cy(&db, tag, n, text, &[]).await?;
        }
        // A9 parameter on either side.
        cy(
            &db,
            "A9 ctl n.uid=$p",
            n,
            "MATCH (n:N) WHERE n.uid = $p RETURN n.id",
            &[("p", s("u42"))],
        )
        .await?;
        cy(
            &db,
            "A9 $p=n.uid",
            n,
            "MATCH (n:N) WHERE $p = n.uid RETURN n.id",
            &[("p", s("u42"))],
        )
        .await?;
        cy(
            &db,
            "A9 inline {uid:$p}",
            n,
            "MATCH (n:N {uid: $p}) RETURN n.id",
            &[("p", s("u42"))],
        )
        .await?;
        // A8 unbound target, only at the small scale (|B| BFS).
        if n <= 2000 {
            cy(
                &db,
                "A8 unbound target",
                n,
                "MATCH p = shortestPath((a:N {uid: 'u0'})-[:R*]->(b:N)) RETURN count(p)",
                &[],
            )
            .await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Locy measurement
// ---------------------------------------------------------------------------

async fn lc(
    db: &Uni,
    tag: &str,
    n: usize,
    program: &str,
    per_iter: bool,
) -> Result<Option<uni_db::locy::LocyResult>> {
    let config = LocyConfig {
        max_iterations: 500,
        timeout: Some(Duration::from_secs(240)),
        ..Default::default()
    };
    let t = Instant::now();
    let res = db
        .session()
        .locy_with(program)
        .with_config(config)
        .profile()
        .await;
    let wall = t.elapsed().as_secs_f64() * 1000.0;
    let (result, profile) = match res {
        Ok(x) => x,
        Err(e) => {
            eprintln!("LC|{tag}|N={n}|ERROR {e}");
            return Ok(None);
        }
    };
    let rows = result.rows().map_or(0, |r| r.len());
    let mut facts: Vec<String> = result
        .derived
        .iter()
        .map(|(k, v)| format!("{k}:{}", v.len()))
        .collect();
    facts.sort();
    eprintln!(
        "LC|{tag}|N={n}|rows={rows}|rows_scanned={}|iterations={}|derived_total={}|facts=[{}]|wall_ms={wall:.1}|locy_ms={:.1}",
        result.metrics().rows_scanned,
        result.stats.total_iterations,
        result.stats.derived_nodes,
        facts.join(","),
        profile.total_time_ms,
    );
    for st in &profile.profile.strata {
        let ops: Vec<String> = st
            .operators
            .iter()
            .map(|o| format!("{}={}r", short_op(&o.operator), o.actual_rows))
            .collect();
        let mut rule_tot: Vec<String> = Vec::new();
        for r in &st.rules {
            let mut agg: BTreeMap<String, usize> = BTreeMap::new();
            let mut deltas = Vec::new();
            for it in &r.iterations {
                deltas.push(it.delta_facts);
                for o in &it.operators {
                    *agg.entry(short_op(&o.operator)).or_default() += o.actual_rows;
                }
            }
            let a: Vec<String> = agg.iter().map(|(k, v)| format!("{k}={v}")).collect();
            rule_tot.push(format!(
                "{}[facts={} iters={} sum_ops: {}]",
                r.name,
                r.facts,
                r.iterations.len(),
                a.join(" ")
            ));
            if per_iter {
                for it in &r.iterations {
                    let mut agg: BTreeMap<String, usize> = BTreeMap::new();
                    for o in &it.operators {
                        *agg.entry(short_op(&o.operator)).or_default() += o.actual_rows;
                    }
                    let a: Vec<String> = agg.iter().map(|(k, v)| format!("{k}={v}")).collect();
                    eprintln!(
                        "    {tag} N={n} {} it{} delta={} {:.1}ms | {}",
                        r.name,
                        it.iteration,
                        it.delta_facts,
                        it.elapsed_ms,
                        a.join(" ")
                    );
                }
            }
        }
        eprintln!(
            "  LCS|{tag}|N={n}|stratum {} rec={} iters={} facts={} {:.1}ms | ops: {} | {}",
            st.index,
            st.recursive,
            st.iterations,
            st.facts_derived,
            st.elapsed_ms,
            ops.join(" "),
            rule_tot.join(" ; ")
        );
    }
    Ok(Some(result))
}

/// `k` disjoint chains of length `len` of `:E {uid, seed}` linked by `:T {w}`.
fn chains_fixture(k: usize, len: usize) -> Fixture {
    let mut vertices = Vec::new();
    let mut edges = Vec::new();
    for c in 0..k {
        for p in 0..len {
            vertices.push((
                "E",
                HashMap::from([
                    ("uid".to_string(), s(&format!("c{c}_{p}"))),
                    ("seed".to_string(), Value::Bool(c == 0 && p == 0)),
                ]),
            ));
            if p > 0 {
                let i = c * len + p;
                edges.push((
                    "T",
                    i - 1,
                    i,
                    HashMap::from([("w".to_string(), Value::Float(1.0))]),
                ));
            }
        }
    }
    Fixture {
        ddl: locy_ddl(),
        vertices,
        edges,
    }
}

fn locy_ddl() -> Vec<String> {
    vec![
        "CREATE LABEL E (uid STRING, seed BOOL)".into(),
        "CREATE EDGE TYPE T (w FLOAT) FROM E TO E".into(),
        "CREATE INDEX idx_e_uid FOR (e:E) ON (e.uid)".into(),
    ]
}

const TC: &str = "\
CREATE RULE reach AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
CREATE RULE reach AS MATCH (a:E)-[:T]->(m:E) WHERE m IS reach TO b YIELD KEY a, KEY b \n";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf-class measurement probe; run with --run-ignored all --no-capture"]
async fn perf_class_probe_locy_anchor() -> Result<()> {
    // Chains of length 20; scale = number of chains (nodes = 20 * k).
    for k in scales(&[100, 400]).into_iter().map(|n| n / 20) {
        let n = k * 20;
        let dir = tempfile::tempdir()?;
        let t = Instant::now();
        let db = build(&chains_fixture(k, 20), dir.path()).await?;
        eprintln!(
            "\n##### locy-anchor N={n} (k={k} chains x 20) load {:.1}s",
            t.elapsed().as_secs_f64()
        );

        // A1: one-key QUERY over the full transitive closure vs anchored rule.
        lc(
            &db,
            "A1 TC QUERY WHERE a.uid",
            n,
            &format!("{TC}QUERY reach WHERE a.uid = 'c0_0' RETURN b.uid AS uid"),
            false,
        )
        .await?;
        lc(
            &db,
            "A1 TC QUERY all",
            n,
            &format!("{TC}QUERY reach RETURN b.uid AS uid"),
            false,
        )
        .await?;
        lc(
            &db,
            "A1 ctl anchored rule",
            n,
            "CREATE RULE from0 AS MATCH (a:E)-[:T]->(b:E) WHERE a.uid = 'c0_0' YIELD KEY b \n\
             CREATE RULE from0 AS MATCH (m:E)-[:T]->(b:E) WHERE m IS from0 YIELD KEY b \n\
             QUERY from0 RETURN b.uid AS uid",
            false,
        )
        .await?;

        // A2: cross-stratum IS ref from a non-recursive clause.
        lc(
            &db,
            "A2 cross-stratum IS",
            n,
            "CREATE RULE sd AS MATCH (e:E) WHERE e.uid = 'c0_0' YIELD KEY e \n\
             CREATE RULE nb AS MATCH (e:E)-[:T]->(f:E) WHERE e IS sd YIELD KEY f \n\
             QUERY nb RETURN f.uid AS uid",
            false,
        )
        .await?;
        lc(
            &db,
            "A2 ctl single-rule",
            n,
            "CREATE RULE nb AS MATCH (e:E)-[:T]->(f:E) WHERE e.uid = 'c0_0' YIELD KEY f \n\
             QUERY nb RETURN f.uid AS uid",
            false,
        )
        .await?;
        cy(
            &db,
            "A2 ctl cypher",
            n,
            "MATCH (e:E {uid: 'c0_0'})-[:T]->(f:E) RETURN f.uid",
            &[],
        )
        .await?;

        // A3: hydration of an unreturned rule.
        let small = "CREATE RULE sd AS MATCH (e:E) WHERE e.uid = 'c0_0' YIELD KEY e \n\
                     QUERY sd RETURN e.uid AS uid";
        lc(&db, "A3 small only", n, small, false).await?;
        let big = format!("CREATE RULE allv AS MATCH (e:E) YIELD KEY e \n{small}");
        if let Some(r) = lc(&db, "A3 small + unreturned allv", n, &big, false).await? {
            let kind = r
                .derived_facts("allv")
                .and_then(|v| v.first())
                .and_then(|row| row.get("e"))
                .map(|v| match v {
                    Value::Node(_) => "Node",
                    Value::Int(_) => "Int",
                    _ => "other",
                })
                .unwrap_or("none");
            eprintln!(
                "A3|N={n}|allv rows={} KEY e hydrated as {kind}",
                r.derived_facts("allv").map_or(0, |v| v.len())
            );
        }
        lc(&db, "A3 ctl allv alone (no hydrate diff)", n, "CREATE RULE allv AS MATCH (e:E) YIELD KEY e \nQUERY allv WHERE e.uid = 'c0_0' RETURN e.uid AS uid", false).await?;

        // A4: ASSUME re-evaluation.
        lc(
            &db,
            "A4 ctl plain",
            n,
            &format!("{TC}QUERY reach WHERE a.uid = 'c0_0' RETURN b.uid AS uid"),
            false,
        )
        .await?;
        lc(
            &db,
            "A4 ASSUME",
            n,
            &format!("{TC}ASSUME {{ CREATE (:E {{uid: 'zz', seed: false}}) }} THEN {{ QUERY reach WHERE a.uid = 'c0_0' RETURN b.uid AS uid }}"),
            false,
        )
        .await?;

        // R3 non-linear TC (iterations, per-iteration rows).
        lc(
            &db,
            "R3 non-linear TC",
            n,
            "CREATE RULE tc AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
             CREATE RULE tc AS MATCH (a:E) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b \n\
             QUERY tc WHERE a.uid = 'c0_0' RETURN b.uid AS uid",
            true,
        )
        .await?;
        lc(
            &db,
            "R3 ctl linear TC",
            n,
            &format!("{TC}QUERY reach WHERE a.uid = 'c0_0' RETURN b.uid AS uid"),
            true,
        )
        .await?;
    }
    Ok(())
}

/// #301 shape: chain `c0..c{chain-1}` (seed `c0`) plus `noise` edges among
/// disconnected noise nodes.
fn chain_noise_fixture(chain: usize, noise: usize) -> Fixture {
    let mut vertices: Vec<(&'static str, HashMap<String, Value>)> = (0..chain)
        .map(|i| {
            (
                "E",
                HashMap::from([
                    ("uid".to_string(), s(&format!("c{i}"))),
                    ("seed".to_string(), Value::Bool(i == 0)),
                ]),
            )
        })
        .collect();
    let len = noise / 2 + 2;
    vertices.extend((0..len).map(|i| {
        (
            "E",
            HashMap::from([
                ("uid".to_string(), s(&format!("n{i}"))),
                ("seed".to_string(), Value::Bool(false)),
            ]),
        )
    }));
    let w = |x: f64| HashMap::from([("w".to_string(), Value::Float(x))]);
    let mut edges: Vec<(&'static str, usize, usize, HashMap<String, Value>)> =
        (0..chain - 1).map(|i| ("T", i, i + 1, w(1.0))).collect();
    for i in 0..noise {
        let (a, b) = (i % len, (i * 7 + 3) % len);
        if a != b {
            edges.push(("T", chain + a, chain + b, w(1.0)));
        }
    }
    Fixture {
        ddl: locy_ddl(),
        vertices,
        edges,
    }
}

/// `k` diamonds: s_i -> {a_i, b_i} -> s_{i+1}, all weights 1.0. Paths from
/// s_0 to s_k: 2^k; distinct (node, cost) pairs: O(k).
fn diamond_fixture(k: usize) -> Fixture {
    let mut vertices = Vec::new();
    let v = |uid: String, seed: bool| {
        (
            "E",
            HashMap::from([
                ("uid".to_string(), Value::String(uid)),
                ("seed".to_string(), Value::Bool(seed)),
            ]),
        )
    };
    // index: s_i = 3i, a_i = 3i+1, b_i = 3i+2
    for i in 0..=k {
        vertices.push(v(format!("s{i}"), i == 0));
        vertices.push(v(format!("a{i}"), false));
        vertices.push(v(format!("b{i}"), false));
    }
    let w = || HashMap::from([("w".to_string(), Value::Float(1.0))]);
    let mut edges = Vec::new();
    for i in 0..k {
        edges.push(("T", 3 * i, 3 * i + 1, w()));
        edges.push(("T", 3 * i, 3 * i + 2, w()));
        edges.push(("T", 3 * i + 1, 3 * (i + 1), w()));
        edges.push(("T", 3 * i + 2, 3 * (i + 1), w()));
    }
    Fixture {
        ddl: locy_ddl(),
        vertices,
        edges,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf-class measurement probe; run with --run-ignored all --no-capture"]
async fn perf_class_probe_recursion() -> Result<()> {
    // R1: ALONG facts on a diamond lattice (k and 2k diamonds).
    for k in [6usize, 12] {
        let dir = tempfile::tempdir()?;
        let db = build(&diamond_fixture(k), dir.path()).await?;
        eprintln!("\n##### diamond k={k} nodes={}", 3 * (k + 1));
        lc(
            &db,
            "R1 ALONG no BEST BY",
            k,
            "CREATE RULE p AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true ALONG c = r.w YIELD KEY b, c \n\
             CREATE RULE p AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS p ALONG c = prev.c + r.w YIELD KEY b, c \n\
             QUERY p WHERE b.uid = 's1' RETURN c",
            false,
        )
        .await?;
        lc(
            &db,
            "R1 ALONG BEST BY",
            k,
            "CREATE RULE p AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true ALONG c = r.w BEST BY c ASC YIELD KEY b, c \n\
             CREATE RULE p AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS p ALONG c = prev.c + r.w BEST BY c ASC YIELD KEY b, c \n\
             QUERY p WHERE b.uid = 's1' RETURN c",
            false,
        )
        .await?;
        lc(
            &db,
            "R1 FOLD SUM",
            k,
            "CREATE RULE p AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true FOLD c = SUM(r.w) YIELD KEY b, c \n\
             CREATE RULE p AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS p FOLD c = SUM(r.w) YIELD KEY b, c \n\
             QUERY p WHERE b.uid = 's1' RETURN c",
            false,
        )
        .await?;
        lc(
            &db,
            "R1 ctl plain reach",
            k,
            "CREATE RULE p AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
             CREATE RULE p AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS p YIELD KEY b \n\
             QUERY p RETURN b.uid AS uid",
            false,
        )
        .await?;
    }

    // R2 / R3 mutual / R5: chain length 20 and 80 over 4000 noise edges.
    for chain in [20usize, 80] {
        let dir = tempfile::tempdir()?;
        let db = build(&chain_noise_fixture(chain, 4000), dir.path()).await?;
        eprintln!("\n##### chain+noise chain={chain} noise=4000");
        lc(
            &db,
            "R2 BEST BY shortest",
            chain,
            "CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true ALONG c = r.w BEST BY c ASC YIELD KEY b, c \n\
             CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d ALONG c = prev.c + r.w BEST BY c ASC YIELD KEY b, c \n\
             QUERY d RETURN b.uid AS uid",
            true,
        )
        .await?;
        lc(
            &db,
            "R2 ctl plain reach",
            chain,
            "CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
             CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d YIELD KEY b \n\
             QUERY d RETURN b.uid AS uid",
            true,
        )
        .await?;
        lc(
            &db,
            "R3 mutual even/odd",
            chain,
            "CREATE RULE ev AS MATCH (e:E) WHERE e.seed = true YIELD KEY e \n\
             CREATE RULE od AS MATCH (o:E)-[:T]->(e:E) WHERE o IS ev YIELD KEY e \n\
             CREATE RULE ev AS MATCH (o:E)-[:T]->(e:E) WHERE o IS od YIELD KEY e \n\
             QUERY ev RETURN e.uid AS uid",
            true,
        )
        .await?;
        lc(
            &db,
            "R5 IS NOT banned",
            chain,
            "CREATE RULE banned AS MATCH (e:E) WHERE e.seed = false AND e.uid STARTS WITH 'n' YIELD KEY e \n\
             CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
             CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d AND b IS NOT banned YIELD KEY b \n\
             QUERY d RETURN b.uid AS uid",
            true,
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Counter-blind candidates: wall-time medians, varied along one axis.
// ---------------------------------------------------------------------------

async fn median_ms(db: &Uni, q: &str, reps: usize) -> Result<(f64, usize, u64)> {
    let mut ts = Vec::new();
    let mut rows = 0;
    let mut idx = 0;
    for _ in 0..reps {
        let t = Instant::now();
        let r = db.session().query(q).await?;
        ts.push(t.elapsed().as_secs_f64() * 1000.0);
        rows = r
            .rows()
            .first()
            .and_then(|r| r.get::<i64>("c").ok())
            .unwrap_or(-1) as usize;
        idx = r.metrics().index_scans;
    }
    ts.sort_by(|a, b| a.partial_cmp(b).unwrap());
    Ok((ts[reps / 2], rows, idx))
}

/// Mean per-iteration ms of rule `rule` over iterations >= 3.
async fn per_iter_ms(db: &Uni, program: &str, rule: &str) -> Result<(f64, usize, f64)> {
    let config = LocyConfig {
        max_iterations: 500,
        timeout: Some(Duration::from_secs(240)),
        ..Default::default()
    };
    let (_r, p) = db
        .session()
        .locy_with(program)
        .with_config(config)
        .profile()
        .await?;
    let its: Vec<f64> = p
        .profile
        .strata
        .iter()
        .flat_map(|s| s.rules.iter())
        .filter(|r| r.name == rule)
        .flat_map(|r| r.iterations.iter())
        .filter(|it| it.iteration >= 3)
        .map(|it| it.elapsed_ms)
        .collect();
    let mean = its.iter().sum::<f64>() / its.len().max(1) as f64;
    Ok((mean, its.len(), p.total_time_ms))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf-class measurement probe; run with --run-ignored all --no-capture"]
async fn perf_class_probe_timing() -> Result<()> {
    // A5: VLP with inline edge / target property maps (rows_scanned is blind
    // to the pre-scan in build_edge_property_filter / build_accepting_vid_filter).
    for n in scales(&[2000, 8000, 32000])
        .into_iter()
        .filter(|_| std::env::var("PERF_PROBE_SKIP_A5").is_err())
    {
        let dir = tempfile::tempdir()?;
        let db = build(&graph_fixture(n), dir.path()).await?;
        for (tag, q) in [
            (
                "A5 ctl VLP",
                "MATCH (a:N {uid: 'u1'})-[:R*1..3]->(b) RETURN count(*) AS c",
            ),
            (
                "A5 VLP edge propmap {w:1}",
                "MATCH (a:N {uid: 'u1'})-[:R*1..3 {w: 1}]->(b) RETURN count(*) AS c",
            ),
            (
                "A5 ctl VLP target label",
                "MATCH (a:N {uid: 'u1'})-[:R*1..3]->(b:N) RETURN count(*) AS c",
            ),
            (
                "A5 VLP target propmap {x:2}",
                "MATCH (a:N {uid: 'u1'})-[:R*1..3]->(b:N {x: 2}) RETURN count(*) AS c",
            ),
            (
                "A5 ctl 1-hop",
                "MATCH (a:N {uid: 'u1'})-[:R]->(b) RETURN count(*) AS c",
            ),
            (
                "A5 1-hop edge propmap {w:1}",
                "MATCH (a:N {uid: 'u1'})-[:R {w: 1}]->(b) RETURN count(*) AS c",
            ),
        ] {
            let (ms, c, idx) = median_ms(&db, q, 5).await?;
            eprintln!("TM|{tag}|N={n}|median_ms={ms:.1}|count={c}|index_scans={idx}");
        }
    }

    // R4 / R5 / R6: chain 80 over a noise graph of 4000 vs 16000 edges.
    for noise in [4000usize, 16000] {
        let dir = tempfile::tempdir()?;
        let db = build(&chain_noise_fixture(80, noise), dir.path()).await?;
        let plain = "CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
                     CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d YIELD KEY b \n\
                     QUERY d WHERE b.uid = 'c79' RETURN b.uid AS uid";
        let big_base = "CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
                        CREATE RULE d AS MATCH (b:E) WHERE b.uid STARTS WITH 'n' YIELD KEY b \n\
                        CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d YIELD KEY b \n\
                        QUERY d WHERE b.uid = 'c79' RETURN b.uid AS uid";
        let banned = "CREATE RULE banned AS MATCH (e:E) WHERE e.uid STARTS WITH 'n' YIELD KEY e \n\
                      CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
                      CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d AND b IS NOT banned YIELD KEY b \n\
                      QUERY d WHERE b.uid = 'c79' RETURN b.uid AS uid";
        // Interleave 3 rounds and report each, so drift/contention shows.
        for round in 0..3 {
            for (tag, prog) in [
                ("R6 ctl plain (per-iter floor)", plain),
                ("R4 accumulated facts ~noise/2", big_base),
                ("R5 IS NOT banned ~noise/2", banned),
            ] {
                let (ms, k, total) = per_iter_ms(&db, prog, "d").await?;
                eprintln!(
                    "TM|{tag}|noise={noise}|round={round}|mean_iter_ms(it>=3)={ms:.2}|iters={k}|total_ms={total:.1}"
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// BEST BY merge cost (Wave C): a wide layered graph, so the fact set is large
// while each iteration's delta is one layer.
// ---------------------------------------------------------------------------

/// `depth` layers of `width` `:E` nodes. Every node has three `:T` edges into
/// the next layer (weights 1..10) and one expensive skip edge two layers on
/// (weight 25), so a node is first reached through a skip and then improved.
/// Layer 0 is the seed set.
fn layered_fixture(width: usize, depth: usize) -> Fixture {
    let mut vertices = Vec::new();
    for layer in 0..depth {
        for j in 0..width {
            vertices.push((
                "E",
                HashMap::from([
                    ("uid".to_string(), s(&format!("l{layer}_{j}"))),
                    ("seed".to_string(), Value::Bool(layer == 0)),
                ]),
            ));
        }
    }
    let w = |x: f64| HashMap::from([("w".to_string(), Value::Float(x))]);
    let mut edges = Vec::new();
    for layer in 0..depth.saturating_sub(1) {
        for j in 0..width {
            let from = layer * width + j;
            for k in 0..3 {
                let to = (layer + 1) * width + (j * 7 + k * 13 + 1) % width;
                let weight = 1 + (layer * 31 + j * 17 + k * 5) % 10;
                edges.push(("T", from, to, w(weight as f64)));
            }
            if layer + 2 < depth {
                edges.push((
                    "T",
                    from,
                    (layer + 2) * width + (j * 3 + 5) % width,
                    w(25.0),
                ));
            }
        }
    }
    Fixture {
        ddl: locy_ddl(),
        vertices,
        edges,
    }
}

/// Total time and rows of every operator named `op` across all iterations.
fn op_totals(profile: &uni_db::locy::LocyProfileOutput, op: &str) -> (f64, usize) {
    profile
        .profile
        .strata
        .iter()
        .flat_map(|s| s.rules.iter())
        .flat_map(|r| r.iterations.iter())
        .flat_map(|it| it.operators.iter())
        .filter(|o| o.operator == op)
        .fold((0.0, 0), |(t, n), o| (t + o.time_ms, n + o.actual_rows))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf-class measurement probe; run with --run-ignored all --no-capture"]
async fn perf_class_probe_best_by_merge() -> Result<()> {
    let config = LocyConfig {
        max_iterations: 500,
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    };
    for width in scales(&[500, 2000]) {
        let depth = 40;
        let dir = tempfile::tempdir()?;
        let db = build(&layered_fixture(width, depth), dir.path()).await?;
        for (tag, op, program) in [
            (
                "BEST BY shortest",
                "LocyBestByMerge",
                "CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true ALONG c = r.w BEST BY c ASC YIELD KEY b, c \n\
                 CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d ALONG c = prev.c + r.w BEST BY c ASC YIELD KEY b, c \n\
                 QUERY d WHERE b.uid = 'l39_0' RETURN c",
            ),
            (
                "ctl plain reach",
                "LocyFactDedup",
                "CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
                 CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d YIELD KEY b \n\
                 QUERY d WHERE b.uid = 'l39_0' RETURN b.uid AS uid",
            ),
        ] {
            for round in 0..3 {
                let t = Instant::now();
                let (result, profile) = db
                    .session()
                    .locy_with(program)
                    .with_config(config.clone())
                    .profile()
                    .await?;
                let wall = t.elapsed().as_secs_f64() * 1000.0;
                let (merge_ms, merge_rows) = op_totals(&profile, op);
                let facts = result.derived.get("d").map_or(0, Vec::len);
                eprintln!(
                    "BB|{tag}|width={width}|round={round}|facts={facts}|iterations={}|wall_ms={wall:.1}|{op}_ms={merge_ms:.1}|{op}_rows={merge_rows}",
                    result.stats.total_iterations,
                );
            }
        }
    }
    Ok(())
}

/// A `:T` chain `c0 -> ... -> c{len-1}` plus `noise x noise` edges from
/// `x*` to `y*` nodes, which no chain pair composes with: the non-linear
/// clause's old and full scans carry them every pass while no derivation
/// reads them.
fn chain_plus_bipartite_fixture(len: usize, noise: usize) -> Fixture {
    let mut vertices = Vec::new();
    let v = |uid: String| {
        (
            "E",
            HashMap::from([
                ("uid".to_string(), Value::String(uid)),
                ("seed".to_string(), Value::Bool(false)),
            ]),
        )
    };
    for i in 0..len {
        vertices.push(v(format!("c{i}")));
    }
    for i in 0..noise {
        vertices.push(v(format!("x{i}")));
    }
    for i in 0..noise {
        vertices.push(v(format!("y{i}")));
    }
    let w = || HashMap::from([("w".to_string(), Value::Float(1.0))]);
    let mut edges = Vec::new();
    for i in 1..len {
        edges.push(("T", i - 1, i, w()));
    }
    for i in 0..noise {
        for j in 0..noise {
            edges.push(("T", len + i, len + noise + j, w()));
        }
    }
    Fixture {
        ddl: locy_ddl(),
        vertices,
        edges,
    }
}

/// Non-linear recursion: per-iteration cost against the size of the old/full
/// relation the semi-naive passes read, at a fixed chain (fixed derivations).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "perf-class measurement probe; run with --run-ignored all --no-capture"]
async fn perf_class_probe_nonlinear_full_scan() -> Result<()> {
    let config = LocyConfig {
        max_iterations: 500,
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    };
    let program = "CREATE RULE tc AS MATCH (a:E)-[:T]->(b:E) YIELD KEY a, KEY b \n\
                   CREATE RULE tc AS MATCH (a:E) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b \n\
                   QUERY tc WHERE a.uid = 'c0' RETURN b.uid AS uid";
    for noise in [0usize, 100, 300] {
        let len = 128;
        let dir = tempfile::tempdir()?;
        let db = build(&chain_plus_bipartite_fixture(len, noise), dir.path()).await?;
        for round in 0..3 {
            let (result, profile) = db
                .session()
                .locy_with(program)
                .with_config(config.clone())
                .profile()
                .await?;
            let its: Vec<_> = profile
                .profile
                .strata
                .iter()
                .flat_map(|s| s.rules.iter())
                .filter(|r| r.name == "tc")
                .flat_map(|r| r.iterations.iter())
                .filter(|it| it.iteration >= 1)
                .collect();
            let rec_ms: f64 = its.iter().map(|it| it.elapsed_ms).sum();
            let join: (f64, usize) = its
                .iter()
                .flat_map(|it| it.operators.iter())
                .filter(|o| o.operator.starts_with("HashJoinExec"))
                .fold((0.0, 0), |(t, n), o| (t + o.time_ms, n + o.actual_rows));
            let scan: (f64, usize) = its
                .iter()
                .flat_map(|it| it.operators.iter())
                .filter(|o| o.operator.starts_with("DerivedScanExec"))
                .fold((0.0, 0), |(t, n), o| (t + o.time_ms, n + o.actual_rows));
            eprintln!(
                "NL|len={len}|noise_facts={}|round={round}|facts={}|iterations={}|recursive_iters_ms={rec_ms:.1}|hashjoin_ms={:.1}|hashjoin_rows={}|derivedscan_rows={}|total_ms={:.1}",
                noise * noise,
                result.derived.get("tc").map_or(0, Vec::len),
                result.stats.total_iterations,
                join.0,
                join.1,
                scan.1,
                profile.total_time_ms,
            );
        }
    }
    Ok(())
}
