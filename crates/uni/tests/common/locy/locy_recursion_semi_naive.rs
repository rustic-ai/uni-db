// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! P5 (2026-10-17 perf-class audit): semi-naive feeding of recursive strata.
//!
//! Four places read more of a derived relation per fixpoint iteration than
//! the iteration's delta, measured in
//! `docs/proposals/perf_class_measurements_2026_10_17.md`:
//!
//! * **R2** — a `BEST BY` rule's delta was its whole fact set whenever any
//!   row changed, so a self-reference re-read every fact each iteration;
//! * **R3 non-linear** — a clause with two references into its own stratum
//!   read the whole relation at both, every iteration;
//! * **R3 mutual** — a reference to the *other* rule of a mutually recursive
//!   stratum read that rule's whole relation, every iteration;
//! * **R5** — an `IS NOT` rebuilt its banned-key set from the whole negated
//!   relation per clause per iteration, though that relation (a lower
//!   stratum) cannot change while the stratum iterates.
//!
//! Each guard counts operator rows from `profile()` at two scales, so it is
//! not timing sensitive, and checks the answer. Correctness across random
//! programs is the oracle differential's job (`uni-locy-oracle`, recursion
//! families).
//!
//! Run with `cargo nextest run -p uni-db -E 'test(recursion_semi_naive)'`.

// Rust guideline compliant

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use anyhow::Result;
use uni_db::locy::{LocyConfig, LocyResult};
use uni_db::{Uni, Value};

/// Chain `c0 -> c1 -> ... -> c{len-1}` (weight 1, seed `c0`) plus `noise`
/// edges among `noise / 2 + 2` noise nodes `n*` the chain never reaches.
async fn chain_with_noise(len: usize, noise: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING, seed BOOL)").await?;
    tx.execute("CREATE EDGE TYPE T (w FLOAT) FROM E TO E")
        .await?;
    tx.execute("CREATE INDEX idx_e_uid FOR (e:E) ON (e.uid)")
        .await?;
    tx.commit().await?;

    let noise_nodes = noise / 2 + 2;
    let mut vertices: Vec<Value> = (0..len).map(|i| vertex(&format!("c{i}"), i == 0)).collect();
    vertices.extend((0..noise_nodes).map(|i| vertex(&format!("n{i}"), false)));
    let mut edges: Vec<Value> = (1..len)
        .map(|i| edge(&format!("c{}", i - 1), &format!("c{i}"), 1.0))
        .collect();
    for i in 0..noise {
        let (a, b) = (i % noise_nodes, (i * 7 + 3) % noise_nodes);
        if a != b {
            edges.push(edge(&format!("n{a}"), &format!("n{b}"), 1.0));
        }
    }
    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $rows AS r CREATE (:E {uid: r.uid, seed: r.seed})")
        .param("rows", Value::List(vertices))
        .run()
        .await?;
    tx.execute_with(
        "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) \
         CREATE (a)-[:T {w: r.w}]->(b)",
    )
    .param("rows", Value::List(edges))
    .run()
    .await?;
    tx.commit().await?;
    Ok(db)
}

fn vertex(uid: &str, seed: bool) -> Value {
    Value::Map(HashMap::from([
        ("uid".to_string(), Value::String(uid.to_string())),
        ("seed".to_string(), Value::Bool(seed)),
    ]))
}

fn edge(a: &str, b: &str, w: f64) -> Value {
    Value::Map(HashMap::from([
        ("a".to_string(), Value::String(a.to_string())),
        ("b".to_string(), Value::String(b.to_string())),
        ("w".to_string(), Value::Float(w)),
    ]))
}

/// The operator name without its parameters.
fn short_op(name: &str) -> String {
    name.split(['(', ':', ' '])
        .next()
        .unwrap_or(name)
        .to_string()
}

/// One iteration of one rule: its delta and its operator rows by name.
#[derive(Debug)]
struct Iteration {
    iteration: usize,
    delta: usize,
    rows: BTreeMap<String, usize>,
}

impl Iteration {
    fn rows(&self, op: &str) -> usize {
        self.rows.get(op).copied().unwrap_or(0)
    }

    fn total(&self) -> usize {
        self.rows.values().sum()
    }
}

/// The QUERY's rows and, per rule, its iterations, from `profile()`.
async fn profile(
    db: &Uni,
    program: &str,
) -> Result<(LocyResult, BTreeMap<String, Vec<Iteration>>)> {
    let config = LocyConfig {
        max_iterations: 500,
        timeout: Some(Duration::from_secs(600)),
        ..Default::default()
    };
    let (result, profile) = db
        .session()
        .locy_with(program)
        .with_config(config)
        .profile()
        .await?;
    let mut rules: BTreeMap<String, Vec<Iteration>> = BTreeMap::new();
    for stratum in &profile.profile.strata {
        for rule in &stratum.rules {
            let its = rules.entry(rule.name.clone()).or_default();
            for it in &rule.iterations {
                let mut rows: BTreeMap<String, usize> = BTreeMap::new();
                for op in &it.operators {
                    *rows.entry(short_op(&op.operator)).or_default() += op.actual_rows;
                }
                its.push(Iteration {
                    iteration: it.iteration,
                    delta: it.delta_facts,
                    rows,
                });
            }
        }
    }
    Ok((result, rules))
}

/// The QUERY rows' string column `col`, sorted.
fn strings(result: &LocyResult, col: &str) -> Vec<String> {
    let mut v: Vec<String> = result
        .rows()
        .expect("QUERY rows")
        .iter()
        .filter_map(|r| match r.get(col) {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

/// The QUERY rows as `(uid, number)` pairs, sorted.
fn uid_numbers(result: &LocyResult, num: &str) -> Vec<(String, f64)> {
    let mut v: Vec<(String, f64)> = result
        .rows()
        .expect("QUERY rows")
        .iter()
        .map(|r| {
            let uid = match r.get("uid") {
                Some(Value::String(s)) => s.clone(),
                other => panic!("uid: {other:?}"),
            };
            let n = match r.get(num) {
                Some(Value::Float(f)) => *f,
                Some(Value::Int(i)) => *i as f64,
                other => panic!("{num}: {other:?}"),
            };
            (uid, n)
        })
        .collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

fn chain_uids(len: usize, keep: impl Fn(usize) -> bool) -> Vec<String> {
    let mut v: Vec<String> = (0..len)
        .filter(|&i| keep(i))
        .map(|i| format!("c{i}"))
        .collect();
    v.sort();
    v
}

const BEST_BY: &str = "\
CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true ALONG c = r.w BEST BY c ASC YIELD KEY b, c \n\
CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d ALONG c = prev.c + r.w BEST BY c ASC YIELD KEY b, c \n\
QUERY d RETURN b.uid AS uid, c";

/// R2: a `BEST BY` rule's self-reference reads only the rows the previous
/// merge added or improved — on a chain, one row per iteration.
///
/// Before the fix the delta was the whole pruned fact set whenever anything
/// changed, so iteration `i` re-read `i` rows: `DerivedScanExec` summed to
/// 1 + 2 + ... over the run (190 / 3,160 at length 20 / 80) against 19 / 79.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_best_by_reads_only_improved_rows() -> Result<()> {
    for len in [20usize, 80] {
        let db = chain_with_noise(len, 400).await?;
        let (result, rules) = profile(&db, BEST_BY).await?;
        let want: Vec<(String, f64)> = {
            let mut v: Vec<(String, f64)> = (1..len).map(|i| (format!("c{i}"), i as f64)).collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v
        };
        assert_eq!(
            uid_numbers(&result, "c"),
            want,
            "len={len}: shortest distances"
        );

        let its = &rules["d"];
        let mut previous_delta = 0;
        for it in its {
            assert_eq!(
                it.rows("DerivedScanExec"),
                previous_delta,
                "len={len}: iteration {} read other than the previous delta",
                it.iteration
            );
            previous_delta = it.delta;
        }
        let read: usize = its.iter().map(|it| it.rows("DerivedScanExec")).sum();
        assert_eq!(read, len - 1, "len={len}: every fact read exactly once");
    }
    Ok(())
}

/// R2 correctness: an improved row is in the delta, so a value that gets
/// shorter is propagated past the node it improved.
///
/// `s -> x` costs 10; `s -> a -> b -> x` costs 3, found two iterations later.
/// `y` behind `x` must end at 4, not 11.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_best_by_propagates_an_improvement() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (s:E {uid: 's', seed: true}), (a:E {uid: 'a', seed: false}), \
         (b:E {uid: 'b', seed: false}), (x:E {uid: 'x', seed: false}), \
         (y:E {uid: 'y', seed: false}), \
         (s)-[:T {w: 10.0}]->(x), (s)-[:T {w: 1.0}]->(a), (a)-[:T {w: 1.0}]->(b), \
         (b)-[:T {w: 1.0}]->(x), (x)-[:T {w: 1.0}]->(y)",
    )
    .await?;
    tx.commit().await?;
    let (result, _) = profile(&db, BEST_BY).await?;
    assert_eq!(
        uid_numbers(&result, "c"),
        vec![
            ("a".to_string(), 1.0),
            ("b".to_string(), 2.0),
            ("x".to_string(), 3.0),
            ("y".to_string(), 4.0),
        ]
    );
    Ok(())
}

const EVEN_ODD: &str = "\
CREATE RULE ev AS MATCH (e:E) WHERE e.seed = true YIELD KEY e \n\
CREATE RULE od AS MATCH (o:E)-[:T]->(e:E) WHERE o IS ev YIELD KEY e \n\
CREATE RULE ev AS MATCH (o:E)-[:T]->(e:E) WHERE o IS od YIELD KEY e \n\
QUERY ev RETURN e.uid AS uid";

/// R3 mutual: a reference to the other rule of a mutually recursive stratum
/// reads that rule's latest delta, so every fact is read exactly once.
///
/// Before the fix each cross-reference read the other rule's whole relation
/// every iteration: `DerivedScanExec` summed to 55 + 65 / 820 + 860 at chain
/// length 20 / 80, against 20 / 80.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_mutual_cross_refs_read_deltas() -> Result<()> {
    for len in [20usize, 80] {
        let db = chain_with_noise(len, 400).await?;
        let (result, rules) = profile(&db, EVEN_ODD).await?;
        assert_eq!(
            strings(&result, "uid"),
            chain_uids(len, |i| i % 2 == 0),
            "len={len}: even chain positions"
        );
        let facts: usize = rules
            .values()
            .flat_map(|its| its.iter().map(|it| it.delta))
            .sum();
        assert_eq!(facts, len, "len={len}: one fact per chain node");
        let read: usize = rules
            .values()
            .flat_map(|its| its.iter().map(|it| it.rows("DerivedScanExec")))
            .sum();
        assert_eq!(
            read, facts,
            "len={len}: each fact of one rule read once by the other"
        );
    }
    Ok(())
}

const NONLINEAR_TC: &str = "\
CREATE RULE tc AS MATCH (a:E)-[:T]->(b:E) WHERE a.uid STARTS WITH 'c' YIELD KEY a, KEY b \n\
CREATE RULE tc AS MATCH (a:E) WHERE a IS tc TO m, m IS tc TO b YIELD KEY a, KEY b \n\
QUERY tc WHERE a.uid = 'c0' RETURN b.uid AS uid";

/// R3 non-linear: `a IS tc TO m, m IS tc TO b` joins each pair of facts once,
/// in the pass of its newer member, instead of re-joining every pair every
/// iteration.
///
/// The non-linear clause's hash join emits one row per derivation `(a, m, b)`
/// it forms. On a chain of `len` nodes there are C(len, 3) such triples (220
/// at 12, 2,024 at 24); each is formed exactly once over the whole run.
/// Before the fix every iteration re-formed every triple available to it, the
/// converging iteration included.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_nonlinear_joins_each_pair_once() -> Result<()> {
    for len in [12usize, 24] {
        let db = chain_with_noise(len, 400).await?;
        let (result, rules) = profile(&db, NONLINEAR_TC).await?;
        assert_eq!(
            strings(&result, "uid"),
            chain_uids(len, |i| i > 0),
            "len={len}: c0 reaches the whole chain"
        );
        let its = &rules["tc"];
        for it in its {
            eprintln!(
                "len={len} it{} delta={} {:?}",
                it.iteration, it.delta, it.rows
            );
        }
        let facts: usize = its.iter().map(|it| it.delta).sum();
        assert_eq!(
            facts,
            len * (len - 1) / 2,
            "len={len}: every ordered chain pair"
        );
        assert_eq!(
            its.last().map(|it| it.delta),
            Some(0),
            "len={len}: converged"
        );
        // The base clause runs in iteration 0 only and has no hash join; every
        // later `HashJoinExec` row is one `(a, m, b)` the non-linear clause
        // joined.
        let joined: usize = its.iter().map(|it| it.rows("HashJoinExec")).sum();
        let triples = len * (len - 1) * (len - 2) / 6;
        assert_eq!(
            joined, triples,
            "len={len}: each derivation (a, m, b) joined exactly once"
        );
    }
    Ok(())
}

const IS_NOT_BANNED: &str = "\
CREATE RULE banned AS MATCH (e:E) WHERE e.uid STARTS WITH 'n' YIELD KEY e \n\
CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true YIELD KEY b \n\
CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d AND b IS NOT banned YIELD KEY b \n\
QUERY d RETURN b.uid AS uid";

/// R5: the banned-key set of an `IS NOT` against a lower stratum is built once
/// per fixpoint, not once per clause per iteration.
///
/// `LocyIsNotBuild` reports the negated facts each build read. With 202
/// banned facts the sum over the run is 202 at chain length 20 and at 80;
/// before the fix it was 202 times the iteration count.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_is_not_builds_banned_set_once() -> Result<()> {
    for len in [20usize, 80] {
        let db = chain_with_noise(len, 400).await?;
        let (result, rules) = profile(&db, IS_NOT_BANNED).await?;
        assert_eq!(
            strings(&result, "uid"),
            chain_uids(len, |i| i > 0),
            "len={len}: the chain, no noise node"
        );
        // `noise / 2 + 2` noise nodes, every one banned.
        let banned = 202;
        let its = &rules["d"];
        assert!(
            its.len() >= len - 1,
            "len={len}: one iteration per chain node"
        );
        let built: usize = its.iter().map(|it| it.rows("LocyIsNotBuild")).sum();
        assert_eq!(
            built, banned,
            "len={len}: the banned set is built from the negated facts once"
        );
    }
    Ok(())
}

/// BEST BY ranks after `IS NOT`, not before: the clause body used to keep the
/// best row per KEY and the fixpoint then anti-joined it away, so the
/// runner-up that negation keeps was lost. Pre-dates P5 (it reproduces under
/// full feeding too); the P5 oracle families found it.
///
/// Linear: `t` is reached at cost 2 through the banned `x` and at cost 4
/// through `y`, in the same iteration. The answer is 4; it was missing.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_best_by_ranks_after_is_not_linear() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (s:E {uid: 's', seed: true}), (x:E {uid: 'x', seed: false}), \
         (y:E {uid: 'y', seed: false}), (t:E {uid: 't', seed: false}), \
         (s)-[:T {w: 1.0}]->(x), (s)-[:T {w: 2.0}]->(y), \
         (x)-[:T {w: 1.0}]->(t), (y)-[:T {w: 2.0}]->(t)",
    )
    .await?;
    tx.commit().await?;
    let program = "\
CREATE RULE banned AS MATCH (n:E) WHERE n.uid = 'x' YIELD KEY n \n\
CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true ALONG c = r.w BEST BY c ASC YIELD KEY b, c \n\
CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d, o IS NOT banned ALONG c = prev.c + r.w BEST BY c ASC YIELD KEY b, c \n\
QUERY d RETURN b.uid AS uid, c";
    let (result, _) = profile(&db, program).await?;
    assert_eq!(
        uid_numbers(&result, "c"),
        vec![
            ("t".to_string(), 4.0),
            ("x".to_string(), 1.0),
            ("y".to_string(), 2.0),
        ]
    );
    Ok(())
}

/// The non-linear form, as the oracle soak found it: `(0, 0)` costs 6
/// through the excluded node 3 and 8 through node 1. The answer is 8; under
/// full feeding `(0, 0)` was missing, under semi-naive passes it came back as
/// 16 (through node 2, from a later pass).
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_best_by_ranks_after_is_not_nonlinear() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n0:N {id: 0}), (n1:N {id: 1}), (n2:N {id: 2}), (n3:N {id: 3}), \
         (n0)-[:E {w: 3}]->(n1), (n1)-[:E {w: 5}]->(n0), (n0)-[:E {w: 1}]->(n3), \
         (n3)-[:E {w: 5}]->(n0), (n1)-[:E {w: 3}]->(n2), (n2)-[:E {w: 5}]->(n1)",
    )
    .await?;
    tx.commit().await?;
    let program = "\
CREATE RULE xcl AS MATCH (x:N) WHERE x.id >= 3 YIELD KEY x \n\
CREATE RULE p AS MATCH (a:N)-[e:E]->(b:N) BEST BY d ASC YIELD KEY a, KEY b, e.w AS d \n\
CREATE RULE p AS MATCH (a:N) WHERE (a, m, x) IS p, (m, b, y) IS p, m IS NOT xcl BEST BY d ASC YIELD KEY a, KEY b, x + y AS d \n\
QUERY p WHERE a.id = 0 AND b.id = 0 RETURN a.id AS a, d";
    let (result, _) = profile(&db, program).await?;
    let rows = result.rows().expect("QUERY rows");
    assert_eq!(rows.len(), 1, "(0, 0) is derived: {rows:?}");
    let d = match rows[0].get("d") {
        Some(Value::Int(i)) => *i as f64,
        Some(Value::Float(f)) => *f,
        other => panic!("d: {other:?}"),
    };
    assert_eq!(
        d, 8.0,
        "(0, 0) through node 1, the cheapest path negation keeps"
    );
    Ok(())
}

/// Arithmetic in YIELD over integers stays an integer.
///
/// `e.w AS d` over a schemaless integer property yielded `Int`, but `e.w + e.w`
/// and `p + e.w` (with `p` read from a referenced rule's value column) yielded
/// `Float`, so a rule whose base clause yields `e.w` and recursive clause
/// `p + e.w` held both kinds in one column. Found by the P5 oracle families
/// (mutual `BEST BY`); not recursion-specific: every case here is
/// non-recursive.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_yield_arithmetic_keeps_integers() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (n0:Node {id: 0}), (n1:Node {id: 1}), (n2:Node {id: 2}), \
         (n0)-[:EDGE {w: 1}]->(n1), (n1)-[:EDGE {w: 2}]->(n2)",
    )
    .await?;
    tx.commit().await?;
    for (yield_expr, want) in [
        ("e.w + e.w", vec![Value::Int(2), Value::Int(4)]),
        ("p + e.w", vec![Value::Int(3)]),
        ("p * 2 - e.w", vec![Value::Int(3)]),
        ("e.w + 0.5", vec![Value::Float(1.5), Value::Float(2.5)]),
    ] {
        let body = if yield_expr.contains('p') {
            "MATCH (a:Node)-[e:EDGE]->(m:Node) WHERE (m, b, p) IS base"
        } else {
            "MATCH (a:Node)-[e:EDGE]->(b:Node)"
        };
        let program = format!(
            "CREATE RULE base AS MATCH (a:Node)-[e:EDGE]->(b:Node) YIELD KEY a, KEY b, e.w AS d \n\
             CREATE RULE r AS {body} YIELD KEY a, KEY b, {yield_expr} AS d \n\
             QUERY r RETURN a.id AS a, d ORDER BY a"
        );
        let result = db.session().locy(&program).await?;
        let got: Vec<Value> = result
            .rows()
            .expect("QUERY rows")
            .iter()
            .map(|r| r.get("d").cloned().unwrap_or(Value::Null))
            .collect();
        assert_eq!(got, want, "YIELD {yield_expr}");
        let derived: Vec<Value> = result.derived_rules().await.unwrap()["r"]
            .iter()
            .map(|row| row.get("d").cloned().unwrap_or(Value::Null))
            .collect();
        assert!(
            derived
                .iter()
                .all(|v| std::mem::discriminant(v) == std::mem::discriminant(&want[0])),
            "derived YIELD {yield_expr}: {derived:?}"
        );
    }
    Ok(())
}

/// BEST BY over a cyclic graph whose cheapest paths are found late: the
/// incremental merge keeps exactly the shortest distance per node.
///
/// `merge_best_by` keeps the held best row of every KEY in an index and
/// displaces it from its fact batch when a candidate is strictly better,
/// instead of re-sorting all facts per merge. The graph mixes cheap long
/// paths, expensive short-cuts (each node is first reached expensively and
/// then improved, often several times, so rows are displaced from many
/// batches) and back edges (cycles, so dominated re-derivations arrive every
/// iteration). Distances are checked against Dijkstra.
#[tokio::test(flavor = "multi_thread")]
async fn recursion_semi_naive_best_by_matches_dijkstra_with_improvements() -> Result<()> {
    let n = 60usize;
    let mut adj: Vec<Vec<(usize, f64)>> = vec![Vec::new(); n];
    let mut edges: Vec<Value> = Vec::new();
    let mut add = |a: usize, b: usize, w: f64, edges: &mut Vec<Value>| {
        adj[a].push((b, w));
        edges.push(edge(&format!("c{a}"), &format!("c{b}"), w));
    };
    for i in 0..n {
        // Cheap step forward, an expensive jump, and a back edge.
        if i + 1 < n {
            add(i, i + 1, 1.0 + (i % 3) as f64, &mut edges);
        }
        if i + 5 < n {
            add(i, i + 5, 4.0 + (i * 7 % 11) as f64, &mut edges);
        }
        if i >= 3 {
            add(i, i - 3, 0.5 + (i % 4) as f64, &mut edges);
        }
    }
    // Dijkstra from c0 (the seed's out-edges start every path).
    let mut dist = vec![f64::INFINITY; n];
    let mut done = vec![false; n];
    dist[0] = 0.0;
    for _ in 0..n {
        let Some(u) = (0..n)
            .filter(|&v| !done[v] && dist[v].is_finite())
            .min_by(|&a, &b| dist[a].total_cmp(&dist[b]))
        else {
            break;
        };
        done[u] = true;
        for &(v, w) in &adj[u] {
            if dist[u] + w < dist[v] {
                dist[v] = dist[u] + w;
            }
        }
    }

    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL E (uid STRING, seed BOOL)").await?;
    tx.execute("CREATE EDGE TYPE T (w FLOAT) FROM E TO E")
        .await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute_with("UNWIND $rows AS r CREATE (:E {uid: r.uid, seed: r.seed})")
        .param(
            "rows",
            Value::List((0..n).map(|i| vertex(&format!("c{i}"), i == 0)).collect()),
        )
        .run()
        .await?;
    tx.execute_with(
        "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) \
         CREATE (a)-[:T {w: r.w}]->(b)",
    )
    .param("rows", Value::List(edges))
    .run()
    .await?;
    tx.commit().await?;

    let program = "\
CREATE RULE d AS MATCH (a:E)-[r:T]->(b:E) WHERE a.seed = true ALONG c = r.w BEST BY c ASC YIELD KEY b, c \n\
CREATE RULE d AS MATCH (o:E)-[r:T]->(b:E) WHERE o IS d ALONG c = prev.c + r.w BEST BY c ASC YIELD KEY b, c \n\
QUERY d RETURN b.uid AS uid, c";
    let (result, rules) = profile(&db, program).await?;
    // c0 is reached again only through a back edge (c3 -> c0).
    let mut expected: Vec<(String, f64)> = (0..n)
        .filter(|&i| i != 0 || dist[3].is_finite())
        .map(|i| {
            let d = if i == 0 {
                // A path back to the seed: shortest cycle through c0.
                (3..n)
                    .filter(|&u| adj[u].iter().any(|&(v, _)| v == 0))
                    .map(|u| dist[u] + adj[u].iter().find(|&&(v, _)| v == 0).unwrap().1)
                    .fold(f64::INFINITY, f64::min)
            } else {
                dist[i]
            };
            (format!("c{i}"), d)
        })
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    let got = uid_numbers(&result, "c");
    assert_eq!(
        got.len(),
        expected.len(),
        "one fact per reached node: {got:?}"
    );
    for ((gu, gc), (eu, ec)) in got.iter().zip(&expected) {
        assert_eq!(gu, eu);
        assert!(
            (gc - ec).abs() < 1e-9,
            "{gu}: BEST BY kept {gc}, the shortest distance is {ec}"
        );
    }
    // Improvements happened: more delta rows than facts.
    let deltas: usize = rules["d"].iter().map(|it| it.delta).sum();
    assert!(
        deltas > n,
        "the fixture should improve held rows ({deltas} delta rows for {n} facts)"
    );
    Ok(())
}
