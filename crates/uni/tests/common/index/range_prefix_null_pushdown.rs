// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Range, prefix and null predicates on a scalar-indexed property are served
//! by the index, and return exactly what the unpushed plan returns.
//!
//! Perf audit 2026-10-17, row F1: only `=` and `IN` on an Online scalar index
//! reached Lance. `n.x < 10`, `n.x >= 100 AND n.x < 110`,
//! `n.uid STARTS WITH 'u42'` and `n.uid IS NULL` read the whole label
//! (`rows_scanned` 2,000 → 8,000 for a 10-row answer) and filtered it in a
//! `FilterExec`.
//!
//! Two kinds of test:
//!
//! * **Counter guards** — each translated form reads O(answer) rows and
//!   consults the index, at two scales.
//! * **Differentials** — each translated form, over every bound type an
//!   operand can have (int, float, NaN, ±inf, ±0.0, out-of-range, string,
//!   bool, NULL) and over rows holding NULL, NaN, ±inf, ±0.0, extreme ints and
//!   unicode / wildcard / quote strings, returns the same rows as an unpushed
//!   oracle (`WITH n WHERE …`, a filter above a projection that no pushdown
//!   reaches). The oracle's own unpushed-ness is asserted through
//!   `index_scans == 0`. Run flushed, with half the label in L0, with
//!   updates / deletes / NULL flips sitting in L0 over flushed versions, with
//!   those flushed as a second L1 version, inside a transaction, and on a
//!   fork. A pushed predicate that is data-independent has been a live wrong
//!   answer before (Lance reads `"col"` as a string literal); a pushed bound
//!   that is *narrower* than the Cypher predicate loses rows silently, which is
//!   what the boundary rows are for.

use std::collections::HashMap;

use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig, Value};

/// Where the rows live when the query runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Residency {
    /// Every row flushed to L1.
    Flushed,
    /// Ids `0..n/2` flushed, the rest only in L0.
    SplitL0,
    /// Everything flushed, then updated / nulled / deleted in L0.
    MutatedL0,
    /// As `MutatedL0`, then flushed: two versions of a vid sit in L1.
    MutatedFlushed,
}

const ALL_RESIDENCIES: [Residency; 4] = [
    Residency::Flushed,
    Residency::SplitL0,
    Residency::MutatedL0,
    Residency::MutatedFlushed,
];

fn cfg() -> UniConfig {
    // No automatic flush: the test decides what is in L1 and what is in L0.
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

/// `:N {id, x, f, s, t, h, ux, uf, us}`.
///
/// `x` INT, `f` FLOAT, `s` STRING, `t` STRING carry BTree indexes, `h` INT a
/// Hash index; `ux`, `uf`, `us` are unindexed copies of `x`, `f`, `s`.
async fn schema(db: &Uni) {
    db.schema()
        .label("N")
        .property("id", DataType::Int64)
        .property_nullable("x", DataType::Int64)
        .property_nullable("f", DataType::Float64)
        .property_nullable("s", DataType::String)
        .property_nullable("t", DataType::String)
        .property_nullable("h", DataType::Int64)
        .property_nullable("ux", DataType::Int64)
        .property_nullable("uf", DataType::Float64)
        .property_nullable("us", DataType::String)
        .index("x", IndexType::Scalar(ScalarType::BTree))
        .index("f", IndexType::Scalar(ScalarType::BTree))
        .index("s", IndexType::Scalar(ScalarType::BTree))
        .index("t", IndexType::Scalar(ScalarType::BTree))
        .index("h", IndexType::Scalar(ScalarType::Hash))
        .done()
        .apply()
        .await
        .unwrap();
}

/// Strings chosen to break a sloppy prefix-to-range translation: the maximal
/// scalar value U+10FFFF (no successor), the last scalar before the surrogate
/// gap U+D7FF (its successor is U+E000, not U+D800), SQL wildcards, a quote,
/// multi-byte characters, the empty string, case, and a datetime-shaped
/// string that a datetime normalizer would rewrite.
const SPECIAL_STRINGS: [&str; 22] = [
    "",
    "a",
    "a\u{10FFFF}",
    "a\u{10FFFF}b",
    "a\u{10FFFF}\u{10FFFF}",
    "b",
    "\u{10FFFF}",
    "\u{10FFFF}\u{10FFFF}",
    "u\u{D7FF}",
    "u\u{D7FF}x",
    "u\u{E000}",
    "a%b",
    "a_b",
    "axb",
    "it's",
    "it'",
    "é",
    "éa",
    "e",
    "U42",
    "2021-06-01T00:00Z",
    "2021-06-01T00:00:00Z",
];

/// Floats that sit on the edges of IEEE comparison: NaN, ±inf, both zeros,
/// the first float above 2^53, and values one ulp-ish around an int bound.
fn special_floats() -> Vec<f64> {
    vec![
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        9_007_199_254_740_994.0,
        10.0,
        9.999_999_999_999_998,
        10.000_000_000_000_002,
        -2.5,
        f64::MAX,
        f64::MIN_POSITIVE,
    ]
}

fn special_ints() -> Vec<i64> {
    vec![
        i64::MAX,
        i64::MIN,
        9_007_199_254_740_993,
        -9_007_199_254_740_993,
        0,
        10,
        9,
        11,
    ]
}

/// One row as a property map; `None` leaves the property NULL.
fn row(
    id: i64,
    x: Option<i64>,
    f: Option<f64>,
    s: Option<String>,
    t: Option<String>,
    h: Option<i64>,
) -> Value {
    let mut m = HashMap::new();
    m.insert("id".to_string(), Value::Int(id));
    let opt_i = |v: Option<i64>| v.map_or(Value::Null, Value::Int);
    let opt_f = |v: Option<f64>| v.map_or(Value::Null, Value::Float);
    let opt_s = |v: Option<String>| v.map_or(Value::Null, Value::String);
    m.insert("x".into(), opt_i(x));
    m.insert("ux".into(), opt_i(x));
    m.insert("f".into(), opt_f(f));
    m.insert("uf".into(), opt_f(f));
    m.insert("s".into(), opt_s(s.clone()));
    m.insert("us".into(), opt_s(s));
    m.insert("t".into(), opt_s(t));
    m.insert("h".into(), opt_i(h));
    Value::Map(m)
}

/// The regular rows `from..to`: `x = i - 50` (NULL every 13th), `f = x + 0.5`,
/// `s = 'u{i}'` (NULL every 11th), `t`/`h` NULL only for `i < 5`.
fn regular_rows(from: i64, to: i64) -> Vec<Value> {
    (from..to)
        .map(|i| {
            let x = (i % 13 != 5).then_some(i - 50);
            let s = (i % 11 != 3).then(|| format!("u{i}"));
            let t = (i >= 5).then(|| format!("t{i}"));
            let h = (i >= 5).then_some(i);
            row(i, x, x.map(|v| v as f64 + 0.5), s, t, h)
        })
        .collect()
}

/// Boundary rows, ids from `base` upward.
fn special_rows(base: i64) -> Vec<Value> {
    let mut out = Vec::new();
    let mut id = base;
    for s in SPECIAL_STRINGS {
        out.push(row(
            id,
            None,
            None,
            Some(s.to_string()),
            Some(s.to_string()),
            None,
        ));
        id += 1;
    }
    for f in special_floats() {
        out.push(row(id, None, Some(f), None, None, None));
        id += 1;
    }
    for x in special_ints() {
        out.push(row(id, Some(x), None, None, None, Some(x)));
        id += 1;
    }
    out
}

const SPECIAL_BASE: i64 = 1_000_000;

async fn insert_rows(db: &Uni, rows: Vec<Value>) {
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.query_with(
        "UNWIND $rows AS r \
         CREATE (:N {id: r.id, x: r.x, f: r.f, s: r.s, t: r.t, h: r.h, \
                     ux: r.ux, uf: r.uf, us: r.us})",
    )
    .param("rows", Value::List(rows))
    .fetch_all()
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// The L0 mutations of [`Residency::MutatedL0`]: values moved across every
/// bound the differential uses, set to NULL, set from NULL, and deleted.
const MUTATIONS: [&str; 5] = [
    "MATCH (n:N) WHERE n.id % 7 = 0 AND (n.x IS NULL OR abs(n.x) < 1000000000) \
     SET n.x = n.x + 1000, n.ux = n.ux + 1000, n.f = -n.f, n.uf = -n.uf, \
         n.s = 'z' + n.s, n.us = 'z' + n.us, n.t = 'a' + n.t, n.h = n.h + 1000",
    "MATCH (n:N) WHERE n.id % 7 = 1 \
     SET n.x = null, n.ux = null, n.f = null, n.uf = null, n.s = null, n.us = null, \
         n.t = null, n.h = null",
    "MATCH (n:N) WHERE n.id % 13 = 5 SET n.x = 7, n.ux = 7, n.f = 7.5, n.uf = 7.5",
    "MATCH (n:N) WHERE n.id % 7 = 2 DETACH DELETE n",
    "MATCH (n:N) WHERE n.id = 1000003 SET n.s = 'u42x', n.us = 'u42x'",
];

async fn mutate(db: &Uni) {
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for m in MUTATIONS {
        tx.execute(m).await.unwrap();
    }
    tx.commit().await.unwrap();
}

async fn fixture(n: i64, residency: Residency) -> Uni {
    let db = Uni::temporary().config(cfg()).build().await.unwrap();
    schema(&db).await;
    let mut first = regular_rows(0, n);
    first.extend(special_rows(SPECIAL_BASE));
    match residency {
        Residency::SplitL0 => {
            let split = n / 2;
            let (flushed, later): (Vec<_>, Vec<_>) = first.into_iter().partition(|r| {
                let Value::Map(m) = r else { unreachable!() };
                matches!(m.get("id"), Some(Value::Int(i)) if *i < split || *i >= SPECIAL_BASE + 11)
            });
            insert_rows(&db, flushed).await;
            db.flush().await.unwrap();
            insert_rows(&db, later).await;
        }
        _ => {
            insert_rows(&db, first).await;
            db.flush().await.unwrap();
        }
    }
    match residency {
        Residency::MutatedL0 => mutate(&db).await,
        Residency::MutatedFlushed => {
            mutate(&db).await;
            db.flush().await.unwrap();
        }
        _ => {}
    }
    db
}

/// Rows (ids, ascending), `rows_scanned`, `index_scans`, or the error text.
type Outcome = Result<(Vec<i64>, u64, u64), String>;

fn ids_of(rows: &[uni_db::Row]) -> Vec<i64> {
    rows.iter()
        .map(|r| match r.values().first() {
            Some(Value::Int(i)) => *i,
            other => panic!("unexpected id cell {other:?}"),
        })
        .collect()
}

async fn run(db: &Uni, q: &str, params: &[(&str, Value)]) -> Outcome {
    let session = db.session();
    let mut b = session.query_with(q);
    for (k, v) in params {
        b = b.param(*k, v.clone());
    }
    match b.fetch_all().await {
        Ok(r) => {
            let m = r.metrics();
            Ok((
                ids_of(r.rows()),
                m.rows_scanned as u64,
                m.index_scans as u64,
            ))
        }
        Err(e) => Err(e.to_string()),
    }
}

/// One predicate to check, with its parameters.
struct Case {
    pred: String,
    params: Vec<(&'static str, Value)>,
}

fn case(pred: impl Into<String>, params: Vec<(&'static str, Value)>) -> Case {
    Case {
        pred: pred.into(),
        params,
    }
}

/// Bound operands for an INT column: exact ints, floats needing floor/ceil,
/// non-finite floats, out-of-range magnitudes, and type mismatches.
fn int_bounds() -> Vec<Value> {
    vec![
        Value::Int(10),
        Value::Int(-3),
        Value::Int(0),
        Value::Int(i64::MAX),
        Value::Int(i64::MIN),
        Value::Int(9_007_199_254_740_993),
        Value::Float(9.5),
        Value::Float(10.0),
        Value::Float(-2.5),
        Value::Float(-0.0),
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(1e19),
        Value::Float(-1e19),
        Value::Float(9_007_199_254_740_994.0),
        Value::String("10".into()),
        Value::Bool(true),
        Value::Null,
    ]
}

fn float_bounds() -> Vec<Value> {
    vec![
        Value::Float(10.5),
        Value::Float(10.0),
        Value::Float(0.0),
        Value::Float(-0.0),
        Value::Float(-2.5),
        Value::Float(f64::NAN),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(f64::MAX),
        Value::Float(f64::MIN_POSITIVE),
        Value::Float(9.999_999_999_999_998),
        Value::Int(10),
        Value::Int(0),
        Value::Int(9_007_199_254_740_993),
        Value::Int(i64::MAX),
        Value::String("a".into()),
        Value::Null,
    ]
}

fn string_bounds() -> Vec<Value> {
    let mut v: Vec<Value> = SPECIAL_STRINGS
        .iter()
        .map(|s| Value::String((*s).to_string()))
        .collect();
    v.extend([
        Value::String("u4".into()),
        Value::String("u42".into()),
        Value::Int(5),
        Value::Float(1.5),
        Value::Null,
    ]);
    v
}

const RANGE_OPS: [&str; 4] = ["<", "<=", ">", ">="];

/// Every translated form, plus the shapes around them that must stay exact.
fn cases() -> Vec<Case> {
    let mut out = Vec::new();
    let cols: [(&str, fn() -> Vec<Value>); 7] = [
        ("x", int_bounds),
        ("h", int_bounds),
        ("ux", int_bounds),
        ("f", float_bounds),
        ("uf", float_bounds),
        ("s", string_bounds),
        ("us", string_bounds),
    ];
    for (col, bounds) in cols {
        for v in bounds() {
            for op in RANGE_OPS {
                out.push(case(format!("n.{col} {op} $p"), vec![("p", v.clone())]));
            }
            // Equality rides the same operands: the type-exactness gate for
            // point lookups must hold for the new operand kinds too.
            out.push(case(format!("n.{col} = $p"), vec![("p", v.clone())]));
        }
    }
    // Literal spellings (the planner sees a literal, not a parameter).
    for pred in [
        "n.x < 10",
        "n.x <= 10",
        "n.x > 9.5",
        "n.x >= -2.5",
        "n.x < 9.5",
        "n.x <= -0.5",
        "n.f < 10",
        "n.f >= 0",
        "n.f > -0.0",
        "n.f <= 0.0",
        "n.s < 'u2'",
        "n.s >= 'it''s'",
        "n.s > ''",
        "n.s = '2021-06-01T00:00Z'",
        "n.t = '2021-06-01T00:00Z'",
        "n.t >= '2021-06-01T00:00Z' AND n.t < '2021-06-02'",
        "n.x < 'a'",
        "n.s < 5",
        "n.x < null",
        "n.x < true",
        // BETWEEN-style conjunctions.
        "n.x >= 10 AND n.x < 20",
        "n.x > 20 AND n.x < 10",
        "n.x >= 10 AND n.x <= 10",
        "n.x >= 10.5 AND n.x <= 12.5",
        "n.x > 5 AND n.x > 7",
        "10 <= n.x AND n.x < 20",
        "n.h >= 10 AND n.h < 20",
        "n.f >= -1.0 AND n.f < 3.0",
        "n.s >= 'u1' AND n.s < 'u2'",
        "n.x < 10 AND n.ux > 3",
        "n.x < 10 AND n.s STARTS WITH 'u'",
        "n.x IN [1, 2, 50] AND n.x < 2",
        "n.x = 5 AND n.x >= 2 AND n.x <= 9",
        // Shapes that are not a single translated conjunct.
        "n.x < 10 OR n.x > 100",
        "NOT n.x < 10",
        "NOT (n.s STARTS WITH 'u4')",
        "n.x < n.h",
        // Null tests.
        "n.x IS NULL",
        "n.x IS NOT NULL",
        "n.f IS NULL",
        "n.s IS NULL",
        "n.s IS NOT NULL",
        "n.t IS NULL",
        "n.t IS NOT NULL",
        "n.h IS NULL",
        "n.h IS NOT NULL",
        "n.ux IS NULL",
        "n.t IS NULL AND n.x < 0",
    ] {
        out.push(case(pred, vec![]));
    }
    // Prefixes, as literals and parameters.
    let mut prefixes: Vec<String> = SPECIAL_STRINGS.iter().map(|s| (*s).to_string()).collect();
    prefixes.extend(["u4", "u42", "u", "z", "t1", "\u{10FFFF}a"].map(String::from));
    for p in prefixes {
        for col in ["s", "t", "us"] {
            let lit = p.replace('\'', "''");
            out.push(case(format!("n.{col} STARTS WITH '{lit}'"), vec![]));
            out.push(case(
                format!("n.{col} STARTS WITH $p"),
                vec![("p", Value::String(p.clone()))],
            ));
        }
    }
    for v in [Value::Int(4), Value::Null] {
        out.push(case("n.s STARTS WITH $p", vec![("p", v)]));
    }
    out
}

/// Whether the predicate touches an indexed column, so the oracle's
/// `index_scans == 0` is a meaningful check.
fn touches_index(pred: &str) -> bool {
    ["n.x", "n.f", "n.s", "n.t", "n.h"]
        .iter()
        .any(|c| pred.contains(&format!("{c} ")) || pred.contains(&format!("{c})")))
}

/// What a differential run covered.
struct Coverage {
    checked: usize,
    /// Cases whose oracle returned at least one row.
    non_empty: usize,
    /// Cases whose predicate names an indexed column.
    on_index: usize,
    /// Cases whose pushed arm consulted an index.
    index_served: usize,
}

/// Pushed vs unpushed over `cases`, against one query surface.
async fn differential<F, Fut>(ctx: &str, cases: &[Case], mut exec: F) -> Coverage
where
    F: FnMut(String, Vec<(&'static str, Value)>) -> Fut,
    Fut: std::future::Future<Output = Outcome>,
{
    let ret = "RETURN n.id AS id ORDER BY id";
    let mut non_empty = 0usize;
    let mut index_served = 0usize;
    let mut on_index = 0usize;
    // Every disagreement is collected before failing, so one run shows the
    // whole set of broken forms rather than the first.
    let mut mismatches: Vec<String> = Vec::new();
    for c in cases {
        on_index += usize::from(touches_index(&c.pred));
        let pushed = format!("MATCH (n:N) WHERE {} {ret}", c.pred);
        let oracle = format!("MATCH (n:N) WITH n WHERE {} {ret}", c.pred);
        let want = exec(oracle, c.params.clone()).await;
        let got = exec(pushed, c.params.clone()).await;
        let label = format!("{ctx}: `{}` {:?}", c.pred, c.params);
        match (&want, &got) {
            (Ok((w, _, oracle_index_scans)), Ok((g, _, pushed_index_scans))) => {
                index_served += usize::from(*pushed_index_scans > 0);
                if touches_index(&c.pred) && *oracle_index_scans != 0 {
                    mismatches.push(format!(
                        "{label}: the oracle consulted an index, so it is not unpushed"
                    ));
                }
                if g != w {
                    mismatches.push(format!("{label}: pushed {g:?} vs unpushed oracle {w:?}"));
                }
                non_empty += usize::from(!w.is_empty());
            }
            (Err(_), Err(_)) => {}
            _ => mismatches.push(format!("{label}: pushed {got:?} vs oracle {want:?}")),
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} of {} cases disagree:\n{}",
        mismatches.len(),
        cases.len(),
        mismatches.join("\n")
    );
    Coverage {
        checked: cases.len(),
        non_empty,
        on_index,
        index_served,
    }
}

/// Every translated form agrees with the unpushed oracle in every residency.
#[tokio::test]
async fn translated_forms_match_the_unpushed_oracle() {
    let cases = cases();
    for residency in ALL_RESIDENCIES {
        let db = fixture(300, residency).await;
        let cov = differential(&format!("{residency:?}"), &cases, |q, p| {
            let db = &db;
            async move { run(db, &q, &p).await }
        })
        .await;
        eprintln!(
            "{residency:?}: checked {}, non-empty {}, on an index {}, index-served {}",
            cov.checked, cov.non_empty, cov.on_index, cov.index_served
        );
        // Anti-vacuity: the grid must exercise matches, not only empty sets,
        // and the pushed arm must actually reach the index for most cases on
        // an indexed column, or the comparison is between two unpushed plans.
        // (NULL, NaN, infinite, zero-float, out-of-range and type-mismatched
        // operands, Hash ranges, and the empty prefix are not pushed by
        // design, so not every such case can be served.)
        assert!(
            cov.non_empty > cov.checked / 3,
            "{residency:?}: only {}/{} cases returned rows",
            cov.non_empty,
            cov.checked
        );
        // Measured: 296 of 571 flushed, 284 of 571 with half the label in L0.
        assert!(
            cov.index_served * 5 > cov.on_index * 2,
            "{residency:?}: only {}/{} pushed cases on an indexed column consulted an index",
            cov.index_served,
            cov.on_index
        );
    }
}

/// Inside a transaction, with uncommitted writes that cross every bound.
#[tokio::test]
async fn translated_forms_match_the_unpushed_oracle_in_a_transaction() {
    let cases = cases();
    let db = fixture(300, Residency::Flushed).await;
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for m in MUTATIONS {
        tx.execute(m).await.unwrap();
    }
    tx.query_with(
        "UNWIND range(5000, 5040) AS i \
         CREATE (:N {id: i, x: i % 20, f: toFloat(i % 20) - 0.5, s: 'u4' + toString(i), \
                     t: 't' + toString(i), h: i % 20})",
    )
    .fetch_all()
    .await
    .unwrap();
    let txr = &tx;
    let cov = differential("tx", &cases, |q, p| async move {
        let mut b = txr.query_with(&q);
        for (k, v) in &p {
            b = b.param(*k, v.clone());
        }
        match b.fetch_all().await {
            Ok(r) => {
                let m = r.metrics();
                Ok((
                    ids_of(r.rows()),
                    m.rows_scanned as u64,
                    m.index_scans as u64,
                ))
            }
            Err(e) => Err(e.to_string()),
        }
    })
    .await;
    assert!(
        cov.non_empty > cov.checked / 3,
        "tx: {}/{}",
        cov.non_empty,
        cov.checked
    );
    tx.rollback();
}

/// On a fork, with fork-local writes both flushed into the fork's branch and
/// left in the fork's L0.
#[tokio::test]
async fn translated_forms_match_the_unpushed_oracle_on_a_fork() {
    let cases = cases();
    let db = fixture(300, Residency::Flushed).await;
    let primary = db.session();
    let fork = primary.fork("f1_range").await.unwrap();
    let tx = fork.tx().await.unwrap();
    for m in &MUTATIONS[..2] {
        tx.execute(m).await.unwrap();
    }
    tx.commit().await.unwrap();
    fork.flush().await.unwrap();
    let tx = fork.tx().await.unwrap();
    for m in &MUTATIONS[2..] {
        tx.execute(m).await.unwrap();
    }
    tx.commit().await.unwrap();
    let fork = &fork;
    let cov = differential("fork", &cases, |q, p| async move {
        let mut b = fork.query_with(&q);
        for (k, v) in &p {
            b = b.param(*k, v.clone());
        }
        match b.fetch_all().await {
            Ok(r) => {
                let m = r.metrics();
                Ok((
                    ids_of(r.rows()),
                    m.rows_scanned as u64,
                    m.index_scans as u64,
                ))
            }
            Err(e) => Err(e.to_string()),
        }
    })
    .await;
    assert!(
        cov.non_empty > cov.checked / 3,
        "fork: {}/{}",
        cov.non_empty,
        cov.checked
    );
}

/// Non-selective forms over a label with superseded flushed versions.
///
/// A pushed predicate hides a vid's current row when that row no longer
/// matches, so the scan re-reads `_vid`/`_version` for the vids it returned
/// and drops stale rows. With more than 1,000 such vids filling their vid span
/// the re-read is one `_vid` range instead of chunked `IN` lists (a cost
/// change; see the comment in `drop_superseded_pushdown_rows`).
/// This runs forms that return thousands of vids after updates, NULL flips and
/// deletes were flushed as second versions, so that path decides the answer.
#[tokio::test]
async fn non_selective_forms_drop_superseded_versions() {
    let cases: Vec<Case> = [
        "n.x > -1000",
        "n.x IS NOT NULL",
        "n.s >= 'u'",
        "n.f > -100.5",
        "n.t IS NOT NULL",
        "n.x >= -50 AND n.x < 1000000",
    ]
    .into_iter()
    .map(|p| case(p, vec![]))
    .collect();
    let db = fixture(3000, Residency::MutatedFlushed).await;
    let cov = differential("dense", &cases, |q, p| {
        let db = &db;
        async move { run(db, &q, &p).await }
    })
    .await;
    assert_eq!(cov.non_empty, cases.len(), "every form must return rows");
    // Every form must be index-served and return enough vids to take the
    // dense path.
    for c in &cases {
        let q = format!("MATCH (n:N) WHERE {} RETURN n.id AS id", c.pred);
        let (rows, _, index_scans) = run(&db, &q, &[]).await.unwrap();
        assert!(index_scans > 0, "`{}` consulted no index", c.pred);
        assert!(
            rows.len() > 1000,
            "`{}` returned only {} rows",
            c.pred,
            rows.len()
        );
    }
}

/// The forms the counter guard measures. Each answer is O(1) or O(answer) in
/// the label size, so a full scan is visible as a gap at 8,000 rows.
const GUARDS: [&str; 9] = [
    "n.s STARTS WITH $p",
    // A negative float literal is a literal, not `-(10.5)`.
    "n.f >= -10.5 AND n.f < 0.5",
    "n.x < 10",
    "n.x >= 100 AND n.x < 110",
    "n.f > 100.0 AND n.f <= 110.0",
    "n.s STARTS WITH 'u42'",
    "n.s >= 'u42' AND n.s < 'u43'",
    "n.t IS NULL",
    "n.h IS NULL",
];

/// Each translated form reads O(answer) rows and consults the index, at two
/// scales, with every row flushed. The expected rows come from the unpushed
/// oracle, so the guard measures cost without restating the data.
#[tokio::test]
async fn translated_forms_are_served_by_the_index_at_two_scales() {
    for n in [2000_i64, 8000] {
        let db = fixture(n, Residency::Flushed).await;
        for pred in GUARDS {
            let ret = "RETURN n.id AS id ORDER BY id";
            let q = format!("MATCH (n:N) WHERE {pred} {ret}");
            let oracle = format!("MATCH (n:N) WITH n WHERE {pred} {ret}");
            // Only the parameterized prefix form reads `$p`.
            let params = [("p", Value::String("u42".into()))];
            let (want, oracle_scanned, _) = run(&db, &oracle, &params).await.unwrap();
            let (rows, scanned, index_scans) = run(&db, &q, &params).await.unwrap();
            assert_eq!(rows, want, "N={n} `{pred}`");
            assert!(
                want.len() < 200 && !want.is_empty(),
                "N={n} `{pred}`: answer of {} rows is not selective enough to guard",
                want.len()
            );
            assert!(
                oracle_scanned >= n as u64,
                "N={n} `{pred}`: the oracle read only {oracle_scanned} rows"
            );
            assert!(
                index_scans >= 1,
                "N={n} `{pred}` consulted no index (index_scans={index_scans}) (F1)"
            );
            assert!(
                scanned as usize <= want.len() + 2,
                "N={n} `{pred}` scanned {scanned} rows for a {}-row answer; the index \
                 should have bounded the scan (F1)",
                want.len()
            );
        }
    }
}

/// Indexed range forms over unflushed rows read the matching L0 rows, not the
/// label's whole L0 set, at two scales: all rows in L0, and half flushed.
///
/// The L0 half of a scan builds every L0 row of the label unless the scan can
/// tell which rows a pushed predicate rules out (W3b did this for equality).
/// Ranges are recovered the same way; `STARTS WITH` and null tests are not
/// (their runtime filter is not a column-versus-literal comparison), so they
/// are not guarded here.
#[tokio::test]
async fn translated_ranges_prune_unflushed_rows_at_two_scales() {
    const L0_GUARDS: [&str; 4] = [
        "n.x < 10",
        "n.x >= 100 AND n.x < 110",
        "n.f > 100.0 AND n.f <= 110.0",
        "n.s >= 'u42' AND n.s < 'u43'",
    ];
    for n in [2000_i64, 8000] {
        for flushed_half in [false, true] {
            let db = Uni::temporary().config(cfg()).build().await.unwrap();
            schema(&db).await;
            if flushed_half {
                insert_rows(&db, regular_rows(0, n / 2)).await;
                db.flush().await.unwrap();
                insert_rows(&db, regular_rows(n / 2, n)).await;
            } else {
                insert_rows(&db, regular_rows(0, n)).await;
            }
            for pred in L0_GUARDS {
                let ret = "RETURN n.id AS id ORDER BY id";
                let q = format!("MATCH (n:N) WHERE {pred} {ret}");
                let oracle = format!("MATCH (n:N) WITH n WHERE {pred} {ret}");
                let (want, _, _) = run(&db, &oracle, &[]).await.unwrap();
                let (rows, scanned, _) = run(&db, &q, &[]).await.unwrap();
                let ctx = format!("N={n} flushed_half={flushed_half} `{pred}`");
                assert_eq!(rows, want, "{ctx}");
                assert!(
                    !want.is_empty() && want.len() < 200,
                    "{ctx}: {} rows",
                    want.len()
                );
                assert!(
                    scanned as usize <= want.len() + 2,
                    "{ctx}: scanned {scanned} rows for a {}-row answer; the L0 side built \
                     rows the range rules out (F1)",
                    want.len()
                );
            }
        }
    }
}

/// Wall-time and counter probe for the translated forms; asserts nothing.
///
/// Prints, per scale, the pushed form and the unpushed oracle side by side,
/// including non-selective indexed forms (where the pushed scan returns most
/// of the label and pays the superseded-version check per returned vid) and
/// unindexed forms (which are not pushed). Run with
/// `cargo nextest run -p uni-db --test integration --run-ignored all
///  -E 'test(f1_pushdown_probe)' --no-capture`; `F1_PROBE_SCALES=2000,8000`
/// overrides the scales.
#[tokio::test]
#[ignore = "measurement probe, not a guard"]
async fn f1_pushdown_probe() {
    let forms = [
        "n.x < 10",
        "n.x >= 100 AND n.x < 110",
        "n.s STARTS WITH 'u42'",
        "n.t IS NULL",
        "n.x > -1000",
        "n.x IS NOT NULL",
        "n.s >= 'u'",
        "n.ux < 10",
        "n.us STARTS WITH 'u42'",
        "n.ux IS NULL",
    ];
    let scales: Vec<i64> = std::env::var("F1_PROBE_SCALES")
        .ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![2000, 8000, 32000]);
    for n in scales {
        let db = fixture(n, Residency::Flushed).await;
        for pred in forms {
            for (arm, q) in [
                (
                    "pushed",
                    format!("MATCH (n:N) WHERE {pred} RETURN count(*) AS c"),
                ),
                (
                    "oracle",
                    format!("MATCH (n:N) WITH n WHERE {pred} RETURN count(*) AS c"),
                ),
            ] {
                // Warm once, then take the best of five.
                let _ = run_count(&db, &q).await;
                let mut best = f64::MAX;
                let mut last = (0, 0, 0);
                for _ in 0..5 {
                    let t = std::time::Instant::now();
                    last = run_count(&db, &q).await;
                    best = best.min(t.elapsed().as_secs_f64() * 1000.0);
                }
                eprintln!(
                    "F1PROBE|N={n}|{pred}|{arm}|count={}|rows_scanned={}|index_scans={}|best_ms={best:.2}",
                    last.0, last.1, last.2
                );
            }
        }
    }
}

async fn run_count(db: &Uni, q: &str) -> (i64, u64, u64) {
    let r = db.session().query(q).await.unwrap();
    let m = r.metrics();
    let c = match r.rows()[0].values().first() {
        Some(Value::Int(c)) => *c,
        other => panic!("count cell {other:?}"),
    };
    (c, m.rows_scanned as u64, m.index_scans as u64)
}
