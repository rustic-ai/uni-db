// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An indexed equality on committed-but-unflushed vertices reads the matching
//! L0 rows, not the label's whole L0 set (W3b).
//!
//! Measured 2026-10-17: `MATCH (a:E {uid: $s})` reported `rows_scanned = V`
//! for `V` unflushed vertices and 1 after a flush. The L0 half of the scan
//! built every L0 row of the label (cloning every property, building every
//! column, sorting for the MVCC dedup) and only then filtered, so a loop of
//! single-row writes cost O(N·V) until the next flush. The scan now recovers
//! the pushed equality and skips L0 rows that cannot match before building
//! them (`columnar_scan::l0_prefilter_candidates`).
//!
//! The trap that fix walks past: an L0 row that **no longer** matches must
//! still reach the MVCC merge when the flushed side returned that vid, or the
//! stale flushed row wins the dedup and passes the filter. The differential
//! below exercises every way a vertex's L0 version can diverge from its
//! flushed one and compares against a formulation no pushdown reaches.

use std::collections::HashMap;

use uni_db::{Uni, UniConfig, Value};

/// No automatic flush: each test decides what sits in L1 and what in L0.
fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

/// `:E {uid, ix, grp}` with `uid` and `ix` indexed; `:G {uid, ix}` unindexed.
async fn schema(db: &Uni) {
    let tx = db.session().tx().await.unwrap();
    for ddl in [
        "CREATE LABEL E (uid STRING, ix INT, grp INT)",
        "CREATE LABEL G (uid STRING, ix INT)",
        "CREATE EDGE TYPE T () FROM E TO E",
        "CREATE INDEX idx_e_uid FOR (e:E) ON (e.uid)",
        "CREATE INDEX idx_e_ix FOR (e:E) ON (e.ix)",
    ] {
        tx.execute(ddl).await.unwrap();
    }
    tx.commit().await.unwrap();
}

/// Commit `:E {uid: 'u{i}', ix: i, grp: i % 10}` for `i` in `from..to`.
async fn insert(db: &Uni, from: i64, to: i64) {
    let tx = db.session().tx().await.unwrap();
    tx.query_with(
        "UNWIND range($from, $to - 1) AS i \
         CREATE (:E {uid: 'u' + toString(i), ix: i, grp: i % 10})",
    )
    .param("from", Value::Int(from))
    .param("to", Value::Int(to))
    .fetch_all()
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// `(rows_scanned, index_scans, row count)` of one lookup by `uid`.
async fn lookup(db: &Uni, uid: &str) -> (u64, u64, usize) {
    let r = db
        .session()
        .query_with("MATCH (a:E {uid: $s}) RETURN a.ix AS ix")
        .param("s", Value::String(uid.into()))
        .fetch_all()
        .await
        .unwrap();
    let m = r.metrics();
    (m.rows_scanned as u64, m.index_scans as u64, r.rows().len())
}

/// An equality lookup over unflushed vertices reads O(1) rows, at two scales,
/// for a key at either end of the label and for a missing key.
#[tokio::test]
async fn unflushed_equality_lookup_reads_only_matching_rows() {
    for v in [2000_i64, 8000] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        schema(&db).await;
        insert(&db, 0, v).await;
        for (uid, want) in [
            ("u0".to_string(), 1),
            (format!("u{}", v - 1), 1),
            ("absent".to_string(), 0),
        ] {
            let (scanned, _, rows) = lookup(&db, &uid).await;
            assert_eq!(rows, want, "V={v} uid={uid}");
            assert!(
                scanned <= 1,
                "V={v} uid={uid}: scanned {scanned} rows of {v} unflushed — the L0 side \
                 built the whole label before filtering (W3b)"
            );
        }
    }
}

/// With part of the label flushed, the lookup reads the indexed L1 row and
/// the matching L0 row, not the L0 population.
#[tokio::test]
async fn split_equality_lookup_reads_only_matching_rows() {
    for v in [2000_i64, 8000] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        schema(&db).await;
        insert(&db, 0, v).await;
        db.flush().await.unwrap();
        insert(&db, v, 2 * v).await;
        for uid in ["u42".to_string(), format!("u{}", v + 42)] {
            let (scanned, index_scans, rows) = lookup(&db, &uid).await;
            assert_eq!(rows, 1, "V={v} uid={uid}");
            assert!(index_scans >= 1, "V={v} uid={uid}: no index consulted");
            assert!(
                scanned <= 2,
                "V={v} uid={uid}: scanned {scanned} rows ({v} unflushed) (W3b)"
            );
        }
    }
}

/// A loop of single-row read-modify-writes costs the same per statement
/// however many unflushed vertices precede it.
///
/// Each statement looks a vertex up by `uid` and writes it, the shape that was
/// O(N·V) until a flush. `rows_scanned` per statement is asserted flat across
/// the loop and across two scales.
#[tokio::test]
async fn loop_of_single_row_writes_has_flat_per_statement_cost() {
    for v in [2000_i64, 8000] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        schema(&db).await;
        insert(&db, 0, v).await;
        let mut worst = 0_u64;
        for i in 0..200_i64 {
            let tx = db.session().tx().await.unwrap();
            let r = tx
                .query_with(
                    "MATCH (a:E {uid: $s}) \
                     CREATE (a)-[:T]->(:E {uid: $n, ix: $i, grp: 0}) \
                     RETURN a.ix AS ix",
                )
                .param("s", Value::String(format!("u{}", (i * 37) % v)))
                .param("n", Value::String(format!("w{i}")))
                .param("i", Value::Int(v + i))
                .fetch_all()
                .await
                .unwrap();
            assert_eq!(r.rows().len(), 1, "V={v} step {i}");
            worst = worst.max(r.metrics().rows_scanned as u64);
            tx.commit().await.unwrap();
        }
        assert!(
            worst <= 2,
            "V={v}: a single-row write scanned {worst} rows; per-statement cost grows \
             with the unflushed population (W3b)"
        );
    }
}

/// Every row of `q` (debug-rendered, sorted), run on `session`.
async fn rows(session: &uni_db::Session, q: &str, p: &Value) -> Vec<String> {
    let r = session
        .query_with(q)
        .param("p", p.clone())
        .fetch_all()
        .await
        .unwrap_or_else(|e| panic!("query failed: {q}: {e}"));
    let mut out: Vec<String> = r
        .rows()
        .iter()
        .map(|row| format!("{:?}", row.values()))
        .collect();
    out.sort();
    out
}

/// Pushed form and an unpushed reference over the same predicate.
///
/// The reference filters above a projection (`WITH a WHERE …`), which no
/// pushdown reaches — its unpushed-ness is asserted in
/// [`shadowing_cases_match_an_unpushed_reference`] via `index_scans == 0`.
fn forms(pred: &str) -> (String, String) {
    let ret = "RETURN a.uid AS uid, a.ix AS ix, labels(a) AS l";
    (
        format!("MATCH (a:E) WHERE {pred} {ret}"),
        format!("MATCH (a:E) WITH a WHERE {pred} {ret}"),
    )
}

/// Predicates covering every value the shadowing mutations write or abandon.
fn probes() -> Vec<(&'static str, Value)> {
    let s = |v: &str| Value::String(v.into());
    vec![
        ("a.uid = $p", s("u1")),
        ("a.uid = $p", s("zz1")),
        ("a.uid = $p", s("u2")),
        ("a.uid = $p", s("u2b")),
        ("a.uid = $p", s("u4")),
        ("a.uid = $p", s("u5")),
        ("a.uid = $p", s("u6")),
        ("a.uid = $p", s("u7")),
        ("a.uid = $p", s("g1")),
        ("a.uid = $p", s("u8")),
        ("a.uid = $p", s("x8")),
        ("a.uid = $p", s("n1")),
        ("a.uid = $p", s("l2")),
        ("a.uid = $p", s("l1")),
        ("a.uid = $p", s("u10")),
        ("a.uid = $p", s("u11")),
        ("a.uid = $p", Value::Null),
        (
            "a.uid IN $p",
            Value::List(vec![s("u1"), s("zz1"), s("u4"), s("n1")]),
        ),
        ("a.uid = $p AND a.grp = 3", s("u4")),
        ("a.ix = $p", Value::Int(10)),
        ("a.ix = $p", Value::Int(1010)),
        ("a.ix = $p", Value::Float(10.0)),
        ("a.ix = $p", Value::Int(11)),
        ("a.ix = $p", Value::Int(600)),
    ]
}

/// Apply the shadowing mutations to the flushed base `u0..u100` and leave them
/// in L0.
///
/// Each one makes a vertex's L0 version disagree with its flushed version:
/// update away from a key (`u1`), update onto a new key (`u2`) and onto an
/// existing one (`u3` → `u4`), delete (`u5`), delete and recreate (`u6`),
/// remove the label (`u7`), gain the label by relabel (`g1`, which has no L0
/// property row), an L0-only vertex rewritten twice (`l1` → `l2`), an L0-only
/// vertex with no `uid`, an integer key moved away and onto (`ix`), and a key
/// removed (`u10`) or set to NULL (`u11`).
async fn mutate(db: &Uni) {
    let s = db.session();
    for q in [
        "MATCH (a:E {uid: 'u1'}) SET a.uid = 'zz1'",
        "MATCH (a:E {uid: 'u2'}) SET a.uid = 'u2b'",
        "MATCH (a:E {uid: 'u3'}) SET a.uid = 'u4'",
        "MATCH (a:E {uid: 'u5'}) DETACH DELETE a",
        "MATCH (a:E {uid: 'u6'}) DETACH DELETE a",
        "CREATE (:E {uid: 'u6', ix: 600, grp: 6})",
        "MATCH (a:E {uid: 'u7'}) REMOVE a:E",
        "MATCH (g:G {uid: 'g1'}) SET g:E",
        "CREATE (:E {uid: 'l1', ix: 2000, grp: 1})",
        "MATCH (a:E {uid: 'l1'}) SET a.uid = 'l2'",
        "CREATE (:E {ix: 2001, grp: 1})",
        "MATCH (a:E {ix: 10}) SET a.ix = 1010",
        "MATCH (a:E {ix: 12}) SET a.ix = 11",
        "MATCH (a:E {uid: 'u10'}) REMOVE a.uid",
        "MATCH (a:E {uid: 'u11'}) SET a.uid = null",
    ] {
        let tx = s.tx().await.unwrap();
        tx.execute(q)
            .await
            .unwrap_or_else(|e| panic!("mutation failed: {q}: {e}"));
        tx.commit().await.unwrap();
    }
}

async fn base(db: &Uni) {
    schema(db).await;
    insert(db, 0, 100).await;
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:G {uid: 'g1', ix: 900})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// Compare pushed and reference forms of every probe on `session`.
async fn assert_probes_agree(session: &uni_db::Session, ctx: &str) {
    for (pred, p) in probes() {
        let (pushed, reference) = forms(pred);
        let got = rows(session, &pushed, &p).await;
        let want = rows(session, &reference, &p).await;
        assert_eq!(got, want, "{ctx}: `{pred}` with $p = {p:?}");
    }
}

/// L0 versions shadow their flushed versions under a pushed equality, in the
/// main L0, a transaction's private L0, and a fork.
#[tokio::test]
async fn shadowing_cases_match_an_unpushed_reference() {
    // Base flushed, mutations in L0: every L1/L0 disagreement.
    let db = Uni::temporary().config(cfg()).build().await.unwrap();
    base(&db).await;
    db.flush().await.unwrap();

    // The reference is genuinely unpushed: on flushed data it consults no
    // index, where the pushed form does.
    let (pushed, reference) = forms("a.uid = $p");
    let p = Value::String("u1".into());
    let pm = db
        .session()
        .query_with(&pushed)
        .param("p", p.clone())
        .fetch_all()
        .await
        .unwrap();
    let rm = db
        .session()
        .query_with(&reference)
        .param("p", p)
        .fetch_all()
        .await
        .unwrap();
    assert!(pm.metrics().index_scans >= 1, "pushed form did not push");
    assert_eq!(rm.metrics().index_scans, 0, "reference form was pushed");

    mutate(&db).await;
    let s = db.session();
    assert_probes_agree(&s, "flushed base + L0 mutations").await;

    // Spot-check the cases whose wrong answer is a stale flushed row.
    let one = |q: &'static str| {
        let s = db.session();
        async move { rows(&s, q, &Value::Null).await }
    };
    assert!(one("MATCH (a:E {uid: 'u1'}) RETURN a.ix").await.is_empty());
    assert!(one("MATCH (a:E {uid: 'u5'}) RETURN a.ix").await.is_empty());
    assert!(one("MATCH (a:E {uid: 'u7'}) RETURN a.ix").await.is_empty());
    assert!(one("MATCH (a:E {ix: 10}) RETURN a.ix").await.is_empty());
    assert_eq!(one("MATCH (a:E {uid: 'u4'}) RETURN a.ix").await.len(), 2);
    assert_eq!(
        one("MATCH (a:E {uid: 'u6'}) RETURN a.ix").await,
        vec!["[Int(600)]".to_string()]
    );
    assert_eq!(one("MATCH (a:E {uid: 'g1'}) RETURN a.ix").await.len(), 1);

    // Uncommitted writes in a transaction's private L0 shadow the committed
    // L0 and the flushed rows, for reads inside that transaction.
    let tx = s.tx().await.unwrap();
    tx.execute("MATCH (a:E {uid: 'u8'}) SET a.uid = 'x8'")
        .await
        .unwrap();
    tx.execute("MATCH (a:E {uid: 'zz1'}) SET a.uid = 'u1'")
        .await
        .unwrap();
    tx.execute("CREATE (:E {uid: 'n1', ix: 3000, grp: 0})")
        .await
        .unwrap();
    tx.execute("MATCH (a:E {uid: 'u2b'}) DETACH DELETE a")
        .await
        .unwrap();
    for (pred, p) in probes() {
        let (pushed, reference) = forms(pred);
        let run = |q: String, p: Value| {
            let tx = &tx;
            async move {
                let r = tx
                    .query_with(&q)
                    .param("p", p)
                    .fetch_all()
                    .await
                    .unwrap_or_else(|e| panic!("query failed: {q}: {e}"));
                let mut out: Vec<String> = r
                    .rows()
                    .iter()
                    .map(|row| format!("{:?}", row.values()))
                    .collect();
                out.sort();
                out
            }
        };
        let got = run(pushed, p.clone()).await;
        let want = run(reference, p.clone()).await;
        assert_eq!(got, want, "in-transaction: `{pred}` with $p = {p:?}");
    }
    tx.rollback();
    // The rolled-back writes are invisible again.
    assert_probes_agree(&s, "after rollback").await;
    assert!(one("MATCH (a:E {uid: 'x8'}) RETURN a.ix").await.is_empty());

    // A fork: its L0 diverges from primary's flushed and L0 rows.
    let fork = s.fork("w3b_shadow").await.unwrap();
    let ftx = fork.tx().await.unwrap();
    ftx.execute("MATCH (a:E {uid: 'u9'}) SET a.uid = 'x8'")
        .await
        .unwrap();
    ftx.execute("MATCH (a:E {uid: 'u4'}) SET a.uid = 'l1'")
        .await
        .unwrap();
    ftx.execute("CREATE (:E {uid: 'n1', ix: 4000, grp: 0})")
        .await
        .unwrap();
    ftx.commit().await.unwrap();
    assert_probes_agree(&fork, "fork L0").await;
    assert_probes_agree(&s, "primary after fork writes").await;
}

/// The same mutations with nothing flushed: every version lives in L0.
#[tokio::test]
async fn all_l0_shadowing_cases_match_an_unpushed_reference() {
    let db = Uni::temporary().config(cfg()).build().await.unwrap();
    base(&db).await;
    mutate(&db).await;
    assert_probes_agree(&db.session(), "all in L0").await;

    // And once flushed, the same answers.
    let before: HashMap<String, Vec<String>> = {
        let s = db.session();
        let mut m = HashMap::new();
        for (pred, p) in probes() {
            let (pushed, _) = forms(pred);
            m.insert(format!("{pred} {p:?}"), rows(&s, &pushed, &p).await);
        }
        m
    };
    db.flush().await.unwrap();
    let s = db.session();
    for (pred, p) in probes() {
        let (pushed, _) = forms(pred);
        assert_eq!(
            rows(&s, &pushed, &p).await,
            before[&format!("{pred} {p:?}")],
            "flush changed `{pred}` with $p = {p:?}"
        );
    }
    assert_probes_agree(&s, "after flush").await;
}

// ---------------------------------------------------------------------------
// Schemaless (main-table) scans: `MATCH (a {uid: $s})` with no label, and a
// label the schema does not declare. Measured 2026-10-18: the label-less
// lookup reported `rows_scanned = V` over `V` unflushed vertices — the L0 side
// built every L0 vertex before the filter above the scan ran. The scan now
// reads an equality out of its Cypher filter and skips L0 rows that cannot
// match (`columnar_scan::schemaless_l0_prefilter_candidates`), keeping every
// vid the flushed side returned so newer L0 versions still shadow it.
// ---------------------------------------------------------------------------

/// `(rows_scanned, row count)` of one main-table lookup by `uid`: label-less,
/// or under the label `E`, which the schema does not declare.
async fn lookup_unlabelled(db: &Uni, uid: &str, labelled: bool) -> (u64, usize) {
    let q = if labelled {
        "MATCH (a:E {uid: $s}) RETURN a.ix AS ix"
    } else {
        "MATCH (a {uid: $s}) RETURN a.ix AS ix"
    };
    let r = db
        .session()
        .query_with(q)
        .param("s", Value::String(uid.into()))
        .fetch_all()
        .await
        .unwrap();
    (r.metrics().rows_scanned as u64, r.rows().len())
}

/// A label-less equality lookup over unflushed vertices reads O(1) rows, at
/// two scales, for a key at either end and for a missing key.
#[tokio::test]
async fn unflushed_unlabelled_lookup_reads_only_matching_rows() {
    for v in [2000_i64, 8000] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        insert(&db, 0, v).await;
        for (uid, want) in [
            ("u0".to_string(), 1),
            (format!("u{}", v - 1), 1),
            ("absent".to_string(), 0),
        ] {
            for labelled in [false, true] {
                let (scanned, rows) = lookup_unlabelled(&db, &uid, labelled).await;
                assert_eq!(rows, want, "V={v} uid={uid} labelled={labelled}");
                assert!(
                    scanned <= 1,
                    "V={v} uid={uid} labelled={labelled}: scanned {scanned} rows of {v} \
                     unflushed — the main-table scan built every L0 vertex before \
                     filtering (W3b)"
                );
            }
        }
    }
}

/// A loop of label-less single-row read-modify-writes costs the same per
/// statement however many unflushed vertices precede it.
#[tokio::test]
async fn unlabelled_loop_of_single_row_writes_has_flat_per_statement_cost() {
    for v in [2000_i64, 8000] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        insert(&db, 0, v).await;
        let mut worst = 0_u64;
        for i in 0..100_i64 {
            let tx = db.session().tx().await.unwrap();
            let r = tx
                .query_with(
                    "MATCH (a {uid: $s}) \
                     CREATE (a)-[:T]->(:E {uid: $n, ix: $i, grp: 0}) \
                     RETURN a.ix AS ix",
                )
                .param("s", Value::String(format!("u{}", (i * 37) % v)))
                .param("n", Value::String(format!("w{i}")))
                .param("i", Value::Int(v + i))
                .fetch_all()
                .await
                .unwrap();
            assert_eq!(r.rows().len(), 1, "V={v} step {i}");
            worst = worst.max(r.metrics().rows_scanned as u64);
            tx.commit().await.unwrap();
        }
        assert!(
            worst <= 2,
            "V={v}: a label-less single-row write scanned {worst} rows; per-statement \
             cost grows with the unflushed population (W3b)"
        );
    }
}

/// Pushed and unpushed forms of `pred` over a main-table scan: label-less, or
/// under the undeclared label `E`.
fn schemaless_forms(pred: &str, labelled: bool) -> (String, String) {
    let pat = if labelled { "(a:E)" } else { "(a)" };
    let ret = "RETURN a.uid AS uid, a.ix AS ix, labels(a) AS l";
    (
        format!("MATCH {pat} WHERE {pred} {ret}"),
        format!("MATCH {pat} WITH a WHERE {pred} {ret}"),
    )
}

/// Compare pushed and reference forms of every probe, both scan shapes, on
/// `session`.
async fn assert_schemaless_probes_agree(session: &uni_db::Session, ctx: &str) {
    for labelled in [false, true] {
        for (pred, p) in probes() {
            let (pushed, reference) = schemaless_forms(pred, labelled);
            let got = rows(session, &pushed, &p).await;
            let want = rows(session, &reference, &p).await;
            assert_eq!(
                got, want,
                "{ctx} (labelled {labelled}): `{pred}` with $p = {p:?}"
            );
        }
    }
}

/// The schemaless base: `u0..u100` and `:G {uid: 'g1'}`, no schema declared.
async fn schemaless_base(db: &Uni) {
    insert(db, 0, 100).await;
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE (:G {uid: 'g1', ix: 900})")
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// L0 versions shadow their flushed versions under a schemaless scan's L0
/// prefilter: update away, update onto, delete, recreate, REMOVE label,
/// relabel, NULL, in a transaction, after rollback, and in a fork.
#[tokio::test]
async fn schemaless_shadowing_cases_match_an_unpushed_reference() {
    let db = Uni::temporary().config(cfg()).build().await.unwrap();
    schemaless_base(&db).await;
    db.flush().await.unwrap();

    // The reference keeps its predicate above the projection: the scan it
    // reads from carries no filter, so no L0 prefilter applies to it, where
    // the pushed form's scan carries the predicate. Only the label-less form
    // is checked: the planner does not push a WHERE into a scan of an
    // undeclared label (`ScanMainByLabels`), so that form exercises the
    // prefilter only through an inline property map.
    let s = db.session();
    {
        let (pushed, reference) = schemaless_forms("a.uid = $p", false);
        let plan = |q: String| {
            let s = &s;
            async move { s.query_with(&q).explain().await.unwrap().plan_text }
        };
        let (pp, rp) = (plan(pushed).await, plan(reference).await);
        assert!(!pp.contains("filter: None"), "pushed form not pushed: {pp}");
        assert!(
            rp.contains("filter: None"),
            "reference form was pushed: {rp}"
        );
    }

    mutate(&db).await;
    assert_schemaless_probes_agree(&s, "flushed base + L0 mutations").await;

    // Spot-check the cases whose wrong answer is a stale flushed row.
    for q in [
        "MATCH (a {uid: 'u1'}) RETURN a.ix",
        "MATCH (a {uid: 'u5'}) RETURN a.ix",
        "MATCH (a:E {uid: 'u7'}) RETURN a.ix",
        "MATCH (a {ix: 10}) RETURN a.ix",
    ] {
        assert!(rows(&s, q, &Value::Null).await.is_empty(), "{q}");
    }
    assert_eq!(
        rows(&s, "MATCH (a {uid: 'u4'}) RETURN a.ix", &Value::Null)
            .await
            .len(),
        2
    );
    assert_eq!(
        rows(&s, "MATCH (a {uid: 'u6'}) RETURN a.ix", &Value::Null).await,
        vec!["[Int(600)]".to_string()]
    );
    assert_eq!(
        rows(&s, "MATCH (a:E {uid: 'g1'}) RETURN a.ix", &Value::Null)
            .await
            .len(),
        1
    );
    // `u7` lost its label but not its key: label-less it is still there, with
    // its L0 label set rather than the flushed one.
    assert_eq!(
        rows(&s, "MATCH (a {uid: 'u7'}) RETURN labels(a)", &Value::Null).await,
        vec!["[List([])]".to_string()]
    );

    // A transaction's private L0.
    let tx = s.tx().await.unwrap();
    for q in [
        "MATCH (a:E {uid: 'u8'}) SET a.uid = 'x8'",
        "MATCH (a:E {uid: 'zz1'}) SET a.uid = 'u1'",
        "CREATE (:E {uid: 'n1', ix: 3000, grp: 0})",
        "MATCH (a:E {uid: 'u2b'}) DETACH DELETE a",
    ] {
        tx.execute(q).await.unwrap();
    }
    for labelled in [false, true] {
        for (pred, p) in probes() {
            let (pushed, reference) = schemaless_forms(pred, labelled);
            let run = |q: String, p: Value| {
                let tx = &tx;
                async move {
                    let r = tx
                        .query_with(&q)
                        .param("p", p)
                        .fetch_all()
                        .await
                        .unwrap_or_else(|e| panic!("query failed: {q}: {e}"));
                    let mut out: Vec<String> = r
                        .rows()
                        .iter()
                        .map(|row| format!("{:?}", row.values()))
                        .collect();
                    out.sort();
                    out
                }
            };
            let got = run(pushed, p.clone()).await;
            let want = run(reference, p.clone()).await;
            assert_eq!(
                got, want,
                "in-transaction (labelled {labelled}): `{pred}` with $p = {p:?}"
            );
        }
    }
    tx.rollback();
    assert_schemaless_probes_agree(&s, "after rollback").await;
    assert!(
        rows(&s, "MATCH (a {uid: 'x8'}) RETURN a.ix", &Value::Null)
            .await
            .is_empty()
    );

    // A fork.
    let fork = s.fork("w3b_schemaless_shadow").await.unwrap();
    let ftx = fork.tx().await.unwrap();
    for q in [
        "MATCH (a:E {uid: 'u9'}) SET a.uid = 'x8'",
        "MATCH (a:E {uid: 'u4'}) SET a.uid = 'l1'",
        "CREATE (:E {uid: 'n1', ix: 4000, grp: 0})",
    ] {
        ftx.execute(q).await.unwrap();
    }
    ftx.commit().await.unwrap();
    assert_schemaless_probes_agree(&fork, "fork L0").await;
    assert_schemaless_probes_agree(&s, "primary after fork writes").await;
}

/// The schemaless mutations with nothing flushed, then flushed.
#[tokio::test]
async fn all_l0_schemaless_shadowing_cases_match_an_unpushed_reference() {
    let db = Uni::temporary().config(cfg()).build().await.unwrap();
    schemaless_base(&db).await;
    mutate(&db).await;
    assert_schemaless_probes_agree(&db.session(), "all in L0").await;
    db.flush().await.unwrap();
    assert_schemaless_probes_agree(&db.session(), "after flush").await;
}

// ── `ext_id` on a labelled scan ───────────────────────────────────────────
//
// Measured 2026-10-18: `MATCH (a:E {ext_id: $s})` over `V` unflushed vertices
// reported `rows_scanned = V` (2000 / 8000) while the flushed lookup read 1.
// `ext_id` is never a declared property, so the equality reached the scan
// only as Lance SQL (A10) with no runtime filter, and the scan had nothing to
// recover an L0 prefilter from. The planner now hands the scan the equality
// directly, and the L0 pass reasons about `ext_id` as the string column every
// per-label table carries.

/// Commit `:E {uid, ext_id: 'x{i}', ix: i, grp: 0}` for `i` in `from..to`.
async fn insert_ext(db: &Uni, from: i64, to: i64) {
    let tx = db.session().tx().await.unwrap();
    tx.query_with(
        "UNWIND range($from, $to - 1) AS i \
         CREATE (:E {uid: 'u' + toString(i), ext_id: 'x' + toString(i), ix: i, grp: 0})",
    )
    .param("from", Value::Int(from))
    .param("to", Value::Int(to))
    .fetch_all()
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// An `ext_id` lookup over unflushed labelled vertices reads O(1) rows, in
/// both spellings, at two scales, for present and absent keys.
#[tokio::test]
async fn unflushed_ext_id_lookup_reads_only_matching_rows() {
    for v in [2000_i64, 8000] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        schema(&db).await;
        insert_ext(&db, 0, v).await;
        for q in [
            "MATCH (a:E {ext_id: $s}) RETURN a.ix AS ix",
            "MATCH (a:E) WHERE a.ext_id = $s RETURN a.ix AS ix",
        ] {
            for (key, want) in [
                ("x0".to_string(), 1),
                (format!("x{}", v - 1), 1),
                ("absent".to_string(), 0),
            ] {
                let r = db
                    .session()
                    .query_with(q)
                    .param("s", Value::String(key.clone()))
                    .fetch_all()
                    .await
                    .unwrap();
                let scanned = r.metrics().rows_scanned;
                assert_eq!(r.rows().len(), want, "V={v} `{q}` {key}");
                assert!(
                    scanned <= 1,
                    "V={v} `{q}` {key}: scanned {scanned} rows of {v} unflushed — the L0 \
                     side of an ext_id lookup built the whole label"
                );
            }
        }
    }
}

/// Every way an L0 version's `ext_id` can diverge from its flushed one gives
/// the same rows through the pushed lookup as through a form no pushdown
/// reaches, flushed and unflushed, in a transaction and in a fork.
#[tokio::test]
async fn ext_id_shadowing_cases_match_an_unpushed_reference() {
    async fn agree(session: &uni_db::Session, ctx: &str) {
        for key in [
            "x1", "x2", "x3", "x4", "x5", "y1", "y2", "n1", "5", "absent",
        ] {
            let p = Value::String(key.into());
            let got = rows(session, "MATCH (a:E {ext_id: $p}) RETURN a.ix", &p).await;
            let alt = rows(session, "MATCH (a:E) WHERE a.ext_id = $p RETURN a.ix", &p).await;
            let want = rows(
                session,
                "MATCH (a:E) WHERE [a.ext_id] = [$p] RETURN a.ix",
                &p,
            )
            .await;
            assert_eq!(got, want, "{ctx}: inline ext_id = {key}");
            assert_eq!(alt, want, "{ctx}: WHERE ext_id = {key}");
        }
    }
    let mutations = [
        // Away from a flushed key, onto a new one, and onto another
        // vertex's freed key.
        "MATCH (a:E {uid: 'u1'}) SET a.ext_id = 'y1'",
        "MATCH (a:E {uid: 'u2'}) SET a.ext_id = 'x1'",
        // Removed, deleted, recreated under a flushed key.
        "MATCH (a:E {uid: 'u3'}) REMOVE a.ext_id",
        "MATCH (a:E {uid: 'u4'}) DETACH DELETE a",
        "CREATE (:E {uid: 'u4b', ext_id: 'x4', ix: 104, grp: 0})",
        // A non-string ext_id, which stays out of the string column.
        "MATCH (a:E {uid: 'u5'}) SET a.ext_id = 5",
        // Label removed and restored.
        "MATCH (a:E {uid: 'u6'}) REMOVE a:E",
        "MATCH (a {uid: 'u6'}) SET a:E",
        "CREATE (:E {uid: 'n', ext_id: 'n1', ix: 200, grp: 0})",
    ];

    for flush_base in [true, false] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        schema(&db).await;
        insert_ext(&db, 0, 10).await;
        if flush_base {
            db.flush().await.unwrap();
        }
        let s = db.session();
        agree(&s, &format!("base (flushed {flush_base})")).await;
        for m in mutations {
            let tx = s.tx().await.unwrap();
            tx.execute(m).await.unwrap();
            tx.commit().await.unwrap();
        }
        agree(&s, &format!("mutated (flushed base {flush_base})")).await;

        // In a transaction, over its private L0.
        let tx = s.tx().await.unwrap();
        tx.execute("MATCH (a:E {uid: 'u7'}) SET a.ext_id = 'y2'")
            .await
            .unwrap();
        for key in ["x7", "y2"] {
            let p = Value::String(key.into());
            let run = |q: &'static str, p: Value| {
                let tx = &tx;
                async move {
                    let mut out: Vec<String> = tx
                        .query_with(q)
                        .param("p", p)
                        .fetch_all()
                        .await
                        .unwrap()
                        .rows()
                        .iter()
                        .map(|row| format!("{:?}", row.values()))
                        .collect();
                    out.sort();
                    out
                }
            };
            let got = run("MATCH (a:E {ext_id: $p}) RETURN a.ix", p.clone()).await;
            let want = run("MATCH (a:E) WHERE [a.ext_id] = [$p] RETURN a.ix", p.clone()).await;
            assert_eq!(got, want, "in-transaction: ext_id = {key}");
        }
        tx.rollback();
        agree(&s, "after rollback").await;

        // A fork.
        let fork = s
            .fork(&format!("ext_id_shadow_{flush_base}"))
            .await
            .unwrap();
        let ftx = fork.tx().await.unwrap();
        ftx.execute("MATCH (a:E {uid: 'u8'}) SET a.ext_id = 'y2'")
            .await
            .unwrap();
        ftx.commit().await.unwrap();
        agree(&fork, "fork L0").await;
        agree(&s, "primary after fork writes").await;

        db.flush().await.unwrap();
        agree(&s, &format!("all flushed (flushed base {flush_base})")).await;
    }
}

/// A `WHERE` equality on a label the schema does not declare reads O(1)
/// unflushed rows, like the inline map.
///
/// Measured 2026-10-18: `MATCH (a:E) WHERE a.uid = $s` read `V` rows (2000 /
/// 8000) where `MATCH (a:E {uid: $s})` read 1. The planner pushed a WHERE
/// conjunct into a declared-label `Scan` or a label-less `ScanAll` but never
/// into the undeclared label's `ScanMainByLabels`, so the conjunct stayed in
/// a filter above the scan and the scan saw no equality to narrow its L0
/// read with. The cross-product and OPTIONAL rows pin the two places the
/// pushdown also had to learn the new scan: choosing the side of a cross
/// product (it would have sent the predicate to the side that does not bind
/// the variable, losing it) and null-extending under OPTIONAL MATCH.
#[tokio::test]
async fn unflushed_where_on_undeclared_label_reads_only_matching_rows() {
    for v in [2000_i64, 8000] {
        let db = Uni::temporary().config(cfg()).build().await.unwrap();
        insert(&db, 0, v).await;
        let tx = db.session().tx().await.unwrap();
        tx.execute("CREATE (:G {uid: 'g1', ix: -1})").await.unwrap();
        tx.commit().await.unwrap();
        let last = format!("u{}", v - 1);
        for (q, key, want, max_scanned) in [
            ("MATCH (a:E) WHERE a.uid = $s RETURN a.ix", "u0", 1, 1),
            (
                "MATCH (a:E) WHERE $s = a.uid RETURN a.ix",
                last.as_str(),
                1,
                1,
            ),
            (
                "MATCH (a:E) WHERE a.uid IN [$s, 'none'] RETURN a.ix",
                "u7",
                1,
                1,
            ),
            (
                "MATCH (a:E) WHERE a.uid = $s AND a.ix >= 0 RETURN a.ix",
                "u7",
                1,
                1,
            ),
            ("MATCH (a:E) WHERE a.uid = $s RETURN a.ix", "absent", 0, 1),
            (
                "MATCH (g:G), (a:E) WHERE a.uid = $s AND g.uid = 'g1' RETURN a.ix, g.ix",
                "u3",
                1,
                2,
            ),
            (
                "MATCH (a:E), (g:G) WHERE a.uid = $s AND g.uid = 'g1' RETURN a.ix, g.ix",
                "u3",
                1,
                2,
            ),
            (
                "OPTIONAL MATCH (a:E) WHERE a.uid = $s RETURN a.ix",
                "absent",
                1,
                1,
            ),
        ] {
            let r = db
                .session()
                .query_with(q)
                .param("s", Value::String(key.into()))
                .fetch_all()
                .await
                .unwrap();
            let scanned = r.metrics().rows_scanned as u64;
            assert_eq!(r.rows().len(), want, "V={v} `{q}` {key}");
            assert!(
                scanned <= max_scanned,
                "V={v} `{q}` {key}: scanned {scanned} rows of {v} unflushed — the WHERE \
                 equality did not reach the undeclared label's scan"
            );
        }
        let r = db
            .session()
            .query("OPTIONAL MATCH (a:E) WHERE a.uid = 'absent' RETURN a.ix AS ix")
            .await
            .unwrap();
        assert_eq!(
            r.rows()[0].values(),
            &[Value::Null],
            "V={v}: OPTIONAL null row"
        );
    }
}
