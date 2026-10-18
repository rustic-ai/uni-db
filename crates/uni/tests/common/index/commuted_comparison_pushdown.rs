// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A comparison written constant-first (`$p = n.uid`, `'u42' = n.uid`,
//! `5 > n.x`) is planned exactly like its property-first mirror.
//!
//! Perf audit 2026-10-17, row A9: every pushdown and index matcher recognized a
//! property comparison only with the property on the left, so `$p = n.uid`
//! scanned the whole label (`rows_scanned` 2,000 → 8,000, `index_scans` 0)
//! while `n.uid = $p` read one row from the index. The WHERE planner now
//! orients such comparisons once, before any matcher runs
//! (`pushdown::orient_property_comparisons`).
//!
//! Two kinds of test:
//!
//! * **Counter guards** — the commuted forms read one row and consult the index,
//!   at two scales, with the data flushed and with it split across L1 and L0.
//! * **Differentials** — every orientation × operator × {indexed, unindexed} ×
//!   {int, float, string, NULL, mixed-type} × {flushed, L0} returns the same
//!   rows as the canonical form *and* as an unpushed oracle (`WITH n WHERE …`,
//!   a filter above a projection, which no pushdown reaches). The oracle's own
//!   unpushed-ness is asserted through `index_scans == 0`, so a pushdown bug
//!   shared by both orientations cannot hide behind it. A pushed predicate
//!   that is data-independent has been a live wrong answer before (Lance reads
//!   `"col"` as a string literal), which is what the oracle arm is for.

use uni_db::{Uni, UniConfig, Value};

/// Where the rows live when the query runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Residency {
    /// Every row flushed to L1 before the query.
    Flushed,
    /// Half the rows flushed, the other half only in L0.
    SplitL0,
}

/// `:N {id, uid, x, f, ux, uf, us}` with `uid`, `x`, `f` indexed and `ux`,
/// `uf`, `us` unindexed copies of the same values; `x`/`ux` and `f`/`uf` are
/// NULL on every 13th row. `:M {id, v}` is schemaless, with `v` cycling
/// through an int, a float, a string and NULL.
///
/// With [`Residency::SplitL0`], ids `0..n` are flushed and `n..2n` are written
/// afterwards and left in L0.
async fn fixture(n: i64, residency: Residency) -> Uni {
    // No automatic flush: the test decides what is in L1 and what is in L0,
    // and the counters of consecutive queries compare like for like.
    let db = Uni::temporary()
        .config(UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for ddl in [
        "CREATE LABEL N (id INT, uid STRING, x INT, f FLOAT, ux INT, uf FLOAT, us STRING)",
        "CREATE INDEX idx_n_uid FOR (n:N) ON (n.uid)",
        "CREATE INDEX idx_n_x FOR (n:N) ON (n.x)",
        "CREATE INDEX idx_n_f FOR (n:N) ON (n.f)",
    ] {
        tx.execute(ddl).await.unwrap();
    }
    tx.commit().await.unwrap();

    insert(&db, 0, n).await;
    db.flush().await.unwrap();
    if residency == Residency::SplitL0 {
        insert(&db, n, 2 * n).await;
    }
    db
}

async fn insert(db: &Uni, from: i64, to: i64) {
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.query_with(
        "UNWIND range($from, $to - 1) AS i \
         WITH i, CASE WHEN i % 13 = 5 THEN null ELSE i END AS xi \
         CREATE (:N {id: i, uid: 'u' + toString(i), x: xi, f: toFloat(xi) + 0.5, \
                     ux: xi, uf: toFloat(xi) + 0.5, us: 'u' + toString(i)}) \
         CREATE (:M {id: i, v: CASE i % 4 WHEN 0 THEN i WHEN 1 THEN toFloat(i) + 0.5 \
                                 WHEN 2 THEN 'u' + toString(i) ELSE null END})",
    )
    .param("from", Value::Int(from))
    .param("to", Value::Int(to))
    .fetch_all()
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// Rows (debug-rendered, in query order) plus `(rows_scanned, index_scans)`.
async fn run(db: &Uni, q: &str, p: &Value) -> (Vec<String>, u64, u64) {
    let r = db
        .session()
        .query_with(q)
        .param("p", p.clone())
        .fetch_all()
        .await
        .unwrap_or_else(|e| panic!("query failed: {q}: {e}"));
    let rows = r
        .rows()
        .iter()
        .map(|row| format!("{:?}", row.values()))
        .collect();
    let m = r.metrics();
    (rows, m.rows_scanned as u64, m.index_scans as u64)
}

/// The three spellings the guard covers, for a key `key`.
fn guard_forms(key: &str) -> [(String, String); 3] {
    [
        (
            "n.uid = $p".into(),
            "MATCH (n:N) WHERE n.uid = $p RETURN n.id AS id".into(),
        ),
        (
            "$p = n.uid".into(),
            "MATCH (n:N) WHERE $p = n.uid RETURN n.id AS id".into(),
        ),
        (
            format!("'{key}' = n.uid"),
            format!("MATCH (n:N) WHERE '{key}' = n.uid RETURN n.id AS id"),
        ),
    ]
}

/// Commuted equality reads one row and consults the index, at two scales.
#[tokio::test]
async fn commuted_equality_uses_the_index_when_flushed() {
    for n in [2000_i64, 8000] {
        let db = fixture(n, Residency::Flushed).await;
        let key = "u42";
        for (form, q) in guard_forms(key) {
            let (rows, scanned, index_scans) = run(&db, &q, &Value::String(key.into())).await;
            assert_eq!(rows.len(), 1, "N={n} `{form}` returned {rows:?}");
            assert_eq!(
                scanned, 1,
                "N={n} `{form}` scanned {scanned} rows; the index should have served one (A9)"
            );
            assert_eq!(
                index_scans, 1,
                "N={n} `{form}` consulted no index (index_scans={index_scans}) (A9)"
            );
        }
    }
}

/// With half the label in L0, the commuted forms cost exactly what the
/// canonical form costs, for a key on either side of the flush, and still
/// consult the index for the flushed half.
///
/// Measured: every form read `n + 1` rows here — the `n` L0 rows, which the
/// L0 path built in full before filtering, plus the one indexed L1 row. Since
/// W3b the L0 path skips rows the equality cannot match before building them,
/// so every form reads at most the matching L0 row plus the indexed L1 row.
#[tokio::test]
async fn commuted_equality_uses_the_index_with_l0_rows() {
    for n in [2000_i64, 8000] {
        let db = fixture(n, Residency::SplitL0).await;
        for key in ["u42".to_string(), format!("u{}", n + 42)] {
            let mut canonical: Option<(Vec<String>, u64, u64)> = None;
            for (form, q) in guard_forms(&key) {
                let got = run(&db, &q, &Value::String(key.clone())).await;
                assert_eq!(
                    got.0.len(),
                    1,
                    "N={n} key={key} `{form}` returned {:?}",
                    got.0
                );
                // Measured 1 at N=2000 and 3 at N=8000, identical for every
                // form. Consistent with the scan's VID-range slicing issuing
                // one Lance lookup per slice; not traced per slice. Equality
                // with the canonical form is asserted below.
                assert!(
                    got.2 >= 1,
                    "N={n} key={key} `{form}` consulted no index (index_scans={})",
                    got.2
                );
                // The L0 half is pre-filtered before it is built (W3b); the
                // flushed half must be served by the index. A label scan
                // reads `2n`.
                assert!(
                    got.1 <= 2,
                    "N={n} key={key} `{form}` scanned {} of {} rows — the flushed half \
                     was scanned, not looked up",
                    got.1,
                    2 * n
                );
                match &canonical {
                    None => canonical = Some(got),
                    Some(c) => assert_eq!(
                        &got, c,
                        "N={n} key={key} `{form}` differs from `n.uid = $p` in rows or counters"
                    ),
                }
            }
        }
    }
}

const OPS: [(&str, &str); 6] = [
    ("=", "="),
    ("<>", "<>"),
    ("<", ">"),
    ("<=", ">="),
    (">", "<"),
    (">=", "<="),
];

/// A literal spelling of `v`, for the literal-left arm.
fn literal(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{f:?}"),
        Value::String(s) => format!("'{s}'"),
        other => panic!("no literal for {other:?}"),
    }
}

/// Every orientation agrees with the canonical form and with the unpushed
/// oracle, for every operator, column kind, value type and residency.
#[tokio::test]
async fn commuted_comparisons_match_canonical_and_unpushed_results() {
    let n = 300;
    // (label, column, indexed)
    let columns = [
        ("N", "x", true),
        ("N", "ux", false),
        ("N", "f", true),
        ("N", "uf", false),
        ("N", "uid", true),
        ("N", "us", false),
        ("M", "v", false),
    ];
    let values = [
        Value::Int(42),
        Value::Float(41.5),
        Value::String("u42".into()),
        Value::Null,
    ];
    let mut checked = 0usize;
    let mut non_empty = 0usize;
    for residency in [Residency::Flushed, Residency::SplitL0] {
        let db = fixture(n, residency).await;
        for (label, col, indexed) in columns {
            for v in &values {
                for (op, mirror) in OPS {
                    let ctx = format!("{residency:?} {label}.{col} {op} {v:?}");
                    let ret = "RETURN n.id AS id ORDER BY id";
                    let lit = literal(v);
                    let canonical = format!("MATCH (n:{label}) WHERE n.{col} {op} $p {ret}");
                    let forms = [
                        format!("MATCH (n:{label}) WHERE $p {mirror} n.{col} {ret}"),
                        format!("MATCH (n:{label}) WHERE {lit} {mirror} n.{col} {ret}"),
                        format!("MATCH (n:{label}) WHERE n.{col} {op} {lit} {ret}"),
                    ];
                    let oracle = format!("MATCH (n:{label}) WITH n WHERE n.{col} {op} $p {ret}");

                    let (want, _, _) = run(&db, &canonical, v).await;
                    let (unpushed, _, oracle_index_scans) = run(&db, &oracle, v).await;
                    // The schemaless label reads the shared vertex table,
                    // whose label lookup reports an index scan of its own, so
                    // only an indexed column discriminates here.
                    if indexed {
                        assert_eq!(
                            oracle_index_scans, 0,
                            "{ctx}: the oracle consulted an index, so it is not unpushed"
                        );
                    }
                    assert_eq!(want, unpushed, "{ctx}: canonical vs unpushed oracle");
                    for q in &forms {
                        let (got, _, _) = run(&db, q, v).await;
                        assert_eq!(got, want, "{ctx}: `{q}` vs canonical");
                    }
                    // Commuted, inside a disjunction and under NOT: the
                    // orientation descends the boolean skeleton.
                    let nested = format!(
                        "MATCH (n:{label}) WHERE NOT ($p {mirror} n.{col}) OR n.id < 0 {ret}"
                    );
                    let nested_oracle = format!(
                        "MATCH (n:{label}) WITH n WHERE NOT (n.{col} {op} $p) OR n.id < 0 {ret}"
                    );
                    assert_eq!(
                        run(&db, &nested, v).await.0,
                        run(&db, &nested_oracle, v).await.0,
                        "{ctx}: NOT/OR nesting"
                    );
                    if indexed && op == "=" && matches!(v, Value::String(_) | Value::Int(_)) {
                        let (_, _, idx) = run(
                            &db,
                            &format!("MATCH (n:{label}) WHERE $p = n.{col} {ret}"),
                            v,
                        )
                        .await;
                        let (_, _, canon_idx) = run(&db, &canonical, v).await;
                        assert_eq!(idx, canon_idx, "{ctx}: index use differs by orientation");
                    }
                    checked += 1;
                    non_empty += usize::from(!want.is_empty());
                }
            }
        }
    }
    // Anti-vacuity: the grid must exercise matches, not only empty answers.
    assert_eq!(checked, 2 * 7 * 4 * 6);
    assert!(
        non_empty > checked / 3,
        "only {non_empty}/{checked} cases returned rows; the differential is too weak"
    );
}

/// A Locy rule-body `WHERE $p = n.uid` reaches the scan like `n.uid = $p`.
#[tokio::test]
async fn locy_rule_body_commuted_equality_uses_the_index() {
    let n = 4000;
    let db = fixture(n, Residency::Flushed).await;
    let mut seen: Vec<(String, usize, u64)> = Vec::new();
    for where_ in ["n.uid = $p", "$p = n.uid", "'u42' = n.uid"] {
        let program = format!(
            "CREATE RULE hit AS MATCH (n:N) WHERE {where_} YIELD KEY n \n\
             QUERY hit RETURN n.id AS id"
        );
        let r = db
            .session()
            .locy_with(&program)
            .param("p", Value::String("u42".into()))
            .run()
            .await
            .unwrap_or_else(|e| panic!("`{where_}`: {e}"));
        let rows = r.rows().map_or(0, |rows| rows.len());
        let scanned = r.metrics().rows_scanned as u64;
        assert_eq!(rows, 1, "`{where_}` returned {rows} rows");
        assert!(
            scanned < 16,
            "Locy rule body `WHERE {where_}` scanned {scanned} of {n} rows (A9)"
        );
        seen.push((where_.to_string(), rows, scanned));
    }
    assert!(
        seen.windows(2).all(|w| w[0].2 == w[1].2),
        "orientations scanned different row counts: {seen:?}"
    );
}
