// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A correlated equality between a `CypherValue` and a typed column must plan
//! as a hash join and answer exactly what a filter over the cross product does.
//!
//! `UNWIND $rows AS r MATCH (a:E {uid: r.a})` compares a declared `STRING`
//! column with `r.a`, a `CypherValue`. The hash-join rewrite refused that dtype
//! pairing and left a `CrossJoinExec` of N × V rows (502,000 at N = 1k), which
//! made an in-memory load super-linear. The integer form went through
//! `toInteger` instead, which truncates `1.5` to `1` and parses `'1'` as `1`.
//!
//! The differential tests below run each join twice: once in the shape the
//! rewrite takes, once wrapped in `CASE` so the classifier never sees an equi
//! conjunct and the fallback filter over the cross product answers. Each test
//! asserts which operator each arm used, so neither can pass by both arms
//! taking the same path.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{Uni, Value};

fn s(v: &str) -> Value {
    Value::String(v.to_string())
}

fn row(tag: &str, k: Option<Value>) -> Value {
    let mut m = HashMap::from([("tag".to_string(), s(tag))]);
    if let Some(k) = k {
        m.insert("k".to_string(), k);
    }
    Value::Map(m)
}

/// Probe keys of every kind the join must tell apart: strings, integers, the
/// float twin of an integer, a non-integral float, a numeric-looking string, a
/// boolean, a list, NULL, a missing field, duplicates, and two values whose
/// join key collides with an unequal stored value (2^53 + 1 and NaN), which
/// only the join filter's re-check can reject.
fn probe_rows() -> Vec<Value> {
    vec![
        row("s_a", Some(s("a"))),
        row("s_a_dup", Some(s("a"))),
        row("s_b", Some(s("b"))),
        row("s_zz", Some(s("zz"))),
        row("s_1", Some(s("1"))),
        row("i_1", Some(Value::Int(1))),
        row("i_1_dup", Some(Value::Int(1))),
        row("i_2", Some(Value::Int(2))),
        row("f_1.0", Some(Value::Float(1.0))),
        row("f_1.5", Some(Value::Float(1.5))),
        row("f_2.0", Some(Value::Float(2.0))),
        row("b_true", Some(Value::Bool(true))),
        row("l_1", Some(Value::List(vec![Value::Int(1)]))),
        // Shares a join-key bucket with 2^53 without being equal to it.
        row("i_2^53+1", Some(Value::Int((1 << 53) + 1))),
        row("f_nan", Some(Value::Float(f64::NAN))),
        row("null", Some(Value::Null)),
        row("missing", None),
    ]
}

/// Typed labels give `Utf8`, `Int64` and `Float64` columns; `Loose` is
/// schemaless, so its `p` is a `CypherValue` of mixed kinds, some absent.
async fn fixture() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL S (name STRING, uid STRING)")
        .await?;
    tx.execute("CREATE LABEL I (name STRING, ix INT)").await?;
    tx.execute("CREATE LABEL F (name STRING, fx FLOAT, ref INT)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    for (name, uid) in [("s_a", "a"), ("s_a2", "a"), ("s_b", "b"), ("s_1", "1")] {
        tx.execute_with("CREATE (:S {name: $n, uid: $u})")
            .param("n", s(name))
            .param("u", s(uid))
            .run()
            .await?;
    }
    for (name, ix) in [
        ("i_1", 1),
        ("i_1b", 1),
        ("i_2", 2),
        ("i_3", 3),
        ("i_2^53", 1 << 53),
    ] {
        tx.execute_with("CREATE (:I {name: $n, ix: $x})")
            .param("n", s(name))
            .param("x", Value::Int(ix))
            .run()
            .await?;
    }
    for (name, fx) in [
        ("f_1", 1.0),
        ("f_1.5", 1.5),
        ("f_2", 2.0),
        ("f_-0", -0.0),
        ("f_nan", f64::NAN),
    ] {
        tx.execute_with("CREATE (:F {name: $n, fx: $x})")
            .param("n", s(name))
            .param("x", Value::Float(fx))
            .run()
            .await?;
    }
    let loose: Vec<(&str, Option<Value>)> = vec![
        ("p_a", Some(s("a"))),
        ("p_1", Some(Value::Int(1))),
        ("p_1.0", Some(Value::Float(1.0))),
        ("p_1.5", Some(Value::Float(1.5))),
        ("p_s1", Some(s("1"))),
        ("p_true", Some(Value::Bool(true))),
        ("p_list", Some(Value::List(vec![Value::Float(1.0)]))),
        ("p_none", None),
    ];
    for (name, p) in loose {
        match p {
            Some(p) => {
                tx.execute_with("CREATE (:Loose {name: $n, p: $p})")
                    .param("n", s(name))
                    .param("p", p)
                    .run()
                    .await?
            }
            None => {
                tx.execute_with("CREATE (:Loose {name: $n})")
                    .param("n", s(name))
                    .run()
                    .await?
            }
        };
    }
    tx.commit().await?;

    // Point each F at an I by vid, so a vid-anchored join with a mixed-type
    // secondary pair can be exercised.
    let tx = db.session().tx().await?;
    tx.execute("MATCH (f:F), (i:I) WHERE f.fx = 1.0 AND i.name = 'i_1' SET f.ref = id(i)")
        .await?;
    tx.execute("MATCH (f:F), (i:I) WHERE f.fx = 2.0 AND i.name = 'i_2' SET f.ref = id(i)")
        .await?;
    tx.execute("MATCH (f:F), (i:I) WHERE f.fx = 1.5 AND i.name = 'i_1b' SET f.ref = id(i)")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// Run `query` profiled; return its rows as a sorted bag and its operators.
async fn run(db: &Uni, query: &str, rows: &[Value]) -> Result<(Vec<String>, Vec<String>)> {
    let (result, profile) = db
        .session()
        .query_with(query)
        .param("rows", Value::List(rows.to_vec()))
        .profile()
        .await?;
    let mut bag: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    bag.sort();
    let ops = profile
        .runtime_stats
        .iter()
        .map(|o| o.operator.clone())
        .collect();
    Ok((bag, ops))
}

fn uses(ops: &[String], name: &str) -> bool {
    ops.iter().any(|o| o.contains(name))
}

/// The join under test, keyed on `lhs = rhs`.
struct Case {
    label: &'static str,
    /// `MATCH` clause(s) and the equality, as `(pattern, lhs, rhs)`.
    pattern: &'static str,
    lhs: &'static str,
    rhs: &'static str,
    ret: &'static str,
    /// Whether `pattern` follows an `UNWIND $rows AS r`.
    unwind: bool,
}

const CASES: &[Case] = &[
    // W3a: declared STRING against a CypherValue.
    Case {
        label: "utf8_vs_cv",
        pattern: "MATCH (n:S)",
        lhs: "n.uid",
        rhs: "r.k",
        ret: "r.tag, n.name",
        unwind: true,
    },
    // Declared INT against a CypherValue: the old `toInteger` path.
    Case {
        label: "int_vs_cv",
        pattern: "MATCH (n:I)",
        lhs: "n.ix",
        rhs: "r.k",
        ret: "r.tag, n.name",
        unwind: true,
    },
    Case {
        label: "float_vs_cv",
        pattern: "MATCH (n:F)",
        lhs: "n.fx",
        rhs: "r.k",
        ret: "r.tag, n.name",
        unwind: true,
    },
    // Both sides CypherValue: the old raw-bytes path.
    Case {
        label: "cv_vs_cv",
        pattern: "MATCH (n:Loose)",
        lhs: "n.p",
        rhs: "r.k",
        ret: "r.tag, n.name",
        unwind: true,
    },
    // Native Int64 against native Float64, no UNWIND.
    Case {
        label: "int_vs_float",
        pattern: "MATCH (i:I), (f:F)",
        lhs: "i.ix",
        rhs: "f.fx",
        ret: "i.name, f.name",
        unwind: false,
    },
    // Native Float64 against CypherValue holding every kind.
    Case {
        label: "float_vs_loose",
        pattern: "MATCH (f:F), (n:Loose)",
        lhs: "f.fx",
        rhs: "n.p",
        ret: "f.name, n.name",
        unwind: false,
    },
];

/// Hash-join answer equals the cross-join-filter answer, for every dtype
/// pairing, before and after a flush.
#[tokio::test]
async fn mixed_type_equi_join_matches_the_filter_answer() -> Result<()> {
    let db = fixture().await?;
    let rows = probe_rows();
    for flushed in [false, true] {
        if flushed {
            db.flush().await?;
        }
        for case in CASES {
            let prefix = if case.unwind {
                "UNWIND $rows AS r "
            } else {
                ""
            };
            let hash_q = format!(
                "{prefix}{} WHERE {} = {} RETURN {}",
                case.pattern, case.lhs, case.rhs, case.ret
            );
            let filter_q = format!(
                "{prefix}{} WHERE CASE WHEN {} = {} THEN true ELSE false END RETURN {}",
                case.pattern, case.lhs, case.rhs, case.ret
            );
            let (hash, hash_ops) = run(&db, &hash_q, &rows).await?;
            let (filter, filter_ops) = run(&db, &filter_q, &rows).await?;
            let ctx = format!("{} (flushed={flushed})", case.label);
            assert!(
                uses(&hash_ops, "HashJoinExec") && !uses(&hash_ops, "CrossJoinExec"),
                "{ctx}: equi form must hash-join, ran {hash_ops:?}"
            );
            assert!(
                uses(&filter_ops, "CrossJoinExec"),
                "{ctx}: reference must take the cross-join filter, ran {filter_ops:?}"
            );
            assert!(!filter.is_empty(), "{ctx}: reference matched nothing");
            assert_eq!(hash, filter, "{ctx}: hash join disagrees with the filter");
        }
    }
    Ok(())
}

/// The expected matches, spelled out, for the two pairings the old rewrite
/// got wrong. The differential above proves agreement; this pins the answer
/// itself against Cypher equality, so a shared defect cannot hide.
#[tokio::test]
async fn mixed_type_equi_join_follows_cypher_equality() -> Result<()> {
    let db = fixture().await?;
    let rows = probe_rows();
    let bag = |pairs: &[(&str, &str)]| {
        let mut v: Vec<String> = pairs
            .iter()
            .map(|(a, b)| format!("{:?}", vec![s(a), s(b)]))
            .collect();
        v.sort();
        v
    };

    let (got, _) = run(
        &db,
        "UNWIND $rows AS r MATCH (n:S) WHERE n.uid = r.k RETURN r.tag, n.name",
        &rows,
    )
    .await?;
    assert_eq!(
        got,
        bag(&[
            ("s_a", "s_a"),
            ("s_a", "s_a2"),
            ("s_a_dup", "s_a"),
            ("s_a_dup", "s_a2"),
            ("s_b", "s_b"),
            ("s_1", "s_1"),
        ]),
        "a string never equals a number, a boolean or a list"
    );

    let (got, _) = run(
        &db,
        "UNWIND $rows AS r MATCH (n:I) WHERE n.ix = r.k RETURN r.tag, n.name",
        &rows,
    )
    .await?;
    assert_eq!(
        got,
        bag(&[
            ("i_1", "i_1"),
            ("i_1", "i_1b"),
            ("i_1_dup", "i_1"),
            ("i_1_dup", "i_1b"),
            ("f_1.0", "i_1"),
            ("f_1.0", "i_1b"),
            ("i_2", "i_2"),
            ("f_2.0", "i_2"),
        ]),
        "1 = 1.0 holds; 1 = 1.5, 1 = '1' and 1 = true do not"
    );
    Ok(())
}

/// OPTIONAL MATCH keeps every probe row, NULL-padding the ones the join
/// re-check rejects rather than dropping them.
#[tokio::test]
async fn optional_mixed_type_equi_join_null_pads() -> Result<()> {
    let db = fixture().await?;
    let rows = probe_rows();
    for flushed in [false, true] {
        if flushed {
            db.flush().await?;
        }
        let (got, ops) = run(
            &db,
            "UNWIND $rows AS r OPTIONAL MATCH (n:I) WHERE n.ix = r.k RETURN r.tag, n.name",
            &rows,
        )
        .await?;
        assert!(
            uses(&ops, "HashJoinExec") && !uses(&ops, "CrossJoinExec"),
            "flushed={flushed}: the outer equi-join must hash-join, ran {ops:?}"
        );
        let (want, _) = run(
            &db,
            "UNWIND $rows AS r OPTIONAL MATCH (n:I) \
             WHERE CASE WHEN n.ix = r.k THEN true ELSE false END RETURN r.tag, n.name",
            &rows,
        )
        .await?;
        assert_eq!(got, want, "flushed={flushed}");
        // 17 probe rows; i_1, i_1_dup and f_1.0 match twice.
        assert_eq!(got.len(), 20, "flushed={flushed}: {got:?}");
    }
    Ok(())
}

/// A vid-anchored join whose second pair is `Int64 = Float64`.
///
/// `VidLookupJoinExec` compares non-anchor pairs by `ScalarValue` equality, so
/// `Int64(1)` and `Float64(1.0)` never matched there; such a pair must take the
/// hash join, whose re-check uses Cypher equality.
#[tokio::test]
async fn vid_anchored_join_with_mixed_type_secondary_pair() -> Result<()> {
    let db = fixture().await?;
    let (got, ops) = run(
        &db,
        "MATCH (f:F), (i:I) WHERE id(i) = f.ref AND i.ix = f.fx RETURN f.name, i.name",
        &[],
    )
    .await?;
    let (want, _) = run(
        &db,
        "MATCH (f:F), (i:I) WHERE id(i) = f.ref \
         AND CASE WHEN i.ix = f.fx THEN true ELSE false END RETURN f.name, i.name",
        &[],
    )
    .await?;
    assert!(uses(&ops, "HashJoinExec"), "ran {ops:?}");
    assert_eq!(got, want);
    // f_1 -> i_1 (1 = 1.0) and f_2 -> i_2 (2 = 2.0); f_1.5 -> i_1b is not equal.
    assert_eq!(got.len(), 2, "{got:?}");
    Ok(())
}

/// `n` edges over `n / 2 + 2` vertices, the shape of the W3 load probe.
fn load_fixture(n: usize) -> (Vec<Value>, Vec<Value>, Vec<Value>) {
    let len = n / 2 + 2;
    let vertices: Vec<Value> = (0..len)
        .map(|i| Value::Map(HashMap::from([("uid".to_string(), s(&format!("n{i}")))])))
        .collect();
    let mut edges = Vec::new();
    let mut uids = Vec::new();
    for i in 0..n {
        let (a, b) = (i % len, (i * 7 + 3) % len);
        if a != b {
            edges.push(Value::Map(HashMap::from([
                ("a".to_string(), s(&format!("n{a}"))),
                ("b".to_string(), s(&format!("n{b}"))),
            ])));
            uids.push(s(&format!("n{a}")));
        }
    }
    (vertices, edges, uids)
}

/// Counter guard for the in-memory load: no cross join, and no operator ever
/// holds more than 2N rows. The cross join held N × V (502,000 at N = 1k).
#[tokio::test(flavor = "multi_thread")]
async fn unwind_match_by_string_uid_load_stays_linear() -> Result<()> {
    for n in [1_000usize, 4_000] {
        let (vertices, edges, uids) = load_fixture(n);
        let db = Uni::in_memory().build().await?;
        let tx = db.session().tx().await?;
        tx.execute("CREATE LABEL E (uid STRING)").await?;
        tx.execute("CREATE EDGE TYPE T () FROM E TO E").await?;
        tx.commit().await?;
        let tx = db.session().tx().await?;
        tx.execute_with("UNWIND $rows AS r CREATE (:E {uid: r.uid})")
            .param("rows", Value::List(vertices))
            .run()
            .await?;
        tx.commit().await?;

        let tx = db.session().tx().await?;
        let (_, profile) = tx
            .execute_with(
                "UNWIND $rows AS r MATCH (a:E {uid: r.a}), (b:E {uid: r.b}) \
                 CREATE (a)-[:T]->(b)",
            )
            .param("rows", Value::List(edges.clone()))
            .profile()
            .await?;
        tx.commit().await?;
        let (_, list_profile) = db
            .session()
            .query_with("UNWIND $uids AS u MATCH (a:E {uid: u}) RETURN count(*) AS c")
            .param("uids", Value::List(uids))
            .profile()
            .await?;

        for (what, stats) in [
            ("loader", &profile.runtime_stats),
            ("string list", &list_profile.runtime_stats),
        ] {
            let ops: Vec<(String, usize)> = stats
                .iter()
                .map(|o| (o.operator.clone(), o.actual_rows))
                .collect();
            assert!(
                !ops.iter().any(|(o, _)| o.contains("CrossJoinExec")),
                "{what} N={n}: cross join planned: {ops:?}"
            );
            let max = ops.iter().map(|(_, r)| *r).max().unwrap_or(0);
            assert!(
                max <= 2 * n,
                "{what} N={n}: an operator held {max} rows (> 2N): {ops:?}"
            );
        }

        let created = db
            .session()
            .query("MATCH (:E)-[t:T]->(:E) RETURN count(t) AS c")
            .await?;
        assert_eq!(
            created.rows()[0].get::<i64>("c")?,
            edges.len() as i64,
            "N={n}: every edge row must find both endpoints"
        );
    }
    Ok(())
}
