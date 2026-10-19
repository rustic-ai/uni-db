// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Pushed filters on property names that are SQL keywords or need quoting.
//!
//! `FilterExpr::to_sql` used to emit column names bare. Lance reads a bare
//! keyword (`order`, `select`, `not`), a name with a space or dash, or a name
//! carrying a quote as a parse or planning error, and reads bare `null` and
//! `true` as *literals*, which turns the predicate into a constant: a silent
//! wrong answer. (A double-quoted name is no fix — Lance reads `"col"` as a
//! string literal, which was a live wrong answer once.) Lance's identifier
//! quote is the backtick, and the renderer now backtick-quotes every name.
//!
//! Each case runs pushed (`MATCH (n:L) WHERE …`) and unpushed (`MATCH (n:L)
//! WITH n WHERE …`, a filter above a projection no pushdown reaches; its
//! unpushed-ness is asserted through `index_scans == 0`) over three labels with
//! the same awkward property names: BTree-indexed, Hash-indexed, unindexed.
//! Both arms must succeed and agree, flushed and with half the rows in L0.

use std::collections::HashMap;

use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig, Value};

/// Property names a Cypher user can write with backticks, and a Lance column
/// can carry. A name containing `.` is absent: Lance refuses it as a top-level
/// field name at write time, so it can never be a storage column.
const NAMES: &[&str] = &[
    "order",
    "select",
    "group",
    "limit",
    "from",
    "value",
    "date",
    "where",
    "not",
    "null",
    "true",
    "my prop",
    "a-b",
    "Upper",
    "MixedCase",
    "ünïcödé",
    "has'sq",
    "1lead",
];

// A name containing a backtick is absent too. Cypher can spell it (the
// doubled-backtick escape, `n.`has``tick``), but it cannot be a declared
// property: Lance writes such a column and then cannot project it, so
// `validate_property_name` refuses the declaration (see uni-store's
// `lance_backtick_column_probe`). The filter renderer refuses it as well and
// leaves the conjunct to the residual filter. As an undeclared property it is
// covered by `bugs::escaped_identifier_round_trip`.

/// The three labels, by index kind (`ScalarType` is neither `Copy` nor
/// `Clone`, so the kind is rebuilt per use).
const LABELS: [(&str, Option<fn() -> ScalarType>); 3] = [
    ("KB", Some(|| ScalarType::BTree)),
    ("KH", Some(|| ScalarType::Hash)),
    ("KU", None),
];

const VALS: [Option<&str>; 4] = [Some("a"), Some("b"), Some("c"), None];
const ROWS: i64 = 60;

fn all_names() -> impl Iterator<Item = &'static str> {
    NAMES.iter().copied()
}

/// A Cypher backtick-quoted identifier.
fn q(name: &str) -> String {
    format!("`{name}`")
}

fn cfg() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    }
}

async fn schema(db: &Uni) {
    let mut s = db.schema();
    for (label, index) in LABELS {
        let mut l = s.label(label).property("id", DataType::Int64);
        for name in all_names() {
            l = l.property_nullable(name, DataType::String);
        }
        if let Some(kind) = index {
            for name in all_names() {
                l = l.index(name, IndexType::Scalar(kind()));
            }
        }
        s = l.done();
    }
    let mut e = s
        .edge_type("R", &["KU"], &["KU"])
        .property("id", DataType::Int64);
    for name in NAMES {
        e = e.property_nullable(name, DataType::String);
    }
    e.done().apply().await.unwrap();
}

/// Row `i`: property `k` of [`all_names`] holds `VALS[(i + k) % 4]`.
fn row(i: i64) -> Value {
    let mut m = HashMap::new();
    m.insert("id".to_string(), Value::Int(i));
    for (k, name) in all_names().enumerate() {
        let v =
            VALS[(i as usize + k) % VALS.len()].map_or(Value::Null, |s| Value::String(s.into()));
        m.insert(name.to_string(), v);
    }
    Value::Map(m)
}

async fn insert(db: &Uni, ids: std::ops::Range<i64>) {
    let sets: Vec<String> = all_names()
        .map(|n| format!("n.{} = r[{}]", q(n), q_str(n)))
        .collect();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    for (label, _) in LABELS {
        tx.query_with(&format!(
            "UNWIND $rows AS r CREATE (n:{label} {{id: r.id}}) SET {}",
            sets.join(", ")
        ))
        .param("rows", Value::List(ids.clone().map(row).collect()))
        .fetch_all()
        .await
        .unwrap();
    }
    // A chain of edges `i -> i+1` between `KU` vertices, carrying the same
    // awkward properties, for the variable-length edge-filter route.
    let esets: Vec<String> = NAMES
        .iter()
        .map(|n| format!("e.{} = r[{}]", q(n), q_str(n)))
        .collect();
    tx.query_with(&format!(
        "UNWIND $rows AS r MATCH (a:KU {{id: r.id}}), (b:KU {{id: r.id + 1}}) \
         CREATE (a)-[e:R {{id: r.id}}]->(b) SET {}",
        esets.join(", ")
    ))
    .param("rows", Value::List(ids.map(row).collect()))
    .fetch_all()
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

/// A Cypher string literal.
fn q_str(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// `split_l0`: the first half flushed, the second half only in L0.
async fn fixture(split_l0: bool) -> Uni {
    let db = Uni::temporary().config(cfg()).build().await.unwrap();
    schema(&db).await;
    if split_l0 {
        insert(&db, 0..ROWS / 2).await;
        db.flush().await.unwrap();
        insert(&db, ROWS / 2..ROWS).await;
    } else {
        insert(&db, 0..ROWS).await;
        db.flush().await.unwrap();
    }
    db
}

/// Every pushed predicate shape, over one property.
fn preds(name: &str) -> Vec<(String, Vec<(&'static str, Value)>)> {
    let p = format!("n.{}", q(name));
    let none = Vec::new;
    vec![
        (format!("{p} = 'b'"), none()),
        (format!("{p} = $v"), vec![("v", Value::String("c".into()))]),
        (format!("{p} IN ['a', 'c']"), none()),
        (format!("{p} < 'b'"), none()),
        (format!("{p} >= 'b' AND {p} < 'c'"), none()),
        (format!("{p} STARTS WITH 'b'"), none()),
        (format!("{p} IS NULL"), none()),
        (format!("{p} IS NOT NULL"), none()),
        (format!("NOT {p} = 'a'"), none()),
    ]
}

type Outcome = Result<(Vec<i64>, u64), String>;

async fn run(db: &Uni, query: &str, params: &[(&'static str, Value)]) -> Outcome {
    let session = db.session();
    let mut b = session.query_with(query);
    for (k, v) in params {
        b = b.param(*k, v.clone());
    }
    let r = b.fetch_all().await.map_err(|e| e.to_string())?;
    let ids = r
        .rows()
        .iter()
        .map(|row| match row.values().first() {
            Some(Value::Int(i)) => *i,
            other => panic!("unexpected id cell {other:?}"),
        })
        .collect();
    Ok((ids, r.metrics().index_scans as u64))
}

/// Pushed vs unpushed, every name, every shape, every label.
#[tokio::test]
async fn awkward_property_names_match_the_unpushed_oracle() {
    for split_l0 in [false, true] {
        let db = fixture(split_l0).await;
        let mut failures = Vec::new();
        let mut checked = 0usize;
        let mut non_empty = 0usize;
        let mut on_index = 0usize;
        let mut index_served = 0usize;
        for (label, index) in LABELS {
            for name in all_names() {
                for (pred, params) in preds(name) {
                    checked += 1;
                    let ret = "RETURN n.id AS id ORDER BY id";
                    let pushed = format!("MATCH (n:{label}) WHERE {pred} {ret}");
                    let oracle = format!("MATCH (n:{label}) WITH n WHERE {pred} {ret}");
                    let ctx = format!("split_l0={split_l0} {label} `{pred}`");
                    match (
                        run(&db, &oracle, &params).await,
                        run(&db, &pushed, &params).await,
                    ) {
                        (Ok((want, oracle_ix)), Ok((got, pushed_ix))) => {
                            if oracle_ix != 0 {
                                failures.push(format!("{ctx}: the oracle consulted an index"));
                            }
                            if got != want {
                                failures.push(format!("{ctx}: pushed {got:?} vs oracle {want:?}"));
                            }
                            non_empty += usize::from(!want.is_empty());
                            if index.is_some() {
                                on_index += 1;
                                index_served += usize::from(pushed_ix > 0);
                            }
                        }
                        (want, got) => {
                            failures.push(format!("{ctx}: pushed {got:?} vs oracle {want:?}"))
                        }
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} of {checked} cases failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
        eprintln!(
            "split_l0={split_l0}: checked {checked}, non-empty {non_empty}, \
             on an index {on_index}, index-served {index_served}"
        );
        assert!(
            non_empty * 2 > checked,
            "only {non_empty}/{checked} returned rows"
        );
        // Quoting must not cost the index: on the BTree label every shape is
        // pushable; on the Hash label only `=`/`IN`/null tests are.
        assert!(
            index_served * 2 > on_index,
            "only {index_served}/{on_index} indexed cases consulted an index"
        );
    }
}

/// Awkward edge property names in a variable-length pattern's property map.
///
/// The map is applied per edge during the traversal (from
/// `edge_property_conditions`); no Lance filter is rendered for it. (An
/// `edge_lance_filter` string used to be rendered and handed to the VLP execs,
/// which never read it; it was removed.) So this test does not exercise the
/// renderer: it pins the end-to-end answer for awkward edge property names.
#[tokio::test]
async fn awkward_edge_property_names_in_a_var_length_filter() {
    for split_l0 in [false, true] {
        let db = fixture(split_l0).await;
        let mut failures = Vec::new();
        for name in NAMES {
            let p = q(name);
            let pushed = format!(
                "MATCH (a:KU)-[:R*1..2 {{{p}: 'b'}}]->(b:KU) \
                 RETURN a.id * 1000 + b.id AS id ORDER BY id"
            );
            let oracle = format!(
                "MATCH path = (a:KU)-[:R*1..2]->(b:KU) \
                 WITH a, b, relationships(path) AS rs \
                 WHERE all(r IN rs WHERE r.{p} = 'b') \
                 RETURN a.id * 1000 + b.id AS id ORDER BY id"
            );
            match (run(&db, &oracle, &[]).await, run(&db, &pushed, &[]).await) {
                (Ok((want, _)), Ok((got, _))) if want == got && !want.is_empty() => {}
                (want, got) => failures.push(format!(
                    "split_l0={split_l0} `{name}`: pushed {got:?} vs oracle {want:?}"
                )),
            }
        }
        assert!(
            failures.is_empty(),
            "{} names failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }
}
