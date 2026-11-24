// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T9: a BTIC index answers exactly as no index does.
//!
//! Every interval predicate with one constant side, in both argument
//! positions, as a literal and as a parameter; unbounded and half-unbounded
//! intervals, granularity and certainty variants, intervals sharing an
//! endpoint with the constant (the `meets` / `adjacent` / `starts` /
//! `finishes` edges), `=` against `btic_equals`, NULLs, and the
//! `btic_lo` / `btic_hi` comparison forms, whose accessors are NULL for an
//! unbounded side.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(btic_differential)'

// Rust guideline compliant

use super::harness::{Case, Differential, Writes};
use anyhow::Result;
use uni_db::Value;

pub(super) const SCHEMA: &[&str] = &["CREATE LABEL Span (id INT, v BTIC, n INT)"];
pub(super) const INDEX: &str = "CREATE INDEX idx_v FOR (e:Span) ON (e.v)";

/// The stored intervals, by id. Chosen around `1985`, the main constant:
/// equal bounds with other granularity and certainty, contained, meeting on
/// either side, starting and finishing with it, containing it, and
/// unbounded on one or both sides, including ones ending or starting
/// exactly at the constant's bounds.
pub(super) const ROWS: &[(i64, &str)] = &[
    (1, "1985"),
    (2, "1985-01/1985-12"),
    (3, "~1985"),
    (4, "?1985"),
    (5, "1985-03"),
    (6, "1984"),
    (7, "1986"),
    (8, "1980/1990"),
    (9, "1985/1987"),
    (10, "1985-01/1985-06"),
    (11, "1985-07/1985-12"),
    (12, "1983/1985"),
    (13, "/"),
    (14, "2020-03/"),
    (15, "/1980"),
    (16, "/1985"),
    (17, "1986/"),
    (18, "/1984"),
    (20, "1985-06-15T14:30Z"),
    (21, "1979"),
    (22, "1970"),
    (23, "1969"),
];

pub(super) fn writes() -> Writes {
    let mut creates: Vec<String> = ROWS
        .iter()
        .map(|(id, lit)| format!("(:Span {{id: {id}, v: btic('{lit}')}})"))
        .collect();
    // No interval at all.
    creates.push("(:Span {id: 19, n: 1})".to_string());
    Writes {
        initial: vec![format!("CREATE {}", creates.join(", "))],
        changes: vec![
            "MATCH (e:Span {id: 5}) SET e.v = btic('1987')".to_string(),
            "MATCH (e:Span {id: 6}) DELETE e".to_string(),
            "MATCH (e:Span {id: 19}) SET e.v = btic('1985-02')".to_string(),
            "MATCH (e:Span {id: 7}) SET e.v = null".to_string(),
            "MATCH (e:Span {id: 1}) SET e.n = 2".to_string(),
            "CREATE (:Span {id: 30, v: btic('1985')}), (:Span {id: 31, v: btic('/1986')})"
                .to_string(),
        ],
    }
}

/// The value of `btic('<lit>')`, for a parameter.
pub(super) async fn btic_value(diff: &Differential, lit: &str) -> Result<Value> {
    let rows = diff
        .plain
        .session()
        .query(&format!("RETURN btic('{lit}') AS b"))
        .await?;
    Ok(rows.rows()[0].value("b").expect("b").clone())
}

/// The interval-against-interval predicates.
const PREDICATES: &[&str] = &[
    "btic_overlaps",
    "btic_contains",
    "btic_during",
    "btic_before",
    "btic_after",
    "btic_meets",
    "btic_starts",
    "btic_finishes",
    "btic_equals",
    "btic_disjoint",
    "btic_adjacent",
];

/// The constants each predicate is run against, as literals.
const CONSTANTS: &[&str] = &[
    "1985",
    "~1985",
    "?1985",
    "1985-03",
    "/",
    "2020-03/",
    "/1980",
    "/1985",
    "1986/",
    "1985-06-15T14:30Z",
];

/// The constants also run as parameters.
const PARAM_CONSTANTS: &[&str] = &["1985", "/1985", "2020-03/"];

const RET: &str = "RETURN e.id AS id";

fn q(w: &str) -> String {
    format!("MATCH (e:Span) WHERE {w} {RET}")
}

/// Whether the key bounds `pred` with the constant `lit` at all. A bound
/// that every key satisfies is not pushed: `btic_contains(btic('/'), e.v)`
/// holds for every interval.
fn bounds(pred: &str, lit: &str, property_first: bool) -> bool {
    !(pred == "btic_contains" && lit == "/" && !property_first)
}

async fn predicate_cases(diff: &Differential, preds: &[&str]) -> Result<Vec<Case>> {
    let mut cases = Vec::new();
    for pred in preds {
        for lit in CONSTANTS {
            let c = format!("btic('{lit}')");
            for property_first in [true, false] {
                let call = if property_first {
                    format!("{pred}(e.v, {c})")
                } else {
                    format!("{pred}({c}, e.v)")
                };
                cases.push(if bounds(pred, lit, property_first) {
                    Case::served(q(&call))
                } else {
                    Case::unserved(q(&call))
                });
            }
        }
        for lit in PARAM_CONSTANTS {
            let value = btic_value(diff, lit).await?;
            cases.push(Case::served(q(&format!("{pred}(e.v, $c)"))).param("c", value.clone()));
            cases.push(Case::served(q(&format!("{pred}($c, e.v)"))).param("c", value));
        }
    }
    Ok(cases)
}

async fn run_predicates(preds: &[&str]) -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Span", "v").await?;
    let cases = predicate_cases(&diff, preds).await?;
    diff.stages(&writes(), &cases).await
}

#[tokio::test]
async fn btic_differential_overlaps_contains_during() -> Result<()> {
    run_predicates(&PREDICATES[0..3]).await
}

#[tokio::test]
async fn btic_differential_before_after_meets() -> Result<()> {
    run_predicates(&PREDICATES[3..6]).await
}

#[tokio::test]
async fn btic_differential_starts_finishes_equals() -> Result<()> {
    run_predicates(&PREDICATES[6..9]).await
}

#[tokio::test]
async fn btic_differential_disjoint_adjacent() -> Result<()> {
    run_predicates(&PREDICATES[9..]).await
}

/// `=` compares the whole value (granularity and certainty too), while
/// `btic_equals` compares bounds; the key serves both through the bounds.
/// `btic_lo` / `btic_hi` comparisons, NULL and other-type operands.
#[tokio::test]
async fn btic_differential_equality_accessors_and_unserved() -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Span", "v").await?;
    let b1985 = btic_value(&diff, "1985").await?;
    let mut cases = vec![
        Case::served(q("e.v = btic('1985')")),
        Case::served(q("btic('1985') = e.v")),
        Case::served(q("e.v = btic('~1985')")),
        Case::served(q("e.v = btic('/')")),
        Case::served(q("e.v = $c")).param("c", b1985.clone()),
        Case::served(format!("MATCH (e:Span {{v: btic('1985')}}) {RET}")),
        Case::served(q("e.v IN [btic('1985'), btic('1986/'), null]")),
        Case::served(q("e.v IN $list"))
            .param("list", Value::List(vec![b1985.clone(), Value::Int(3)])),
        Case::served(q("btic_equals(e.v, btic('1985')) AND e.n IS NULL")),
        Case::served(q(
            "btic_overlaps(e.v, btic('1980/1990')) AND btic_before(e.v, btic('1986/'))",
        )),
        // A disjunction is not a conjunct on the property.
        Case::unserved(q("btic_overlaps(e.v, btic('1985')) OR e.n = 1")),
        // Never rewritten: NULL, other-type and row-dependent operands.
        Case::unserved(q("btic_overlaps(e.v, null)")),
        Case::unserved(q("btic_overlaps(e.v, $c)")).param("c", Value::Null),
        Case::unserved(q("btic_overlaps(e.v, e.v)")),
        Case::unserved(q("e.v = '1985'")),
        Case::unserved(q("e.v = 5")),
        Case::unserved(q("e.v <> btic('1985')")),
        Case::unserved(q("e.v IS NULL")),
        Case::unserved(q("btic_contains_point(e.v, null)")),
    ];
    // `btic_lo` / `btic_hi` are datetimes, NULL for an unbounded side.
    for at in [
        "datetime('1985-01-01T00:00:00Z')",
        "datetime('1985-01-01T00:00:00.0000001Z')",
        "datetime('1984-12-31T23:59:59.9999999Z')",
        "datetime('1986-01-01T00:00:00Z')",
    ] {
        for op in ["=", "<", "<=", ">", ">="] {
            for f in ["btic_lo", "btic_hi"] {
                cases.push(Case::served(q(&format!("{f}(e.v) {op} {at}"))));
                cases.push(Case::served(q(&format!("{at} {op} {f}(e.v)"))));
            }
        }
    }
    cases.extend([
        Case::served(q("btic_lo(e.v) <= $t AND btic_hi(e.v) > $t")).param(
            "t",
            Value::Temporal(uni_db::common::TemporalValue::DateTime {
                nanos_since_epoch: 486_000_000_000_000_000,
                offset_seconds: 0,
                timezone_name: None,
            }),
        ),
        // Cross-type: a datetime against an integer is never true.
        Case::unserved(q("btic_lo(e.v) <= 486000000000")),
        Case::unserved(q("btic_lo(e.v) <= localdatetime('1985-01-01T00:00:00')")),
        Case::unserved(q("btic_lo(e.v) IS NULL")),
        Case::unserved(q("btic_lo(e.v) <> datetime('1985-01-01T00:00:00Z')")),
    ]);
    diff.stages(&writes(), &cases).await
}

/// Declared over flushed rows after a flush that wrote a delete: the key
/// columns are backfilled, a deleted row's tombstone is not decoded as an
/// interval, and the next flush sees the rewritten table's schema.
#[tokio::test]
async fn btic_differential_index_after_data() -> Result<()> {
    let diff = Differential::open(SCHEMA, "RETURN 1", "Span", "v").await?;
    let w = writes();
    for stmt in &w.initial {
        diff.write(stmt).await?;
    }
    diff.flush().await?;
    diff.write("MATCH (e:Span {id: 8}) DELETE e").await?;
    diff.flush().await?;
    super::harness::run(&diff.indexed, INDEX).await?;
    let mut cases = predicate_cases(&diff, &["btic_overlaps", "btic_starts"]).await?;
    cases.push(Case::served(q("e.v = btic('1985')")));
    diff.check("declared after a flush", &cases, true).await?;
    for stmt in &w.changes {
        diff.write(stmt).await?;
    }
    diff.check("changed, unflushed", &cases, false).await?;
    diff.flush().await?;
    diff.check("changed, flushed", &cases, true).await
}
