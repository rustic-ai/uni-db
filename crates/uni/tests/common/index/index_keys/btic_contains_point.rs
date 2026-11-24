// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T10: `btic_contains_point` through a BTIC index answers exactly as no
//! index does, for every point form the predicate converts: an integer (ms),
//! a datetime (nanoseconds divided by 10^6, truncating toward zero, so a
//! pre-epoch sub-millisecond instant rounds up to the next millisecond), a
//! localdatetime and a date (days times 86 400 000).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(btic_contains_point)'

// Rust guideline compliant

use super::btic_differential::{INDEX, SCHEMA, writes};
use super::harness::{Case, Differential};
use anyhow::Result;
use uni_db::Value;
use uni_db::common::TemporalValue;

/// `1985-01-01T00:00:00Z` in ms: the lower bound of `btic('1985')`.
const Y1985: i64 = 473_385_600_000;
/// `1986-01-01T00:00:00Z` in ms.
const Y1986: i64 = 504_921_600_000;

fn q(w: &str) -> String {
    format!("MATCH (e:Span) WHERE {w} RETURN e.id AS id")
}

fn datetime(nanos: i64) -> Value {
    Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: nanos,
        offset_seconds: 0,
        timezone_name: None,
    })
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for ms in [
        Y1985,
        Y1985 - 1,
        Y1986,
        Y1986 - 1,
        0,
        -1,
        1,
        1_600_000_000_000,
    ] {
        cases.push(Case::served(q(&format!("btic_contains_point(e.v, {ms})"))));
        cases.push(Case::served(q("btic_contains_point(e.v, $t)")).param("t", Value::Int(ms)));
    }
    for at in [
        "datetime('1985-01-01T00:00:00Z')",
        "datetime('1985-12-31T23:59:59.999999999Z')",
        "datetime('1986-01-01T00:00:00Z')",
        "datetime('1986-01-01T01:00:00+01:00')",
        // Pre-epoch, sub-millisecond: -0.5 ms truncates to 0 ms, inside 1970.
        "datetime('1969-12-31T23:59:59.9995Z')",
        "datetime('1969-12-31T23:59:59.999Z')",
        "localdatetime('1985-01-01T00:00:00')",
        "date('1985-01-01')",
        "date('1970-01-01')",
        "date('1969-12-31')",
    ] {
        cases.push(Case::served(q(&format!("btic_contains_point(e.v, {at})"))));
    }
    for nanos in [-500_000, -1, -1_000_000, Y1985 * 1_000_000 - 1] {
        cases.push(Case::served(q("btic_contains_point(e.v, $t)")).param("t", datetime(nanos)));
    }
    cases.push(Case::served(q("btic_contains_point(e.v, $t)")).param(
        "t",
        Value::Temporal(TemporalValue::Date {
            days_since_epoch: -1,
        }),
    ));
    cases.extend([
        Case::served(q(
            "btic_contains_point(e.v, 486000000000) AND btic_overlaps(e.v, btic('1985'))",
        )),
        // Never rewritten: NULL and row-dependent points.
        Case::unserved(q("btic_contains_point(e.v, $t)")).param("t", Value::Null),
        Case::unserved(q("btic_contains_point(e.v, e.n)")),
    ]);
    cases
}

#[tokio::test]
async fn btic_contains_point_every_point_form() -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Span", "v").await?;
    diff.stages(&writes(), &cases()).await
}
