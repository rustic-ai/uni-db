// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T2: a DATETIME index answers exactly as no index does.
//!
//! The same instant at different offsets and in a zone, values 1 ns apart, a
//! pre-epoch value, a `localdatetime` stored in the column (offset NULL), a
//! NULL; `=`, ranges, `IN` (structural) against `=` (by instant), a pattern
//! map, literals and parameters, commuted comparisons; `<>` and cross-type
//! operands, which are never rewritten.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(datetime_differential)'

// Rust guideline compliant

use super::harness::{Case, Differential, Writes};
use anyhow::Result;
use uni_db::Value;
use uni_db::common::TemporalValue;

pub(super) const SCHEMA: &[&str] = &["CREATE LABEL Event (id INT, ts DATETIME, n INT)"];
pub(super) const INDEX: &str = "CREATE INDEX idx_ts FOR (e:Event) ON (e.ts)";

/// The instant `2026-01-01T12:00:00Z`, in nanoseconds.
pub(super) const NOON: i64 = 1_767_268_800_000_000_000;

pub(super) fn datetime(nanos: i64, offset_seconds: i32, zone: Option<&str>) -> Value {
    Value::Temporal(TemporalValue::DateTime {
        nanos_since_epoch: nanos,
        offset_seconds,
        timezone_name: zone.map(str::to_string),
    })
}

pub(super) fn writes() -> Writes {
    Writes {
        initial: vec![
            "CREATE (:Event {id: 1, ts: datetime('2026-01-01T12:00:00Z')}), \
                    (:Event {id: 2, ts: datetime('2026-01-01T13:00:00+01:00')}), \
                    (:Event {id: 3, ts: datetime('2026-01-01T13:00:00[Europe/Paris]')}), \
                    (:Event {id: 4, ts: datetime('2026-01-01T12:00:00.000000001Z')}), \
                    (:Event {id: 5, ts: datetime('2026-01-01T11:59:59.999999999Z')}), \
                    (:Event {id: 6, ts: localdatetime('2026-01-01T12:00:00')}), \
                    (:Event {id: 7, n: 1}), \
                    (:Event {id: 8, ts: datetime('1969-12-31T23:59:59Z')})"
                .to_string(),
        ],
        changes: vec![
            "MATCH (e:Event {id: 4}) SET e.ts = datetime('2025-06-01T00:00:00Z')".to_string(),
            "MATCH (e:Event {id: 5}) DELETE e".to_string(),
            "MATCH (e:Event {id: 7}) SET e.ts = datetime('2026-01-01T07:00:00-05:00')".to_string(),
            "MATCH (e:Event {id: 1}) SET e.n = 2".to_string(),
            "CREATE (:Event {id: 9, ts: datetime('2026-01-01T12:00:00Z')})".to_string(),
        ],
    }
}

fn cases() -> Vec<Case> {
    const RET: &str = "RETURN e.id AS id";
    let q = |w: &str| format!("MATCH (e:Event) WHERE {w} {RET}");
    let noon = "datetime('2026-01-01T12:00:00Z')";
    let mut cases = Vec::new();
    for op in ["=", "<", "<=", ">", ">="] {
        cases.push(Case::served(q(&format!("e.ts {op} {noon}"))));
        cases.push(Case::served(q(&format!("e.ts {op} $t"))).param("t", datetime(NOON, 0, None)));
        // The same instant at another offset is the same bound.
        cases.push(
            Case::served(q(&format!("e.ts {op} $t")))
                .param("t", datetime(NOON, -18_000, Some("America/New_York"))),
        );
        // Commuted: oriented before analysis.
        cases.push(Case::served(q(&format!("{noon} {op} e.ts"))));
        // 1 ns away from stored values.
        cases.push(
            Case::served(q(&format!("e.ts {op} $t"))).param("t", datetime(NOON + 1, 0, None)),
        );
    }
    cases.extend([
        Case::served(q(
            "e.ts = datetime({year: 2026, month: 1, day: 1, hour: 13, timezone: '+01:00'})",
        )),
        Case::served(q(&format!(
            "e.ts IN [{noon}, datetime('1969-12-31T23:59:59Z'), null]"
        ))),
        Case::served(q("e.ts IN $list")).param(
            "list",
            Value::List(vec![
                datetime(NOON, 3600, None),
                datetime(NOON + 1, 0, None),
            ]),
        ),
        Case::served(format!("MATCH (e:Event {{ts: {noon}}}) {RET}")),
        Case::served(q(&format!(
            "e.ts >= {noon} AND e.ts < datetime('2027-01-01T00:00:00Z')"
        ))),
        // A disjunction is not a conjunct on the property.
        Case::unserved(q(&format!("e.ts > {noon} OR e.n = 1"))),
        // Never rewritten: `<>`, cross-type and NULL operands.
        Case::unserved(q(&format!("e.ts <> {noon}"))),
        Case::unserved(q("e.ts = '2026-01-01T12:00:00Z'")),
        Case::unserved(q("e.ts = localdatetime('2026-01-01T12:00:00')")),
        Case::unserved(q("e.ts < date('2026-01-02')")),
        Case::unserved(q("e.ts = 5")),
        Case::unserved(q("e.ts = null")),
        Case::unserved(q("e.ts = $s")).param("s", Value::String("2026-01-01T12:00:00Z".into())),
        Case::unserved(q("e.ts IN [1, 'x']")),
        Case::unserved(q("e.ts IS NULL")),
        Case::unserved(q(&format!("e.ts.year = 2026 AND e.ts <> {noon}"))),
    ]);
    cases
}

#[tokio::test]
async fn datetime_differential_index_before_data() -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Event", "ts").await?;
    diff.stages(&writes(), &cases()).await
}

#[tokio::test]
async fn datetime_differential_index_after_data() -> Result<()> {
    let diff = Differential::open(SCHEMA, "RETURN 1", "Event", "ts").await?;
    let w = writes();
    for stmt in &w.initial {
        diff.write(stmt).await?;
    }
    diff.flush().await?;
    // Declared over flushed rows: the key columns are backfilled.
    super::harness::run(&diff.indexed, INDEX).await?;
    let cases = cases();
    diff.check("declared after a flush", &cases, true).await?;
    for stmt in &w.changes {
        diff.write(stmt).await?;
    }
    diff.check("changed, unflushed", &cases, false).await?;
    diff.flush().await?;
    diff.check("changed, flushed", &cases, true).await
}

/// Declared after a flush that wrote only a delete: the backfill's rewrite
/// adds the key column to a table whose schema the delete had cached, and
/// the next flush must see the rewritten schema (the cache was not
/// invalidated by `replace_table_atomic`, so the flush refused the key
/// column as foreign).
#[tokio::test]
async fn datetime_differential_index_after_a_flushed_delete() -> Result<()> {
    let diff = Differential::open(SCHEMA, "RETURN 1", "Event", "ts").await?;
    let w = writes();
    for stmt in &w.initial {
        diff.write(stmt).await?;
    }
    diff.flush().await?;
    diff.write("MATCH (e:Event {id: 8}) DELETE e").await?;
    diff.flush().await?;
    super::harness::run(&diff.indexed, INDEX).await?;
    let cases = cases();
    diff.check("declared after a flushed delete", &cases, true)
        .await?;
    for stmt in &w.changes {
        diff.write(stmt).await?;
    }
    diff.flush().await?;
    diff.check("changed, flushed", &cases, true).await
}
