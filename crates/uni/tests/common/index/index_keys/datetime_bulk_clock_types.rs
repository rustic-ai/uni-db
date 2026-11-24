// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T6: bulk load and compaction fill the key column; answers equal an
//! unindexed database after each.
//!
//! T7: the statement clock. A no-argument `datetime()` folds once per
//! statement, to the instant the predicate kept above the scan reads too;
//! `datetime.realtime()` never folds.
//!
//! T8: a literal of another type against a DATETIME property is never pushed
//! to storage (`index_operand_fits_column`): the query answers as without the
//! index, rather than failing or coercing the literal.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(datetime_bulk) | test(datetime_clock) | test(datetime_types)'

// Rust guideline compliant

use super::datetime_differential::{INDEX, NOON, SCHEMA, datetime, writes};
use super::harness::{Case, Differential};
use anyhow::Result;
use std::collections::HashMap;
use uni_db::Value;
use uni_db::store::storage::compaction::Compactor;

fn range_cases() -> Vec<Case> {
    let q = |w: &str| format!("MATCH (e:Event) WHERE {w} RETURN e.id AS id");
    vec![
        Case::served(q("e.ts = datetime('2026-01-01T12:00:00Z')")),
        Case::served(q("e.ts >= $t")).param("t", datetime(NOON, 0, None)),
        Case::served(q("e.ts < datetime('2026-01-01T12:00:00Z')")),
    ]
}

#[tokio::test]
async fn datetime_bulk_load_and_compaction_fill_the_key() -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Event", "ts").await?;
    for db in [&diff.indexed, &diff.plain] {
        let tx = db.session().tx().await?;
        let mut bulk = tx.bulk_writer().build()?;
        let rows: Vec<HashMap<String, Value>> = (0..20_i64)
            .map(|i| {
                HashMap::from([
                    ("id".to_string(), Value::Int(i)),
                    (
                        "ts".to_string(),
                        datetime(NOON + (i - 10) * 1_000, (i as i32 % 3) * 3600, None),
                    ),
                ])
            })
            .collect();
        bulk.insert_vertices("Event", rows).await?;
        bulk.commit().await?;
        tx.commit().await?;
    }
    let cases = range_cases();
    diff.check("bulk loaded", &cases, true).await?;

    for stmt in writes().initial {
        diff.write(&stmt).await?;
    }
    diff.flush().await?;
    // A second version of some rows, so compaction merges them.
    diff.write("MATCH (e:Event) WHERE e.id < 5 SET e.n = 1")
        .await?;
    diff.flush().await?;
    for db in [&diff.indexed, &diff.plain] {
        let report = Compactor::new(db.storage())
            .compact_vertices("Event")
            .await?;
        assert!(report.ran);
    }
    diff.check("compacted", &cases, true).await
}

/// `datetime()` reads the statement clock: a row stamped just before the
/// statement is before it, one far in the future is after it, whatever the
/// index. `.realtime()` is answered, not folded into a bound.
#[tokio::test]
async fn datetime_clock_folds_once_per_statement() -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Event", "ts").await?;
    diff.write(
        "CREATE (:Event {id: 1, ts: datetime('2020-01-01T00:00:00Z')}), \
                (:Event {id: 2, ts: datetime('2200-01-01T00:00:00Z')})",
    )
    .await?;
    diff.flush().await?;
    let q = |w: &str| format!("MATCH (e:Event) WHERE {w} RETURN e.id AS id");
    let cases = vec![
        Case::served(q("e.ts < datetime()")),
        Case::served(q("e.ts > datetime()")),
        Case::served(q("datetime() >= e.ts")),
        Case::served(q("e.ts < datetime.statement()")),
        Case::served(q("e.ts > datetime.transaction()")),
        Case::unserved(q("e.ts < datetime.realtime()")),
    ];
    diff.check("clock", &cases, true).await?;
    // The same `datetime()` on both sides of one statement is one instant:
    // nothing is both before and at-or-after it.
    let both = diff
        .indexed
        .session()
        .query("MATCH (e:Event) WHERE e.ts < datetime() AND e.ts >= datetime() RETURN e.id AS id")
        .await?;
    assert!(both.is_empty());
    Ok(())
}

/// Every operand type against a DATETIME property, flushed: the answer is the
/// unindexed one and the query does not fail.
#[tokio::test]
async fn datetime_types_other_operands_are_not_pushed() -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Event", "ts").await?;
    for stmt in writes().initial {
        diff.write(&stmt).await?;
    }
    diff.flush().await?;
    let q = |w: &str| format!("MATCH (e:Event) WHERE {w} RETURN e.id AS id");
    let mut cases = Vec::new();
    for operand in [
        "'2026-01-01T12:00:00Z'",
        "1767268800000000000",
        "1.5",
        "true",
        "date('2026-01-01')",
        "localdatetime('2026-01-01T12:00:00')",
        "time('12:00:00Z')",
        "duration('P1D')",
        "[1, 2]",
        "{a: 1}",
    ] {
        for op in ["=", "<", ">="] {
            cases.push(Case::unserved(q(&format!("e.ts {op} {operand}"))));
        }
        cases.push(Case::unserved(q(&format!("e.ts IN [{operand}]"))));
    }
    for value in [
        Value::String("2026-01-01T12:00:00Z".into()),
        Value::Int(NOON),
        Value::Float(1.0),
        Value::Bool(false),
        Value::Null,
    ] {
        cases.push(Case::unserved(q("e.ts = $v")).param("v", value.clone()));
        cases.push(Case::unserved(q("e.ts > $v")).param("v", value));
    }
    diff.check("other operand types", &cases, true).await
}
