// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T11: a TIME index answers exactly as no index does.
//!
//! `WHERE` compares TIMEs by `nanos_since_midnight − offset · 10⁹` with no
//! modulo, so `time('23:00-05:00')` (28 h UTC) is after `time('04:00Z')` and
//! not equal to it, and `time('10:00+14:00')` is before midnight UTC. A
//! `localtime` stored in the TIME column (offset NULL) compares as NULL with
//! every TIME, so its key is NULL. Values 1 ns apart, NULL; `=`, ranges,
//! `IN` (structural), a pattern map, literals and parameters, commuted
//! comparisons; `<>`, `localtime` and other-type operands, which are never
//! rewritten. The key's lifecycle (declared before and after data, reopen,
//! the upgrade of a #356-era entry) is here too.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(time_differential)'

// Rust guideline compliant

use super::harness::{Case, Differential, Writes, run};
use anyhow::Result;
use uni_db::common::TemporalValue;
use uni_db::{Uni, UniConfig, Value};

const SCHEMA: &[&str] = &["CREATE LABEL Shift (id INT, t TIME, n INT)"];
const INDEX: &str = "CREATE INDEX idx_t FOR (s:Shift) ON (s.t)";
const HOUR: i64 = 3_600_000_000_000;

fn time(nanos: i64, offset_seconds: i32) -> Value {
    Value::Temporal(TemporalValue::Time {
        nanos_since_midnight: nanos,
        offset_seconds,
    })
}

fn writes() -> Writes {
    Writes {
        initial: vec![
            "CREATE (:Shift {id: 1, t: time('23:00-05:00')}), \
                    (:Shift {id: 2, t: time('04:00Z')}), \
                    (:Shift {id: 3, t: time('10:00+14:00')}), \
                    (:Shift {id: 4, t: localtime('12:00')}), \
                    (:Shift {id: 5, t: time('12:00Z')}), \
                    (:Shift {id: 6, t: time('13:00+01:00')}), \
                    (:Shift {id: 7, t: time('12:00:00.000000001Z')}), \
                    (:Shift {id: 8, t: time('11:59:59.999999999Z')}), \
                    (:Shift {id: 9, n: 1}), \
                    (:Shift {id: 10, t: time('00:00Z')}), \
                    (:Shift {id: 11, t: time('23:59:59.999999999Z')})"
                .to_string(),
        ],
        changes: vec![
            "MATCH (s:Shift {id: 7}) SET s.t = time('01:00Z')".to_string(),
            "MATCH (s:Shift {id: 8}) DELETE s".to_string(),
            "MATCH (s:Shift {id: 9}) SET s.t = time('07:00-05:00')".to_string(),
            "MATCH (s:Shift {id: 4}) SET s.t = time('12:00Z')".to_string(),
            "MATCH (s:Shift {id: 5}) SET s.n = 2".to_string(),
            "CREATE (:Shift {id: 12, t: time('12:00Z')}), (:Shift {id: 13, t: localtime('04:00')})"
                .to_string(),
        ],
    }
}

fn cases() -> Vec<Case> {
    const RET: &str = "RETURN s.id AS id";
    let q = |w: &str| format!("MATCH (s:Shift) WHERE {w} {RET}");
    let mut cases = Vec::new();
    for op in ["=", "<", "<=", ">", ">="] {
        for lit in [
            "time('12:00Z')",
            "time('04:00Z')",
            "time('23:00-05:00')",
            "time('00:00+14:00')",
        ] {
            cases.push(Case::served(q(&format!("s.t {op} {lit}"))));
        }
        cases.push(Case::served(q(&format!("s.t {op} $t"))).param("t", time(12 * HOUR, 0)));
        // The same UTC time at another offset is the same bound.
        cases.push(Case::served(q(&format!("s.t {op} $t"))).param("t", time(7 * HOUR, -18_000)));
        // +14:00: before midnight UTC.
        cases.push(Case::served(q(&format!("s.t {op} $t"))).param("t", time(10 * HOUR, 50_400)));
        // Commuted: oriented before analysis.
        cases.push(Case::served(q(&format!("time('12:00Z') {op} s.t"))));
        // 1 ns away from stored values.
        cases.push(Case::served(q(&format!("s.t {op} $t"))).param("t", time(12 * HOUR + 1, 0)));
    }
    cases.extend([
        Case::served(q("s.t = time({hour: 13, minute: 0, timezone: '+01:00'})")),
        Case::served(q("s.t IN [time('12:00Z'), time('04:00Z'), null]")),
        Case::served(q("s.t IN $list")).param(
            "list",
            Value::List(vec![time(13 * HOUR, 3600), time(23 * HOUR, -18_000)]),
        ),
        Case::served(format!("MATCH (s:Shift {{t: time('12:00Z')}}) {RET}")),
        Case::served(q(
            "s.t >= time('04:00Z') AND s.t < time('12:00:00.000000001Z')",
        )),
        // A disjunction is not a conjunct on the property.
        Case::unserved(q("s.t > time('12:00Z') OR s.n = 1")),
        // Never rewritten: `<>`, `localtime`, cross-type and NULL operands.
        Case::unserved(q("s.t <> time('12:00Z')")),
        Case::unserved(q("s.t = localtime('12:00')")),
        Case::unserved(q("s.t < localtime('13:00')")),
        Case::unserved(q("s.t IN [localtime('12:00')]")),
        Case::unserved(q("s.t = '12:00Z'")),
        Case::unserved(q("s.t < datetime('2026-01-01T12:00:00Z')")),
        Case::unserved(q("s.t = 5")),
        Case::unserved(q("s.t = null")),
        Case::unserved(q("s.t = $s")).param("s", Value::String("12:00Z".into())),
        Case::unserved(q("s.t IS NULL")),
        Case::unserved(q("s.t.hour = 12 AND s.t <> time('04:00Z')")),
    ]);
    cases
}

#[tokio::test]
async fn time_differential_index_before_data() -> Result<()> {
    let diff = Differential::open(SCHEMA, INDEX, "Shift", "t").await?;
    diff.stages(&writes(), &cases()).await
}

#[tokio::test]
async fn time_differential_index_after_data() -> Result<()> {
    let diff = Differential::open(SCHEMA, "RETURN 1", "Shift", "t").await?;
    let w = writes();
    for stmt in &w.initial {
        diff.write(stmt).await?;
    }
    diff.flush().await?;
    // Declared over flushed rows: the key column is backfilled.
    run(&diff.indexed, INDEX).await?;
    let cases = cases();
    diff.check("declared after a flush", &cases, true).await?;
    for stmt in &w.changes {
        diff.write(stmt).await?;
    }
    diff.check("changed, unflushed", &cases, false).await?;
    diff.flush().await?;
    diff.check("changed, flushed", &cases, true).await
}

const QUERY: &str = "MATCH (s:Shift) WHERE s.t >= time('12:00Z') RETURN s.id AS id";

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

async fn ids(db: &Uni) -> Result<Vec<i64>> {
    let rows = db.session().query(QUERY).await?;
    let mut out: Vec<i64> = rows
        .rows()
        .iter()
        .map(|r| r.get::<i64>("id"))
        .collect::<Result<_, _>>()?;
    out.sort_unstable();
    Ok(out)
}

fn keyed(db: &Uni) -> bool {
    matches!(
        db.storage().schema_manager().get_index("idx_t"),
        Some(uni_db::core::schema::IndexDefinition::Scalar(cfg)) if cfg.key.is_some()
    )
}

fn key_registered(db: &Uni) -> bool {
    db.storage()
        .schema_manager()
        .schema()
        .properties
        .get("Shift")
        .is_some_and(|p| p.contains_key("__key_time_t"))
}

async fn status(db: &Uni) -> Result<Option<String>> {
    Ok(db
        .get_label_info("Shift")
        .await?
        .and_then(|info| info.indexes.into_iter().find(|i| i.name == "idx_t"))
        .map(|i| i.status))
}

async fn used(db: &Uni) -> Result<bool> {
    let explain = db.session().query_with(QUERY).explain().await?;
    Ok(explain
        .index_usage
        .iter()
        .any(|u| u.property == "t" && u.used))
}

/// Declared before data, dropped (the key column goes with it), declared
/// again over flushed rows, and reopened.
#[tokio::test]
async fn time_differential_lifecycle_and_reopen() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    {
        let db = Uni::open(&path).config(config()).build().await?;
        run(&db, SCHEMA[0]).await?;
        run(&db, INDEX).await?;
        assert!(keyed(&db) && key_registered(&db));
        run(
            &db,
            "CREATE (:Shift {id: 1, t: time('13:00+01:00')}), (:Shift {id: 2, t: time('11:00Z')})",
        )
        .await?;
        db.flush().await?;
        assert_eq!(status(&db).await?, Some("ONLINE".into()));
        assert_eq!(ids(&db).await?, vec![1]);
        assert!(used(&db).await?);
        run(&db, "DROP INDEX idx_t").await?;
        assert!(!key_registered(&db), "the key column outlived its index");
        run(&db, "CREATE (:Shift {id: 3, t: time('07:00-05:00')})").await?;
        db.flush().await?;
        run(&db, INDEX).await?;
        assert_eq!(ids(&db).await?, vec![1, 3]);
        assert!(used(&db).await?);
        db.shutdown().await?;
    }
    let db = Uni::open(&path).config(config()).build().await?;
    assert!(keyed(&db));
    assert_eq!(status(&db).await?, Some("ONLINE".into()));
    assert_eq!(ids(&db).await?, vec![1, 3]);
    assert!(used(&db).await?);
    run(&db, "CREATE (:Shift {id: 5, t: time('23:00Z')})").await?;
    assert_eq!(ids(&db).await?, vec![1, 3, 5], "unflushed after reopen");
    db.flush().await?;
    assert_eq!(ids(&db).await?, vec![1, 3, 5]);
    Ok(())
}

/// The state a #356-era catalog holds for a TIME index (recorded unkeyed
/// and `Failed`) is upgraded to a keyed index by the next writable open.
#[tokio::test]
async fn time_differential_upgrade_of_a_failed_index_at_open() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    {
        let db = Uni::open(&path).config(config()).build().await?;
        // Declared while `t` was undeclared, then `t` typed TIME.
        run(&db, "CREATE LABEL Shift (id INT)").await?;
        run(&db, "CREATE (:Shift {id: 0})").await?;
        db.flush().await?;
        run(&db, INDEX).await?;
        run(&db, "ALTER LABEL Shift ADD PROPERTY t TIME").await?;
        assert!(!keyed(&db), "the scenario needs an unkeyed index");
        db.storage()
            .schema_manager()
            .update_index_metadata("idx_t", |m| {
                m.status = uni_db::core::schema::IndexStatus::Failed;
            })?;
        db.storage().schema_manager().save().await?;
        run(
            &db,
            "CREATE (:Shift {id: 1, t: time('12:00Z')}), (:Shift {id: 2, t: time('12:00+01:00')})",
        )
        .await?;
        db.flush().await?;
        assert!(!keyed(&db), "a flush must not upgrade it");
        db.shutdown().await?;
    }
    let db = Uni::open(&path).config(config()).build().await?;
    assert!(keyed(&db), "the writable open must key it");
    assert!(key_registered(&db));
    assert_eq!(status(&db).await?, Some("ONLINE".into()));
    assert_eq!(ids(&db).await?, vec![1]);
    assert!(used(&db).await?);
    Ok(())
}
