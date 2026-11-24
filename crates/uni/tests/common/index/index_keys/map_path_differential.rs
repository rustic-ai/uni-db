// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! T12: an index on a typed MAP key path (`n.m.k`) answers exactly as no
//! index does.
//!
//! The key is the value at the path as a read of `n.m.k` returns it (an
//! integer written into a float map as a float), NULL where the key is
//! missing. INT32 and FLOAT32 maps are keyed too (unit tests in
//! `index_keys.rs`), but are not here: an unflushed row of such a map fails
//! to read today (`Incorrect datatype for StructArray field "value"`),
//! indexed or not. Two paths on one map coexist; integer, float and string maps;
//! `=`, ranges, `IN`, literals and parameters, commuted comparisons; other
//! types, `<>` and the whole map are never rewritten. Declared through Cypher
//! DDL, `uni.schema.createIndex` and the schema builder, before and after
//! data, and reopened.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(map_path_differential)'

// Rust guideline compliant

use super::harness::{Case, Differential, Writes, run};
use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig, Value};

const SCHEMA: &[&str] = &["CREATE LABEL Doc (id INT, m MAP<STRING, INT>, \
     f MAP<STRING, FLOAT>, s MAP<STRING, STRING>, n INT)"];

/// Every index of the run, with the logical property it is reported on.
const INDEXES: &[(&str, &str)] = &[
    ("CREATE INDEX idx_ma FOR (d:Doc) ON (d.m.a)", "m.a"),
    ("CREATE INDEX idx_mb FOR (d:Doc) ON (d.m.b)", "m.b"),
    ("CREATE INDEX idx_fx FOR (d:Doc) ON (d.f.x)", "f.x"),
    ("CREATE INDEX idx_sname FOR (d:Doc) ON (d.s.name)", "s.name"),
];

fn writes() -> Writes {
    Writes {
        initial: vec![
            "CREATE (:Doc {id: 1, m: {a: 1, b: 10}, f: {x: 1.5}, s: {name: 'ann'}}), \
                    (:Doc {id: 2, m: {a: 2}, f: {x: -0.0}, s: {name: 'bob'}}), \
                    (:Doc {id: 3, m: {b: 10}, f: {x: 2}, s: {other: 'x'}}), \
                    (:Doc {id: 4, m: {a: -5, b: 20}, f: {y: 1.0}, s: {name: 'Ann'}}), \
                    (:Doc {id: 5, n: 1}), \
                    (:Doc {id: 6, m: {}, f: {x: 0.0}, s: {name: ''}}), \
                    (:Doc {id: 7, m: {a: 1}, f: {x: 1e300}, s: {name: 'ann'}})"
                .to_string(),
        ],
        changes: vec![
            "MATCH (d:Doc {id: 1}) SET d.m = {a: 3, b: 10}".to_string(),
            "MATCH (d:Doc {id: 2}) DELETE d".to_string(),
            "MATCH (d:Doc {id: 5}) SET d.m = {a: 1}, d.f = {x: 1.5}, d.s = {name: 'bob'}"
                .to_string(),
            "MATCH (d:Doc {id: 4}) SET d.n = 2".to_string(),
            "MATCH (d:Doc {id: 7}) REMOVE d.m".to_string(),
            "CREATE (:Doc {id: 8, m: {a: 2, b: 2}, f: {x: -2.5}, s: {name: 'cy'}})".to_string(),
        ],
    }
}

/// The cases per reported property.
fn cases() -> Vec<(&'static str, Vec<Case>)> {
    const RET: &str = "RETURN d.id AS id";
    let q = |w: &str| format!("MATCH (d:Doc) WHERE {w} {RET}");
    let ops = ["=", "<", "<=", ">", ">="];
    let mut m_a = Vec::new();
    let mut m_b = Vec::new();
    let mut f_x = Vec::new();
    let mut s_name = Vec::new();
    for op in ops {
        m_a.push(Case::served(q(&format!("d.m.a {op} 1"))));
        m_a.push(Case::served(q(&format!("d.m.a {op} $v"))).param("v", Value::Int(2)));
        m_a.push(Case::served(q(&format!("-5 {op} d.m.a"))));
        m_b.push(Case::served(q(&format!("d.m.b {op} 10"))));
        f_x.push(Case::served(q(&format!("d.f.x {op} 1.5"))));
        f_x.push(Case::served(q(&format!("d.f.x {op} 2"))));
        f_x.push(Case::served(q(&format!("d.f.x {op} -0.0"))));
        f_x.push(Case::served(q(&format!("d.f.x {op} $v"))).param("v", Value::Float(0.0)));
        s_name.push(Case::served(q(&format!("d.s.name {op} 'ann'"))));
        s_name.push(
            Case::served(q(&format!("d.s.name {op} $v"))).param("v", Value::String("Ann".into())),
        );
    }
    m_a.extend([
        Case::served(q("d.m.a IN [1, 2, null]")),
        Case::served(q("d.m.a IN $list"))
            .param("list", Value::List(vec![Value::Int(-5), Value::Int(3)])),
        Case::served(q("d.m.a >= 1 AND d.m.a < 3")),
        // Both paths of one map in one query.
        Case::served(q("d.m.a = 1 AND d.m.b = 10")),
        // Never rewritten: `<>`, other types, NULL, a disjunction.
        Case::unserved(q("d.m.a <> 1")),
        Case::unserved(q("d.m.a = 1.0")),
        Case::unserved(q("d.m.a < 1.5")),
        Case::unserved(q("d.m.a = '1'")),
        Case::unserved(q("d.m.a = null")),
        Case::unserved(q("d.m.a IS NULL")),
        Case::unserved(q("d.m.a = 1 OR d.n = 1")),
        Case::unserved(q("d.m.a IN ['x']")),
    ]);
    f_x.extend([
        Case::served(q("d.f.x IN [1.5, 2, -0.0]")),
        Case::unserved(q("d.f.x = '1.5'")),
        Case::unserved(q("d.f.x < $v")).param("v", Value::Float(f64::NAN)),
        Case::unserved(q("d.f.x < $v")).param("v", Value::Float(f64::INFINITY)),
    ]);
    s_name.extend([
        Case::served(q("d.s.name IN ['ann', 'cy']")),
        Case::unserved(q("d.s.name = 1")),
        Case::unserved(q("d.s.name STARTS WITH 'a'")),
    ]);
    vec![("m.a", m_a), ("m.b", m_b), ("f.x", f_x), ("s.name", s_name)]
}

/// Run every property's cases at one stage.
async fn check(
    diff: &mut Differential,
    stage: &str,
    cases: &[(&'static str, Vec<Case>)],
    flushed: bool,
) -> Result<()> {
    for (property, cases) in cases {
        diff.property = (*property).to_string();
        diff.check(&format!("{stage}, {property}"), cases, flushed)
            .await?;
    }
    Ok(())
}

async fn open() -> Result<Differential> {
    Differential::open(SCHEMA, "RETURN 1", "Doc", "m.a").await
}

async fn declare(db: &Uni) -> Result<()> {
    for (stmt, _) in INDEXES {
        run(db, stmt).await?;
    }
    Ok(())
}

/// The four stages over `writes`.
async fn stages(diff: &mut Differential) -> Result<()> {
    let w = writes();
    let cases = cases();
    for stmt in &w.initial {
        diff.write(stmt).await?;
    }
    check(diff, "unflushed", &cases, false).await?;
    diff.flush().await?;
    check(diff, "flushed", &cases, true).await?;
    for stmt in &w.changes {
        diff.write(stmt).await?;
    }
    check(diff, "changed, unflushed", &cases, false).await?;
    diff.flush().await?;
    check(diff, "changed, flushed", &cases, true).await
}

#[tokio::test]
async fn map_path_differential_index_before_data() -> Result<()> {
    let mut diff = open().await?;
    declare(&diff.indexed).await?;
    stages(&mut diff).await
}

#[tokio::test]
async fn map_path_differential_index_after_data() -> Result<()> {
    let mut diff = open().await?;
    let w = writes();
    for stmt in &w.initial {
        diff.write(stmt).await?;
    }
    diff.flush().await?;
    // Declared over flushed rows: the key columns are backfilled, each
    // rewrite keeping the other paths' indexes.
    declare(&diff.indexed).await?;
    let cases = cases();
    check(&mut diff, "declared after a flush", &cases, true).await?;
    for stmt in &w.changes {
        diff.write(stmt).await?;
    }
    check(&mut diff, "changed, unflushed", &cases, false).await?;
    diff.flush().await?;
    check(&mut diff, "changed, flushed", &cases, true).await
}

fn config() -> UniConfig {
    let mut config = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    config.compaction.enabled = false;
    config
}

const QUERY: &str = "MATCH (d:Doc) WHERE d.m.a >= 2 RETURN d.id AS id";

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

/// The keyed index on `m.a`, if any: its name and key columns.
fn path_index(db: &Uni) -> Option<(String, Vec<String>)> {
    let schema = db.storage().schema_manager().schema();
    schema.indexes.iter().find_map(|idx| match idx {
        uni_db::core::schema::IndexDefinition::Scalar(cfg)
            if cfg.label == "Doc" && cfg.properties == ["m.a"] =>
        {
            let key = cfg.key.as_ref()?;
            matches!(key.kind, uni_db::core::schema::KeyKind::MapPath { .. })
                .then(|| (cfg.name.clone(), key.columns.clone()))
        }
        _ => None,
    })
}

async fn status(db: &Uni, name: &str) -> Result<Option<String>> {
    Ok(db
        .get_label_info("Doc")
        .await?
        .and_then(|info| info.indexes.into_iter().find(|i| i.name == name))
        .map(|i| i.status))
}

async fn used(db: &Uni) -> Result<bool> {
    let explain = db.session().query_with(QUERY).explain().await?;
    Ok(explain
        .index_usage
        .iter()
        .any(|u| u.property == "m.a" && u.used))
}

async fn insert(db: &Uni, from: i64) -> Result<()> {
    run(
        db,
        &format!(
            "CREATE (:Doc {{id: {from}, m: {{a: 2}}}}), (:Doc {{id: {}, m: {{a: 1}}}})",
            from + 1
        ),
    )
    .await
}

/// The index through a declaration surface, before data or after it, is a
/// keyed `MapPath` index that serves `d.m.a >= 2`, and survives a reopen.
async fn surface(declare: impl AsyncFnOnce(&Uni) -> Result<()>, before: bool) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_string_lossy().to_string();
    let name = {
        let db = Uni::open(&path).config(config()).build().await?;
        db.schema()
            .label("Doc")
            .property("id", DataType::Int64)
            .property(
                "m",
                DataType::Map(Box::new(DataType::String), Box::new(DataType::Int64)),
            )
            .apply()
            .await?;
        if !before {
            insert(&db, 1).await?;
            db.flush().await?;
        }
        declare(&db).await?;
        if before {
            insert(&db, 1).await?;
            db.flush().await?;
        }
        let (name, columns) = path_index(&db).expect("a keyed MapPath index on m.a");
        assert!(
            db.storage()
                .schema_manager()
                .schema()
                .properties
                .get("Doc")
                .is_some_and(|p| columns.iter().all(|c| p.contains_key(c))),
            "key columns registered"
        );
        assert_eq!(status(&db, &name).await?, Some("ONLINE".into()));
        assert_eq!(ids(&db).await?, vec![1]);
        assert!(used(&db).await?);
        insert(&db, 3).await?;
        assert_eq!(ids(&db).await?, vec![1, 3], "unflushed");
        db.flush().await?;
        assert_eq!(ids(&db).await?, vec![1, 3]);
        db.shutdown().await?;
        name
    };
    let db = Uni::open(&path).config(config()).build().await?;
    assert_eq!(path_index(&db).map(|(n, _)| n), Some(name.clone()));
    assert_eq!(status(&db, &name).await?, Some("ONLINE".into()));
    assert_eq!(ids(&db).await?, vec![1, 3]);
    assert!(used(&db).await?);
    Ok(())
}

#[tokio::test]
async fn map_path_differential_cypher_ddl() -> Result<()> {
    for before in [true, false] {
        surface(
            async |db: &Uni| run(db, "CREATE INDEX idx_ma FOR (d:Doc) ON (d.m.a)").await,
            before,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn map_path_differential_procedure() -> Result<()> {
    for before in [true, false] {
        surface(
            async |db: &Uni| {
                run(
                    db,
                    "CALL uni.schema.createIndex('Doc', 'm.a', {type: 'BTREE', name: 'idx_ma'})",
                )
                .await
            },
            before,
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn map_path_differential_schema_builder() -> Result<()> {
    for before in [true, false] {
        surface(
            async |db: &Uni| {
                db.schema()
                    .label("Doc")
                    .index("m.a", IndexType::Scalar(ScalarType::BTree))
                    .apply()
                    .await?;
                Ok(())
            },
            before,
        )
        .await?;
    }
    Ok(())
}

/// A key path into a map whose values cannot be keyed, a deeper path, and
/// a composite index over a path are refused; a path whose head is not a
/// MAP is not a key path.
#[tokio::test]
async fn map_path_differential_refusals() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    run(
        &db,
        "CREATE LABEL Doc (id INT, b MAP<STRING, BOOL>, m MAP<STRING, INT>, l MAP<STRING, LIST<INT>>)",
    )
    .await?;
    for stmt in [
        "CREATE INDEX i1 FOR (d:Doc) ON (d.b.k)",
        "CREATE INDEX i2 FOR (d:Doc) ON (d.l.k)",
        "CREATE INDEX i3 FOR (d:Doc) ON (d.m.a.b)",
        "CREATE INDEX i4 FOR (d:Doc) ON (d.m.a, d.id)",
    ] {
        let err = run(&db, stmt).await.expect_err(stmt);
        assert!(err.to_string().contains("cannot index"), "{stmt}: {err}");
    }
    Ok(())
}
