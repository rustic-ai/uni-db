// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Declaring a property over values it cannot hold is refused, before anything
//! changes; values it can hold are stored as a write of the declared property
//! would store them.
//!
//! Declaring a label over schemaless vertices (#304) or a property over
//! schemaless values (#249) moves the existing values into a typed column,
//! whose builder stores what it cannot hold as NULL. Without a check the
//! declaration silently lost those values.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(declared_type_conflict)'

use tempfile::TempDir;
use uni_db::{DataType, Result, Uni, UniError};

/// `(uid, age)` rows of `Repository`, ordered by uid, `age` rendered as text.
async fn ages(db: &Uni) -> Result<Vec<(String, String)>> {
    let r = db
        .session()
        .query(
            "MATCH (n:Repository) RETURN n.uid AS uid, toString(n.age) AS age \
             ORDER BY uid",
        )
        .await?;
    let mut out = Vec::new();
    for row in r.rows() {
        out.push((row.get::<String>("uid")?, row.get::<String>("age")?));
    }
    Ok(out)
}

fn expected_ages() -> Vec<(String, String)> {
    vec![
        ("a".to_string(), "old".to_string()),
        ("b".to_string(), "5".to_string()),
    ]
}

async fn run(db: &Uni, q: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(q).await?;
    tx.commit().await?;
    Ok(())
}

/// The refusal names the label, the property, the declared type, how many
/// values conflict and one of them.
fn assert_conflict(err: UniError) {
    match err {
        UniError::DeclaredTypeConflict { detail } => {
            let uni_db::DeclaredTypeConflict {
                entity,
                property,
                declared,
                count,
                example_id,
                example_value,
            } = *detail;
            assert_eq!(entity, "Repository");
            assert_eq!(property, "age");
            assert_eq!(declared, "Int64");
            assert_eq!(count, 1);
            assert!(example_id.starts_with("vid "), "{example_id}");
            assert!(example_value.contains("old"), "{example_value}");
        }
        other => panic!("expected DeclaredTypeConflict, got {other:?}"),
    }
}

/// A store whose `Repository` vertices carry `age` as a string on one and an
/// integer on the other; `declared` declares the label (without `age`) first.
async fn seeded(path: &str, declared: bool, flush: bool) -> Result<Uni> {
    let db = Uni::open(path).build().await?;
    if declared {
        db.schema()
            .label("Repository")
            .property("uid", DataType::String)
            .done()
            .apply()
            .await?;
    }
    run(
        &db,
        "CREATE (:Repository {uid: 'a', age: 'old'}), (:Repository {uid: 'b', age: 5})",
    )
    .await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// After a refusal: the catalog does not declare `age`, every value is still
/// there with its type, and so after a reopen.
async fn assert_unchanged(db: Uni, path: &str, label_declared: bool) -> Result<()> {
    let info = db.get_label_info("Repository").await?;
    assert_eq!(
        info.is_some(),
        label_declared,
        "label declaration unchanged"
    );
    if let Some(info) = info {
        assert!(
            info.properties.iter().all(|p| p.name != "age"),
            "age undeclared"
        );
    }
    assert_eq!(ages(&db).await?, expected_ages(), "values intact");
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    assert_eq!(
        ages(&db).await?,
        expected_ages(),
        "values intact after reopen"
    );
    db.flush().await?;
    assert_eq!(
        ages(&db).await?,
        expected_ages(),
        "values intact after flush"
    );
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn declared_type_conflict_refuses_label_declaration() -> Result<()> {
    for flush in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();
        let db = seeded(path, false, flush).await?;
        let err = db
            .schema()
            .label("Repository")
            .property("uid", DataType::String)
            .property_nullable("age", DataType::Int64)
            .done()
            .apply()
            .await
            .expect_err("a string age cannot become Int64");
        assert_conflict(err);
        assert_unchanged(db, path, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn declared_type_conflict_refuses_cypher_create_label() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded(path, false, true).await?;
    let err = run(&db, "CREATE LABEL Repository (uid STRING, age INT64)")
        .await
        .expect_err("a string age cannot become Int64");
    assert_conflict(err);
    assert_unchanged(db, path, false).await
}

#[tokio::test]
async fn declared_type_conflict_refuses_create_label_procedure() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded(path, false, true).await?;
    let err = db
        .session()
        .run(
            r#"CALL uni.schema.createLabel('Repository', {
                "properties": { "age": { "type": "INT64" } }
            })"#,
        )
        .await
        .expect_err("a string age cannot become Int64");
    assert_conflict(err);
    assert_unchanged(db, path, false).await
}

#[tokio::test]
async fn declared_type_conflict_refuses_load_schema() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    let path = path.to_str().unwrap();
    let db = seeded(path, false, true).await?;
    let schema_file = dir.path().join("schema.json");
    let donor = Uni::temporary().build().await?;
    donor
        .schema()
        .label("Repository")
        .property_nullable("age", DataType::Int64)
        .done()
        .apply()
        .await?;
    donor.save_schema(&schema_file).await?;
    donor.shutdown().await?;

    let err = db
        .load_schema(&schema_file)
        .await
        .expect_err("a string age cannot become Int64");
    assert_conflict(err);
    assert_unchanged(db, path, false).await
}

/// #249's path: a property added to a declared, populated label.
#[tokio::test]
async fn declared_type_conflict_refuses_property_on_declared_label() -> Result<()> {
    for flush in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();
        let db = seeded(path, true, flush).await?;
        let err = db
            .schema()
            .label("Repository")
            .property_nullable("age", DataType::Int64)
            .done()
            .apply()
            .await
            .expect_err("a string age cannot become Int64");
        assert_conflict(err);
        assert_unchanged(db, path, true).await?;
    }
    Ok(())
}

#[tokio::test]
async fn declared_type_conflict_refuses_alter_add_property() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded(path, true, true).await?;
    let err = run(&db, "ALTER LABEL Repository ADD PROPERTY age INT64")
        .await
        .expect_err("a string age cannot become Int64");
    assert_conflict(err);
    assert_unchanged(db, path, true).await
}

/// `(score, when)` of `label`'s single vertex as text, read through the
/// label (its typed columns) and through an unlabeled scan (the main table's
/// `props_json`), which must agree.
async fn widened(db: &Uni, label: &str) -> Result<(String, String)> {
    let mut seen = Vec::new();
    for q in [
        format!(
            "MATCH (n:{label}) RETURN toString(n.score) AS score, toString(n.when) AS w, \
             n.when.year AS y, toString(n.score) AS ts, \
             n.when = datetime('2024-01-02T03:04:05Z') AS tw"
        ),
        format!(
            "MATCH (n) WHERE '{label}' IN labels(n) RETURN toString(n.score) AS score, \
             toString(n.when) AS w, n.when.year AS y, toString(n.score) AS ts, \
             n.when = datetime('2024-01-02T03:04:05Z') AS tw"
        ),
    ] {
        let r = db.session().query(&q).await?;
        let row = &r.rows()[0];
        assert_eq!(row.get::<i64>("y")?, 2024, "{label}.when: {q}");
        assert_eq!(
            row.get::<bool>("tw").ok(),
            Some(true),
            "{label}.when must be a datetime: {q}"
        );
        seen.push((
            row.get::<String>("score")?,
            row.get::<String>("w")?,
            row.get::<String>("ts")?,
            row.get::<bool>("tw").unwrap_or(false),
        ));
    }
    assert_eq!(
        seen[0], seen[1],
        "{label}: labeled and unlabeled reads differ"
    );
    Ok((
        format!("{} {}", seen[0].0, seen[0].2),
        format!("{} {}", seen[0].1, seen[0].3),
    ))
}

/// Values the write path converts into a declared column -- an integer into a
/// float column, an ISO string into a datetime column -- are converted the same
/// way whether they were written before the declaration (flushed or not, new
/// label or new property) or after it.
#[tokio::test]
async fn declared_conversions_agree_with_the_write_path() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = Uni::open(path).build().await?;

    // Declare, then write: the reference.
    db.schema()
        .label("After")
        .property_nullable("score", DataType::Float64)
        .property_nullable("when", DataType::DateTime)
        .done()
        .apply()
        .await?;
    run(
        &db,
        "CREATE (:After {score: 3, when: '2024-01-02T03:04:05Z'})",
    )
    .await?;

    // Write, then declare: a new label, flushed and unflushed.
    run(
        &db,
        "CREATE (:Flushed {score: 3, when: '2024-01-02T03:04:05Z'})",
    )
    .await?;
    db.flush().await?;
    run(
        &db,
        "CREATE (:Unflushed {score: 3, when: '2024-01-02T03:04:05Z'})",
    )
    .await?;
    // Write, then declare: new properties on a declared label (#249).
    db.schema()
        .label("Promoted")
        .property_nullable("uid", DataType::String)
        .done()
        .apply()
        .await?;
    run(
        &db,
        "CREATE (:Promoted {score: 3, when: '2024-01-02T03:04:05Z'})",
    )
    .await?;
    db.flush().await?;
    for label in ["Flushed", "Unflushed", "Promoted"] {
        db.schema()
            .label(label)
            .property_nullable("score", DataType::Float64)
            .property_nullable("when", DataType::DateTime)
            .done()
            .apply()
            .await?;
    }

    let reference = widened(&db, "After").await?;
    for label in ["Flushed", "Unflushed", "Promoted"] {
        assert_eq!(widened(&db, label).await?, reference, "{label}");
    }
    db.flush().await?;
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    for label in ["After", "Flushed", "Unflushed", "Promoted"] {
        assert_eq!(
            widened(&db, label).await?,
            reference,
            "{label} after reopen"
        );
    }
    db.shutdown().await?;
    Ok(())
}

/// A transaction that wrote a property before it was declared, committing
/// after the declaration: its statements validated the value against the old
/// schema, so the commit re-validates it against the new one. A value the
/// declared type cannot hold fails the commit (and nothing of the
/// transaction lands); one the write path converts is converted.
#[tokio::test]
async fn declared_type_conflict_fails_a_commit_racing_the_declaration() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("Repository")
        .property("uid", DataType::String)
        .done()
        .apply()
        .await?;

    // Both validated before `age` / `when` are declared.
    let bad = db.session().tx().await?;
    bad.execute("CREATE (:Repository {uid: 'bad', age: 'old'})")
        .await?;
    let good = db.session().tx().await?;
    good.execute("CREATE (:Repository {uid: 'good', when: '2024-01-02T03:04:05Z'})")
        .await?;

    db.schema()
        .label("Repository")
        .property_nullable("age", DataType::Int64)
        .property_nullable("when", DataType::DateTime)
        .done()
        .apply()
        .await?;

    let err = bad
        .commit()
        .await
        .expect_err("'old' cannot be stored as Int64");
    assert!(
        matches!(&err, UniError::Type { expected, .. }
            if expected.contains("'age'") && expected.contains("Int64")),
        "expected a type error naming age, got {err:?}"
    );
    good.commit().await?;

    let check = |db: Uni| async move {
        let r = db
            .session()
            .query(
                "MATCH (n:Repository) RETURN n.uid AS uid, n.when.year AS y, \
                 n.when = datetime('2024-01-02T03:04:05Z') AS t",
            )
            .await?;
        assert_eq!(r.rows().len(), 1, "only the good transaction landed");
        let row = &r.rows()[0];
        assert_eq!(row.get::<String>("uid")?, "good");
        assert_eq!(row.get::<i64>("y")?, 2024);
        assert_eq!(row.get::<bool>("t")?, true, "converted to a datetime");
        Ok::<Uni, UniError>(db)
    };
    let db = check(db).await?;
    db.flush().await?;
    let db = check(db).await?;
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    let db = check(db).await?;
    db.shutdown().await?;
    Ok(())
}
