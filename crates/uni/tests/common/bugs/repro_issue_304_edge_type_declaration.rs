// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #304's edge-type counterpart: declaring an edge type, with properties, over
//! relationships written schemaless keeps their property values, and refuses
//! values the declared type cannot hold.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(edge_type_declaration)'

use tempfile::TempDir;
use uni_db::{DataType, Result, Uni, UniError};

async fn run(db: &Uni, q: &str) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(q).await?;
    tx.commit().await?;
    Ok(())
}

/// `(k, toString(r.p))` of every `R` relationship, ordered by `k`.
async fn rels(db: &Uni) -> Result<Vec<(i64, Option<String>)>> {
    let r = db
        .session()
        .query("MATCH ()-[r:R]->() RETURN r.k AS k, toString(r.p) AS p ORDER BY k")
        .await?;
    let mut out = Vec::new();
    for row in r.rows() {
        out.push((row.get::<i64>("k")?, row.get::<String>("p").ok()));
    }
    Ok(out)
}

async fn seeded(path: &str, p_values: [&str; 2], flush: bool) -> Result<Uni> {
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("N")
        .property("uid", DataType::String)
        .done()
        .apply()
        .await?;
    run(
        &db,
        &format!(
            "CREATE (a:N {{uid: 'a'}}), (b:N {{uid: 'b'}}), \
             (a)-[:R {{k: 1, p: {}}}]->(b), (b)-[:R {{k: 2, p: {}}}]->(a)",
            p_values[0], p_values[1]
        ),
    )
    .await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

fn declare_r(db: &Uni) -> uni_db::SchemaBuilder<'_> {
    db.schema()
        .edge_type("R", &["N"], &["N"])
        .property_nullable("k", DataType::Int64)
        .property_nullable("p", DataType::Float64)
        .done()
}

/// The values survive the declaration, converted as a write of the declared
/// property would convert them (`Int` into a float column).
#[tokio::test]
async fn edge_type_declaration_keeps_schemaless_property_values() -> Result<()> {
    for flush in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();
        let db = seeded(path, ["5", "6.5"], flush).await?;
        let before = vec![(1, Some("5".to_string())), (2, Some("6.5".to_string()))];
        assert_eq!(rels(&db).await?, before, "flush={flush} before declaring");
        let want = vec![(1, Some("5.0".to_string())), (2, Some("6.5".to_string()))];
        declare_r(&db).apply().await?;
        assert_eq!(rels(&db).await?, want, "flush={flush} after declaring");
        db.flush().await?;
        assert_eq!(rels(&db).await?, want, "flush={flush} after flush");
        // A write under the declared type flushes typed columns into the
        // relationship tables; the older rows must keep their values.
        run(
            &db,
            "MATCH (a:N {uid: 'a'}), (b:N {uid: 'b'}) CREATE (a)-[:R {k: 3, p: 7}]->(b)",
        )
        .await?;
        db.flush().await?;
        let mut want = want;
        want.push((3, Some("7.0".to_string())));
        assert_eq!(rels(&db).await?, want, "flush={flush} after a typed write");
        db.shutdown().await?;
        let db = Uni::open(path).build().await?;
        assert_eq!(rels(&db).await?, want, "flush={flush} after reopen");
        db.shutdown().await?;
    }
    Ok(())
}

/// A value the declared type cannot hold refuses the declaration, leaving the
/// catalog and the values as they were.
#[tokio::test]
async fn edge_type_declaration_refuses_conflicting_values() -> Result<()> {
    for flush in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();
        let db = seeded(path, ["5", "'x'"], flush).await?;
        let err = declare_r(&db)
            .apply()
            .await
            .expect_err("a string p cannot become Float64");
        match err {
            UniError::DeclaredTypeConflict { detail } => {
                assert_eq!(detail.entity, "R");
                assert_eq!(detail.property, "p");
                assert_eq!(detail.count, 1);
                assert!(
                    detail.example_id.starts_with("eid "),
                    "{}",
                    detail.example_id
                );
                assert!(
                    detail.example_value.contains('x'),
                    "{}",
                    detail.example_value
                );
            }
            other => panic!("flush={flush}: expected DeclaredTypeConflict, got {other:?}"),
        }
        assert!(
            db.get_edge_type_info("R").await?.is_none(),
            "R stays undeclared"
        );
        let want = vec![(1, Some("5".to_string())), (2, Some("x".to_string()))];
        assert_eq!(rels(&db).await?, want, "flush={flush} values intact");
        db.shutdown().await?;
        let db = Uni::open(path).build().await?;
        assert_eq!(
            rels(&db).await?,
            want,
            "flush={flush} values intact after reopen"
        );
        db.shutdown().await?;
    }
    Ok(())
}

/// `toString(r.when)` and `r.when.year` of the `ty` relationship, read
/// through the type (its delta tables) and through an untyped pattern, which
/// must agree.
async fn when_of(db: &Uni, ty: &str) -> Result<(String, i64)> {
    let mut seen = Vec::new();
    for q in [
        format!(
            "MATCH ()-[r:{ty}]->() RETURN toString(r.when) AS w, r.when.year AS y, \
             r.when = datetime('2024-01-02T03:04:05Z') AS t"
        ),
        format!(
            "MATCH ()-[r]->() WHERE type(r) = '{ty}' RETURN toString(r.when) AS w, \
             r.when.year AS y, r.when = datetime('2024-01-02T03:04:05Z') AS t"
        ),
    ] {
        let r = db.session().query(&q).await?;
        let row = &r.rows()[0];
        assert_eq!(
            row.get::<bool>("t").ok(),
            Some(true),
            "{ty}.when must be a datetime: {q}"
        );
        seen.push((row.get::<String>("w")?, row.get::<i64>("y")?));
    }
    assert_eq!(seen[0], seen[1], "{ty}: typed and untyped reads differ");
    Ok(seen.remove(0))
}

/// A value the write path converts on the way into a declared column (an ISO
/// string into a datetime) reads the same whether it was written before the
/// edge type was declared (flushed or not) or after.
#[tokio::test]
async fn edge_type_declaration_converts_like_the_write_path() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("N")
        .property("uid", DataType::String)
        .done()
        .edge_type("After", &["N"], &["N"])
        .property_nullable("when", DataType::DateTime)
        .done()
        .apply()
        .await?;
    let create = |ty: &str| {
        format!(
            "CREATE (a:N {{uid: 'a'}})-[:{ty} {{when: '2024-01-02T03:04:05Z'}}]->(b:N {{uid: 'b'}})"
        )
    };
    run(&db, &create("After")).await?;
    run(&db, &create("Flushed")).await?;
    db.flush().await?;
    run(&db, &create("Unflushed")).await?;
    for ty in ["Flushed", "Unflushed"] {
        db.schema()
            .edge_type(ty, &["N"], &["N"])
            .property_nullable("when", DataType::DateTime)
            .done()
            .apply()
            .await?;
    }
    let reference = when_of(&db, "After").await?;
    assert_eq!(reference.1, 2024);
    for ty in ["Flushed", "Unflushed"] {
        assert_eq!(when_of(&db, ty).await?, reference, "{ty}");
    }
    db.flush().await?;
    db.shutdown().await?;
    let db = Uni::open(path).build().await?;
    for ty in ["After", "Flushed", "Unflushed"] {
        assert_eq!(when_of(&db, ty).await?, reference, "{ty} after reopen");
    }
    db.shutdown().await?;
    Ok(())
}
