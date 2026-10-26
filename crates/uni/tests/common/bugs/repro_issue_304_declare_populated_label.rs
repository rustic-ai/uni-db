// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #304: declaring a label that already holds schemaless nodes hides them.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(issue_304)' --no-capture

use tempfile::TempDir;
use uni_db::{DataType, IndexType, Result, ScalarType, Uni};

async fn count(db: &Uni, q: &str) -> Result<i64> {
    let r = db.session().query(q).await?;
    Ok(r.rows()[0].get::<i64>("c")?)
}

async fn counts(db: &Uni) -> Result<(i64, i64, i64)> {
    // Is the data still physically present? An unlabelled scan reads the main table.
    let all = count(
        db,
        "MATCH (n) WHERE 'Repository' IN labels(n) RETURN count(n) AS c",
    )
    .await?;
    let edges = count(db, "MATCH ()-[r:FORKED_FROM]->() RETURN count(r) AS c").await?;
    eprintln!(
        "ISSUE304   unlabelled-scan Repository nodes={all} untyped-src FORKED_FROM edges={edges}"
    );
    Ok((
        count(db, "MATCH (n:Repository) RETURN count(n) AS c").await?,
        count(
            db,
            "MATCH (:Repository)-[r:FORKED_FROM]->() RETURN count(r) AS c",
        )
        .await?,
        count(db, "MATCH (n:Other) RETURN count(n) AS c").await?,
    ))
}

async fn run_case(flush_before: bool, with_index: bool) -> Result<Vec<(i64, i64, i64)>> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let mut seen = Vec::new();
    {
        let db = Uni::open(&path).build().await?;
        let s = db.session();
        let tx = s.tx().await?;
        tx.execute(
            "CREATE (:Repository {uid: 'a', json: '{}'}), (:Repository {uid: 'b', json: '{}'}), (:Other {uid: 'o'})",
        )
        .await?;
        tx.execute(
            "MATCH (a:Repository {uid:'a'}), (b:Repository {uid:'b'}) CREATE (a)-[:FORKED_FROM]->(b)",
        )
        .await?;
        tx.commit().await?;
        if flush_before {
            db.flush().await?;
        }
        seen.push(counts(&db).await?);

        let mut lb = db
            .schema()
            .label("Repository")
            .property("uid", DataType::String)
            .property("json", DataType::String);
        if with_index {
            lb = lb.index("uid", IndexType::Scalar(ScalarType::BTree));
        }
        let applied = lb.done().apply().await;
        eprintln!("ISSUE304 flush_before={flush_before} index={with_index} apply -> {applied:?}");
        applied?;
        seen.push(counts(&db).await?);
        db.flush().await?;
        seen.push(counts(&db).await?);
        drop(s);
        db.shutdown().await?;
    }
    let db = Uni::open(&path).build().await?;
    seen.push(counts(&db).await?);
    eprintln!(
        "ISSUE304 flush_before={flush_before} index={with_index} \
         (Repository, FORKED_FROM, Other) [before, after-apply, after-flush, after-reopen] = {seen:?}"
    );
    Ok(seen)
}

#[tokio::test]
async fn issue_304_declare_populated_label_hides_nodes() -> Result<()> {
    let mut failures = Vec::new();
    for flush_before in [true, false] {
        for with_index in [true, false] {
            let seen = run_case(flush_before, with_index).await?;
            if seen.iter().any(|c| *c != (2, 1, 1)) {
                failures.push(format!(
                    "flush_before={flush_before} index={with_index}: expected (2,1,1) at every step, got {seen:?}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

// ---------------------------------------------------------------------------
// The other declaration paths, and the row states a backfill must respect.
// ---------------------------------------------------------------------------

/// Open a store at `path`, run each of `writes` schemaless and flush after
/// it, so the rows live in the main vertex table only.
async fn seeded_schemaless(path: &str, writes: &[&str]) -> Result<Uni> {
    let db = Uni::open(path).build().await?;
    for q in writes {
        let tx = db.session().tx().await?;
        tx.execute(q).await?;
        tx.commit().await?;
        db.flush().await?;
    }
    Ok(db)
}

async fn reopen(db: Uni, path: &str) -> Result<Uni> {
    db.shutdown().await?;
    Uni::open(path).build().await
}

async fn strings(db: &Uni, q: &str) -> Result<Vec<String>> {
    let r = db.session().query(q).await?;
    let mut out = Vec::new();
    for row in r.rows() {
        out.push(row.get::<String>("v")?);
    }
    Ok(out)
}

const SEED: &str =
    "CREATE (:Repository {uid: 'a', json: '{}'}), (:Repository {uid: 'b', json: '{}'})";
const REPO_COUNT: &str = "MATCH (n:Repository) RETURN count(n) AS c";

/// Cypher `CREATE LABEL` over flushed schemaless rows keeps them, and a
/// lookup through the new typed column finds them.
#[tokio::test]
async fn issue_304_cypher_create_label_keeps_flushed_nodes() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(path, &[SEED]).await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Repository (uid STRING, json STRING)")
        .await?;
    tx.commit().await?;

    assert_eq!(count(&db, REPO_COUNT).await?, 2, "after CREATE LABEL");
    assert_eq!(
        strings(
            &db,
            "MATCH (n:Repository) WHERE n.uid = 'b' RETURN n.uid AS v"
        )
        .await?,
        vec!["b"]
    );
    let db = reopen(db, path).await?;
    assert_eq!(count(&db, REPO_COUNT).await?, 2, "after reopen");
    db.shutdown().await?;
    Ok(())
}

/// `Uni::load_schema` introducing the label.
#[tokio::test]
async fn issue_304_load_schema_keeps_flushed_nodes() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    let path = path.to_str().unwrap();
    let db = seeded_schemaless(path, &[SEED]).await?;

    let schema_file = dir.path().join("schema.json");
    let donor = Uni::temporary().build().await?;
    donor
        .schema()
        .label("Repository")
        .property_nullable("uid", DataType::String)
        .done()
        .apply()
        .await?;
    donor.save_schema(&schema_file).await?;
    donor.shutdown().await?;

    db.load_schema(&schema_file).await?;
    assert_eq!(count(&db, REPO_COUNT).await?, 2, "after load_schema");
    db.flush().await?;
    assert_eq!(count(&db, REPO_COUNT).await?, 2, "after flush");
    db.shutdown().await?;
    Ok(())
}

/// The `uni.schema.createLabel` procedure path.
#[tokio::test]
async fn issue_304_procedure_create_label_keeps_flushed_nodes() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(path, &[SEED]).await?;

    db.session()
        .run(
            r#"CALL uni.schema.createLabel('Repository', {
                "properties": { "uid": { "type": "STRING" } }
            })"#,
        )
        .await?;

    assert_eq!(count(&db, REPO_COUNT).await?, 2, "after createLabel");
    let db = reopen(db, path).await?;
    assert_eq!(count(&db, REPO_COUNT).await?, 2, "after reopen");
    db.shutdown().await?;
    Ok(())
}

/// A fork declaring a label keeps both the rows it inherited from primary and
/// the ones it flushed itself, and primary is unaffected.
#[tokio::test]
async fn issue_304_fork_declared_label_keeps_flushed_nodes() -> Result<()> {
    let db = Uni::temporary().build().await?;
    let primary = db.session();
    let tx = primary.tx().await?;
    tx.execute(SEED).await?;
    tx.commit().await?;
    db.flush().await?;

    let fork = primary.fork("decl304").await?;
    let tx = fork.tx().await?;
    tx.execute("CREATE (:Repository {uid: 'f', json: '{}'})")
        .await?;
    tx.commit().await?;
    fork.flush().await?;

    fork.fork_schema().label("Repository").apply().await?;

    let fork_count = || async {
        let r = fork.query(REPO_COUNT).await?;
        r.rows()[0].get::<i64>("c")
    };
    assert_eq!(fork_count().await?, 3, "fork after declaring");
    fork.flush().await?;
    assert_eq!(fork_count().await?, 3, "fork after flush");
    assert_eq!(count(&db, REPO_COUNT).await?, 2, "primary is unaffected");
    assert!(
        db.get_label_info("Repository").await?.is_none(),
        "primary does not declare the fork's label"
    );
    drop(fork);
    db.shutdown().await?;
    Ok(())
}

/// A NOT NULL property declared together with a label whose existing,
/// schemaless rows lack it is recorded nullable (#249's contract), and the
/// rows stay visible.
#[tokio::test]
async fn issue_304_not_null_on_schemaless_populated_label_is_recorded_nullable() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(
        path,
        &["CREATE (:Repository {uid: 'a'}), (:Repository {uid: 'b'})"],
    )
    .await?;

    db.schema()
        .label("Repository")
        .property("uid", DataType::String)
        .property("json", DataType::String)
        .done()
        .apply()
        .await?;

    let info = db
        .get_label_info("Repository")
        .await?
        .expect("label declared");
    let json = info
        .properties
        .iter()
        .find(|p| p.name == "json")
        .expect("json declared");
    assert!(json.nullable, "rows without `json` exist; must be nullable");
    assert_eq!(count(&db, REPO_COUNT).await?, 2);
    db.flush().await?;
    let db = reopen(db, path).await?;
    assert_eq!(count(&db, REPO_COUNT).await?, 2, "after reopen");
    db.shutdown().await?;
    Ok(())
}

/// An index declared with the label covers the backfilled rows: an equality
/// lookup on the indexed property finds an old vertex.
#[tokio::test]
async fn issue_304_index_declared_with_label_covers_old_rows() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(path, &[SEED]).await?;

    db.schema()
        .label("Repository")
        .property("uid", DataType::String)
        .index("uid", IndexType::Scalar(ScalarType::BTree))
        .done()
        .apply()
        .await?;

    let q = "MATCH (n:Repository) WHERE n.uid = 'a' RETURN n.uid AS v";
    assert_eq!(strings(&db, q).await?, vec!["a"], "after apply");
    let db = reopen(db, path).await?;
    assert_eq!(strings(&db, q).await?, vec!["a"], "after reopen");
    db.shutdown().await?;
    Ok(())
}

/// A vertex deleted before the declaration stays deleted.
#[tokio::test]
async fn issue_304_deleted_before_declaration_stays_deleted() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(
        path,
        &[
            "CREATE (:Repository {uid: 'a'}), (:Repository {uid: 'b'}), (:Repository {uid: 'c'})",
            "MATCH (n:Repository {uid: 'c'}) DETACH DELETE n",
        ],
    )
    .await?;

    db.schema().label("Repository").apply().await?;

    let q = "MATCH (n:Repository) RETURN n.uid AS v ORDER BY v";
    assert_eq!(strings(&db, q).await?, vec!["a", "b"], "after apply");
    db.flush().await?;
    let db = reopen(db, path).await?;
    assert_eq!(strings(&db, q).await?, vec!["a", "b"], "after reopen");
    db.shutdown().await?;
    Ok(())
}

/// A vertex relabeled away from the label before the declaration stays out
/// of it, and keeps its new label.
#[tokio::test]
async fn issue_304_relabeled_away_before_declaration_stays_absent() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(
        path,
        &[
            "CREATE (:Repository {uid: 'a'}), (:Repository {uid: 'b'})",
            "MATCH (n:Repository {uid: 'b'}) REMOVE n:Repository SET n:Archived",
        ],
    )
    .await?;

    db.schema().label("Repository").apply().await?;

    let q = "MATCH (n:Repository) RETURN n.uid AS v ORDER BY v";
    assert_eq!(strings(&db, q).await?, vec!["a"], "after apply");
    let db = reopen(db, path).await?;
    assert_eq!(strings(&db, q).await?, vec!["a"], "after reopen");
    assert_eq!(
        strings(&db, "MATCH (n:Archived) RETURN n.uid AS v").await?,
        vec!["b"]
    );
    db.shutdown().await?;
    Ok(())
}

/// Updating an old vertex after the declaration leaves exactly one current
/// row for it, carrying the new value, flushed or not.
#[tokio::test]
async fn issue_304_set_after_declaration_yields_one_current_row() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(path, &["CREATE (:Repository {uid: 'a', json: 'old'})"]).await?;

    db.schema()
        .label("Repository")
        .property("uid", DataType::String)
        .property_nullable("json", DataType::String)
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Repository {uid: 'a'}) SET n.json = 'new'")
        .await?;
    tx.commit().await?;
    let q = "MATCH (n:Repository) RETURN n.json AS v";
    assert_eq!(strings(&db, q).await?, vec!["new"], "before flush");
    db.flush().await?;
    assert_eq!(strings(&db, q).await?, vec!["new"], "after flush");
    let db = reopen(db, path).await?;
    assert_eq!(strings(&db, q).await?, vec!["new"], "after reopen");
    db.shutdown().await?;
    Ok(())
}

/// The backfilled rows count in the label's manifest row count, which the
/// index growth trigger compares with `row_count_at_build` (and `SHOW
/// STATISTICS` reports): published with the next flush, and kept on reopen.
#[tokio::test]
async fn issue_304_backfilled_rows_count_in_the_manifest() -> Result<()> {
    async fn manifest_count(db: &Uni) -> Result<i64> {
        let r = db.session().query("SHOW STATISTICS").await?;
        for row in r.rows() {
            if row.get::<String>("type")? == "Label" && row.get::<String>("name")? == "Repository" {
                return Ok(row.get::<i64>("count")?);
            }
        }
        Ok(0)
    }

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(path, &[SEED]).await?;
    db.schema().label("Repository").apply().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Repository {uid: 'c'})").await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(manifest_count(&db).await?, 3, "2 backfilled + 1 flushed");
    let db = reopen(db, path).await?;
    assert_eq!(manifest_count(&db).await?, 3, "after reopen");
    db.shutdown().await?;
    Ok(())
}

/// A bulk load open across the declaration of a label it writes.
///
/// A bulk load cannot leave main-table-only rows for a label: it refuses an
/// undeclared label outright (`BulkWriter::insert_vertices` checks the live
/// schema), and once the label is declared it writes the label table and the
/// main table together. So a load started before the declaration, writing the
/// label after it and committing after that, loses nothing from the label
/// scan, and neither do the schemaless rows the declaration backfilled.
#[tokio::test]
async fn issue_304_bulk_load_spanning_a_declaration() -> Result<()> {
    use std::collections::HashMap;
    use uni_db::Value;

    fn rows(uids: &[&str]) -> Vec<HashMap<String, Value>> {
        uids.iter()
            .map(|u| HashMap::from([("uid".to_string(), Value::String((*u).to_string()))]))
            .collect()
    }

    let dir = TempDir::new().unwrap();
    let path = dir.path().to_str().unwrap();
    let db = seeded_schemaless(path, &[SEED]).await?;
    db.schema()
        .label("Other")
        .property("uid", DataType::String)
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices("Other", rows(&["o1"])).await?;
    let refused = bulk
        .insert_vertices("Repository", rows(&["early"]))
        .await
        .expect_err("a bulk load refuses an undeclared label");
    assert!(
        refused.to_string().contains("Repository"),
        "unexpected refusal: {refused}"
    );

    // Declared mid-load.
    db.schema()
        .label("Repository")
        .property("uid", DataType::String)
        .done()
        .apply()
        .await?;
    bulk.insert_vertices("Repository", rows(&["c", "d"]))
        .await?;
    bulk.commit().await?;
    tx.commit().await?;

    let q = "MATCH (n:Repository) RETURN n.uid AS v ORDER BY v";
    let want = vec!["a", "b", "c", "d"];
    assert_eq!(strings(&db, q).await?, want, "after the load");
    db.flush().await?;
    assert_eq!(strings(&db, q).await?, want, "after flush");
    let db = reopen(db, path).await?;
    assert_eq!(strings(&db, q).await?, want, "after reopen");
    db.shutdown().await?;
    Ok(())
}
