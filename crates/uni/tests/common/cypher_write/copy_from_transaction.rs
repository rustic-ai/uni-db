// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `COPY … FROM` inside a transaction is part of that transaction.
//!
//! Until wave P1 the import wrote straight into the main L0 and then flushed
//! it to L1: the transaction itself saw none of its rows, another session
//! saw them before the commit, they were on disk before the commit, and a
//! rollback left them in place. The rows now go to the transaction's private
//! L0: visible to it alone, gone on rollback, durable through its commit, and
//! bounded by its memory limit.

// Rust guideline compliant

use std::io::Write as _;

use anyhow::Result;
use uni_db::{DataType, Session, Uni, UniConfig};
use uni_store::backend::table_names;
use uni_store::backend::types::ScanRequest;

async fn open(path: &str, config: UniConfig) -> Result<Uni> {
    let db = Uni::open(path).config(config).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;
    Ok(db)
}

fn people_csv(dir: &std::path::Path) -> Result<String> {
    let p = dir.join("people.csv");
    let mut f = std::fs::File::create(&p)?;
    writeln!(f, "name")?;
    writeln!(f, "Alice")?;
    writeln!(f, "Bob")?;
    Ok(p.to_str().expect("utf-8").to_owned())
}

async fn count(s: &Session) -> Result<i64> {
    Ok(s.query("MATCH (n:Person) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?)
}

async fn stored_rows(db: &Uni) -> Result<usize> {
    let table = table_names::vertex_table_name("Person");
    Ok(
        match db.storage().backend().scan(ScanRequest::all(&table)).await {
            Ok(batches) => batches.iter().map(|b| b.num_rows()).sum(),
            // No table yet: nothing stored.
            Err(_) => 0,
        },
    )
}

#[tokio::test]
async fn copy_from_is_visible_to_its_transaction_only_and_rolls_back() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let db = open(db_path.to_str().expect("utf-8"), UniConfig::default()).await?;
    let csv = people_csv(dir.path())?;
    let s = db.session();
    let other = db.session();

    let tx = s.tx().await?;
    let r = tx.execute(&format!("COPY Person FROM '{csv}'")).await?;
    assert_eq!(r.nodes_created(), 2, "COPY reports the nodes it created");
    let seen = tx
        .query("MATCH (n:Person) RETURN count(n) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    assert_eq!(seen, 2, "the transaction sees its own import");
    assert_eq!(count(&other).await?, 0, "another session must not see it");
    assert_eq!(
        stored_rows(&db).await?,
        0,
        "nothing reaches L1 before commit"
    );
    tx.rollback();
    assert_eq!(count(&other).await?, 0, "rollback discards the import");
    db.flush().await?;
    assert_eq!(stored_rows(&db).await?, 0, "and nothing was left to flush");

    let tx = s.tx().await?;
    tx.execute(&format!("COPY Person FROM '{csv}'")).await?;
    tx.commit().await?;
    assert_eq!(count(&other).await?, 2, "commit publishes the import");
    db.shutdown().await?;

    let db = open(db_path.to_str().expect("utf-8"), UniConfig::default()).await?;
    assert_eq!(count(&db.session()).await?, 2, "the commit is durable");
    db.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn copy_from_edges_in_a_transaction_roll_back() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let db = open(db_path.to_str().expect("utf-8"), UniConfig::default()).await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.execute("CREATE (:Person {name: 'a'}), (:Person {name: 'b'})")
        .await?;
    tx.commit().await?;
    let ids = s
        .query("MATCH (n:Person) RETURN id(n) AS v ORDER BY n.name")
        .await?;
    let a = ids.rows()[0].get::<i64>("v")?;
    let b = ids.rows()[1].get::<i64>("v")?;
    let csv = dir.path().join("knows.csv");
    {
        let mut f = std::fs::File::create(&csv)?;
        writeln!(f, "src,dst")?;
        writeln!(f, "{a},{b}")?;
    }
    let edges = "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r) AS c";

    let tx = s.tx().await?;
    let r = tx
        .execute(&format!("COPY KNOWS FROM '{}'", csv.display()))
        .await?;
    assert_eq!(r.relationships_created(), 1);
    assert_eq!(tx.query(edges).await?.rows()[0].get::<i64>("c")?, 1);
    assert_eq!(s.query(edges).await?.rows()[0].get::<i64>("c")?, 0);
    tx.rollback();
    assert_eq!(s.query(edges).await?.rows()[0].get::<i64>("c")?, 0);
    db.shutdown().await?;
    Ok(())
}

/// An import larger than the transaction memory limit fails loudly and
/// leaves nothing behind.
#[tokio::test]
async fn copy_from_over_the_transaction_memory_limit_fails() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("db");
    let config = UniConfig {
        max_transaction_memory: 4 * 1024,
        ..UniConfig::default()
    };
    let db = open(db_path.to_str().expect("utf-8"), config).await?;
    let csv = dir.path().join("many.csv");
    {
        let mut f = std::fs::File::create(&csv)?;
        writeln!(f, "name")?;
        for i in 0..2_000 {
            writeln!(f, "person-{i:06}-with-a-long-enough-name")?;
        }
    }
    let s = db.session();
    let tx = s.tx().await?;
    let err = tx
        .execute(&format!("COPY Person FROM '{}'", csv.display()))
        .await
        .expect_err("an import over the transaction memory limit must fail");
    assert!(
        err.to_string()
            .contains("Transaction memory limit exceeded"),
        "{err}"
    );
    drop(tx);
    assert_eq!(count(&s).await?, 0);
    db.flush().await?;
    assert_eq!(stored_rows(&db).await?, 0);
    db.shutdown().await?;
    Ok(())
}
