// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for DERIVE visibility in trailing Cypher commands.
//!
//! These tests verify that trailing Cypher queries within a Locy program
//! can see edges materialized by preceding DERIVE commands, both at the
//! session level and the transaction level.
//!
//! # Bug context
//!
//! Session-level `session.locy()` sets `collect_derive: true` and
//! `tx_l0_override: None`. DERIVE commands collect mutation ASTs without
//! executing them. Trailing Cypher calls `execute_cypher_read()` which
//! reads the committed DB only — no ephemeral L0 overlay exists.

use std::collections::HashMap;

use anyhow::Result;
use uni_common::Value;
use uni_db::{Uni, locy::CommandResult};

type FactRow = HashMap<String, Value>;

/// Extract rows from the last command result, expecting it to be a Cypher variant.
fn cypher_rows(result: &uni_db::locy::LocyResult) -> &[FactRow] {
    match result.command_results.last().expect("no command results") {
        CommandResult::Cypher(rows) => rows,
        other => panic!("expected trailing Cypher, got {other:?}"),
    }
}

// ── Group 1: Session-level — trailing Cypher sees DERIVE edges ───────────

#[tokio::test]
async fn test_session_trailing_cypher_sees_derive_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link \n\
             MATCH (a:X)-[:LINKED]->(b:X) \
             RETURN a.name AS src, b.name AS dst",
        )
        .await?;

    let rows = cypher_rows(&result);
    assert!(
        !rows.is_empty(),
        "trailing Cypher after DERIVE should see derived edges (got 0 rows)"
    );
    assert_eq!(rows.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_session_trailing_cypher_after_derive_counts_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})-[:R]->(:X {name: 'C'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link \n\
             MATCH ()-[r:LINKED]->() RETURN count(r) AS cnt",
        )
        .await?;

    let rows = cypher_rows(&result);
    assert!(!rows.is_empty(), "trailing Cypher should return rows");
    let cnt = rows[0]
        .get("cnt")
        .expect("missing 'cnt' column")
        .as_i64()
        .expect("cnt should be i64");
    assert_eq!(cnt, 2, "expected 2 derived :LINKED edges, got {cnt}");
    Ok(())
}

#[tokio::test]
async fn test_session_trailing_cypher_joins_derived_and_existing() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})-[:R]->(:X {name: 'C'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link \n\
             MATCH (a:X)-[:LINKED]->(b:X)-[:R]->(c:X) \
             RETURN a.name AS src, c.name AS dst",
        )
        .await?;

    let rows = cypher_rows(&result);
    assert!(
        !rows.is_empty(),
        "trailing Cypher should join derived :LINKED edges with existing :R edges"
    );
    // A -[:LINKED]-> B -[:R]-> C
    assert_eq!(rows.len(), 1);
    Ok(())
}

#[tokio::test]
async fn test_session_multiple_derives_then_trailing_cypher() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})-[:R]->(:X {name: 'C'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE l1 AS \
               MATCH (a:X)-[:R]->(b:X) DERIVE (a)-[:LINK1]->(b) \n\
             CREATE RULE l2 AS \
               MATCH (a:X)-[:R]->(b:X) DERIVE (a)-[:LINK2]->(b) \n\
             DERIVE l1 \n\
             DERIVE l2 \n\
             MATCH (a:X)-[:LINK1]->(b:X)-[:LINK2]->(c:X) \
             RETURN a.name AS src, c.name AS dst",
        )
        .await?;

    let rows = cypher_rows(&result);
    assert!(
        !rows.is_empty(),
        "trailing Cypher should see edges from both DERIVE commands"
    );
    Ok(())
}

#[tokio::test]
async fn test_session_query_then_derive_then_cypher() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE linked AS \
               MATCH (a:X)-[:R]->(b:X) YIELD KEY a, KEY b \n\
             CREATE RULE derive_d AS \
               MATCH (a:X)-[:R]->(b:X) DERIVE (a)-[:D]->(b) \n\
             QUERY linked WHERE a = a RETURN a.name AS n \n\
             DERIVE derive_d \n\
             MATCH ()-[r:D]->() RETURN count(r) AS cnt",
        )
        .await?;

    assert!(
        result.command_results.len() >= 3,
        "expected 3+ command results, got {}",
        result.command_results.len()
    );

    // QUERY result (command 0)
    let query_rows = match &result.command_results[0] {
        CommandResult::Query(rows) => rows,
        other => panic!("expected Query, got {other:?}"),
    };
    assert!(!query_rows.is_empty(), "QUERY should return rows");

    // Trailing Cypher (last) should see derived :D edges
    let rows = cypher_rows(&result);
    assert!(
        !rows.is_empty(),
        "trailing Cypher should see derived :D edges"
    );
    Ok(())
}

// ── Group 2: Transaction-level — verify DERIVE visibility in tx.locy() ──

#[tokio::test]
async fn test_tx_trailing_cypher_sees_derive_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    let result = tx
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link \n\
             MATCH (a:X)-[:LINKED]->(b:X) \
             RETURN a.name AS src, b.name AS dst",
        )
        .await?;

    let rows = cypher_rows(&result);
    assert!(
        !rows.is_empty(),
        "tx trailing Cypher after DERIVE should see derived edges (got 0 rows)"
    );

    tx.commit().await?;

    let check = session
        .query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert_eq!(cnt, 1, "committed DERIVE edges should persist");
    Ok(())
}

#[tokio::test]
async fn test_tx_derive_then_cypher_then_commit_persists() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})-[:R]->(:X {name: 'C'})")
        .await?;
    tx.commit().await?;

    let tx = session.tx().await?;
    let result = tx
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link \n\
             MATCH ()-[r:LINKED]->() RETURN count(r) AS cnt",
        )
        .await?;

    let rows = cypher_rows(&result);
    assert!(!rows.is_empty(), "tx trailing Cypher should return rows");

    tx.commit().await?;

    let check = session
        .query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert_eq!(cnt, 2, "committed DERIVE edges should persist");
    Ok(())
}

// ── Group 3: Session DERIVE + tx.apply() roundtrip ──────────────────────

#[tokio::test]
async fn test_session_derive_apply_then_query_sees_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link",
        )
        .await?;

    let derived = result
        .derived_fact_set
        .clone()
        .expect("session DERIVE should produce DerivedFactSet");
    assert!(!derived.is_empty());

    let tx = session.tx().await?;
    let apply_result = tx.apply(derived).await?;
    assert!(apply_result.facts_applied > 0);
    tx.commit().await?;

    let check = session
        .query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert_eq!(cnt, 1, "applied DERIVE edges should be visible");
    Ok(())
}

#[tokio::test]
async fn test_session_derive_without_apply_does_not_persist() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link",
        )
        .await?;

    assert!(result.derived_fact_set.is_some());

    let check = session
        .query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert_eq!(cnt, 0, "DERIVE without apply should not persist edges");
    Ok(())
}

// ── Group 4: Edge cases ─────────────────────────────────────────────────

#[tokio::test]
async fn test_session_derive_empty_result_then_cypher() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // No data — DERIVE matches nothing

    let result = db
        .session()
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link \n\
             MATCH ()-[r:LINKED]->() RETURN count(r) AS cnt",
        )
        .await?;

    let rows = cypher_rows(&result);
    assert!(!rows.is_empty());
    let cnt = rows[0]
        .get("cnt")
        .expect("missing 'cnt' column")
        .as_i64()
        .expect("cnt should be i64");
    assert_eq!(cnt, 0, "empty DERIVE should correctly produce 0 edges");
    Ok(())
}

#[tokio::test]
async fn test_session_derive_does_not_leak_to_next_locy_call() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await?;
    tx.commit().await?;

    // First call: DERIVE collects edges
    let _result1 = session
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link",
        )
        .await?;

    // Second call: fresh evaluation should NOT see edges from first call
    let check = session
        .query("MATCH ()-[r:LINKED]->() RETURN count(r) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert_eq!(
        cnt, 0,
        "ephemeral L0 from first locy() should not leak to second locy() call"
    );
    Ok(())
}

#[tokio::test]
async fn test_session_derive_isolation_between_sessions() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session1 = db.session();
    let tx = session1.tx().await?;
    tx.execute("CREATE (:X {name: 'A'})-[:R]->(:X {name: 'B'})")
        .await?;
    tx.commit().await?;

    // Session 1 derives but does not apply
    let _result = session1
        .locy(
            "CREATE RULE link AS \
               MATCH (a:X)-[:R]->(b:X) \
               DERIVE (a)-[:LINKED]->(b) \n\
             DERIVE link",
        )
        .await?;

    // Session 2 should see nothing
    let session2 = db.session();
    let check = session2
        .query("MATCH ()-[:LINKED]->() RETURN count(*) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert_eq!(
        cnt, 0,
        "DERIVE in session1 should not be visible to session2"
    );
    Ok(())
}

// Rust guideline compliant

// ── Group 5: NEW node properties ─────────────────────────────────────────

const NEW_NODE_RULE: &str = "CREATE RULE categorize AS \
       MATCH (c:City) \
       DERIVE (NEW x:Country {name: c.country, tags: [c.name, 'derived'], size: c.pop * 2})\
       <-[:IN]-(c) \n\
     DERIVE categorize";

async fn cities(session: &uni_db::Session) -> Result<()> {
    let tx = session.tx().await?;
    tx.execute(
        "CREATE (:City {name: 'Paris', country: 'France', pop: 2}), \
                (:City {name: 'Lyon', country: 'France', pop: 1})",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// A NEW node's properties are evaluated against the matched row and stored,
/// one node per matched row.
#[tokio::test]
async fn test_derive_new_node_stores_its_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    cities(&session).await?;

    // Visible to the program's trailing Cypher.
    let result = session
        .locy(&format!(
            "{NEW_NODE_RULE} \n\
             MATCH (x:Country)<-[:IN]-(c:City) \
             RETURN x.name AS name, x.tags AS tags, x.size AS size, c.name AS city \
             ORDER BY city"
        ))
        .await?;
    let rows = cypher_rows(&result);
    assert_eq!(rows.len(), 2, "one NEW node per matched row: {rows:?}");
    assert_eq!(rows[0].get("name"), Some(&Value::String("France".into())));
    assert_eq!(
        rows[0].get("tags"),
        Some(&Value::List(vec![
            Value::String("Lyon".into()),
            Value::String("derived".into())
        ]))
    );
    assert_eq!(rows[0].get("size"), Some(&Value::Int(2)));
    assert_eq!(rows[1].get("size"), Some(&Value::Int(4)));

    // Reported in the derived fact set and persisted by `tx.apply`.
    let result = session.locy(NEW_NODE_RULE).await?;
    let derived = result
        .derived_fact_set
        .clone()
        .expect("session DERIVE should produce DerivedFactSet");
    let countries = derived
        .vertices
        .get("Country")
        .expect("NEW Country vertices");
    assert_eq!(countries.len(), 2);
    assert!(
        countries
            .iter()
            .all(|p| p.get("name") == Some(&Value::String("France".into()))),
        "derived vertices carry their properties: {countries:?}"
    );
    let tx = session.tx().await?;
    tx.apply(derived).await?;
    tx.commit().await?;
    let check = session
        .query("MATCH (x:Country) RETURN x.name AS name, x.size AS size ORDER BY size")
        .await?;
    let rows = check.rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String>("name")?, "France");
    assert_eq!(rows[1].get::<i64>("size")?, 4);
    Ok(())
}

/// The transaction path writes the properties directly.
#[tokio::test]
async fn test_tx_derive_new_node_stores_its_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    cities(&session).await?;

    let tx = session.tx().await?;
    tx.locy(NEW_NODE_RULE).await?;
    // A temporal value keeps its type.
    tx.execute("MATCH (c:City {name: 'Paris'}) SET c.founded = date('0300-01-01')")
        .await?;
    tx.locy(
        "CREATE RULE founding AS MATCH (c:City) WHERE c.founded IS NOT NULL \
           DERIVE (NEW f:Founding {on: c.founded})<-[:FOUNDED]-(c) \n\
         DERIVE founding",
    )
    .await?;
    tx.commit().await?;
    let check = session
        .query("MATCH (f:Founding) RETURN f.on = date('0300-01-01') AS same")
        .await?;
    assert!(check.rows()[0].get::<bool>("same")?);
    let check = session
        .query("MATCH (x:Country) RETURN x.name AS name, x.size AS size ORDER BY size")
        .await?;
    let rows = check.rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String>("name")?, "France");
    assert_eq!(rows[0].get::<i64>("size")?, 2);
    Ok(())
}
