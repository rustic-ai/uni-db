// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #312 — a CREATE / MERGE endpoint carried its whole property bag.
//!
//! `mark_pattern_variables` marked every CREATE/MERGE pattern variable `"*"`,
//! so a matched endpoint's every property — `_all_props` and `overflow_json`
//! included — rode through every join beneath the write. The writer reads only
//! the endpoint's identity (and MERGE its labels). With one 200 KB property on
//! the matched node that was ~2.4 MB per row, and the issue's CREATE exhausted
//! a 1 GiB query pool at ~430 rows. Reads were unaffected.
//!
//! The endpoint is now marked struct-only. The memory tests pin the fix; the
//! rest pin that every consumer of the *whole* entity still gets it: a later
//! `RETURN a`, `RETURN *`, a named MERGE path, `startNode`, a SET reading a property,
//! and `SET a += …` — on a declared label (with an undeclared, overflow
//! property) and a schemaless one, flushed and unflushed.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use uni_db::{DataType, Uni, UniConfig, Value};

/// Rows the memory tests join the big node to.
const FANOUT: i64 = 1000;

/// Size of the big node's one wide property.
const BIG_BYTES: usize = 200 * 1024;

/// Query pool for the memory tests. The old plan needed ~2.4 MB per row, so
/// [`FANOUT`] rows are an order of magnitude over it; the identity the writer
/// needs fits many times over.
const POOL: usize = 256 * 1024 * 1024;

/// The issue's statement, keyed by `id()` as its reporter wrote it.
const CREATE_Q: &str = "UNWIND $edges AS e \
     MATCH (a:Big) WHERE id(a) = e.src \
     MATCH (b:Slot) WHERE id(b) = e.dst \
     CREATE (a)-[:CONTAINS]->(b) RETURN count(*) AS c";

/// The same, with the endpoints forwarded through `WITH` and no `RETURN`: the
/// projection-provenance pass must narrow a forwarded endpoint just as the
/// write's own marking does.
const CREATE_WITH_Q: &str = "UNWIND $edges AS e \
     MATCH (a:Big) WHERE id(a) = e.src \
     MATCH (b:Slot) WHERE id(b) = e.dst \
     WITH a, b \
     CREATE (a)-[:CONTAINS]->(b)";

/// The MERGE twin.
const MERGE_Q: &str = "UNWIND $edges AS e \
     MATCH (a:Big) WHERE id(a) = e.src \
     MATCH (b:Slot) WHERE id(b) = e.dst \
     MERGE (a)-[:CONTAINS]->(b) RETURN count(*) AS c";

/// The MERGE twin of [`CREATE_WITH_Q`].
const MERGE_WITH_Q: &str = "UNWIND $edges AS e \
     MATCH (a:Big) WHERE id(a) = e.src \
     MATCH (b:Slot) WHERE id(b) = e.dst \
     WITH a, b \
     MERGE (a)-[:CONTAINS]->(b)";

/// One `:Big` node with a 200 KB property, [`FANOUT`] `:Slot` nodes, and the
/// `$edges` list pairing the big node with every slot.
///
/// The query deadline is lifted so that what refuses the old plan is the pool,
/// the thing under test, rather than the default 30 s clock it also overran.
async fn big_fixture(flush: bool) -> Result<(Uni, Value)> {
    let config = UniConfig {
        query_timeout: Duration::from_secs(600),
        ..UniConfig::default()
    };
    let db = Uni::in_memory().config(config).build().await?;
    db.schema()
        .label("Big")
        .property("uid", DataType::String)
        .property("manifest", DataType::String)
        .label("Slot")
        .property("n", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.query_with("CREATE (:Big {uid: 'b1', manifest: $m})")
        .param("m", Value::String("x".repeat(BIG_BYTES)))
        .execute()
        .await?;
    tx.query_with("UNWIND range(1, $n) AS i CREATE (:Slot {n: i})")
        .param("n", Value::Int(FANOUT))
        .execute()
        .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }

    let s = db.session();
    let src = s.query("MATCH (a:Big) RETURN id(a) AS v").await?.rows()[0].get::<i64>("v")?;
    let edges: Vec<Value> = s
        .query("MATCH (b:Slot) RETURN id(b) AS v")
        .await?
        .rows()
        .iter()
        .map(|r| {
            Value::Map(HashMap::from([
                ("src".to_string(), Value::Int(src)),
                ("dst".to_string(), Value::Int(r.get::<i64>("v").unwrap())),
            ]))
        })
        .collect();
    assert_eq!(edges.len() as i64, FANOUT);
    Ok((db, Value::List(edges)))
}

/// Run a write under [`POOL`] and commit it.
async fn run_under_pool(db: &Uni, q: &str, edges: &Value) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.query_with(q)
        .param("edges", edges.clone())
        .max_memory(POOL)
        .fetch_all()
        .await?;
    tx.commit().await?;
    Ok(())
}

/// `CONTAINS` edges now stored.
async fn edge_count(db: &Uni) -> Result<i64> {
    Ok(db
        .session()
        .query("MATCH (:Big)-[r:CONTAINS]->(:Slot) RETURN count(r) AS c")
        .await?
        .rows()[0]
        .get::<i64>("c")?)
}

/// The issue's statement: one wide matched node joined to every row.
#[tokio::test]
async fn create_over_a_wide_matched_node_fits_the_pool() -> Result<()> {
    for q in [CREATE_Q, CREATE_WITH_Q] {
        for flush in [false, true] {
            let ctx = format!("flush={flush} `{q}`");
            let (db, edges) = big_fixture(flush).await?;
            run_under_pool(&db, q, &edges)
                .await
                .unwrap_or_else(|e| panic!("{ctx}: CREATE refused under {POOL} bytes: {e}"));
            assert_eq!(
                edge_count(&db).await?,
                FANOUT,
                "{ctx}: every edge must land"
            );
        }
    }
    Ok(())
}

/// The MERGE twin: the first run creates every edge, the second matches each.
#[tokio::test]
async fn merge_over_a_wide_matched_node_fits_the_pool() -> Result<()> {
    for q in [MERGE_Q, MERGE_WITH_Q] {
        for flush in [false, true] {
            let ctx = format!("flush={flush} `{q}`");
            let (db, edges) = big_fixture(flush).await?;
            for pass in ["create", "match"] {
                run_under_pool(&db, q, &edges).await.unwrap_or_else(|e| {
                    panic!("{ctx} pass={pass}: MERGE refused under {POOL} bytes: {e}")
                });
            }
            assert_eq!(
                edge_count(&db).await?,
                FANOUT,
                "{ctx}: MERGE must not duplicate"
            );
        }
    }
    Ok(())
}

// ---- values must survive ---------------------------------------------------

/// `:L` is declared with `k`, `x`, `y`; `extra` is undeclared and so lives in
/// overflow. `:S` is not declared at all.
async fn value_fixture(label: &str, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("L")
        .property("k", DataType::Int64)
        .property("x", DataType::Int64)
        .property("y", DataType::Int64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "CREATE (:{label} {{k: 1, x: 10, y: 20, extra: 'e'}})"
    ))
    .await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// Every (label, flush) cell the value tests cover.
fn cells() -> Vec<(&'static str, bool)> {
    vec![("L", false), ("L", true), ("S", false), ("S", true)]
}

/// Run a write in a committed transaction and return its rows.
async fn write(db: &Uni, q: &str) -> Result<Vec<uni_db::Row>> {
    let tx = db.session().tx().await?;
    let rows = tx.query_with(q).fetch_all().await?;
    tx.commit().await?;
    Ok(rows.rows().to_vec())
}

/// The node's properties as returned, whichever encoding carries them.
fn node_props(v: &Value) -> std::collections::HashMap<String, Value> {
    match v {
        Value::Node(n) => n.properties.clone(),
        Value::Map(m) => m
            .iter()
            .filter(|(k, _)| !k.starts_with('_'))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        other => panic!("expected a node, got {other:?}"),
    }
}

/// The fixture node's properties, all present.
fn assert_whole(ctx: &str, v: &Value) {
    let p = node_props(v);
    assert_eq!(p.get("k"), Some(&Value::Int(1)), "{ctx}: k in {p:?}");
    assert_eq!(p.get("x"), Some(&Value::Int(10)), "{ctx}: x in {p:?}");
    assert_eq!(p.get("y"), Some(&Value::Int(20)), "{ctx}: y in {p:?}");
    assert_eq!(
        p.get("extra"),
        Some(&Value::String("e".into())),
        "{ctx}: extra in {p:?}"
    );
}

#[tokio::test]
async fn a_create_endpoint_returned_whole_keeps_every_property() -> Result<()> {
    for (label, flush) in cells() {
        let ctx = format!("{label} flush={flush}");
        let db = value_fixture(label, flush).await?;
        let rows = write(
            &db,
            &format!("MATCH (a:{label} {{k: 1}}) CREATE (a)-[:R]->(:M) RETURN a, a.x AS ax, a.extra AS ae"),
        )
        .await?;
        assert_eq!(rows.len(), 1, "{ctx}");
        assert_whole(&ctx, rows[0].value("a").unwrap());
        assert_eq!(rows[0].value("ax"), Some(&Value::Int(10)), "{ctx}");
        assert_eq!(
            rows[0].value("ae"),
            Some(&Value::String("e".into())),
            "{ctx}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_create_endpoint_under_return_star_keeps_every_property() -> Result<()> {
    for (label, flush) in cells() {
        let ctx = format!("{label} flush={flush}");
        let db = value_fixture(label, flush).await?;
        let rows = write(
            &db,
            &format!("MATCH (a:{label} {{k: 1}}) CREATE (a)-[:R]->(b:M) RETURN *"),
        )
        .await?;
        assert_eq!(rows.len(), 1, "{ctx}");
        assert_whole(&ctx, rows[0].value("a").unwrap());
    }
    Ok(())
}

#[tokio::test]
async fn a_named_merge_path_keeps_every_property() -> Result<()> {
    for (label, flush) in cells() {
        let ctx = format!("{label} flush={flush}");
        let db = value_fixture(label, flush).await?;
        let q = format!("MATCH (a:{label} {{k: 1}}) MERGE p = (a)-[:R]->(:M {{n: 1}}) RETURN p");
        for pass in ["create", "match"] {
            let rows = write(&db, &q).await?;
            assert_eq!(rows.len(), 1, "{ctx} {pass}");
            match rows[0].value("p").unwrap() {
                Value::Path(p) => {
                    assert_whole(&format!("{ctx} {pass}"), &Value::Node(p.nodes[0].clone()))
                }
                other => panic!("{ctx} {pass}: expected a path, got {other:?}"),
            }
        }
    }
    Ok(())
}

/// `startNode` of the relationship just created resolves to the row's `a`.
#[tokio::test]
async fn start_node_of_a_created_relationship_keeps_every_property() -> Result<()> {
    for (label, flush) in cells() {
        let ctx = format!("{label} flush={flush}");
        let db = value_fixture(label, flush).await?;
        let rows = write(
            &db,
            &format!("MATCH (a:{label} {{k: 1}}) CREATE (a)-[r:R]->(:M) RETURN startNode(r) AS s"),
        )
        .await?;
        assert_eq!(rows.len(), 1, "{ctx}");
        assert_whole(&ctx, rows[0].value("s").unwrap());
    }
    Ok(())
}

/// `ON MATCH SET a.x = a.y + 1` reads `y` from the struct-only endpoint.
#[tokio::test]
async fn merge_on_match_set_reads_the_endpoint_property() -> Result<()> {
    for (label, flush) in cells() {
        let ctx = format!("{label} flush={flush}");
        let db = value_fixture(label, flush).await?;
        write(&db, "CREATE (:M {n: 1})").await?;
        let q = format!(
            "MATCH (a:{label} {{k: 1}}), (b:M {{n: 1}}) MERGE (a)-[:R]->(b) \
             ON CREATE SET a.x = a.y + 100 ON MATCH SET a.x = a.y + 1 RETURN a.x AS ax"
        );
        let first = write(&db, &q).await?;
        assert_eq!(
            first[0].value("ax"),
            Some(&Value::Int(120)),
            "{ctx} on create"
        );
        let second = write(&db, &q).await?;
        assert_eq!(
            second[0].value("ax"),
            Some(&Value::Int(21)),
            "{ctx} on match"
        );
        let rows = db
            .session()
            .query(&format!("MATCH (a:{label} {{k: 1}}) RETURN a"))
            .await?;
        let p = node_props(rows.rows()[0].value("a").unwrap());
        assert_eq!(
            p.get("x"),
            Some(&Value::Int(21)),
            "{ctx}: stored x in {p:?}"
        );
        assert_eq!(p.get("extra"), Some(&Value::String("e".into())), "{ctx}");
    }
    Ok(())
}

/// `SET a += {…}` after a CREATE merges into the whole entity, not into the
/// identity-only struct the CREATE needed.
#[tokio::test]
async fn set_plus_after_create_keeps_every_property() -> Result<()> {
    for (label, flush) in cells() {
        let ctx = format!("{label} flush={flush}");
        for with in ["", "WITH a"] {
            let ctx = format!("{ctx} `{with}`");
            let db = value_fixture(label, flush).await?;
            write(
                &db,
                &format!(
                    "MATCH (a:{label} {{k: 1}}) {with} CREATE (a)-[:R]->(:M) SET a += {{z: 5}}"
                ),
            )
            .await?;
            let rows = db
                .session()
                .query(&format!("MATCH (a:{label} {{k: 1}}) RETURN a"))
                .await?;
            let v = rows.rows()[0].value("a").unwrap().clone();
            assert_whole(&ctx, &v);
            assert_eq!(node_props(&v).get("z"), Some(&Value::Int(5)), "{ctx}");
        }
    }
    Ok(())
}

/// An endpoint forwarded through `WITH` into a final write (no `RETURN`) is
/// narrowed to its identity: the write must still reuse the matched node,
/// return no rows, and leave every property intact; a final MERGE's SET must
/// still read the property it names.
#[tokio::test]
async fn a_forwarded_endpoint_of_a_final_write_is_reused_whole() -> Result<()> {
    for (label, flush) in cells() {
        let ctx = format!("{label} flush={flush}");
        let db = value_fixture(label, flush).await?;
        let rows = write(
            &db,
            &format!("MATCH (a:{label} {{k: 1}}) WITH a CREATE (a)-[:R]->(:M {{n: 1}})"),
        )
        .await?;
        assert!(
            rows.is_empty(),
            "{ctx}: a write with no RETURN returns no rows"
        );

        let q = format!(
            "MATCH (a:{label} {{k: 1}}), (b:M {{n: 1}}) WITH a, b MERGE (a)-[:R]->(b) \
             ON MATCH SET a.x = a.y + 1"
        );
        write(&db, &q).await?;

        let s = db.session();
        let n = s
            .query(&format!("MATCH (a:{label}) RETURN count(a) AS c"))
            .await?
            .rows()[0]
            .get::<i64>("c")?;
        assert_eq!(n, 1, "{ctx}: CREATE must reuse the bound node");
        let r = s
            .query(&format!(
                "MATCH (:{label} {{k: 1}})-[r:R]->(:M) RETURN count(r) AS c"
            ))
            .await?
            .rows()[0]
            .get::<i64>("c")?;
        assert_eq!(r, 1, "{ctx}: MERGE must match the edge CREATE wrote");
        let rows = s
            .query(&format!("MATCH (a:{label} {{k: 1}}) RETURN a"))
            .await?;
        let p = node_props(rows.rows()[0].value("a").unwrap());
        assert_eq!(
            p.get("x"),
            Some(&Value::Int(21)),
            "{ctx}: ON MATCH SET in {p:?}"
        );
        assert_eq!(p.get("y"), Some(&Value::Int(20)), "{ctx}: y in {p:?}");
        assert_eq!(
            p.get("extra"),
            Some(&Value::String("e".into())),
            "{ctx}: {p:?}"
        );
    }
    Ok(())
}
