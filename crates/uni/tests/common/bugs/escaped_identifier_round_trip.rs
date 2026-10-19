// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Quoted identifiers round-trip end to end: DDL, writes, reads, indexes,
//! constraints, parameters and EXPLAIN all agree on the name a quoted
//! identifier denotes.
//!
//! Before the fix, the grammar had no escape for a backtick inside a quoted
//! name (openCypher's doubled backtick, `` `a``b` ``), and the walker kept the
//! backticks on variables, parameters, index and constraint names and on DDL
//! property names. So `` CREATE LABEL L (`my prop` STRING) `` declared a
//! property literally named `` `my prop` `` that `` n.`my prop` `` never read,
//! and `` MATCH (`n`) RETURN n `` failed to plan.

use std::collections::HashMap;

use uni_db::{Uni, Value};

/// Label and edge-type names may carry `.` and a backtick, not whitespace.
const LABEL: &str = "L.x`1";
const EDGE: &str = "R.x`1";
/// Property names may carry whitespace; a `.` or a backtick only when the
/// property is undeclared (see [`unstorable_property_names_are_schemaless_only`]).
const P_SPACE: &str = "my prop";
const P_KEY: &str = "uniq key";
const P_TICK: &str = "has`tick";
const P_DOT: &str = "a.b";

/// A Cypher quoted identifier, with the doubled-backtick escape.
fn q(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

async fn exec(db: &Uni, cypher: &str) -> anyhow::Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(cypher).await?;
    tx.commit().await?;
    Ok(())
}

async fn rows(db: &Uni, cypher: &str) -> anyhow::Result<Vec<HashMap<String, Value>>> {
    let res = db.session().query(cypher).await?;
    let cols: Vec<String> = res.columns().to_vec();
    Ok(res
        .rows()
        .iter()
        .map(|r| {
            cols.iter()
                .cloned()
                .zip(r.values().iter().cloned())
                .collect::<HashMap<_, _>>()
        })
        .collect())
}

#[tokio::test]
async fn quoted_variables_and_parameters_bind() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    exec(&db, "CREATE (:P {v: 1}), (:P {v: 2})").await?;

    // The quoted and the bare spelling are the same variable.
    let r = rows(&db, "MATCH (`n`:P) RETURN sum(n.v) AS s").await?;
    assert_eq!(r[0]["s"], Value::Int(3));
    let r = rows(&db, "MATCH (`a b`:P) RETURN sum(`a b`.v) AS s").await?;
    assert_eq!(r[0]["s"], Value::Int(3));
    let r = rows(&db, "UNWIND [1, 2] AS `x``y` RETURN sum(`x``y`) AS `s``t`").await?;
    assert_eq!(r[0]["s`t"], Value::Int(3));

    // `$`p q`` is the parameter named `p q`.
    let res = db
        .session()
        .query_with("MATCH (n:P) WHERE n.v = $`p q` RETURN n.v AS v")
        .param("p q", Value::Int(2))
        .fetch_all()
        .await?;
    assert_eq!(res.rows().len(), 1);
    assert_eq!(res.rows()[0].get::<i64>("v")?, 2);
    Ok(())
}

#[tokio::test]
async fn quoted_ddl_names_round_trip() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    let (l, e) = (q(LABEL), q(EDGE));
    // `P_TICK` stays undeclared: a backtick cannot be in a declared name.
    let (ps, pk, pt) = (q(P_SPACE), q(P_KEY), q(P_TICK));

    exec(
        &db,
        &format!("CREATE LABEL {l} (id INT, {ps} STRING, {pk} STRING)"),
    )
    .await?;
    exec(
        &db,
        &format!("CREATE EDGE TYPE {e} ({ps} INT) FROM {l} TO {l}"),
    )
    .await?;

    let info = db.get_label_info(LABEL).await?.expect("label exists");
    let mut declared: Vec<&str> = info.properties.iter().map(|p| p.name.as_str()).collect();
    declared.sort_unstable();
    assert_eq!(declared, vec!["id", P_SPACE, P_KEY]);

    exec(
        &db,
        &format!("CREATE INDEX {} FOR (n:{l}) ON (n.{ps})", q("my idx`1")),
    )
    .await?;
    exec(
        &db,
        &format!(
            "CREATE CONSTRAINT {} ON (n:{l}) ASSERT n.{pk} IS UNIQUE",
            q("uniq`1")
        ),
    )
    .await?;
    let shown = rows(&db, "SHOW INDEXES").await?;
    assert!(
        shown
            .iter()
            .any(|r| r.get("name") == Some(&Value::String("my idx`1".into()))),
        "SHOW INDEXES lacks the unescaped index name: {shown:?}"
    );

    let read = format!(
        "MATCH (n:{l}) WHERE n.{ps} = 's1' AND n.{pt} = 't1' \
         RETURN n.id AS id, n.{pk} AS k, n.{pt} AS t"
    );
    for (i, flush) in [(1, false), (2, true)] {
        exec(
            &db,
            &format!("CREATE (:{l} {{id: {i}, {ps}: 's{i}', {pk}: 'k{i}', {pt}: 't{i}'}})"),
        )
        .await?;
        if flush {
            db.flush().await?;
        }
    }
    exec(
        &db,
        &format!("MATCH (a:{l} {{id: 1}}), (b:{l} {{id: 2}}) CREATE (a)-[:{e} {{{ps}: 7}}]->(b)"),
    )
    .await?;
    db.flush().await?;

    let r = rows(&db, &read).await?;
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(r[0]["id"], Value::Int(1));
    assert_eq!(r[0]["k"], Value::String("k1".into()));
    assert_eq!(r[0]["t"], Value::String("t1".into()));

    let r = rows(
        &db,
        &format!("MATCH (:{l})-[`r``1`:{e}]->(b:{l}) RETURN `r``1`.{ps} AS w, b.id AS id"),
    )
    .await?;
    assert_eq!(r.len(), 1, "{r:?}");
    assert_eq!(r[0]["w"], Value::Int(7));
    assert_eq!(r[0]["id"], Value::Int(2));

    // The constraint holds on the property the quoted name denotes.
    let dup = exec(&db, &format!("CREATE (:{l} {{id: 3, {pk}: 'k1'}})")).await;
    assert!(
        dup.is_err(),
        "the unique constraint on {P_KEY:?} did not fire"
    );

    // EXPLAIN names the unescaped properties.
    let plan = db.session().query_with(&read).explain().await?;
    for name in [P_SPACE, P_TICK] {
        assert!(
            plan.plan_text.contains(name),
            "EXPLAIN lacks {name:?}:\n{}",
            plan.plan_text
        );
    }

    // ALTER and DROP address the same names.
    let pn = q("new p q");
    exec(&db, &format!("ALTER LABEL {l} ADD PROPERTY {pn} STRING")).await?;
    exec(&db, &format!("MATCH (n:{l} {{id: 1}}) SET n.{pn} = 'z'")).await?;
    db.flush().await?;
    let r = rows(&db, &format!("MATCH (n:{l} {{id: 1}}) RETURN n.{pn} AS z")).await?;
    assert_eq!(r[0]["z"], Value::String("z".into()));
    exec(&db, &format!("DROP INDEX {}", q("my idx`1"))).await?;
    exec(&db, &format!("DROP CONSTRAINT {}", q("uniq`1"))).await?;
    exec(&db, &format!("CREATE (:{l} {{id: 4, {pk}: 'k1'}})")).await?;
    Ok(())
}

/// A declared property is a storage column, and Lance cannot carry a column
/// named with `.` (refused at table creation, so every flush failed) or with a
/// backtick (written but not projectable, so every read after a flush failed).
/// Both declarations are now refused up front; the same names undeclared live
/// in the overflow column and round-trip through a flush.
#[tokio::test]
async fn unstorable_property_names_are_schemaless_only() -> anyhow::Result<()> {
    let db = Uni::temporary().build().await?;
    for (name, c) in [(P_DOT, '.'), (P_TICK, '`')] {
        let want = format!("cannot contain '{c}'");
        let err = exec(&db, &format!("CREATE LABEL D (id INT, {} STRING)", q(name)))
            .await
            .expect_err("the property must not be declarable");
        assert!(err.to_string().contains(&want), "{err}");
        // The failed statement leaves no label behind.
        assert!(db.get_label_info("D").await?.is_none(), "label D leaked");
        let err = db
            .schema()
            .label("E")
            .property_nullable(name, uni_db::DataType::String)
            .done()
            .apply()
            .await
            .expect_err("the property must not be declarable via the builder");
        assert!(err.to_string().contains(&want), "{err}");
        assert!(db.get_label_info("E").await?.is_none(), "label E leaked");
    }

    let (pd, pt) = (q(P_DOT), q(P_TICK));
    exec(&db, &format!("CREATE (:S {{id: 1, {pd}: 'd', {pt}: 't'}})")).await?;
    db.flush().await?;
    let r = rows(&db, &format!("MATCH (n:S) RETURN n.{pd} AS d, n.{pt} AS t")).await?;
    assert_eq!(r[0]["d"], Value::String("d".into()));
    assert_eq!(r[0]["t"], Value::String("t".into()));
    Ok(())
}
