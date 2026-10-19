// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A user property whose name starts with `_` reads back like any other.
//!
//! Only the storage layer's own column names (`_vid`, `_eid`, `_labels`,
//! `overflow_json`, ...) are reserved, and a write of one is refused (see
//! `ext_id_on_labelled_nodes_reserved_storage_names_are_refused_at_write`).
//! Any other `_`-prefixed name is accepted on write, but the read paths hid
//! every `_`-prefixed key as if it were internal: the value was stored and
//! then silently missing from `n._foo`, `properties(n)`, `keys(n)`, the
//! returned node, map projections and the `WHERE` filter, depending on the
//! storage state and the read path.
//!
//! Every probe here has an absolute expected answer, and runs on a declared
//! label (`N`, `_foo` undeclared and so stored in the overflow column) and a
//! schemaless one (`M`), for vertices and edges, before and after a flush.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(underscore_property_reads)'

// Rust guideline compliant

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{Session, Uni, UniConfig, Value};

fn config() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        disable_fork_sweeper: true,
        ..Default::default()
    }
}

/// `(vertex label, edge type)` per mode: `N`/`R` are declared, `M`/`Q` are
/// schemaless.
const MODES: [(&str, &str); 2] = [("N", "R"), ("M", "Q")];

async fn seed(session: &Session) -> Result<()> {
    let tx = session.tx().await?;
    for ddl in [
        "CREATE LABEL N (id INT, name STRING)",
        "CREATE EDGE TYPE R (w INT) FROM N TO N",
    ] {
        tx.execute(ddl).await?;
    }
    tx.commit().await?;
    for (l, r) in MODES {
        let tx = session.tx().await?;
        tx.execute(&format!(
            "CREATE (a:{l} {{id: 1, name: 'a', _foo: 5, __dbl: 'd'}}), \
             (b:{l} {{id: 2, name: 'b', _foo: 6}}), \
             (a)-[:{r} {{w: 1, _bar: 7}}]->(b)"
        ))
        .await?;
        tx.commit().await?;
    }
    Ok(())
}

/// A deterministic rendering of a value; property maps sorted by key.
fn canon(v: &Value) -> String {
    fn props(m: &HashMap<String, Value>) -> String {
        let mut kv: Vec<String> = m.iter().map(|(k, v)| format!("{k}:{}", canon(v))).collect();
        kv.sort();
        format!("{{{}}}", kv.join(","))
    }
    match v {
        Value::List(xs) => {
            let mut items: Vec<String> = xs.iter().map(canon).collect();
            items.sort();
            format!("[{}]", items.join(","))
        }
        Value::Map(m) => props(m),
        Value::Node(n) => format!("({:?}{})", n.labels, props(&n.properties)),
        Value::Edge(e) => format!("[{}{}]", e.edge_type, props(&e.properties)),
        other => format!("{other:?}"),
    }
}

async fn bag(session: &Session, q: &str) -> String {
    match session.query(q).await {
        Ok(r) => {
            let mut out: Vec<String> = r
                .rows()
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(canon)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .collect();
            out.sort();
            out.join(" | ")
        }
        Err(e) => format!("ERROR {e}"),
    }
}

/// Every read of a `_`-prefixed property, with its expected answer.
fn probes(l: &str, r: &str) -> Vec<(String, String)> {
    let node =
        format!("([\"{l}\"]{{__dbl:String(\"d\"),_foo:Int(5),id:Int(1),name:String(\"a\")}})");
    let edge = format!("[{r}{{_bar:Int(7),w:Int(1)}}]");
    [
        // Vertex reads.
        ("MATCH (n:L {id: 1}) RETURN n._foo", "Int(5)".to_string()),
        ("MATCH (n:L {id: 1}) RETURN n.__dbl", "String(\"d\")".into()),
        (
            "MATCH (n:L {id: 1}) RETURN properties(n)['_foo']",
            "Int(5)".into(),
        ),
        (
            "MATCH (n:L {id: 1}) RETURN properties(n)",
            "{__dbl:String(\"d\"),_foo:Int(5),id:Int(1),name:String(\"a\")}".into(),
        ),
        (
            "MATCH (n:L {id: 1}) RETURN keys(n)",
            "[String(\"__dbl\"),String(\"_foo\"),String(\"id\"),String(\"name\")]".into(),
        ),
        ("MATCH (n:L {id: 1}) RETURN n", node.clone()),
        ("MATCH (n:L {id: 1}) WITH n RETURN n", node.clone()),
        (
            "MATCH (n:L {id: 1}) RETURN n {._foo}",
            "{_foo:Int(5)}".into(),
        ),
        (
            "MATCH (n:L {id: 1}) RETURN n {.*}",
            "{__dbl:String(\"d\"),_foo:Int(5),id:Int(1),name:String(\"a\")}".into(),
        ),
        ("MATCH (n:L) WHERE n._foo = 5 RETURN n.id", "Int(1)".into()),
        ("MATCH (n:L {_foo: 6}) RETURN n.id", "Int(2)".into()),
        (
            "MATCH (n:L) WHERE n._foo IS NULL RETURN n.id",
            String::new(),
        ),
        (
            "MATCH (n:L) RETURN n.id ORDER BY n._foo DESC",
            "Int(1) | Int(2)".into(),
        ),
        (
            "MATCH (n:L) WITH n WHERE n._foo > 5 RETURN n.id",
            "Int(2)".into(),
        ),
        ("MATCH (a:L)-[:REL]->(b:L) RETURN b._foo", "Int(6)".into()),
        (
            "MATCH (a:L)-[:REL]->(b:L) WHERE b._foo = 6 RETURN a._foo",
            "Int(5)".into(),
        ),
        ("MATCH (a:L)-[:REL]->(b:L) RETURN a", node.clone()),
        // Edge reads.
        ("MATCH (:L)-[e:REL]->() RETURN e._bar", "Int(7)".into()),
        (
            "MATCH (:L)-[e:REL]->() RETURN properties(e)['_bar']",
            "Int(7)".into(),
        ),
        (
            "MATCH (:L)-[e:REL]->() RETURN properties(e)",
            "{_bar:Int(7),w:Int(1)}".into(),
        ),
        (
            "MATCH (:L)-[e:REL]->() RETURN keys(e)",
            "[String(\"_bar\"),String(\"w\")]".into(),
        ),
        ("MATCH (:L)-[e:REL]->() RETURN e", edge.clone()),
        (
            "MATCH (:L)-[e:REL]->() WHERE e._bar = 7 RETURN e.w",
            "Int(1)".into(),
        ),
        (
            "MATCH (:L)-[e:REL {_bar: 7}]->() RETURN e.w",
            "Int(1)".into(),
        ),
        (
            "MATCH (:L)-[e:REL]->() RETURN e {.*}",
            "{_bar:Int(7),w:Int(1)}".into(),
        ),
        // Paths, variable-length and re-bound entities.
        (
            "MATCH p = (:L {id: 1})-[:REL]->() \
             RETURN nodes(p)[0]._foo, properties(nodes(p)[0])['_foo'], \
             relationships(p)[0]._bar, keys(relationships(p)[0]), \
             properties(relationships(p)[0])",
            "Int(5), Int(5), Int(7), [String(\"_bar\"),String(\"w\")], {_bar:Int(7),w:Int(1)}"
                .into(),
        ),
        ("MATCH (:L {id: 2})<-[:REL*1..2]-(a) RETURN a", node.clone()),
        (
            "MATCH (n:L {id: 1}) WITH collect(n) AS ns UNWIND ns AS m \
             RETURN m._foo, keys(m)",
            "Int(5), [String(\"__dbl\"),String(\"_foo\"),String(\"id\"),String(\"name\")]".into(),
        ),
    ]
    .into_iter()
    .map(|(q, want)| (relabel(q, l, r), want))
    .collect()
}

fn relabel(q: &str, l: &str, r: &str) -> String {
    q.replace(":L", &format!(":{l}"))
        .replace(":REL", &format!(":{r}"))
}

/// Writes whose `RETURN` reads the written entity back; each runs in its own
/// transaction, which is rolled back.
fn write_probes(l: &str, r: &str) -> Vec<(String, String)> {
    let node_z =
        format!("([\"{l}\"]{{__dbl:String(\"d\"),_foo:Int(5),id:Int(1),name:String(\"z\")}})");
    let edge_w2 = format!("[{r}{{_bar:Int(7),w:Int(2)}}]");
    [
        ("MATCH (n:L {id: 1}) SET n.name = 'z' RETURN n", node_z),
        (
            "MATCH (n:L {id: 1}) SET n.name = 'z' RETURN properties(n)",
            "{__dbl:String(\"d\"),_foo:Int(5),id:Int(1),name:String(\"z\")}".into(),
        ),
        (
            "MATCH (n:L {id: 2}) SET n._foo = 9 RETURN n._foo, properties(n)['_foo']",
            "Int(9), Int(9)".into(),
        ),
        (
            "MATCH (n:L {id: 2}) SET n += {_baz: 3} RETURN keys(n)",
            "[String(\"_baz\"),String(\"_foo\"),String(\"id\"),String(\"name\")]".into(),
        ),
        // Matches the existing vertex rather than creating one with no name.
        (
            "MERGE (n:L {id: 1, _foo: 5}) RETURN n.name",
            "String(\"a\")".into(),
        ),
        (
            "MERGE (n:L {_foo: 6}) ON MATCH SET n.hit = true RETURN n.id, n._foo, n.hit",
            "Int(2), Int(6), Bool(true)".into(),
        ),
        ("MATCH (:L)-[e:REL]->() SET e.w = 2 RETURN e", edge_w2),
        (
            "MATCH (:L)-[e:REL]->() SET e.w = 2 RETURN properties(e)",
            "{_bar:Int(7),w:Int(2)}".into(),
        ),
        (
            "MATCH (a:L {id: 1}), (b:L {id: 2}) MERGE (a)-[e:REL {_bar: 7}]->(b) RETURN e.w",
            "Int(1)".into(),
        ),
    ]
    .into_iter()
    .map(|(q, want)| (relabel(q, l, r), want))
    .collect()
}

async fn write_bag(session: &Session, q: &str) -> Result<String> {
    let tx = session.tx().await?;
    let got = match tx.query(q).await {
        Ok(r) => {
            let mut out: Vec<String> = r
                .rows()
                .iter()
                .map(|row| {
                    row.values()
                        .iter()
                        .map(canon)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .collect();
            out.sort();
            out.join(" | ")
        }
        Err(e) => format!("ERROR {e}"),
    };
    tx.rollback();
    Ok(got)
}

/// The Locy reads: a rule body filtering and yielding `_foo`.
async fn locy_probe(session: &Session, l: &str) -> Option<String> {
    let program = format!(
        "CREATE RULE hot AS MATCH (n:{l}) WHERE n._foo = 5 YIELD KEY n, n._foo AS f \
         QUERY hot RETURN n.id AS id, f"
    );
    let got = match session.locy(&program).await {
        Ok(result) => {
            let rows = result
                .command_results()
                .iter()
                .find_map(|c| c.as_query())
                .cloned()
                .unwrap_or_default();
            rows.iter()
                .map(|row| {
                    format!(
                        "{}/{}",
                        row.get("id").map(canon).unwrap_or_default(),
                        row.get("f").map(canon).unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join(" | ")
        }
        Err(e) => format!("ERROR {e}"),
    };
    let want = "Int(1)/Int(5)";
    (got != want).then(|| format!("locy {l}: want {want}, got {got}"))
}

async fn failures(session: &Session, state: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (l, r) in MODES {
        for (q, want) in probes(l, r) {
            let got = bag(session, &q).await;
            if got != want {
                out.push(format!("{state}: `{q}`\n   want {want}\n   got  {got}"));
            }
        }
        for (q, want) in write_probes(l, r) {
            let got = match write_bag(session, &q).await {
                Ok(got) => got,
                Err(e) => format!("ERROR {e}"),
            };
            if got != want {
                out.push(format!("{state}: `{q}`\n   want {want}\n   got  {got}"));
            }
        }
        if let Some(f) = locy_probe(session, l).await {
            out.push(format!("{state}: {f}"));
        }
    }
    out
}

/// Every read form, both modes, unflushed and flushed.
#[tokio::test]
async fn underscore_property_reads_every_form_and_state() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    let mut all = failures(&session, "unflushed").await;
    db.flush().await?;
    all.extend(failures(&session, "flushed").await);
    assert!(
        all.is_empty(),
        "{} failures:\n{}",
        all.len(),
        all.join("\n")
    );
    Ok(())
}

/// `MERGE` keyed on an undeclared property of a flushed, declared label.
///
/// Not specific to `_` names: the batched key lookup filtered the per-label
/// table by a column the undeclared key does not have (it lives in the
/// overflow column), so once the label was flushed the MERGE failed with
/// `No field named nick` instead of matching the existing vertex.
#[tokio::test]
async fn underscore_property_reads_merge_on_undeclared_key_after_flush() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    let tx = session.tx().await?;
    tx.execute("MATCH (n:N {id: 1}) SET n.nick = 'q'").await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = session.tx().await?;
    let got = tx
        .query("MERGE (n:N {nick: 'q'}) ON MATCH SET n.hit = true RETURN n.id, n.hit")
        .await?;
    let rows: Vec<String> = got
        .rows()
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(canon)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .collect();
    tx.commit().await?;
    assert_eq!(rows, vec!["Int(1), Bool(true)".to_string()]);
    let count = bag(&session, "MATCH (n:N) RETURN count(n)").await;
    assert_eq!(count, "Int(2)", "MERGE created a duplicate");
    Ok(())
}

/// `_id` is an ordinary user property, not an alias for the entity's id.
///
/// It is a common key in imported JSON and never identifies an entity map on
/// its own, so it is not reserved; several readers nevertheless answered
/// `_id` with the vertex or edge id, shadowing the stored value.
#[tokio::test]
async fn underscore_property_reads_id_is_a_user_property() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    for (l, r) in MODES {
        let tx = session.tx().await?;
        tx.execute(&format!(
            "MATCH (a:{l} {{id: 1}})-[e:{r}]->() SET a._id = 'u1', e._id = 'e1'"
        ))
        .await?;
        tx.commit().await?;
    }
    let mut failures = Vec::new();
    for state in ["unflushed", "flushed"] {
        if state == "flushed" {
            db.flush().await?;
        }
        for (l, r) in MODES {
            for (q, want) in [
                ("MATCH (n:L {id: 1}) RETURN n._id", "String(\"u1\")"),
                (
                    "MATCH (n:L {id: 1}) RETURN properties(n)['_id']",
                    "String(\"u1\")",
                ),
                ("MATCH (n:L {id: 1}) RETURN '_id' IN keys(n)", "Bool(true)"),
                ("MATCH (n:L) WHERE n._id = 'u1' RETURN n.id", "Int(1)"),
                ("MATCH (n:L {_id: 'u1'}) RETURN n.id", "Int(1)"),
                ("MATCH (n:L {id: 1}) WITH n RETURN n._id", "String(\"u1\")"),
                (
                    "MATCH (n:L {id: 1}) RETURN n {._id}",
                    "{_id:String(\"u1\")}",
                ),
                ("MATCH (:L)-[e:REL]->() RETURN e._id", "String(\"e1\")"),
                (
                    "MATCH (:L)-[e:REL]->() RETURN properties(e)['_id']",
                    "String(\"e1\")",
                ),
                (
                    "MATCH (:L)-[e:REL]->() RETURN '_id' IN keys(e)",
                    "Bool(true)",
                ),
            ] {
                let q = relabel(q, l, r);
                let got = bag(&session, &q).await;
                if got != want {
                    failures.push(format!("{state}: `{q}`\n   want {want}\n   got  {got}"));
                }
            }
            let got = bag(&session, &format!("MATCH (n:{l} {{id: 1}}) RETURN n")).await;
            if !got.contains("_id:String(\"u1\")") {
                failures.push(format!("{state}: RETURN n on {l} lacks _id: {got}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}
