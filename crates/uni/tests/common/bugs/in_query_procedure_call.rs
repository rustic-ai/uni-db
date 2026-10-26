// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A procedure called inside a query, rather than standalone.
//!
//! Four defects, all on the per-row `Apply` path:
//!
//! * `MATCH … CALL p(x)` without `YIELD` was dropped from the plan: the
//!   procedure never ran, so its effect and its argument errors were lost.
//!   A void procedure now runs once per row; a procedure with results must
//!   name them with `YIELD`, as openCypher and Neo4j require.
//! * A yield's type came from a built-in table that knew a handful of
//!   `uni.schema.*` procedures, so every other plugin yield was `Utf8`:
//!   `parseInt(s) YIELD result` gave the string `"1"`, and `result + 1` was
//!   `"11"`. The column now takes the procedure's declared type, as the
//!   standalone call does.
//! * A `WRITE` procedure called per row was refused (`inner write requires a
//!   writer-enabled procedure host`): the sub-plan was built without the
//!   transaction's writer.
//! * Equal input rows shared one cached call, so a writing or
//!   non-deterministic procedure ran once for two rows. A unit body also
//!   multiplied its input row by the rows the body made.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(in_query_procedure_call)'

// Rust guideline compliant

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use uni_db::{DataType, Uni, Value};
use uni_plugin::traits::connector::Principal;
use uni_plugin::{Capability, CapabilitySet};
use uni_query::{ProcedureOutput, ProcedureParam, ProcedureValueType, RegisteredProcedure};

/// Nested executors spend one statement's counters, not a private set per call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_inner_queries_share_scan_counters() -> Result<()> {
    // The direct and wrapped reads must observe the same storage tier even
    // when the suite is under load: an automatic flush would change l0_reads.
    let db = Uni::in_memory()
        .config(uni_db::UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("UNWIND range(1, 12) AS i CREATE (:CounterProbe {i: i})")
        .await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.scan_counters', \
        'MATCH (n:CounterProbe) RETURN n.i AS i', 'READ', '[]', '[]')",
    )
    .await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.nested_counters', \
        'CALL t.scan_counters() YIELD row_json RETURN row_json', 'READ', '[]', '[]')",
    )
    .await?;
    tx.commit().await?;
    for flushed in [false, true] {
        if flushed {
            db.flush().await?;
        }
        let direct = session
            .query("MATCH (n:CounterProbe) RETURN n.i AS i")
            .await?;
        assert_eq!(direct.rows().len(), 12);
        assert_eq!(direct.metrics().rows_scanned, 12);
        for (query, calls) in [
            ("CALL t.scan_counters() YIELD i RETURN i", 1),
            ("CALL t.nested_counters() YIELD row_json RETURN row_json", 1),
            (
                "UNWIND [1, 2, 3] AS k CALL t.scan_counters() YIELD row_json RETURN k, row_json",
                3,
            ),
        ] {
            let result = session.query(query).await?;
            assert_eq!(
                result.rows().len(),
                12 * calls,
                "{query}, flushed={flushed}"
            );
            assert_eq!(
                result.metrics().rows_scanned,
                direct.metrics().rows_scanned * calls,
                "{query}, flushed={flushed}"
            );
            assert_eq!(
                result.metrics().l0_reads,
                direct.metrics().l0_reads * calls,
                "{query}, flushed={flushed}"
            );
        }
    }
    Ok(())
}

async fn bag(db: &Uni, query: &str) -> Result<Vec<String>> {
    let result = db.session().query(query).await?;
    let mut rows: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

/// `test.void(n :: INTEGER) :: ()` — no results; the argument is type-checked
/// when the procedure runs.
fn register_void(db: &Uni) {
    db.procedure_registry().register(RegisteredProcedure {
        name: "test.void".to_string(),
        params: vec![ProcedureParam {
            name: "n".to_string(),
            param_type: ProcedureValueType::Integer,
        }],
        outputs: Vec::new(),
        data: Vec::new(),
    });
}

#[tokio::test]
async fn in_query_procedure_call_without_yield_runs_once_per_row() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    register_void(&db);

    // Runs, and keeps each input row once.
    let got = bag(&db, "UNWIND [1, 2, 2] AS x CALL test.void(x) RETURN x").await?;
    assert_eq!(got, ["[Int(1)]", "[Int(2)]", "[Int(2)]"]);

    // It runs: a bad argument is reported rather than skipped with the call.
    let err = db
        .session()
        .query("UNWIND ['a'] AS x CALL test.void(x) RETURN x")
        .await
        .expect_err("the call must run, and its argument is not an INTEGER");
    assert!(
        err.to_string().contains("InvalidArgumentType"),
        "unexpected error: {err}"
    );

    // Over no rows it does not run at all.
    let got = bag(&db, "UNWIND [] AS x CALL test.void('a') RETURN x").await?;
    assert!(got.is_empty());
    Ok(())
}

#[tokio::test]
async fn in_query_procedure_call_with_results_requires_yield() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db
        .session()
        .query("UNWIND ['1'] AS s CALL uni.number.parseInt(s) RETURN s")
        .await
        .expect_err("a procedure with results needs YIELD inside a query");
    assert!(
        err.to_string().contains("must name them with YIELD"),
        "unexpected error: {err}"
    );
    // Standalone, the implicit yield is still allowed.
    let got = bag(&db, "CALL uni.number.parseInt('5')").await?;
    assert_eq!(got, ["[Int(5)]"]);
    Ok(())
}

#[tokio::test]
async fn in_query_procedure_call_yields_keep_their_declared_type() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let cases: &[(&str, &[&str])] = &[
        (
            "UNWIND ['1', '2'] AS s CALL uni.number.parseInt(s) YIELD result \
             RETURN result, result + 1 AS next",
            &["[Int(1), Int(2)]", "[Int(2), Int(3)]"],
        ),
        (
            "UNWIND ['1.5'] AS s CALL uni.number.parseFloat(s) YIELD result \
             RETURN result * 2 AS twice",
            &["[Float(3.0)]"],
        ),
        (
            "UNWIND [3.9] AS f CALL uni.convert.toInteger(f) YIELD result \
             RETURN result, result = 3 AS is_three",
            &["[Int(3), Bool(true)]"],
        ),
        (
            "UNWIND ['abc', 'xyz'] AS s CALL uni.text.contains(s, 'b') YIELD result \
             WITH s, result WHERE result RETURN s",
            &["[String(\"abc\")]"],
        ),
        // A legacy registered procedure declares its output types too.
        (
            "UNWIND [1] AS x CALL test.typed() YIELD i, f RETURN i + 1 AS i1, f * 2 AS f2",
            &["[Int(8), Float(3.0)]"],
        ),
    ];
    db.procedure_registry().register(RegisteredProcedure {
        name: "test.typed".to_string(),
        params: Vec::new(),
        outputs: vec![
            ProcedureOutput {
                name: "i".to_string(),
                output_type: ProcedureValueType::Integer,
            },
            ProcedureOutput {
                name: "f".to_string(),
                output_type: ProcedureValueType::Float,
            },
        ],
        data: vec![HashMap::from([
            ("i".to_string(), Value::Int(7)),
            ("f".to_string(), Value::Float(1.5)),
        ])],
    });
    for (query, want) in cases {
        let got = bag(&db, query)
            .await
            .map_err(|e| anyhow::anyhow!("{query}: {e}"))?;
        assert_eq!(got, *want, "{query}");
    }
    Ok(())
}

/// A list yield (an algorithm's `nodeIds`) is a list per row, as standalone.
#[tokio::test]
async fn in_query_procedure_call_list_yield_is_a_list() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("P")
        .property("id", DataType::Int)
        .edge_type("LINK", &["P"], &["P"])
        .done()
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {id: 1})-[:LINK]->(:P {id: 2})-[:LINK]->(:P {id: 3})")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    let got = bag(
        &db,
        "MATCH (a:P {id: 1}), (b:P {id: 3}) \
         CALL uni.algo.shortestPath(a, b, ['LINK']) YIELD nodeIds, length \
         RETURN length + 1 AS hops_plus_one, nodeIds = [id(a), nodeIds[1], id(b)] AS ends",
    )
    .await?;
    assert_eq!(got, ["[Int(3), Bool(true)]"]);
    let ids = db
        .session()
        .query(
            "MATCH (a:P {id: 1}), (b:P {id: 3}) \
             CALL uni.algo.shortestPath(a, b, ['LINK']) YIELD nodeIds RETURN nodeIds",
        )
        .await?;
    assert!(
        matches!(ids.rows()[0].values()[0], Value::List(ref l) if l.len() == 3),
        "nodeIds must be a list of three ids: {:?}",
        ids.rows()[0].values()
    );
    Ok(())
}

async fn with_marker_procedure() -> Result<(Uni, uni_db::Session)> {
    let db = Uni::in_memory().build().await?;
    let mut caps = CapabilitySet::new();
    caps.insert(Capability::ProcedureWrites);
    let writer = Arc::new(Principal {
        id: "admin".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: caps,
    });
    let session = db.session().with_principal(writer);
    let tx = session.tx().await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.mark', 'CREATE (:Marker)', 'WRITE', '[]', '[]')",
    )
    .await?;
    tx.commit().await?;
    Ok((db, session))
}

async fn markers(db: &Uni) -> Result<i64> {
    let res = db
        .session()
        .query("MATCH (m:Marker) RETURN count(m) AS n")
        .await?;
    Ok(res.rows()[0].get::<i64>("n")?)
}

/// Requires the multi-thread runtime: the declared procedure bridges
/// sync→async with `block_in_place`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_query_procedure_call_writes_once_per_row() -> Result<()> {
    let (db, session) = with_marker_procedure().await?;
    let tx = session.tx().await?;
    tx.execute("UNWIND [1, 1, 2] AS x CALL t.mark() YIELD row_json RETURN x")
        .await?;
    tx.commit().await?;
    assert_eq!(markers(&db).await?, 3, "one marker per input row");
    Ok(())
}

#[tokio::test]
async fn in_query_procedure_call_equal_rows_are_separate_calls() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let got = bag(
        &db,
        "UNWIND [1, 1, 1] AS x CALL uni.create.uuid() YIELD result \
         RETURN count(DISTINCT result) AS n",
    )
    .await?;
    assert_eq!(got, ["[Int(3)]"]);
    Ok(())
}

/// A unit subquery keeps its input row once however many rows its body made.
#[tokio::test]
async fn in_query_procedure_call_unit_body_keeps_row_count() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    let rows = tx
        .query("UNWIND [1, 2] AS x CALL { UNWIND [1, 2, 3] AS i CREATE (:Q) } RETURN x")
        .await?;
    assert_eq!(rows.rows().len(), 2);
    tx.commit().await?;
    let got = bag(&db, "MATCH (q:Q) RETURN count(q)").await?;
    assert_eq!(got, ["[Int(6)]"]);
    Ok(())
}

/// A node-yielding procedure called after a MATCH yields a node on every row.
///
/// The per-row `Apply` typed the call's output with one `Utf8` column for the
/// node, so anything that read the node as one failed to plan: `id(node)`
/// with `No field named "node._vid"`. The node's property is the matched
/// row's, so a mix-up between rows would show.
#[tokio::test]
async fn in_query_procedure_call_node_yield_after_match() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {uid: 'a'}), (:P {uid: 'b'})")
        .await?;
    tx.commit().await?;

    let r = db
        .session()
        .query(
            "MATCH (p:P) \
             CALL uni.create.vNode(['Copy'], {of: p.uid}) YIELD node \
             RETURN p.uid AS uid, node.of AS of, labels(node) AS l, id(node) AS nid, \
                    node \
             ORDER BY uid",
        )
        .await?;
    assert_eq!(r.rows().len(), 2);
    let mut ids = std::collections::HashSet::new();
    for (row, want) in r.rows().iter().zip(["a", "b"]) {
        ids.insert(row.get::<i64>("nid")?);
        assert_eq!(row.get::<String>("uid")?, want);
        assert_eq!(row.get::<String>("of")?, want);
        assert_eq!(row.get::<Vec<String>>("l")?, vec!["Copy"]);
        assert!(
            row.get::<i64>("nid")? < 0,
            "a virtual node's id is negative: {row:?}"
        );
        match row.value("node") {
            Some(Value::Node(n)) => {
                assert!(n.vid.is_ephemeral(), "{n:?}");
                assert_eq!(n.properties.get("of"), Some(&Value::String(want.into())));
            }
            other => panic!("node should be a Node, got {other:?}"),
        }
    }
    assert_eq!(ids.len(), 2, "each row's call mints its own node");
    Ok(())
}
