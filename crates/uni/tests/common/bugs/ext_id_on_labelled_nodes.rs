// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `ext_id` on a labelled node reads the same in every storage state.
//!
//! A flush moves a labelled vertex's `ext_id` out of the property map and
//! into the per-label table's dedicated `ext_id` column (it is stripped from
//! `overflow_json`). Readers of the per-label table assembled a vertex's
//! undeclared properties from `overflow_json` alone, so after a flush the
//! value was gone: `MATCH (n:N {ext_id: 'x'})` found nothing, `n.ext_id` read
//! NULL, and `MERGE (n:N {ext_id: 'x'})` created a duplicate — while the same
//! queries were correct on unflushed rows and through the unlabelled
//! `(n {ext_id: …})` spelling, which reads the main table.
//!
//! It was data loss as well as a wrong read: a write that left `ext_id`
//! alone (`SET n.name = n.name`) on a flushed labelled vertex read the
//! property map without it and staged that map back, so the next flush wrote
//! `ext_id` NULL to both tables — after which even the unlabelled lookup
//! missed the vertex (the `L0-over-flushed` / `re-flushed` states below).
//! A non-string `ext_id` was dropped at flush from the per-label table
//! outright: excluded from `overflow_json` by name, but only a string fits
//! the `Utf8` column.
//!
//! Every probe here is differential: the unflushed state is the reference and
//! each other state (flushed, a write layered on a flushed row, reopened, a
//! fork) must give byte-identical answers. A few absolute anchors pin that the
//! reference itself is right.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(ext_id_on_labelled)'

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

/// `ext_id` is unique across all vertices, so each label gets its own prefix.
fn px(l: &str) -> &'static str {
    if l == "N" { "x" } else { "m" }
}

/// The two label modes: `N` is declared (typed per-label table with schema
/// columns), `M` is schemaless (no declared properties at all).
const LABELS: [&str; 2] = ["N", "M"];

async fn run(session: &Session, stmt: &str) -> Result<()> {
    let tx = session.tx().await?;
    tx.execute(stmt)
        .await
        .map_err(|e| anyhow::anyhow!("`{stmt}` failed: {e}"))?;
    tx.commit().await?;
    Ok(())
}

async fn seed(session: &Session) -> Result<()> {
    let tx = session.tx().await?;
    for ddl in [
        "CREATE LABEL N (id INT, name STRING)",
        "CREATE EDGE TYPE R () FROM N TO N",
    ] {
        tx.execute(ddl).await?;
    }
    tx.commit().await?;
    for l in LABELS {
        let r = if l == "N" { "R" } else { "Q" };
        let x = px(l);
        run(
            session,
            &format!(
                "CREATE (a:{l} {{id: 1, name: 'a', ext_id: '{x}1'}}), \
                 (b:{l} {{id: 2, name: 'b', ext_id: '{x}2'}}), \
                 (c:{l} {{id: 3, name: 'c'}}), \
                 (a)-[:{r}]->(b), (b)-[:{r}]->(c)"
            ),
        )
        .await?;
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
        Value::List(xs) => format!("[{}]", xs.iter().map(canon).collect::<Vec<_>>().join(",")),
        Value::Map(m) => props(m),
        Value::Node(n) => format!("({:?}{})", n.labels, props(&n.properties)),
        Value::Edge(e) => format!("[{}{}]", e.edge_type, props(&e.properties)),
        other => format!("{other:?}"),
    }
}

async fn bag(session: &Session, q: &str) -> Result<Vec<String>> {
    let r = session
        .query(q)
        .await
        .map_err(|e| anyhow::anyhow!("`{q}` failed: {e}"))?;
    let mut out: Vec<String> = r
        .rows()
        .iter()
        .map(|row| {
            format!(
                "[{}]",
                row.values()
                    .iter()
                    .map(canon)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
        .collect();
    out.sort();
    Ok(out)
}

/// Every read of `ext_id` on a labelled node this suite covers.
fn queries(l: &str) -> Vec<String> {
    let r = if l == "N" { "R" } else { "Q" };
    [
        "MATCH (n:L {ext_id: 'x1'}) RETURN n.id",
        "MATCH (n:L) WHERE n.ext_id = 'x1' RETURN n.id",
        "MATCH (n:L) WHERE n.ext_id IN ['x1', 'x2'] RETURN n.id",
        "MATCH (n:L) WHERE n.ext_id STARTS WITH 'x' RETURN n.id",
        "MATCH (n:L) WHERE n.ext_id IS NULL RETURN n.id",
        "MATCH (n:L) RETURN n.id, n.ext_id",
        "MATCH (n:L) RETURN n.id, properties(n)['ext_id']",
        "MATCH (n:L) RETURN n.id, 'ext_id' IN keys(n)",
        "MATCH (n:L) RETURN n",
        "MATCH (n:L) RETURN n.ext_id ORDER BY n.ext_id",
        "MATCH (a:L)-[:REL]->(b:L) RETURN a.id, b.ext_id",
        "MATCH (a:L)-[:REL]->(b:L {ext_id: 'x2'}) RETURN a.id",
        "MATCH (a:L {ext_id: 'x1'})-[:REL]->(b:L) RETURN b.id",
        "MATCH (a:L)-[:REL]->(b:L) WHERE b.ext_id = 'x2' RETURN a.id",
        "MATCH (a:L)-[:REL]->(b:L) RETURN b",
        "MATCH (n {ext_id: 'x1'}) WHERE n:L RETURN n.id",
        "MATCH (n:L) WITH n WHERE n.ext_id = 'x2' RETURN n.id",
        "OPTIONAL MATCH (n:L {ext_id: 'x2'}) RETURN n.id",
    ]
    .iter()
    .map(|q| {
        q.replace(":L", &format!(":{l}"))
            .replace(":REL", &format!(":{r}"))
            .replace("'x", &format!("'{}", px(l)))
    })
    .collect()
}

async fn snapshot(session: &Session) -> Result<Vec<(String, Vec<String>)>> {
    let mut out = Vec::new();
    for l in LABELS {
        for q in queries(l) {
            let b = bag(session, &q).await?;
            out.push((q, b));
        }
    }
    Ok(out)
}

fn diff(
    state: &str,
    reference: &[(String, Vec<String>)],
    got: &[(String, Vec<String>)],
) -> Vec<String> {
    reference
        .iter()
        .zip(got)
        .filter(|(a, b)| a.1 != b.1)
        .map(|(a, b)| {
            format!(
                "{state}: `{}`\n   unflushed {:?}\n   {state} {:?}",
                a.0, a.1, b.1
            )
        })
        .collect()
}

/// Absolute anchors on the reference, so a state agreeing with a wrong
/// reference cannot pass.
fn anchor(reference: &[(String, Vec<String>)]) {
    let get = |q: &str| {
        reference
            .iter()
            .find(|(k, _)| k == q)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no probe `{q}`"))
    };
    for l in LABELS {
        assert_eq!(
            get(&format!(
                "MATCH (n:{l} {{ext_id: '{}1'}}) RETURN n.id",
                px(l)
            )),
            vec!["[Int(1)]"],
            "{l}"
        );
        assert_eq!(
            get(&format!("MATCH (n:{l}) RETURN n.id, n.ext_id")),
            vec![
                format!("[Int(1), String(\"{}1\")]", px(l)),
                format!("[Int(2), String(\"{}2\")]", px(l)),
                "[Int(3), Null]".to_string()
            ],
            "{l}"
        );
    }
}

/// Flushed, write-over-flushed, reopened: each equals unflushed.
#[tokio::test]
async fn ext_id_on_labelled_nodes_reads_identically_in_every_state() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let uri = dir.path().to_str().unwrap().to_string();
    let mut failures = Vec::new();
    let reference = {
        let db = Uni::open(&uri).config(config()).build().await?;
        let session = db.session();
        seed(&session).await?;
        let reference = snapshot(&session).await?;
        anchor(&reference);

        db.flush().await?;
        failures.extend(diff("flushed", &reference, &snapshot(&session).await?));

        // A write that leaves `ext_id` untouched, layered over a flushed row:
        // the L0 delta must not shadow the stored `ext_id`.
        for l in LABELS {
            run(&session, &format!("MATCH (n:{l}) SET n.name = n.name")).await?;
        }
        failures.extend(diff(
            "L0-over-flushed",
            &reference,
            &snapshot(&session).await?,
        ));
        db.flush().await?;
        failures.extend(diff("re-flushed", &reference, &snapshot(&session).await?));
        db.shutdown().await?;
        reference
    };
    let db = Uni::open(&uri).config(config()).build().await?;
    failures.extend(diff(
        "reopened",
        &reference,
        &snapshot(&db.session()).await?,
    ));
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// A fork reads its parent's flushed `ext_id`s, and its own after a flush.
#[tokio::test]
async fn ext_id_on_labelled_nodes_reads_identically_on_a_fork() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    let reference = snapshot(&session).await?;
    anchor(&reference);
    db.flush().await?;
    let mut failures = Vec::new();
    let fork = session.fork("f").await?;
    failures.extend(diff("fork-of-flushed", &reference, &snapshot(&fork).await?));

    // Writes made through the fork, then flushed into the fork's branch.
    for l in LABELS {
        run(
            &fork,
            &format!("CREATE (:{l} {{id: 9, ext_id: '{}9'}})", px(l)),
        )
        .await?;
    }
    let q = |l: &str| format!("MATCH (n:{l} {{ext_id: '{}9'}}) RETURN n.id", px(l));
    for l in LABELS {
        let before = bag(&fork, &q(l)).await?;
        assert_eq!(before, vec!["[Int(9)]"], "{l}: fork unflushed");
    }
    fork.flush().await?;
    for l in LABELS {
        let after = bag(&fork, &q(l)).await?;
        if after != vec!["[Int(9)]"] {
            failures.push(format!("fork-flushed {l}: {after:?}"));
        }
        assert!(
            bag(&session, &q(l)).await?.is_empty(),
            "{l}: a fork write leaked into primary"
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// `MERGE` on `ext_id` matches a flushed vertex instead of creating a second.
#[tokio::test]
async fn ext_id_on_labelled_nodes_merge_matches_flushed_vertex() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    db.flush().await?;
    for l in LABELS {
        run(
            &session,
            &format!(
                "MERGE (n:{l} {{ext_id: '{}1'}}) ON MATCH SET n.hit = true",
                px(l)
            ),
        )
        .await?;
        assert_eq!(
            bag(
                &session,
                &format!(
                    "MATCH (n:{l}) WHERE n.ext_id = '{}1' RETURN n.id, n.hit",
                    px(l)
                )
            )
            .await?,
            vec!["[Int(1), Bool(true)]"],
            "{l}: MERGE must match the flushed vertex, not create a new one"
        );
        assert_eq!(
            bag(&session, &format!("MATCH (n:{l}) RETURN count(n)")).await?,
            vec!["[Int(3)]"],
            "{l}"
        );
    }
    Ok(())
}

/// Changing and removing `ext_id` on a flushed labelled vertex.
#[tokio::test]
async fn ext_id_on_labelled_nodes_set_and_remove_after_flush() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    db.flush().await?;
    for l in LABELS {
        run(
            &session,
            &format!("MATCH (n:{l} {{id: 1}}) SET n.ext_id = '{}y1'", px(l)),
        )
        .await?;
        run(
            &session,
            &format!("MATCH (n:{l} {{id: 2}}) REMOVE n.ext_id"),
        )
        .await?;
    }
    for round in ["unflushed change", "flushed change"] {
        for l in LABELS {
            assert_eq!(
                bag(&session, &format!("MATCH (n:{l}) RETURN n.id, n.ext_id")).await?,
                vec![
                    format!("[Int(1), String(\"{}y1\")]", px(l)),
                    "[Int(2), Null]".to_string(),
                    "[Int(3), Null]".to_string()
                ],
                "{round} {l}"
            );
            assert!(
                bag(
                    &session,
                    &format!("MATCH (n:{l} {{ext_id: '{}1'}}) RETURN n.id", px(l))
                )
                .await?
                .is_empty(),
                "{round} {l}: the old ext_id must not match"
            );
            assert!(
                bag(
                    &session,
                    &format!("MATCH (n:{l} {{ext_id: '{}2'}}) RETURN n.id", px(l))
                )
                .await?
                .is_empty(),
                "{round} {l}: a removed ext_id must not match"
            );
        }
        db.flush().await?;
    }
    Ok(())
}

/// A non-string `ext_id` reads back the same after a flush.
///
/// The per-label `ext_id` column is `Utf8`, so only a string fits it; any
/// other value must survive in the property blob rather than be dropped by
/// both.
#[tokio::test]
async fn ext_id_on_labelled_nodes_non_string_value_survives_flush() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    run(&session, "CREATE (:N {id: 7, ext_id: 70})").await?;
    run(&session, "CREATE (:M {id: 7, ext_id: 71})").await?;
    let mut failures = Vec::new();
    let qs = [
        "MATCH (n:N {id: 7}) RETURN n.ext_id, properties(n)['ext_id']",
        "MATCH (n:M {id: 7}) RETURN n.ext_id, properties(n)['ext_id']",
        "MATCH (n:N {ext_id: 70}) RETURN n.id",
        "MATCH (n {ext_id: 71}) RETURN n.id",
    ];
    let mut reference = Vec::new();
    for q in qs {
        reference.push(bag(&session, q).await?);
    }
    assert_eq!(reference[0], vec!["[Int(70), Int(70)]"]);
    db.flush().await?;
    for (q, want) in qs.iter().zip(&reference) {
        let got = bag(&session, q).await?;
        if &got != want {
            failures.push(format!("`{q}`: unflushed {want:?}, flushed {got:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Promote carries a fork-only `ext_id` and drops a primary-held one.
///
/// Query results used to omit a flushed labelled vertex's `ext_id`, and
/// promote copies query results, so every promoted vertex lost it — a
/// fork-only vertex silently. Now the fork-only one keeps it, and an
/// insert-only twin of a primary vertex, which cannot share the unique
/// `ext_id`, is inserted without it.
#[tokio::test]
async fn ext_id_on_labelled_nodes_survives_promote() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let db = Uni::open(dir.path().to_str().unwrap())
        .config(config())
        .build()
        .await?;
    let session = db.session();
    seed(&session).await?;
    db.flush().await?;
    let fork = session.fork("p").await?;
    run(&fork, "CREATE (:N {id: 8, name: 'h', ext_id: 'f8'})").await?;
    run(&fork, "MATCH (n:N {id: 1}) SET n.name = 'edited'").await?;
    fork.flush().await?;
    drop(fork);
    let report = db
        .promote_from_fork("p", &[uni_db::PromotePattern::label("N")])
        .await?;
    assert_eq!(report.vertices_inserted, 2, "{report:?}");
    assert_eq!(
        bag(
            &session,
            "MATCH (n:N) WHERE n.id IN [1, 8] RETURN n.id, n.name, n.ext_id"
        )
        .await?,
        vec![
            "[Int(1), String(\"a\"), String(\"x1\")]",
            "[Int(1), String(\"edited\"), Null]",
            "[Int(8), String(\"h\"), String(\"f8\")]",
        ]
    );
    db.flush().await?;
    assert_eq!(
        bag(&session, "MATCH (n:N {ext_id: 'f8'}) RETURN n.id").await?,
        vec!["[Int(8)]"]
    );
    Ok(())
}

/// Write `key: 7` on a vertex and on an edge, then compare the reads of it
/// before and after a flush; returns the mismatches, a refused write among them.
async fn reserved_name_round_trip(key: &str) -> Result<Vec<String>> {
    let mut failures = Vec::new();
    for (write, read) in [
        (
            format!("CREATE (:N {{id: 50, {key}: 7}})"),
            format!("MATCH (n:N {{id: 50}}) RETURN n.{key}, properties(n)['{key}']"),
        ),
        (
            format!("MATCH (a:N {{id: 1}}), (b:N {{id: 3}}) CREATE (a)-[:R {{{key}: 7}}]->(b)"),
            format!(
                "MATCH (:N {{id: 1}})-[r:R]->(:N {{id: 3}}) \
                 RETURN r.{key}, properties(r)['{key}']"
            ),
        ),
    ] {
        let db = Uni::in_memory().config(config()).build().await?;
        let session = db.session();
        seed(&session).await?;
        let tx = session.tx().await?;
        if let Err(e) = tx.execute(&write).await {
            tx.rollback();
            failures.push(format!("`{write}` refused: {e}"));
            continue;
        }
        tx.commit().await?;
        let want = bag(&session, &read).await?;
        db.flush().await?;
        let got = bag(&session, &read).await?;
        if got != want || want != vec!["[Int(7), Int(7)]".to_string()] {
            failures.push(format!("`{read}`: unflushed {want:?}, flushed {got:?}"));
        }
    }
    Ok(failures)
}

/// The edge-table storage column names, written as undeclared properties,
/// read back as written before and after a flush.
///
/// `ext_id` was the one reserved name a flush split out of `overflow_json`.
/// Probed 2026-10-18: `op`, `eid`, `src_vid` and `dst_vid` round-trip on both
/// a vertex and an edge, so none shares the split. So do the main-table
/// column names `type`, `labels` and `props_json`, which is why none of these
/// is in `RESERVED_PROPERTY_NAMES`: this test pins that the write-time
/// refusal stays off names that work.
#[tokio::test]
async fn ext_id_on_labelled_nodes_other_reserved_names_round_trip() -> Result<()> {
    let mut failures = Vec::new();
    for key in [
        "op",
        "eid",
        "src_vid",
        "dst_vid",
        "type",
        "labels",
        "props_json",
    ] {
        failures.extend(reserved_name_round_trip(key).await?);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// Every vertex and edge with its keys; what a refused write must not change.
async fn trace(session: &Session) -> Result<Vec<String>> {
    let mut out = bag(session, "MATCH (n) RETURN labels(n), n.id, keys(n)").await?;
    out.extend(bag(session, "MATCH ()-[r]->() RETURN type(r), keys(r)").await?);
    Ok(out)
}

/// A property named after a storage column is refused at write, on every
/// write path, with a typed error, and leaves nothing behind.
///
/// Probed 2026-10-18 (`{overflow_json: 7}`): the write was accepted and then
/// confused with the storage blob of the same name -- `n.overflow_json` and
/// `r.overflow_json` read NULL in both states; `properties(n)` held 7;
/// `properties(r)` held 7 unflushed and, flushed, the raw encoded blob as a
/// list of byte values. The `_`-prefixed storage columns (`_vid`, `_uid`,
/// `_deleted`, ...) were accepted too, then dropped on a declared label,
/// hidden from `keys()` on a schemaless one, or shadowed by the system value
/// (`n._vid` read the vertex id). `declare_property` already refused all of
/// them; the write paths now refuse them the same way.
#[tokio::test]
async fn ext_id_on_labelled_nodes_reserved_storage_names_are_refused_at_write() -> Result<()> {
    use uni_db::{EdgeData, UniError};

    fn refused(what: &str, key: &str, r: Result<(), UniError>, failures: &mut Vec<String>) {
        match r {
            Err(UniError::InvalidIdentifier { name, .. }) if name == key => {}
            other => failures.push(format!("{what} `{key}`: {other:?}")),
        }
    }

    let mut failures = Vec::new();
    let prefixed = uni_db::core::schema::RESERVED_PROPERTY_PREFIXES
        .iter()
        .map(|p| format!("{p}x"));
    let keys: Vec<String> = uni_db::core::schema::RESERVED_PROPERTY_NAMES
        .iter()
        .map(|k| k.to_string())
        .chain(prefixed)
        .collect();
    for key in keys.iter().map(String::as_str) {
        let db = Uni::in_memory().config(config()).build().await?;
        let session = db.session();
        seed(&session).await?;
        let before = trace(&session).await?;
        for stmt in [
            format!("CREATE (:N {{id: 50, `{key}`: 7}})"),
            format!("CREATE (:M {{id: 50, `{key}`: 7}})"),
            format!("CREATE ({{id: 50, `{key}`: 7}})"),
            format!("MERGE (n:N {{id: 50, `{key}`: 7}})"),
            format!("MERGE (n:N {{id: 50}}) ON CREATE SET n.`{key}` = 7"),
            format!("MATCH (n:N {{id: 1}}) SET n.`{key}` = 7"),
            format!("MATCH (n:M {{id: 1}}) SET n.`{key}` = 7"),
            format!("MATCH (n:N {{id: 1}}) SET n += {{`{key}`: 7}}"),
            format!("MATCH (n:N {{id: 1}}) SET n = {{id: 1, `{key}`: 7}}"),
            format!("MATCH (a:N {{id: 1}}), (b:N {{id: 3}}) CREATE (a)-[:R {{`{key}`: 7}}]->(b)"),
            format!("MATCH (a:M {{id: 1}}), (b:M {{id: 3}}) CREATE (a)-[:Q {{`{key}`: 7}}]->(b)"),
            format!("MATCH (:N {{id: 1}})-[r:R]->() SET r.`{key}` = 7"),
        ] {
            let tx = session.tx().await?;
            let r = tx.execute(&stmt).await.map(|_| ());
            tx.rollback();
            refused(&stmt, key, r, &mut failures);
        }

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Int(60));
        row.insert(key.to_string(), Value::Int(7));
        let tx = session.tx().await?;
        let r = tx
            .bulk_insert_vertices("N", vec![row.clone()])
            .await
            .map(|_| ());
        tx.rollback();
        refused("Transaction::bulk_insert_vertices", key, r, &mut failures);

        let vids: Vec<_> = {
            let r = session
                .query("MATCH (n:N) WHERE n.id IN [1, 3] RETURN id(n) AS v ORDER BY n.id")
                .await?;
            r.rows()
                .iter()
                .map(|row| match row.values()[0] {
                    Value::Int(v) => uni_db::Vid::from(v as u64),
                    ref o => panic!("{o:?}"),
                })
                .collect()
        };
        let mut edge_props = HashMap::new();
        edge_props.insert(key.to_string(), Value::Int(7));
        let tx = session.tx().await?;
        let r = tx
            .bulk_insert_edges("R", vec![(vids[0], vids[1], edge_props.clone())])
            .await;
        tx.rollback();
        refused("Transaction::bulk_insert_edges", key, r, &mut failures);

        let tx = session.tx().await?;
        let mut bulk = tx.bulk_writer().build()?;
        let r = bulk
            .insert_vertices("N", vec![row.clone()])
            .await
            .map(|_| ())
            .map_err(|e| e.downcast::<UniError>().unwrap_or_else(UniError::Internal));
        refused("BulkWriter::insert_vertices", key, r, &mut failures);
        let r = bulk
            .insert_edges(
                "R",
                vec![EdgeData::new(vids[0], vids[1], edge_props.clone())],
            )
            .await
            .map(|_| ())
            .map_err(|e| e.downcast::<UniError>().unwrap_or_else(UniError::Internal));
        refused("BulkWriter::insert_edges", key, r, &mut failures);
        bulk.abort().await?;
        tx.rollback();

        let tx = session.tx().await?;
        let mut appender = tx.appender("N").build()?;
        appender.append(row.clone()).await?;
        let r = appender.finish().await.map(|_| ());
        tx.rollback();
        refused("StreamingAppender::finish", key, r, &mut failures);

        // Nothing a refused write touched is visible, before or after a flush.
        assert_eq!(
            trace(&session).await?,
            before,
            "`{key}`: a refused write left a trace"
        );
        db.flush().await?;
        assert_eq!(
            trace(&session).await?,
            before,
            "`{key}`: a refused write left a trace after flush"
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

/// A flushed edge whose type declares no properties reports its undeclared
/// properties and nothing else.
///
/// The planner asks for `overflow_json` beside `_all_props` for such a type,
/// and the property reader answered that request by storing the blob itself,
/// as a list of byte values, in the edge's property map. So after a flush,
/// `keys(r)`, `properties(r)` and the returned relationship all carried an
/// `overflow_json` entry, and a SET on the edge staged it back as a property.
#[tokio::test]
async fn ext_id_on_labelled_nodes_flushed_edge_does_not_expose_overflow_blob() -> Result<()> {
    let db = Uni::in_memory().config(config()).build().await?;
    let session = db.session();
    seed(&session).await?;
    run(
        &session,
        "MATCH (a:N {id: 1}), (b:N {id: 3}) CREATE (a)-[:R {u: 2}]->(b)",
    )
    .await?;
    let reads = [
        "MATCH (:N {id: 1})-[r:R]->(:N {id: 3}) RETURN keys(r), properties(r), r",
        "MATCH ()-[r:R]->() WHERE r.u = 2 RETURN keys(r), properties(r), r",
        "MATCH (:N {id: 1})-[r]->(:N {id: 3}) RETURN keys(r), properties(r)",
    ];
    let mut want = Vec::new();
    for q in reads {
        want.push(bag(&session, q).await?);
    }
    assert_eq!(
        want[0],
        vec!["[[String(\"u\")], {u:Int(2)}, [R{u:Int(2)}]]"]
    );
    db.flush().await?;
    for (q, w) in reads.iter().zip(&want) {
        assert_eq!(&bag(&session, q).await?, w, "flushed: `{q}`");
    }
    run(
        &session,
        "MATCH (:N {id: 1})-[r:R]->(:N {id: 3}) SET r.z = 9",
    )
    .await?;
    db.flush().await?;
    assert_eq!(
        bag(&session, reads[0]).await?,
        vec!["[[String(\"u\"),String(\"z\")], {u:Int(2),z:Int(9)}, [R{u:Int(2),z:Int(9)}]]"]
    );
    Ok(())
}
