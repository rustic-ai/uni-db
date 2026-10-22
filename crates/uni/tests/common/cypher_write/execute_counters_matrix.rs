// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `ExecuteResult` mutation counters against the openCypher TCK side-effect
//! vocabulary (`+nodes`, `-nodes`, `+relationships`, `-relationships`,
//! `+properties`, `-properties`, `+labels`, `-labels`).
//!
//! A property is the triple (entity, key, value), as in the TCK: adding one
//! is `+properties 1`, overwriting one with a different value is
//! `+properties 1` and `-properties 1`, assigning the value it already has
//! is no side effect. A label is counted per node it is added to or removed
//! from (the Neo4j `labelsAdded` / `labelsRemoved` reading), and only when
//! the node's label set actually changes.
//!
//! Until wave P1 nothing guarded these counters, and every update reported
//! itself as a create: `SET n.v = 7` said `+nodes 1 +labels 1 +properties
//! 3`. Each case runs before and after a flush, through both `execute`
//! terminals, with and without a declared schema.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, ExecuteResult, Session, Uni};

/// Expected side effects of one statement; unspecified fields are zero.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Fx {
    nodes_created: usize,
    nodes_deleted: usize,
    rels_created: usize,
    rels_deleted: usize,
    props_set: usize,
    props_removed: usize,
    labels_added: usize,
    labels_removed: usize,
}

impl Fx {
    fn of(r: &ExecuteResult) -> Self {
        Self {
            nodes_created: r.nodes_created(),
            nodes_deleted: r.nodes_deleted(),
            rels_created: r.relationships_created(),
            rels_deleted: r.relationships_deleted(),
            props_set: r.properties_set(),
            props_removed: r.properties_removed(),
            labels_added: r.labels_added(),
            labels_removed: r.labels_removed(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Route {
    Execute,
    Builder,
}

async fn exec(s: &Session, route: Route, stmt: &str) -> Result<Fx> {
    let tx = s.tx().await?;
    let r = match route {
        Route::Execute => tx.execute(stmt).await?,
        Route::Builder => tx.execute_with(stmt).run().await?,
    };
    tx.commit().await?;
    Ok(Fx::of(&r))
}

async fn db(schema: bool) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    if schema {
        db.schema()
            .label("T")
            .property("k", DataType::Int64)
            .property_nullable("w", DataType::Int64)
            .property_nullable("v", DataType::Int64)
            .label("Extra")
            .edge_type("R", &["T"], &["T"])
            .property_nullable("w", DataType::Int64)
            .apply()
            .await?;
    }
    Ok(db)
}

/// The statement matrix, run in order against one database. `flush_before`
/// is the index of the case before which the store is flushed, so the
/// update cases read a stored row instead of an L0 one.
fn cases() -> Vec<(&'static str, Fx)> {
    let fx = Fx::default();
    vec![
        (
            "CREATE (:T {k: 1, w: 9})",
            Fx {
                nodes_created: 1,
                labels_added: 1,
                props_set: 2,
                ..fx
            },
        ),
        (
            "CREATE (:T {k: 2, w: null})",
            Fx {
                nodes_created: 1,
                labels_added: 1,
                props_set: 1,
                ..fx
            },
        ),
        // Set1 [3]: a property that did not exist.
        ("MATCH (n:T {k: 1}) SET n.v = 7", Fx { props_set: 1, ..fx }),
        // Set1 [1]: overwrite with a different value.
        (
            "MATCH (n:T {k: 1}) SET n.v = 8",
            Fx {
                props_set: 1,
                props_removed: 1,
                ..fx
            },
        ),
        // The value it already has: no observable change.
        ("MATCH (n:T {k: 1}) SET n.v = 8", fx),
        // Two keys on one node in one SET item list.
        (
            "MATCH (n:T {k: 1}) SET n.v = 9, n.x = 'a'",
            Fx {
                props_set: 2,
                props_removed: 1,
                ..fx
            },
        ),
        // Null removes an existing property; null on an absent key is nothing.
        (
            "MATCH (n:T {k: 1}) SET n.x = null, n.y = null",
            Fx {
                props_removed: 1,
                ..fx
            },
        ),
        // `+=` touches only the keys it names; `w` keeps its value.
        (
            "MATCH (n:T {k: 1}) SET n += {w: 9, z: 1}",
            Fx { props_set: 1, ..fx },
        ),
        // Set4 [2]: `=` replaces the map; v, w and z go, k stays.
        (
            "MATCH (n:T {k: 1}) SET n = {k: 1, q: 2}",
            Fx {
                props_set: 1,
                props_removed: 3,
                ..fx
            },
        ),
        (
            "MERGE (n:T {k: 4}) ON CREATE SET n.v = 1",
            Fx {
                nodes_created: 1,
                labels_added: 1,
                props_set: 2,
                ..fx
            },
        ),
        (
            "MERGE (n:T {k: 4}) ON MATCH SET n.v = 2",
            Fx {
                props_set: 1,
                props_removed: 1,
                ..fx
            },
        ),
        (
            "MERGE (n:T {k: 5})",
            Fx {
                nodes_created: 1,
                labels_added: 1,
                props_set: 1,
                ..fx
            },
        ),
        (
            "MATCH (n:T {k: 5}) SET n:Extra",
            Fx {
                labels_added: 1,
                ..fx
            },
        ),
        ("MATCH (n:T {k: 5}) SET n:Extra", fx),
        (
            "MATCH (n:T {k: 5}) REMOVE n:Extra",
            Fx {
                labels_removed: 1,
                ..fx
            },
        ),
        ("MATCH (n:T {k: 5}) REMOVE n:Extra", fx),
        (
            "MATCH (n:T {k: 4}) REMOVE n.v",
            Fx {
                props_removed: 1,
                ..fx
            },
        ),
        ("MATCH (n:T {k: 4}) REMOVE n.absent", fx),
        // Set1 [6]: created and modified in one statement is one property.
        (
            "CREATE (n:T {k: 6}) SET n.k = 7",
            Fx {
                nodes_created: 1,
                labels_added: 1,
                props_set: 1,
                ..fx
            },
        ),
        (
            "MATCH (a:T {k: 4}), (b:T {k: 5}) CREATE (a)-[:R {w: 1}]->(b)",
            Fx {
                rels_created: 1,
                props_set: 1,
                ..fx
            },
        ),
        (
            "MATCH ()-[r:R]->() SET r.w = 2",
            Fx {
                props_set: 1,
                props_removed: 1,
                ..fx
            },
        ),
        ("MATCH ()-[r:R]->() SET r.u = 1", Fx { props_set: 1, ..fx }),
        (
            "MATCH ()-[r:R]->() REMOVE r.u",
            Fx {
                props_removed: 1,
                ..fx
            },
        ),
        // A deleted entity's properties are not counted (Neo4j has no such
        // counter); see `deleting_counts_entities_not_their_properties`.
        (
            "MATCH ()-[r:R]->() DELETE r",
            Fx {
                rels_deleted: 1,
                ..fx
            },
        ),
        (
            "MATCH (n:T {k: 7}) DETACH DELETE n",
            Fx {
                nodes_deleted: 1,
                ..fx
            },
        ),
    ]
}

async fn run_matrix(schema: bool, route: Route, flush_at: Option<usize>) -> Result<()> {
    let db = db(schema).await?;
    let s = db.session();
    let mut failures = Vec::new();
    for (i, (stmt, expected)) in cases().into_iter().enumerate() {
        if flush_at == Some(i) {
            s.flush().await?;
        }
        let got = exec(&s, route, stmt).await?;
        if got != expected {
            failures.push(format!(
                "  [{i}] {stmt}\n      expected {expected:?}\n      got      {got:?}"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "counter mismatches (schema={schema}, route={route:?}, flush_at={flush_at:?}):\n{}",
        failures.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn counters_follow_tck_side_effects_in_l0() -> Result<()> {
    for schema in [true, false] {
        for route in [Route::Execute, Route::Builder] {
            run_matrix(schema, route, None).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn counters_follow_tck_side_effects_over_stored_rows() -> Result<()> {
    // Flush after the two creates, so every update reads a stored row.
    for schema in [true, false] {
        run_matrix(schema, Route::Execute, Some(2)).await?;
    }
    Ok(())
}

/// The counters are per statement: two statements in one transaction each
/// report their own effects.
#[tokio::test]
async fn counters_are_per_statement_inside_one_transaction() -> Result<()> {
    let db = db(true).await?;
    let s = db.session();
    let tx = s.tx().await?;
    let a = Fx::of(&tx.execute("CREATE (:T {k: 1})").await?);
    let b = Fx::of(&tx.execute("MATCH (n:T {k: 1}) SET n.v = 1").await?);
    tx.commit().await?;
    assert_eq!(
        a,
        Fx {
            nodes_created: 1,
            labels_added: 1,
            props_set: 1,
            ..Fx::default()
        }
    );
    assert_eq!(
        b,
        Fx {
            props_set: 1,
            ..Fx::default()
        }
    );
    Ok(())
}

/// `DELETE` counts the entities it removes, never their properties:
/// `properties_removed` is for an explicit `REMOVE` or `SET ... = null`, as in
/// Neo4j's `QueryStatistics`, which has no counter for properties a delete
/// takes away. (The openCypher TCK's `-properties` on `Delete2` [3], `Delete6`
/// or `Merge1` [14] is the harness diffing the graph before and after —
/// `uni-tck`'s `capture_before` / `capture_after` — not an execution
/// counter, so it does not bind `ExecuteResult`.) An entity named twice in one
/// statement is deleted, and counted, once. Each case runs in L0 and over
/// flushed rows, with and without a declared schema.
#[tokio::test]
async fn deleting_counts_entities_not_their_properties() -> Result<()> {
    let fx = Fx::default();
    let cases: Vec<(&str, &str, Fx)> = vec![
        (
            "CREATE (:A {num: 1}), (:A {num: 2})",
            "MATCH (a:A) DELETE a MERGE (a2:A) RETURN a2.num",
            Fx {
                nodes_created: 1,
                nodes_deleted: 2,
                labels_added: 1,
                ..fx
            },
        ),
        (
            "CREATE (:A {num: 1})-[:R {num: 5}]->(:A {num: 2})",
            "MATCH ()-[r:R]->() DELETE r",
            Fx {
                rels_deleted: 1,
                ..fx
            },
        ),
        (
            "CREATE (:A {num: 1, s: 'x'})-[:R {num: 5}]->(:A {num: 2})",
            "MATCH (n:A {num: 1}) DETACH DELETE n",
            Fx {
                nodes_deleted: 1,
                rels_deleted: 1,
                ..fx
            },
        ),
        (
            "CREATE (:A {num: 1})-[:R {num: 5}]->(:A {num: 2})",
            "MATCH p = (:A)-[:R]->(:A) DELETE p",
            Fx {
                nodes_deleted: 2,
                rels_deleted: 1,
                ..fx
            },
        ),
        // Named twice, deleted and counted once.
        (
            "CREATE (:A {num: 1})-[:R {num: 5}]->(:A {num: 2})",
            "MATCH (n:A {num: 1})-[r:R]->() WITH n, n AS m, r DETACH DELETE n, m, r",
            Fx {
                nodes_deleted: 1,
                rels_deleted: 1,
                ..fx
            },
        ),
        // An explicit REMOVE before the delete is still counted.
        (
            "CREATE (:A {num: 1, s: 'x'})",
            "MATCH (n:A) REMOVE n.s DELETE n",
            Fx {
                nodes_deleted: 1,
                props_removed: 1,
                ..fx
            },
        ),
    ];

    let mut failures = Vec::new();
    for schema in [true, false] {
        for flush in [false, true] {
            for (setup, stmt, expected) in &cases {
                let db = Uni::in_memory().build().await?;
                if schema {
                    db.schema()
                        .label("A")
                        .property_nullable("num", DataType::Int64)
                        .property_nullable("s", DataType::String)
                        .edge_type("R", &["A"], &["A"])
                        .property_nullable("num", DataType::Int64)
                        .apply()
                        .await?;
                }
                let s = db.session();
                exec(&s, Route::Execute, setup).await?;
                if flush {
                    s.flush().await?;
                }
                for route in [Route::Execute, Route::Builder] {
                    let tx = s.tx().await?;
                    let r = match route {
                        Route::Execute => tx.execute(stmt).await?,
                        Route::Builder => tx.execute_with(stmt).run().await?,
                    };
                    tx.rollback();
                    let got = Fx::of(&r);
                    if got != *expected {
                        failures.push(format!(
                            "  {stmt} (schema={schema}, flush={flush}, {route:?})\n      \
                             expected {expected:?}\n      got      {got:?}"
                        ));
                    }
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "delete counters:\n{}",
        failures.join("\n")
    );
    Ok(())
}
