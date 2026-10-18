// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! An outer node or relationship variable supports, inside a subquery, every
//! operation it supports outside one.
//!
//! Reported (open-issues plan W5 #3, #4, #5a):
//!
//! - `CALL { WITH a WITH * WHERE NOT EXISTS { MATCH (a)-[:R]->(b:N) WHERE
//!   a.x < 3 } RETURN 1 AS b }` returned no rows when schemaless;
//! - `a:L`, `labels(a)` and `shortestPath((a)-[:R*]->(c))` on an outer
//!   variable inside `EXISTS { }` failed to plan or evaluate;
//! - schemaless `shortestPath` failed with `Edge type R not found`.
//!
//! The matrix crosses entity operations with the contexts an outer variable
//! reaches (`WITH`, `EXISTS`, `NOT EXISTS`, `COUNT { }`, `COLLECT { }`,
//! `CALL { WITH … }`, `OPTIONAL MATCH … WHERE`) in both storage modes, flushed
//! and unflushed. The oracle is the operation evaluated per row at top level,
//! where no subquery is involved; the top-level answers of the operations the
//! oracle depends on are themselves pinned by hand in `TOP_LEVEL`.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(outer_entity_in_subquery)'

// Rust guideline compliant

use std::collections::BTreeMap;

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, UniConfig, Value};

/// `n2` is the only `:L` and `n4` the only `:M`; `n2` and `n4` alone carry
/// `tag`; `n6` has no relationship. Every relationship has a distinct `w`, and
/// `n1-[:S]->n5` is the only `:S`.
///
/// `:R` out-neighbours: n1→n2, n2→n3, n3→n4, n3→n1, n4→n5, n5→n3.
const GRAPH: &str = "CREATE (n1:N {id: 1, x: 1}), \
     (n2:N:L {id: 2, x: 2, tag: 't'}), \
     (n3:N {id: 3, x: 3}), \
     (n4:N:M {id: 4, x: 4, tag: 't'}), \
     (n5:N {id: 5, x: 5}), \
     (:N {id: 6, x: 6}), \
     (n1)-[:R {w: 1}]->(n2), (n2)-[:R {w: 2}]->(n3), (n3)-[:R {w: 3}]->(n4), \
     (n3)-[:R {w: 4}]->(n1), (n4)-[:R {w: 5}]->(n5), (n5)-[:R {w: 6}]->(n3), \
     (n1)-[:S {w: 7}]->(n5)";

/// The clause that binds the outer variables, a unique per-row key, and the
/// variables a `CALL { WITH … }` imports.
#[derive(Clone, Copy)]
struct Prefix {
    clause: &'static str,
    key: &'static str,
    imports: &'static str,
}

/// One node per row.
const NODE: Prefix = Prefix {
    clause: "MATCH (a:N)",
    key: "a.id",
    imports: "a",
};

/// Every node except `n4`, paired with `n4` as `c`.
const PAIR: Prefix = Prefix {
    clause: "MATCH (a:N), (c:N) WHERE c.id = 4 AND a.id <> 4",
    key: "a.id",
    imports: "a, c",
};

/// One relationship per row.
const REL: Prefix = Prefix {
    clause: "MATCH (a:N)-[r]->(c:N)",
    key: "r.w",
    imports: "a, r, c",
};

/// Boolean entity operations over the outer variables, by prefix.
const OPS: &[(Prefix, &str)] = &[
    // Labels.
    (NODE, "a:L"),
    (NODE, "a:L OR a:M"),
    (NODE, "NOT a:L"),
    (NODE, "a:L:N"),
    (NODE, "'L' IN labels(a)"),
    (NODE, "size(labels(a)) > 1"),
    // Properties.
    (NODE, "a.x < 3"),
    (NODE, "a['x'] < 3"),
    (NODE, "properties(a).x < 3"),
    (NODE, "'tag' IN keys(a)"),
    (NODE, "a.tag IS NOT NULL"),
    (NODE, "a {.x}.x < 3"),
    // Identity.
    (PAIR, "id(a) = id(c) - 1"),
    (PAIR, "elementId(a) <> elementId(c) AND a.x = c.x - 1"),
    (PAIR, "a <> c AND a.x > 2"),
    (PAIR, "a.x = c.x - 2"),
    // Patterns from the outer variable.
    (NODE, "EXISTS { (a)-[:R]->(:L) }"),
    (NODE, "EXISTS { MATCH (a)-[:R*2..2]->(z:N) WHERE z.x > 4 }"),
    (NODE, "size([(a)-[:R]->(z) | z.id]) > 1"),
    (NODE, "COUNT { (a)<-[:R]-() } > 1"),
    (PAIR, "COUNT { (a)-[:R*1..2]->(c) } > 0"),
    (PAIR, "EXISTS { MATCH (a)-[:R]->(c) }"),
    (
        PAIR,
        "EXISTS { MATCH p = shortestPath((a)-[:R*]->(c)) WHERE length(p) <= 2 }",
    ),
    // Relationships.
    (REL, "type(r) = 'S'"),
    (REL, "r.w > 3"),
    (REL, "r['w'] > 3"),
    (REL, "properties(r).w > 3"),
    (REL, "'w' IN keys(r) AND r.w < 3"),
    (REL, "startNode(r).x < 3"),
    (REL, "endNode(r).x > 3"),
    (REL, "id(startNode(r)) = id(a) AND c.x > 3"),
    (REL, "elementId(r) IS NOT NULL AND r.w <> 2"),
    (REL, "EXISTS { MATCH (x)-[r]->(y) WHERE y.x > 3 }"),
    (REL, "EXISTS { MATCH (c)-[:R]->(:M) }"),
];

/// Hand-computed top-level answers, as `(key, value)` per row: the oracle's
/// own inputs, pinned independently of it.
const TOP_LEVEL: &[(&str, &[(i64, bool)])] = &[
    (
        "MATCH (a:N) RETURN a.id, a:L",
        &[
            (1, false),
            (2, true),
            (3, false),
            (4, false),
            (5, false),
            (6, false),
        ],
    ),
    (
        "MATCH (a:N), (c:N) WHERE c.id = 4 AND a.id <> 4 \
         RETURN a.id, EXISTS { MATCH p = shortestPath((a)-[:R*]->(c)) WHERE length(p) <= 2 }",
        &[(1, false), (2, true), (3, true), (5, true), (6, false)],
    ),
    (
        "MATCH (a:N) RETURN a.id, EXISTS { MATCH (a)-[:R]->(b:N) }",
        &[
            (1, true),
            (2, true),
            (3, true),
            (4, true),
            (5, true),
            (6, false),
        ],
    ),
];

/// Hand-computed answers for the reported shapes, as sorted debug rows.
const REPORTED: &[(&str, &[&str])] = &[
    (
        "MATCH (a:N) CALL { WITH a WITH * WHERE NOT EXISTS \
         { MATCH (a)-[:R]->(b:N) WHERE a.x < 3 } RETURN 1 AS b } RETURN a.id",
        &["[Int(3)]", "[Int(4)]", "[Int(5)]", "[Int(6)]"],
    ),
    (
        "MATCH (a:N) WHERE EXISTS { MATCH (a)-[:R]->(b:N) WHERE a:L } RETURN a.id",
        &["[Int(2)]"],
    ),
    (
        "MATCH (a:N) WHERE EXISTS { MATCH (a)-[:R]->(b:N) WHERE 'M' IN labels(a) } \
         RETURN a.id",
        &["[Int(4)]"],
    ),
    // `n6` reaches nothing, so a mandatory MATCH drops its pair, as openCypher
    // and Neo4j do. Intended semantics change (user decision, 2026-10-17): this
    // row used to pin the opposite contract, `[Int(6), Null]`, under which a
    // no-path pair kept its row with a NULL path.
    (
        "MATCH (a:N), (c:N) WHERE c.id = 4 AND a.id <> 4 \
         MATCH p = shortestPath((a)-[:R*]->(c)) RETURN a.id, length(p)",
        &[
            "[Int(1), Int(3)]",
            "[Int(2), Int(2)]",
            "[Int(3), Int(1)]",
            "[Int(5), Int(2)]",
        ],
    ),
    (
        "MATCH (a:N), (c:N) WHERE c.id = 4 AND a.id <> 4 \
         RETURN a.id, EXISTS { MATCH p = shortestPath((a)-[:R*]->(c)) WHERE length(p) = 3 }",
        &[
            "[Int(1), Bool(true)]",
            "[Int(2), Bool(false)]",
            "[Int(3), Bool(false)]",
            "[Int(5), Bool(false)]",
            "[Int(6), Bool(false)]",
        ],
    ),
];

/// `shortestPath` over relationship types the schema may not declare
/// (schemaless `R` / `S`, and `NOPE`, which has no edges in either mode), with
/// an unbound target of a label the schema may not declare. `n1` reaches `n5`
/// by `S` in one hop and by `R` in four (`n1→n2→n3→n4→n5`); `n6` reaches
/// nothing, so a mandatory MATCH drops its pair and an OPTIONAL MATCH keeps it
/// with a NULL path (see `REPORTED`).
const SHORTEST: &[(&str, &[&str])] = &[
    (
        "MATCH p = shortestPath((a:N {id: 1})-[:R*]->(c:N {id: 4})) RETURN length(p)",
        &["[Int(3)]"],
    ),
    (
        "MATCH (a:N {id: 1}), (c:N {id: 5}) \
         MATCH p = shortestPath((a)-[:R|S*]->(c)) RETURN length(p)",
        &["[Int(1)]"],
    ),
    (
        "MATCH (a:N {id: 1}), (c:N {id: 5}) \
         MATCH p = shortestPath((a)-[:R*]->(c)) RETURN length(p)",
        &["[Int(4)]"],
    ),
    (
        "MATCH (a:N {id: 1}), (c:N {id: 4}) \
         MATCH p = shortestPath((a)-[:NOPE*]->(c)) RETURN count(*), count(p)",
        // No `NOPE` edges, so no path and, under a mandatory MATCH, no row.
        // Intended semantics change (user decision, 2026-10-17): this pinned
        // `[Int(1), Int(0)]`, the NULL-path row of the old contract.
        &["[Int(0), Int(0)]"],
    ),
    (
        "MATCH (a:N), (c:N) WHERE a.id IN [3, 6] AND c.id = 4 \
         OPTIONAL MATCH p = shortestPath((a)-[:R*]->(c)) RETURN a.id, length(p)",
        &["[Int(3), Int(1)]", "[Int(6), Null]"],
    ),
    (
        "MATCH (a:N), (c:N) WHERE a.id IN [3, 6] AND c.id = 4 \
         MATCH p = shortestPath((a)-[:R*]->(c)) RETURN a.id, length(p)",
        // Intended semantics change (user decision, 2026-10-17): `n6` has no
        // path to `n4`, and a mandatory MATCH now drops the pair instead of
        // keeping `[Int(6), Null]`. The OPTIONAL MATCH above still keeps it.
        &["[Int(3), Int(1)]"],
    ),
];

/// An outer node bound by `UNWIND` is one CypherValue column, with no
/// `a._vid`: a body pattern naming it still refers to it. It was taken as a
/// fresh variable, so `EXISTS` held and `COUNT` read 6 (every `:R` edge to an
/// `:N`) on every row; `CALL` failed on the pruned `ns`.
const UNWOUND: &[(&str, &[&str])] = &[
    (
        "MATCH (n:N) WITH collect(n) AS ns UNWIND ns AS a \
         RETURN a.id, EXISTS { MATCH (a)-[:R]->(b:N) WHERE a.x < 3 }",
        &[
            "[Int(1), Bool(true)]",
            "[Int(2), Bool(true)]",
            "[Int(3), Bool(false)]",
            "[Int(4), Bool(false)]",
            "[Int(5), Bool(false)]",
            "[Int(6), Bool(false)]",
        ],
    ),
    (
        "MATCH (n:N) WITH collect(n) AS ns UNWIND ns AS a \
         RETURN a.id, COUNT { MATCH (a)-[:R]->(b:N) }",
        &[
            "[Int(1), Int(1)]",
            "[Int(2), Int(1)]",
            "[Int(3), Int(2)]",
            "[Int(4), Int(1)]",
            "[Int(5), Int(1)]",
            "[Int(6), Int(0)]",
        ],
    ),
    (
        "MATCH (n:N) WITH collect(n) AS ns UNWIND ns AS a \
         RETURN a.id, size([(a)-[:R]->(b:N) | b.id])",
        &[
            "[Int(1), Int(1)]",
            "[Int(2), Int(1)]",
            "[Int(3), Int(2)]",
            "[Int(4), Int(1)]",
            "[Int(5), Int(1)]",
            "[Int(6), Int(0)]",
        ],
    ),
    (
        "MATCH (n:N) WITH collect(n) AS ns UNWIND ns AS a \
         CALL { WITH a MATCH (a)-[:R]->(b:N) RETURN count(b) AS d } RETURN a.id, d",
        &[
            "[Int(1), Int(1)]",
            "[Int(2), Int(1)]",
            "[Int(3), Int(2)]",
            "[Int(4), Int(1)]",
            "[Int(5), Int(1)]",
            "[Int(6), Int(0)]",
        ],
    ),
];

/// `startNode` / `endNode` of a relationship whose endpoints a projection
/// dropped: the case an outer relationship inside a subquery body is in.
/// `startNode(r).x` is 1 for `w` 1 and 7, 2 for `w` 2, and above 2 otherwise.
const ENDPOINTS: &[(&str, &[&str])] = &[
    (
        "MATCH (a:N)-[r]->(c:N) WITH r AS rel WHERE startNode(rel).x < 3 RETURN rel.w",
        &["[Int(1)]", "[Int(2)]", "[Int(7)]"],
    ),
    // `q` is the start node, but carries only the columns its own clause
    // loaded: the endpoint must still be read with its properties.
    (
        "MATCH (a:N)-[r]->(c:N) WITH r AS rel, a AS q WHERE startNode(rel).x < 3 RETURN rel.w",
        &["[Int(1)]", "[Int(2)]", "[Int(7)]"],
    ),
    (
        "MATCH (a:N)-[r]->(c:N) WITH r AS rel, a AS q \
         RETURN rel.w, startNode(rel).x < 3, endNode(rel).id",
        &[
            "[Int(1), Bool(true), Int(2)]",
            "[Int(2), Bool(true), Int(3)]",
            "[Int(3), Bool(false), Int(4)]",
            "[Int(4), Bool(false), Int(1)]",
            "[Int(5), Bool(false), Int(5)]",
            "[Int(6), Bool(false), Int(3)]",
            "[Int(7), Bool(true), Int(5)]",
        ],
    ),
    (
        "MATCH (a:N)-[r]->(c:N) RETURN r.w, \
         EXISTS { MATCH (x)-[r]->(y) WHERE y.x > 3 }",
        &[
            "[Int(1), Bool(false)]",
            "[Int(2), Bool(false)]",
            "[Int(3), Bool(true)]",
            "[Int(4), Bool(false)]",
            "[Int(5), Bool(true)]",
            "[Int(6), Bool(false)]",
            "[Int(7), Bool(true)]",
        ],
    ),
];

/// A correlated operator over an input with no rows yields no rows: neither a
/// `CALL { }` body nor a procedure runs, correlated or not.
const EMPTY_INPUT: &[(&str, &[&str])] = &[
    (
        "MATCH (a:N) WHERE a.id > 100 \
         CALL { WITH a MATCH (a)-[:R]->(b:N) RETURN b.id AS i } RETURN i",
        &[],
    ),
    (
        "MATCH (a:N) WHERE a.id > 100 CALL { WITH a RETURN a:L AS v } RETURN v",
        &[],
    ),
    (
        "MATCH (a:N) WHERE a.id > 100 CALL { MATCH (x:N) RETURN x.id AS i } RETURN i",
        &[],
    ),
    (
        "MATCH (a:N) WHERE a.id > 100 \
         CALL { MATCH (x:N) RETURN count(x) AS c } RETURN count(*)",
        &["[Int(0)]"],
    ),
    (
        "MATCH (a:N) WHERE a.id > 100 CALL uni.system.echo('x') YIELD echo RETURN echo",
        &[],
    ),
    (
        "MATCH (a:N) WHERE a.id > 100 \
         CALL uni.system.echo(toString(a.x)) YIELD echo RETURN echo",
        &[],
    ),
    // A procedure only the row-based fallback executor implements.
    (
        "MATCH (a:N) WHERE a.id > 100 \
         CALL uni.admin.compactionStatus() YIELD l1_runs RETURN l1_runs",
        &[],
    ),
];

/// A label on an outer node in a body pattern tests that node, in every
/// subquery form: `n2` is the only `:L` and has an `:R` out-edge; `n4` the only
/// `:M`, also with one.
const OUTER_PATTERN_LABEL: &[(&str, &[&str])] = &[
    (
        "MATCH (a:N) RETURN a.id, EXISTS { MATCH (a:L)-[:R]->() }",
        &[
            "[Int(1), Bool(false)]",
            "[Int(2), Bool(true)]",
            "[Int(3), Bool(false)]",
            "[Int(4), Bool(false)]",
            "[Int(5), Bool(false)]",
            "[Int(6), Bool(false)]",
        ],
    ),
    (
        "MATCH (a:N) RETURN a.id, COUNT { MATCH (a:M)-[:R]->(b) WHERE b.x > a.x }",
        &[
            "[Int(1), Int(0)]",
            "[Int(2), Int(0)]",
            "[Int(3), Int(0)]",
            "[Int(4), Int(1)]",
            "[Int(5), Int(0)]",
            "[Int(6), Int(0)]",
        ],
    ),
    (
        "MATCH (a:N) RETURN a.id, size([(a:L)-[:R]->(b) | b.id])",
        &[
            "[Int(1), Int(0)]",
            "[Int(2), Int(1)]",
            "[Int(3), Int(0)]",
            "[Int(4), Int(0)]",
            "[Int(5), Int(0)]",
            "[Int(6), Int(0)]",
        ],
    ),
    (
        "MATCH (a:N) CALL { WITH a MATCH (a:L)-[:R]->(b) RETURN count(b) AS d } \
         RETURN a.id, d",
        &[
            "[Int(1), Int(0)]",
            "[Int(2), Int(1)]",
            "[Int(3), Int(0)]",
            "[Int(4), Int(0)]",
            "[Int(5), Int(0)]",
            "[Int(6), Int(0)]",
        ],
    ),
];

/// Rows around an in-query procedure call keep their entity semantics.
///
/// A plan holding a procedure the DataFusion engine does not list as its own
/// (every plugin procedure, `uni.system.echo` among them) ran wholly on the
/// row-based fallback executor: `RETURN a` gave the VID, `labels(a)` and every
/// relationship property NULL. A post-`YIELD` predicate on the input rows is
/// filtered before the call; it must not cross an earlier `LIMIT` or a
/// rebinding aggregation between the call and the predicate.
const AROUND_PROCEDURE: &[(&str, &[&str])] = &[
    (
        "MATCH (a:N)-[r]->(c:N) CALL uni.system.echo('x') YIELD echo RETURN r.w",
        &[
            "[Int(1)]", "[Int(2)]", "[Int(3)]", "[Int(4)]", "[Int(5)]", "[Int(6)]", "[Int(7)]",
        ],
    ),
    (
        "MATCH (a:N) CALL uni.system.echo('x') YIELD echo RETURN a.id, size(labels(a)), a:L",
        &[
            "[Int(1), Int(1), Bool(false)]",
            "[Int(2), Int(2), Bool(true)]",
            "[Int(3), Int(1), Bool(false)]",
            "[Int(4), Int(2), Bool(false)]",
            "[Int(5), Int(1), Bool(false)]",
            "[Int(6), Int(1), Bool(false)]",
        ],
    ),
    (
        "MATCH (a:N) CALL uni.system.echo('x') YIELD echo WHERE a.x = 2.0 RETURN a.id",
        &["[Int(2)]"],
    ),
    (
        "MATCH (a:N), (c:N) WHERE c.id = 1 CALL uni.system.echo('x') YIELD echo \
         WITH a, c, echo ORDER BY a.id LIMIT 3 \
         MATCH (a)-[:R]->(z:N) WHERE a.x > c.x RETURN a.id",
        &["[Int(2)]", "[Int(3)]", "[Int(3)]"],
    ),
    (
        "MATCH (a:N) CALL uni.system.echo('x') YIELD echo \
         WITH a.x AS a, count(*) AS c WHERE a > 4 RETURN a",
        &["[Int(5)]", "[Int(6)]"],
    ),
];

/// `coalesce` over a subquery expression: the expression compiler planned
/// DataFusion's `coalesce` UDF directly, past the simplifier that rewrites it
/// to a CASE, and every such query failed with an internal error.
const COALESCE_SUBQUERY: &[(&str, &[&str])] = &[(
    "MATCH (a:N) RETURN a.id, coalesce(toString(EXISTS { (a)-[:R]->(:L) }), 'null')",
    &[
        "[Int(1), String(\"true\")]",
        "[Int(2), String(\"false\")]",
        "[Int(3), String(\"false\")]",
        "[Int(4), String(\"false\")]",
        "[Int(5), String(\"false\")]",
        "[Int(6), String(\"false\")]",
    ],
)];

/// Schemaless, or a declared schema with scalar indexes on `id` and `x`.
#[derive(Clone, Copy, Debug)]
enum Mode {
    Schemaless,
    Indexed,
}

async fn open(mode: Mode, flush: bool) -> Result<Uni> {
    let db = Uni::in_memory()
        .config(UniConfig {
            auto_flush_interval: None,
            ..Default::default()
        })
        .build()
        .await?;
    if let Mode::Indexed = mode {
        db.schema()
            .label("N")
            .property("id", DataType::Int64)
            .property("x", DataType::Int64)
            .property_nullable("tag", DataType::String)
            .index("x", IndexType::Scalar(ScalarType::BTree))
            .index("id", IndexType::Scalar(ScalarType::BTree))
            .done()
            .label("L")
            .done()
            .label("M")
            .done()
            .edge_type("R", &["N"], &["N"])
            .property("w", DataType::Int64)
            .done()
            .edge_type("S", &["N"], &["N"])
            .property("w", DataType::Int64)
            .done()
            .apply()
            .await?;
    }
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute(GRAPH).await?;
    tx.commit().await?;
    if flush {
        db.flush().await?;
    }
    Ok(db)
}

/// Runs `query`, returning each row's values in debug form, sorted.
async fn bag(db: &Uni, query: &str) -> Result<Vec<String>> {
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut rows: Vec<String> = result
        .rows()
        .iter()
        .map(|r| format!("{:?}", r.values()))
        .collect();
    rows.sort();
    Ok(rows)
}

/// Runs a two-column query, returning `key -> value`.
async fn keyed(db: &Uni, query: &str) -> Result<BTreeMap<String, Value>> {
    let result = db
        .session()
        .query(query)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut out = BTreeMap::new();
    for row in result.rows() {
        let values = row.values();
        anyhow::ensure!(values.len() == 2, "expected two columns: {values:?}");
        let key = format!("{:?}", values[0]);
        anyhow::ensure!(
            out.insert(key.clone(), values[1].clone()).is_none(),
            "duplicate key {key}"
        );
    }
    Ok(out)
}

fn row(key: &str, value: &Value) -> String {
    format!("[{key}, {value:?}]")
}

fn sorted(mut rows: Vec<String>) -> Vec<String> {
    rows.sort();
    rows
}

/// The per-row facts every context's expectation is built from.
struct Oracle {
    /// The operation's value per key (may be NULL).
    value: BTreeMap<String, Value>,
    /// Whether the row's `a` has an `:R` relationship to an `:N`.
    has_out: BTreeMap<String, bool>,
    /// The number of such relationships.
    degree: BTreeMap<String, i64>,
}

impl Oracle {
    fn holds(&self, key: &str) -> bool {
        matches!(self.value.get(key), Some(Value::Bool(true)))
    }
}

/// One context: the query for `(prefix, op)` and its expected rows.
type Context = (
    &'static str,
    fn(Prefix, &str) -> String,
    fn(&Oracle) -> Vec<String>,
);

const CONTEXTS: &[Context] = &[
    (
        "after WITH",
        |p, op| format!("{} WITH * RETURN {}, {op}", p.clause, p.key),
        |o| o.value.iter().map(|(k, v)| row(k, v)).collect(),
    ),
    (
        "EXISTS in RETURN",
        |p, op| {
            format!(
                "{} RETURN {}, EXISTS {{ MATCH (a) WHERE {op} }}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Bool(o.holds(k))))
                .collect()
        },
    ),
    (
        "NOT EXISTS in WHERE",
        |p, op| {
            format!(
                "{} WITH * WHERE NOT EXISTS {{ MATCH (a) WHERE {op} }} RETURN {}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .filter(|k| !o.holds(k))
                .map(|k| format!("[{k}]"))
                .collect()
        },
    ),
    (
        "EXISTS over a traversal",
        |p, op| {
            format!(
                "{} RETURN {}, EXISTS {{ MATCH (a)-[:R]->(b:N) WHERE {op} }}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Bool(o.holds(k) && o.has_out[k])))
                .collect()
        },
    ),
    (
        "EXISTS nested in EXISTS",
        |p, op| {
            format!(
                "{} RETURN {}, EXISTS {{ MATCH (a) WHERE EXISTS {{ MATCH (a) WHERE {op} }} }}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Bool(o.holds(k))))
                .collect()
        },
    ),
    (
        "pattern comprehension WHERE",
        |p, op| {
            format!(
                "{} RETURN {}, size([(a)-[:R]->(pc:N) WHERE {op} | 1])",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Int(if o.holds(k) { o.degree[k] } else { 0 })))
                .collect()
        },
    ),
    // Every pattern variable is fresh, so this is the per-row subquery
    // (`PatternComprehensionSubqueryExpr`), not the vectorized operator.
    (
        "unanchored pattern comprehension",
        |p, op| {
            format!(
                "{} RETURN {}, size([(pcx:N)-[:R]->(pcy:N) WHERE id(pcx) = id(a) AND ({op}) | 1])",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Int(if o.holds(k) { o.degree[k] } else { 0 })))
                .collect()
        },
    ),
    (
        "COUNT { }",
        |p, op| {
            format!(
                "{} RETURN {}, COUNT {{ MATCH (a) WHERE {op} }}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Int(i64::from(o.holds(k)))))
                .collect()
        },
    ),
    (
        "COUNT { } in WHERE",
        |p, op| {
            format!(
                "{} WITH * WHERE COUNT {{ MATCH (a) WHERE {op} }} > 0 RETURN {}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .filter(|k| o.holds(k))
                .map(|k| format!("[{k}]"))
                .collect()
        },
    ),
    (
        "COLLECT { }",
        |p, op| {
            format!(
                "{} RETURN {}, COLLECT {{ MATCH (a) WHERE {op} RETURN 1 AS one }}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| {
                    let list = if o.holds(k) {
                        vec![Value::Int(1)]
                    } else {
                        Vec::new()
                    };
                    row(k, &Value::List(list))
                })
                .collect()
        },
    ),
    (
        "CALL { WITH … RETURN op }",
        |p, op| {
            format!(
                "{} CALL {{ WITH {} RETURN {op} AS v }} RETURN {}, v",
                p.clause, p.imports, p.key
            )
        },
        |o| o.value.iter().map(|(k, v)| row(k, v)).collect(),
    ),
    (
        "CALL { WITH … WHERE op }",
        |p, op| {
            format!(
                "{} CALL {{ WITH {} WITH * WHERE {op} RETURN 1 AS one }} RETURN {}",
                p.clause, p.imports, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .filter(|k| o.holds(k))
                .map(|k| format!("[{k}]"))
                .collect()
        },
    ),
    (
        "CALL { WITH … WHERE NOT EXISTS { traversal WHERE op } }",
        |p, op| {
            format!(
                "{} CALL {{ WITH {} WITH * WHERE NOT EXISTS \
                 {{ MATCH (a)-[:R]->(b:N) WHERE {op} }} RETURN 1 AS one }} RETURN {}",
                p.clause, p.imports, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .filter(|k| !(o.holds(k) && o.has_out[*k]))
                .map(|k| format!("[{k}]"))
                .collect()
        },
    ),
    // A procedure call with an input row is the `Apply` operator: its
    // arguments read the outer row, not an imported body.
    (
        "CALL procedure(op) YIELD",
        |p, op| {
            format!(
                "{} CALL uni.system.echo(toString({op})) YIELD echo RETURN {}, echo",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .iter()
                .map(|(k, v)| {
                    let text = match v {
                        Value::Bool(b) => b.to_string(),
                        other => format!("{other:?}"),
                    };
                    row(k, &Value::String(text))
                })
                .collect()
        },
    ),
    // The post-`YIELD` predicate on outer variables only is pushed into the
    // `Apply` operator's input filter where its shape allows.
    (
        "CALL procedure YIELD … WHERE op",
        |p, op| {
            format!(
                "{} CALL uni.system.echo('x') YIELD echo WHERE {op} RETURN {}",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .filter(|k| o.holds(k))
                .map(|k| format!("[{k}]"))
                .collect()
        },
    ),
    (
        "OPTIONAL MATCH … WHERE op",
        |p, op| {
            format!(
                "{} OPTIONAL MATCH (a)-[:R]->(b:N) WHERE {op} RETURN {}, count(b)",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Int(if o.holds(k) { o.degree[k] } else { 0 })))
                .collect()
        },
    ),
    (
        "OPTIONAL MATCH … WHERE EXISTS { op }",
        |p, op| {
            format!(
                "{} OPTIONAL MATCH (a)-[:R]->(b:N) WHERE EXISTS {{ MATCH (a) WHERE {op} }} \
                 RETURN {}, count(b)",
                p.clause, p.key
            )
        },
        |o| {
            o.value
                .keys()
                .map(|k| row(k, &Value::Int(if o.holds(k) { o.degree[k] } else { 0 })))
                .collect()
        },
    ),
];

async fn oracle(db: &Uni, p: Prefix, op: &str) -> Result<Oracle> {
    let value = keyed(db, &format!("{} RETURN {}, {op}", p.clause, p.key)).await?;
    let has_out = keyed(
        db,
        &format!(
            "{} RETURN {}, EXISTS {{ MATCH (a)-[:R]->(b:N) }}",
            p.clause, p.key
        ),
    )
    .await?
    .into_iter()
    .map(|(k, v)| (k, matches!(v, Value::Bool(true))))
    .collect();
    let degree = keyed(
        db,
        &format!(
            "{} OPTIONAL MATCH (a)-[:R]->(b:N) RETURN {}, count(b)",
            p.clause, p.key
        ),
    )
    .await?
    .into_iter()
    .map(|(k, v)| match v {
        Value::Int(n) => Ok((k, n)),
        other => Err(anyhow::anyhow!("count(b) = {other:?}")),
    })
    .collect::<Result<_>>()?;
    Ok(Oracle {
        value,
        has_out,
        degree,
    })
}

async fn check(mode: Mode, flush: bool) -> Result<()> {
    let db = open(mode, flush).await?;
    let mut failures = Vec::new();
    let mut evaluated = 0usize;

    for (query, rows) in TOP_LEVEL {
        let want: Vec<String> = rows
            .iter()
            .map(|(k, v)| format!("[Int({k}), Bool({v})]"))
            .collect();
        match bag(&db, query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}\n  got  {got:?}\n  want {want:?}")),
            Err(e) => failures.push(format!("{query}\n  error {e}")),
        }
    }
    for (query, rows) in REPORTED
        .iter()
        .chain(ENDPOINTS)
        .chain(SHORTEST)
        .chain(UNWOUND)
        .chain(EMPTY_INPUT)
        .chain(COALESCE_SUBQUERY)
        .chain(AROUND_PROCEDURE)
        .chain(OUTER_PATTERN_LABEL)
    {
        let want: Vec<String> = rows.iter().map(|r| (*r).to_string()).collect();
        match bag(&db, query).await {
            Ok(got) if got == want => {}
            Ok(got) => failures.push(format!("{query}\n  got  {got:?}\n  want {want:?}")),
            Err(e) => failures.push(format!("{query}\n  error {e}")),
        }
    }

    for (prefix, op) in OPS {
        let facts = match oracle(&db, *prefix, op).await {
            Ok(facts) => facts,
            Err(e) => {
                failures.push(format!("[top level] {op}\n  oracle error {e}"));
                continue;
            }
        };
        // An operation true on every row, or on none, cannot tell a context
        // that drops the predicate from one that evaluates it.
        let trues = facts.value.keys().filter(|k| facts.holds(k)).count();
        if trues == 0 || trues == facts.value.len() {
            failures.push(format!(
                "[top level] {op}\n  does not discriminate: {:?}",
                facts.value
            ));
        }
        for (name, build, expect) in CONTEXTS {
            let query = build(*prefix, op);
            let want = sorted(expect(&facts));
            evaluated += 1;
            match bag(&db, &query).await {
                Ok(got) if got == want => {}
                Ok(got) => failures.push(format!(
                    "[{name}] {op}\n  {query}\n  got  {got:?}\n  want {want:?}"
                )),
                Err(e) => failures.push(format!("[{name}] {op}\n  {query}\n  error {e}")),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{mode:?}, flushed {flush}: {} of {evaluated} context cases (plus pinned queries) \
         failing:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn outer_entity_in_subquery_schemaless_unflushed() -> Result<()> {
    check(Mode::Schemaless, false).await
}

#[tokio::test]
async fn outer_entity_in_subquery_schemaless_flushed() -> Result<()> {
    check(Mode::Schemaless, true).await
}

#[tokio::test]
async fn outer_entity_in_subquery_indexed_unflushed() -> Result<()> {
    check(Mode::Indexed, false).await
}

#[tokio::test]
async fn outer_entity_in_subquery_indexed_flushed() -> Result<()> {
    check(Mode::Indexed, true).await
}
