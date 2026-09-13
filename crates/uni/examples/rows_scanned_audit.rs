//! Which query shapes report the rows they scanned, and which report nothing?
//!
//! `rows_scanned` is "rows examined by scans, before filtering and projection".
//! It is the counter this repository reaches for whenever it needs to know what
//! a plan *did* rather than how long it took, and it has repeatedly been
//! reached for while silently reading zero:
//!
//! - a Locy rule evaluation reported 0 however much it scanned, because every
//!   rule body runs under a sub-plan boundary that rebuilt the graph context
//!   without carrying the counters;
//! - a batched MERGE reported only what its *outer* MATCH examined — 395 rows
//!   for a sixteen-second query — because the mutation ran on an executor clone
//!   whose counters nobody harvested.
//!
//! Both are fixed. This sweeps the rest rather than waiting for the next one to
//! be discovered by a wrong conclusion.
//!
//! # Reading the output
//!
//! A shape is **suspect** when it returns rows but reports scanning none. That
//! is not automatically a defect — a query answered entirely from an index, or
//! from a buffer the counter does not cover, can legitimately examine no *scan*
//! rows — so the column to judge is `scanned` against `rows` and what the shape
//! must have touched to answer at all.
//!
//! It is a one-way instrument: a nonzero count proves scanning was counted, a
//! zero does not prove nothing was scanned. So this flags candidates to
//! investigate, and says so, rather than asserting a verdict.
//!
//! ```text
//! cargo run --release -p uni-db --example rows_scanned_audit
//! ```

use std::collections::HashMap;

use uni_db::{Uni, Value};

const N: usize = 4_000;

struct Shape {
    label: &'static str,
    cypher: &'static str,
    /// A write needs its own transaction and is rolled back.
    write: bool,
}

const SHAPES: &[Shape] = &[
    Shape {
        label: "label scan",
        cypher: "MATCH (n:Item) RETURN count(n) AS c",
        write: false,
    },
    Shape {
        label: "indexed equality",
        cypher: "MATCH (n:Item) WHERE n.uid = 'e42' RETURN n.uid AS x",
        write: false,
    },
    Shape {
        label: "unindexed predicate",
        cypher: "MATCH (n:Item) WHERE n.tag = 'hot' RETURN count(n) AS c",
        write: false,
    },
    Shape {
        label: "traversal",
        cypher: "MATCH (a:Item)-[:LINK]->(b:Item) RETURN count(b) AS c",
        write: false,
    },
    Shape {
        label: "variable-length path",
        cypher: "MATCH (a:Item {uid:'e0'})-[:LINK*1..3]->(b:Item) RETURN count(b) AS c",
        write: false,
    },
    Shape {
        label: "optional match",
        cypher: "MATCH (a:Item) OPTIONAL MATCH (a)-[:LINK]->(b:Item) RETURN count(b) AS c",
        write: false,
    },
    Shape {
        label: "aggregation + group",
        cypher: "MATCH (n:Item) RETURN n.tag AS t, count(*) AS c",
        write: false,
    },
    Shape {
        label: "order by + limit",
        cypher: "MATCH (n:Item) RETURN n.uid AS u ORDER BY n.uid LIMIT 10",
        write: false,
    },
    Shape {
        label: "distinct",
        cypher: "MATCH (n:Item) RETURN DISTINCT n.tag AS t",
        write: false,
    },
    Shape {
        label: "exists subquery",
        cypher: "MATCH (a:Item) WHERE EXISTS { MATCH (a)-[:LINK]->(:Item) } RETURN count(a) AS c",
        write: false,
    },
    Shape {
        label: "pattern comprehension",
        cypher: "MATCH (a:Item {uid:'e0'}) RETURN size([(a)-[:LINK]->(b) | b.uid]) AS c",
        write: false,
    },
    Shape {
        label: "call subquery",
        cypher: "CALL { MATCH (n:Item) RETURN n.uid AS u LIMIT 5 } RETURN count(u) AS c",
        write: false,
    },
    Shape {
        label: "union",
        cypher: "MATCH (n:Item) WHERE n.uid = 'e1' RETURN n.uid AS u \
                 UNION MATCH (n:Item) WHERE n.uid = 'e2' RETURN n.uid AS u",
        write: false,
    },
    Shape {
        label: "unwind + match",
        cypher: "UNWIND ['e1','e2','e3'] AS k MATCH (n:Item {uid: k}) RETURN count(n) AS c",
        write: false,
    },
    Shape {
        label: "shortest path",
        cypher: "MATCH (a:Item {uid:'e0'}), (b:Item {uid:'e5'}) \
                 MATCH p = shortestPath((a)-[:LINK*1..6]->(b)) RETURN count(p) AS c",
        write: false,
    },
    // --- writes: each runs in its own transaction and is rolled back --------
    Shape {
        label: "SET over a scan",
        cypher: "MATCH (n:Item) WHERE n.tag = 'hot' SET n.tag = 'warm'",
        write: true,
    },
    Shape {
        label: "DELETE over a scan",
        cypher: "MATCH (n:Item) WHERE n.uid = 'e7' DETACH DELETE n",
        write: true,
    },
    Shape {
        label: "MERGE, keyed node",
        cypher: "MERGE (n:Item {uid: 'e11'}) RETURN n.uid AS u",
        write: true,
    },
    Shape {
        label: "MERGE, relationship",
        cypher: "MATCH (a:Item {uid:'e0'}), (b:Item {uid:'e9'}) MERGE (a)-[:LINK]->(b)",
        write: true,
    },
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().join("g").to_string_lossy().into_owned();

    let db = Uni::open(root.clone()).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Item (uid STRING, tag STRING)")
        .await?;
    tx.execute("CREATE EDGE TYPE LINK FROM Item TO Item")
        .await?;
    tx.execute("CREATE INDEX idx_uid FOR (n:Item) ON (n.uid)")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..N)
        .map(|i| {
            HashMap::from([
                ("uid".to_string(), Value::String(format!("e{i}"))),
                (
                    "tag".to_string(),
                    Value::String(if i % 3 == 0 { "hot" } else { "cold" }.to_string()),
                ),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("Item", vertices).await?;
    let edges: Vec<uni_bulk::EdgeData> = (0..N - 1)
        .map(|i| uni_bulk::EdgeData::new(vids[i], vids[i + 1], HashMap::new()))
        .collect();
    bulk.insert_edges("LINK", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;
    drop(db);

    // Reopen so `BulkWriter` rows are in `VidLabelsIndex` (#269/#264); that
    // fan-out would otherwise inflate the counts under test.
    let db = Uni::open(root).build().await?;
    db.flush().await?;
    println!("{N} vertices / {} LINK edges, uid indexed\n", N - 1);

    println!("  {:<26} {:>12} {:>8}", "shape", "rows_scanned", "rows");
    let mut suspect = Vec::new();
    for shape in SHAPES {
        let (scanned, rows) = if shape.write {
            let tx = db.session().tx().await?;
            let r = tx.query(shape.cypher).await;
            let out = match r {
                Ok(res) => (res.metrics().rows_scanned, res.rows().len()),
                Err(e) => {
                    println!("  {:<26} {:>12} {:>8}  {e}", shape.label, "ERR", "-");
                    tx.rollback();
                    continue;
                }
            };
            tx.rollback();
            out
        } else {
            match db.session().query(shape.cypher).await {
                Ok(res) => (res.metrics().rows_scanned, res.rows().len()),
                Err(e) => {
                    println!("  {:<26} {:>12} {:>8}  {e}", shape.label, "ERR", "-");
                    continue;
                }
            }
        };
        let flag = if scanned == 0 {
            "  <-- reports nothing"
        } else {
            ""
        };
        if scanned == 0 {
            suspect.push(shape.label);
        }
        println!("  {:<26} {scanned:>12} {rows:>8}{flag}", shape.label);
    }

    println!();
    if suspect.is_empty() {
        println!("  Every shape reported scan work.");
    } else {
        println!("  Shapes reporting zero — candidates, not verdicts:");
        for s in &suspect {
            println!("    - {s}");
        }
        println!(
            "  A zero is only a defect if the shape had to examine rows to answer.\n  \
             Check each against its plan before concluding."
        );
    }
    Ok(())
}
