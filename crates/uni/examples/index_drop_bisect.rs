//! Bisects the LDBC SF1 load sequence for the step that drops a scalar index
//! from Lance's manifest, issue #247.
//!
//! On the SF1 store `Dataset::load_indices()` returns empty while six index
//! directories sit on disk. Version history shows the indices present at v7 and
//! gone at v8, with the row count unchanged. `a_scalar_index_survives_a_later_write`
//! already rules out the obvious culprits on a fresh store — a
//! `bulk_insert_vertices_labeled` write, a flush, and a compaction all preserve
//! the index — so the trigger is something else in the bench's sequence, and the
//! remaining differences are that it declares *more labels* afterwards and then
//! loads *edges*.
//!
//! This runs one step per invocation against a persistent store, so the caller
//! can read Lance's manifest between steps and see exactly which one drops it.
//! Steps are cumulative and must run in order.
//!
//!     STEP=1 UNI_DB=/tmp/bisect cargo run --release -p uni-db \
//!         --example index_drop_bisect
//!
//! Throwaway diagnostic, not shipping code.

use std::collections::HashMap;

use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

fn rows(n: i64, offset: i64) -> Vec<HashMap<String, Value>> {
    (offset..offset + n)
        .map(|i| {
            let mut m = HashMap::new();
            m.insert("name".to_string(), Value::String(format!("name-{i}")));
            m.insert("num".to_string(), Value::Int(i));
            m
        })
        .collect()
}

/// Declare `label` with a BTree index on `name`, exactly as the bench's
/// `ensure_label` does, then bulk-load rows and flush.
async fn load_label(db: &Uni, label: &str, n: i64) -> anyhow::Result<()> {
    db.schema()
        .label(label)
        .property("name", DataType::String)
        .property("num", DataType::Int)
        .index("name", IndexType::Scalar(ScalarType::BTree))
        .done()
        .apply()
        .await?;
    let s = db.session();
    let tx = s.tx().await?;
    tx.bulk_insert_vertices_labeled(&[label], rows(n, 0))
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::var("UNI_DB")?;
    let step: u32 = std::env::var("STEP")?.parse()?;
    let db = Uni::open(&path).build().await?;

    match step {
        // The label whose index we track for the rest of the run.
        1 => load_label(&db, "A", 500).await?,
        // A second label declared *after* A's index exists — the bench declares
        // eight, each after the previous one's data has landed.
        2 => load_label(&db, "B", 500).await?,
        // Edge loading, the other phase the bench runs after every label.
        3 => {
            db.schema()
                .edge_type("REL", &["A"], &["B"])
                .done()
                .apply()
                .await?;
            let s = db.session();
            let tx = s.tx().await?;
            tx.execute(
                "MATCH (a:A), (b:B) WHERE a.num = b.num AND a.num < 200 CREATE (a)-[:REL]->(b)",
            )
            .await?;
            tx.commit().await?;
            db.flush().await?;
        }
        // A third label, to separate "any later label" from "the first one".
        4 => load_label(&db, "C", 500).await?,
        // Add a property to A *after* its index exists. v8 on SF1 kept fragment
        // 0 and the row count and still lost the index list, so it was a
        // metadata-only transaction — and its transaction file carries the full
        // schema. A schema write on an already-indexed dataset is the shape that
        // fits.
        5 => {
            db.schema()
                .label("A")
                .property_nullable("added_later", DataType::String)
                .done()
                .apply()
                .await?;
            db.flush().await?;
        }
        // The same, but with a row written afterwards so the new column is
        // actually materialised into the dataset.
        6 => {
            let s = db.session();
            let tx = s.tx().await?;
            let mut m = HashMap::new();
            m.insert("name".to_string(), Value::String("late".into()));
            m.insert("num".to_string(), Value::Int(99_999));
            m.insert("added_later".to_string(), Value::String("x".into()));
            tx.bulk_insert_vertices_labeled(&["A"], vec![m]).await?;
            tx.commit().await?;
            db.flush().await?;
        }
        // The same write through Cypher rather than the bulk API, to see whether
        // the reconciliation of a newly declared column lives on that path.
        7 => {
            let s = db.session();
            let tx = s.tx().await?;
            tx.execute("CREATE (:A {name: 'late', num: 99999, added_later: 'x'})")
                .await?;
            tx.commit().await?;
            db.flush().await?;
        }
        // Give semantic compaction actual work. `VertexDataset::replace` is
        // documented as "used by compaction to rewrite the table with merged
        // data" and goes through `replace_table_atomic` -> `WriteMode::Overwrite`,
        // which drops every index. It is only reached when there is something to
        // merge, which is why a compaction over a clean fixture preserves the
        // index and this one may not.
        8 => {
            let s = db.session();
            let tx = s.tx().await?;
            tx.execute("MATCH (a:A) WHERE a.num < 100 DETACH DELETE a")
                .await?;
            tx.commit().await?;
            db.flush().await?;
            let n = db
                .session()
                .query("MATCH (a:A) RETURN count(a) AS c")
                .await?;
            println!("  rows after delete: {:?}", n.rows()[0].values()[0]);
            let stats = db.compaction().compact("A").await?;
            println!("  compaction stats: {stats:?}");
        }
        n => anyhow::bail!("unknown step {n}"),
    }
    println!("step {step} done");
    Ok(())
}
