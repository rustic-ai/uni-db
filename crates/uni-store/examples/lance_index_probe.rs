// Rust guideline compliant
//! Asks Lance directly why it does not consult the BTree indexes on the LDBC
//! SF1 store, issue #247.
//!
//! Our side of the question is settled: the analyzer collects the BTree column,
//! `build_indexed_property_pushdown` builds the pushdown, and the predicate is
//! handed to `Scanner::filter`. Lance then reports no index load, no parts
//! loaded and no comparisons. Everything above Lance has been ruled out by
//! instrumenting it, so this drops to the layer below and asks Lance itself.
//!
//! Three questions, in the order that narrows fastest:
//!
//!   1. What indices does the dataset actually carry, and which columns and
//!      fragments do they cover? An index whose `fragment_bitmap` does not cover
//!      the live fragments cannot serve a scan over them.
//!   2. What plan does Lance produce for the predicate we push? A
//!      `ScalarIndexQuery` / `MaterializeIndex` node means it chose the index; a
//!      bare `LanceScan` with a filter means it declined.
//!   3. Does the answer change with the literal's form? An `Int64` column
//!      compared against a literal that needs a cast is a common reason an
//!      index is skipped, and it would not show up anywhere above Lance.
//!
//! Throwaway diagnostic, not shipping code.
//!
//!   LDBC_DB=$HOME/uni-bench-tmp/sf1 cargo run --release -p uni-store \
//!       --example lance_index_probe

use lance::Dataset;
use lance::index::DatasetIndexExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("LDBC_DB")?;
    let table = std::env::var("LANCE_TABLE").unwrap_or_else(|_| "vertices_Person".into());
    let uri = format!("{root}/storage/{table}.lance");
    let dataset = Dataset::open(&uri).await?;

    if std::env::var("LANCE_BRIEF").is_ok() {
        let n = dataset.load_indices().await?.len();
        println!(
            "  {table}: version={} rows={} indices={n}",
            dataset.version().version,
            dataset.count_rows(None).await?
        );
        return Ok(());
    }
    println!("dataset version {}", dataset.version().version);
    println!("rows: {}", dataset.count_rows(None).await?);

    let frags = dataset.get_fragments();
    println!("fragments: {}", frags.len());

    println!("\n=== indices ===");
    let indices = dataset.load_indices().await?;
    for idx in indices.iter() {
        let covered = idx
            .fragment_bitmap
            .as_ref()
            .map(|b| b.len() as usize)
            .unwrap_or(0);
        println!(
            "  name={:<28} uuid={} fields={:?} covered_fragments={} dataset_version={}",
            idx.name, idx.uuid, idx.fields, covered, idx.dataset_version
        );
    }

    println!("\n=== indices at each dataset version ===");
    for v in 1..=dataset.version().version {
        match dataset.checkout_version(v).await {
            Ok(d) => {
                let n = d.load_indices().await.map(|i| i.len()).unwrap_or(0);
                let ids: Vec<u64> = d.get_fragments().iter().map(|f| f.id() as u64).collect();
                println!(
                    "  v{v}: indices={n} rows={} fragments={ids:?}",
                    d.count_rows(None).await.unwrap_or(0)
                );
            }
            Err(e) => println!("  v{v}: {e}"),
        }
    }

    println!("\n=== plans ===");
    for filter in [
        "id = 2199023262543",
        "id = 2199023262543 AND id IS NOT NULL",
        "firstName = 'John'",
    ] {
        let mut scanner = dataset.scan();
        scanner.project(&["id"])?;
        match scanner.filter(filter) {
            Ok(_) => {}
            Err(e) => {
                println!("\n[{filter}] filter rejected: {e}");
                continue;
            }
        }
        match scanner.explain_plan(true).await {
            Ok(plan) => {
                let indexed = plan.contains("ScalarIndexQuery")
                    || plan.contains("MaterializeIndex")
                    || plan.contains("index");
                println!("\n[{filter}] uses index: {indexed}");
                for line in plan.lines().take(12) {
                    println!("    {line}");
                }
            }
            Err(e) => println!("\n[{filter}] explain failed: {e}"),
        }
    }
    Ok(())
}
