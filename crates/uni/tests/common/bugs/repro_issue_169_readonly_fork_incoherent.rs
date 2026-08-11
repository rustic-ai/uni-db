// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #169 — a fork taken from a **read-only** parent reports
//! `can_write = true`, accepts a transaction, and commits; afterwards Cypher
//! and Locy disagree about the fork's contents, and neither answer is right.
//! Cypher returns only the base rows; Locy returns only the fork-local row.
//!
//! Candidate root cause, in one branch. `create_fork_2pc` captures the parent's
//! fork point only when the parent has a writer:
//!
//! ```ignore
//! let fork_point = if let Some(writer_lock) = &parent.db.writer {
//!     writer.flush_and_capture_fork_point(...).await?
//! } else {
//!     uni_store::ForkPoint::default()
//! };
//! ```
//!
//! A read-only handle has `writer == None` (`UniBuilder::build` sets
//! `writer_field = None` when `read_only`). It has no L0 to flush — that part is
//! fine — but this branch skips **all** HWM capture, yielding
//! `version_hwm = vid_hwm = eid_hwm = 0` against a perfectly non-empty parent.
//!
//! That zero propagates twice. It becomes `info.fork_point_version_hwm`, the
//! fork writer's MVCC version floor, which is why the first commit reports
//! version 1 instead of parent-HWM + 1. And it is passed to
//! `bootstrap_fork_from_primary_hwm`, bootstrapping the fork's VID allocator
//! from 0 — the exact collision `uni_store::fork::id_alloc`'s rustdoc exists to
//! warn about ("the fork's first writes get shadowed by primary's pre-existing
//! rows at the same VID").
//!
//! A fork of a read-write parent is correct in both languages; that is the only
//! variable, and it is pinned below as the control.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

const CYPHER_Q: &str = "MATCH (t:Thing) RETURN t.n AS n";
const LOCY_P: &str = "CREATE RULE thing AS\n\
                      MATCH (t:Thing)\n\
                      YIELD KEY t, t.n AS n";

/// A store with a single `Thing {n: 'base'}`, committed and flushed.
async fn seed(path: &str) -> Result<()> {
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("Thing")
        .property("n", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Thing {n: 'base'})").await?;
    tx.commit().await?;
    db.flush().await?;
    db.shutdown().await?;
    Ok(())
}

fn sorted_names(rows: &uni_db::QueryResult) -> Vec<String> {
    let mut names: Vec<String> = rows
        .rows()
        .iter()
        .filter_map(|r| r.get::<String>("n").ok())
        .collect();
    names.sort();
    names
}

/// Fork the given handle, write one row into the fork, and report what each
/// language sees afterwards.
async fn fork_write_and_read(db: &Uni, fork_name: &str) -> Result<(Vec<String>, Vec<String>)> {
    let fork = db.session().fork(fork_name).await?;
    assert!(
        fork.capabilities().can_write,
        "precondition: the fork reports itself writable"
    );

    let ftx = fork.tx().await?;
    ftx.execute("CREATE (:Thing {n: 'fork_local'})").await?;
    ftx.commit().await?;

    let cypher = sorted_names(&fork.query(CYPHER_Q).await?);

    let locy_result = fork.locy(LOCY_P).await?;
    let mut locy: Vec<String> = locy_result
        .derived
        .get("thing")
        .map(|facts| {
            facts
                .iter()
                .filter_map(|f| f.get("n").and_then(|v| v.as_str().map(str::to_owned)))
                .collect()
        })
        .unwrap_or_default();
    locy.sort();

    Ok((cypher, locy))
}

/// The reported case: fork of a read-only parent.
#[tokio::test]
async fn fork_of_readonly_parent_reads_base_union_fork_local() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    let (cypher, locy) = fork_write_and_read(&reader, "ro_sandbox").await?;

    let expected = vec!["base".to_string(), "fork_local".to_string()];
    assert_eq!(
        cypher, expected,
        "Cypher must read base ∪ fork-local in a fork of a read-only parent"
    );
    assert_eq!(
        locy, expected,
        "Locy must read base ∪ fork-local in a fork of a read-only parent"
    );
    Ok(())
}

/// Control: the identical sequence against a read-write parent. Same store,
/// same schema, same rule, same statements — the only variable is `read_only`.
#[tokio::test]
async fn fork_of_readwrite_parent_reads_base_union_fork_local() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let rw = Uni::open(path).build().await?;
    let (cypher, locy) = fork_write_and_read(&rw, "rw_sandbox").await?;

    let expected = vec!["base".to_string(), "fork_local".to_string()];
    assert_eq!(cypher, expected, "read-write control: Cypher");
    assert_eq!(locy, expected, "read-write control: Locy");
    Ok(())
}

/// The two languages must at minimum agree with each other. Stated separately
/// because incoherence *between* them is the part with no safe reading: Cypher
/// under-reports (a caller concludes the write was lost and retries), while
/// Locy over-reports relative to the base (a fold derives over a graph missing
/// everything it started with, producing a plausible wrong number).
#[tokio::test]
async fn fork_of_readonly_parent_is_coherent_across_languages() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    let (cypher, locy) = fork_write_and_read(&reader, "coherence").await?;

    assert_eq!(
        cypher, locy,
        "one session, one view: Cypher and Locy must not disagree about the \
         fork's contents"
    );
    Ok(())
}
