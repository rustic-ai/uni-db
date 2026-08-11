// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #168 family — other state a second handle caches once and never
//! revalidates.
//!
//! #168 itself was the adjacency CSR: an in-memory cache over a *derived*
//! structure whose invalidation relied on the local writer mutating it in
//! place, which is sound only for the handle that owns the writer. The same
//! question applies to every other read cache in the engine, so these probe
//! the ones with the widest blast radius.
//!
//! All use the same shape as the #168 repro: a reader handle warmed before an
//! independent writer handle commits and flushes.

// Rust guideline compliant

use anyhow::Result;
use uni_db::{DataType, Uni};

/// A store with one `Thing {n: 'v1', k: 'a'}`.
async fn seed(path: &str) -> Result<()> {
    let db = Uni::open(path).build().await?;
    db.schema()
        .label("Thing")
        .property("n", DataType::String)
        .property("k", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Thing {n: 'v1', k: 'a'})").await?;
    tx.commit().await?;
    db.flush().await?;
    db.shutdown().await?;
    Ok(())
}

/// A property value updated by an independent writer must be observed by a
/// reader that already read the old value.
///
/// `PropertyManager` keeps a capacity-bounded value cache keyed by
/// `(id, property)` with no version stamp; its `invalidate_*` entry points had
/// no cross-handle caller. If that cache serves a second handle, the reader
/// pins whichever value it happened to read first.
#[tokio::test]
async fn second_handle_observes_updated_property_value() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    let warm = reader
        .session()
        .query("MATCH (t:Thing) RETURN t.n AS n")
        .await?;
    assert_eq!(warm.len(), 1, "precondition: the seeded row is visible");
    assert_eq!(warm.rows()[0].get::<String>("n")?, "v1");

    {
        let w = Uni::open(path).build().await?;
        let tx = w.session().tx().await?;
        tx.execute("MATCH (t:Thing) SET t.n = 'v2'").await?;
        tx.commit().await?;
        w.flush().await?;
        w.shutdown().await?;
    }

    let after = reader
        .session()
        .query("MATCH (t:Thing) RETURN t.n AS n")
        .await?;
    assert_eq!(
        after.rows()[0].get::<String>("n")?,
        "v2",
        "a reader that already read the old value must observe the flushed update"
    );
    Ok(())
}

/// A label added by an independent writer must become visible to a reader that
/// has already loaded the schema.
///
/// `SchemaManager` reads `catalog/schema.json` once at open into an
/// `RwLock<Arc<Schema>>`; `add_label` mutates only the calling handle's copy.
/// A reader that never re-reads the file plans against a stale catalog.
#[tokio::test]
async fn second_handle_observes_label_added_elsewhere() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    reader
        .session()
        .query("MATCH (t:Thing) RETURN t.n AS n")
        .await?;

    {
        let w = Uni::open(path).build().await?;
        w.schema()
            .label("Other")
            .property("m", DataType::String)
            .apply()
            .await?;
        let tx = w.session().tx().await?;
        tx.execute("CREATE (:Other {m: 'x'})").await?;
        tx.commit().await?;
        w.flush().await?;
        w.shutdown().await?;
    }

    let rows = reader
        .session()
        .query("MATCH (o:Other) RETURN o.m AS m")
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "a reader must observe a label another handle added; got {} rows",
        rows.len()
    );
    Ok(())
}

/// A fork created by an independent handle must be visible to a reader's fork
/// listing.
///
/// `ForkRegistryHandle` holds an in-memory cache of
/// `catalog/fork_registry.json`, refreshed only by the local handle's own fork
/// lifecycle calls.
///
/// CONFIRMED and deliberately left failing-but-ignored: unlike the CSR, whether
/// this *should* work is a design question rather than a defect. The registry
/// is documented as single-writer — every mutation goes through
/// `ForkRegistryHandle`'s 2PC state machine — so reloading the cache from disk
/// on read would need to be reconciled with that protocol, not just bolted on.
/// Pinned here so the decision is explicit and the test is ready when it is
/// made. Follows the repo convention for deferred findings.
#[ignore = "confirmed; needs a design call on multi-handle fork-registry visibility"]
#[tokio::test]
async fn second_handle_observes_fork_created_elsewhere() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    seed(path).await?;

    let reader = Uni::open_existing(path).read_only().build().await?;
    let before = reader.list_forks().await;
    assert!(before.is_empty(), "precondition: no forks yet");

    {
        let w = Uni::open(path).build().await?;
        let _fork = w.session().fork("elsewhere").await?;
        w.shutdown().await?;
    }

    let after = reader.list_forks().await;
    assert_eq!(
        after.len(),
        1,
        "a reader must observe a fork another handle created; got {}",
        after.len()
    );
    Ok(())
}
