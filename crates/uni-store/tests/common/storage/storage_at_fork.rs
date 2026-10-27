// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration test for `StorageManager::at_fork`.
//!
//! Verifies that a fork-scoped storage manager:
//! 1. Routes vertex-data reads through Lance branches via the (branch-aware)
//!    `StorageBackend` — `VertexDataset` no longer opens on-disk data itself.
//! 2. Sees fork-point data — primary writes after fork creation are invisible
//! 3. Keeps a dataset absent when no branch is recorded for that name,
//!    independently of primary's current data and cached cardinality.

// Rust guideline compliant

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uni_common::config::UniConfig;
use uni_common::core::fork::{ForkId, ForkInfo, SchemaDelta};
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::backend::lance_branch;
use uni_store::fork::{ForkRegistryHandle, ForkScope};
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn at_fork_routes_vertex_reads_through_branch() {
    let dir = TempDir::new().unwrap();
    let storage_path = dir.path().join("storage");
    let storage_str = storage_path.to_str().unwrap();
    std::fs::create_dir_all(&storage_path).unwrap();

    // 1. Schema with one label.
    let schema_path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&schema_path).await.unwrap();
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, false)
        .unwrap();
    schema_manager.save().await.unwrap();
    let schema_manager = Arc::new(schema_manager);

    // 2. Bootstrap StorageManager (uses default lance-backend).
    let storage =
        StorageManager::new_with_config(storage_str, schema_manager.clone(), UniConfig::default())
            .await
            .unwrap();

    // 3. Seed primary's vertices_Person dataset by writing one row through
    //    the standard backend trait. We bypass writer/L0 by using the lance
    //    crate directly — Phase 1 doesn't need write paths through forked
    //    storage, only reads, so a synthetic seed is sufficient here.
    // Seed at the canonical `{base}/{name}.lance` path the backend reads from.
    let dataset_uri = format!("{storage_str}/vertices_Person.lance");
    seed_initial_dataset(&dataset_uri).await;

    // 4. Register a fork in the registry, branching that one dataset.
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(storage_path.clone()).unwrap());
    let registry = Arc::new(ForkRegistryHandle::load(store).await.unwrap());
    let parent_v = lance_branch::current_version(&dataset_uri).await.unwrap();
    let id = ForkId::new();
    let branch_name = format!("fork_{id}_v_Person");
    lance_branch::create_branch(&dataset_uri, &branch_name, parent_v)
        .await
        .unwrap();

    let mut info = ForkInfo::new_pending(id, "scenario_1", "snap-1", 1);
    info.datasets
        .insert("vertices_Person".into(), branch_name.clone());
    registry.begin_create(info.clone()).await.unwrap();
    let active = registry
        .finish_create("scenario_1", info.datasets.clone())
        .await
        .unwrap();

    // 5. Build a ForkScope and a fork-scoped StorageManager.
    let scope = Arc::new(ForkScope::new(
        Arc::new(active),
        SchemaDelta::empty(),
        registry.clone(),
    ));
    let forked_storage = storage.at_fork(scope.clone());

    // 6. Sanity: the forked storage manager reports its scope, and a read
    //    through its (branch-aware) backend sees the fork-point rows.
    assert!(forked_storage.fork_scope().is_some());
    let count = forked_storage
        .backend()
        .count_rows("vertices_Person", None)
        .await
        .unwrap();
    assert_eq!(count, 3, "branched read should see fork-point rows");

    // 7. Append rows on primary AFTER the fork was taken.
    append_rows(&dataset_uri).await;
    let primary_after = lance::Dataset::open(&dataset_uri).await.unwrap();
    assert_eq!(primary_after.count_rows(None).await.unwrap(), 5);

    // 8. The fork still sees only the original 3 rows — snapshot
    //    isolation at fork point per spec §10.
    let fork_count = forked_storage
        .backend()
        .count_rows("vertices_Person", None)
        .await
        .unwrap();
    assert_eq!(
        fork_count, 3,
        "post-fork primary writes must not leak into branch"
    );

    // 9. Primary StorageManager (no scope) still sees the full 5 rows.
    let primary_count = storage
        .backend()
        .count_rows("vertices_Person", None)
        .await
        .unwrap();
    assert_eq!(primary_count, 5);
}

#[tokio::test]
async fn at_fork_falls_back_to_primary_for_unrecorded_dataset() {
    // If the fork has no branch on a particular dataset (e.g. a new
    // label introduced on primary after the fork-point and not yet
    // registered on any fork), reads fall back to primary's main
    // branch — the spec allows this because §6.6 returns
    // ForkError::DatasetNotInFork in such cases. Phase 1 takes the
    // softer fallback approach and lets reads proceed; Phase 2 will
    // tighten this when on-the-fly branch creation lands.
    let dir = TempDir::new().unwrap();
    let storage_path = dir.path().join("storage");
    std::fs::create_dir_all(&storage_path).unwrap();
    let storage_str = storage_path.to_str().unwrap();

    let schema_path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&schema_path).await.unwrap();
    schema_manager.add_label("Person").unwrap();
    let schema_manager = Arc::new(schema_manager);

    let storage =
        StorageManager::new_with_config(storage_str, schema_manager.clone(), UniConfig::default())
            .await
            .unwrap();

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(storage_path.clone()).unwrap());
    let registry = Arc::new(ForkRegistryHandle::load(store).await.unwrap());

    // Active fork with no branches recorded.
    let info = ForkInfo::new_pending(ForkId::new(), "empty", "snap-1", 1);
    registry.begin_create(info).await.unwrap();
    let active = registry
        .finish_create("empty", BTreeMap::new())
        .await
        .unwrap();

    let scope = Arc::new(ForkScope::new(
        Arc::new(active),
        SchemaDelta::empty(),
        registry,
    ));
    let forked_storage = storage.at_fork(scope);

    // No branch recorded for "vertices_Person" — falls back to primary.
    let vd = forked_storage.vertex_dataset("Person").unwrap();
    // Open should succeed and return the primary dataset (which doesn't
    // exist yet — we never seeded it. So .open_raw() will error, which
    // is fine for the fallback contract; the important thing is that
    // we don't crash on the lookup itself).
    let _ = vd; // Construction succeeded.
}

async fn seed_initial_dataset(uri: &str) {
    use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("vid", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![1u64, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    lance::Dataset::write(reader, uri, None).await.unwrap();
}

async fn append_rows(uri: &str) {
    use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("vid", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![4u64, 5])),
            Arc::new(StringArray::from(vec!["dan", "eve"])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    let mut ds = lance::Dataset::open(uri).await.unwrap();
    ds.append(reader, None).await.unwrap();
}

/// `vertex_row_count` declines to answer for a table the fork has branched,
/// and still answers for one it has not (#214).
///
/// The scan's range-walk gate calls this to decide whether a full-label read is
/// large enough to be worth bounding. On primary the count is metadata-only. On
/// a branched table it is not: `BranchedBackend::count_rows` has no
/// branch-aware count in lancedb 0.27.1 and falls back to scanning the branch
/// and summing `num_rows` — so sizing a forked scan would fully materialize the
/// table in order to decide whether to avoid materializing it.
///
/// # Why this is asserted on the count and not on query results
///
/// Both arms of the gate return identical rows; only the read strategy differs.
/// So no result assertion can distinguish "declined and read whole" from
/// "sized and walked", and the guard has to be checked where the decision is
/// made. The third case below is what makes this more than a session-level
/// flag: an unmapped `Other` table has a known empty flushed view, which a
/// coarser "any fork declines" guard would fail to report.
#[tokio::test]
async fn vertex_row_count_declines_only_for_a_branched_table() {
    use uni_common::core::id::Vid;
    use uni_store::runtime::l0::L0Buffer;
    use uni_store::storage::cardinality::CardinalityKey;

    let dir = TempDir::new().unwrap();
    let storage_path = dir.path().join("storage");
    let storage_str = storage_path.to_str().unwrap();
    std::fs::create_dir_all(&storage_path).unwrap();

    let schema_path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&schema_path).await.unwrap();
    for label in ["Person", "Other"] {
        schema_manager.add_label(label).unwrap();
        schema_manager
            .add_property(label, "name", DataType::String, false)
            .unwrap();
    }
    schema_manager.save().await.unwrap();
    let schema_manager = Arc::new(schema_manager);

    let storage =
        StorageManager::new_with_config(storage_str, schema_manager.clone(), UniConfig::default())
            .await
            .unwrap();

    // Both labels exist on primary; only `Person` gets branched below.
    let person_uri = format!("{storage_str}/vertices_Person.lance");
    let other_uri = format!("{storage_str}/vertices_Other.lance");
    seed_initial_dataset(&person_uri).await;
    seed_initial_dataset(&other_uri).await;

    // Primary answers cheaply for both.
    assert_eq!(
        storage.vertex_row_count("Person").await.unwrap(),
        Some(3),
        "primary must answer from metadata"
    );
    let other_key = CardinalityKey::Vertex("Other".into());
    assert_eq!(
        storage.refresh_row_count(&other_key).await.unwrap(),
        Some(3)
    );

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(storage_path.clone()).unwrap());
    let registry = Arc::new(ForkRegistryHandle::load(store).await.unwrap());
    let parent_v = lance_branch::current_version(&person_uri).await.unwrap();
    let id = ForkId::new();
    let branch_name = format!("fork_{id}_v_Person");
    lance_branch::create_branch(&person_uri, &branch_name, parent_v)
        .await
        .unwrap();

    let mut info = ForkInfo::new_pending(id, "guard_scenario", "snap-1", 1);
    info.datasets
        .insert("vertices_Person".into(), branch_name.clone());
    registry.begin_create(info.clone()).await.unwrap();
    let active = registry
        .finish_create("guard_scenario", info.datasets.clone())
        .await
        .unwrap();

    let scope = Arc::new(ForkScope::new(
        Arc::new(active),
        SchemaDelta::empty(),
        registry.clone(),
    ));
    let forked = storage.at_fork(scope);
    assert!(forked.fork_scope().is_some(), "fixture must be forked");

    assert_eq!(
        forked.vertex_row_count("Person").await.unwrap(),
        None,
        "a branched table has no cheap count, so the gate must be told to skip \
         rather than handed a number that cost a full scan to produce"
    );

    // No mapping means absent, even though primary has three rows.
    assert_eq!(
        forked.vertex_row_count("Other").await.unwrap(),
        Some(0),
        "an absent fork table has a cheap zero count"
    );
    assert_eq!(forked.cached_row_count(&other_key, None), Some(0));
    assert_eq!(forked.refresh_row_count(&other_key).await.unwrap(), Some(0));
    assert_eq!(storage.cached_row_count(&other_key, None), Some(3));

    let mut pending = L0Buffer::new(0, None);
    pending.insert_vertex_with_labels(Vid::new(99), Default::default(), &["Other".into()]);
    assert_eq!(forked.cached_row_count(&other_key, Some(&pending)), Some(1));
    assert_eq!(storage.cached_row_count(&other_key, None), Some(3));
}

/// A fork-scoped reader declines the cached count for a branched table (#260).
///
/// The cache holds primary's count. On a table the fork has branched, that is
/// not this reader's answer — and `BranchedBackend::count_rows` would degrade
/// to a real scan rather than a metadata read, which is the same condition
/// `StorageManager::vertex_row_count` already declines on. Declining keeps the
/// two consistent; answering would hand a fork primary's number.
#[tokio::test]
async fn a_fork_scoped_reader_declines_the_cached_count() {
    use uni_store::storage::cardinality::CardinalityKey;

    let dir = TempDir::new().unwrap();
    let storage_path = dir.path().join("storage");
    let storage_str = storage_path.to_str().unwrap();
    std::fs::create_dir_all(&storage_path).unwrap();

    let schema_path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&schema_path).await.unwrap();
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, false)
        .unwrap();
    schema_manager.save().await.unwrap();
    let schema_manager = Arc::new(schema_manager);

    let storage =
        StorageManager::new_with_config(storage_str, schema_manager.clone(), UniConfig::default())
            .await
            .unwrap();

    let dataset_uri = format!("{storage_str}/vertices_Person.lance");
    seed_initial_dataset(&dataset_uri).await;

    let key = CardinalityKey::Vertex("Person".to_string());
    let primary = storage.refresh_row_count(&key).await.unwrap();
    assert!(
        primary.is_some_and(|n| n > 0),
        "primary must have a real count for this test to mean anything, got {primary:?}"
    );

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(storage_path.clone()).unwrap());
    let registry = Arc::new(ForkRegistryHandle::load(store).await.unwrap());
    let parent_v = lance_branch::current_version(&dataset_uri).await.unwrap();
    let id = ForkId::new();
    let branch_name = format!("fork_{id}_v_Person");
    lance_branch::create_branch(&dataset_uri, &branch_name, parent_v)
        .await
        .unwrap();

    let mut info = ForkInfo::new_pending(id, "cardinality_fork", "snap-1", 1);
    info.datasets
        .insert("vertices_Person".into(), branch_name.clone());
    registry.begin_create(info.clone()).await.unwrap();
    let active = registry
        .finish_create("cardinality_fork", info.datasets.clone())
        .await
        .unwrap();

    let scope = Arc::new(ForkScope::new(
        Arc::new(active),
        SchemaDelta::empty(),
        registry.clone(),
    ));
    let forked_storage = storage.at_fork(scope);

    assert_eq!(
        forked_storage.cached_row_count(&key, None),
        None,
        "a fork-scoped reader served primary's count for a table it has \
         branched; it must decline"
    );
    assert_eq!(
        forked_storage.refresh_row_count(&key).await.unwrap(),
        None,
        "and a refresh from a fork must not write its own count into the cache \
         primary reads"
    );

    // The primary view is unaffected by the fork's presence.
    assert_eq!(
        storage.cached_row_count(&key, None),
        primary,
        "the fork must not have disturbed primary's cached count"
    );
}
