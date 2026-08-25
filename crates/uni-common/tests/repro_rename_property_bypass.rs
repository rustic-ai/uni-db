//! Repro for crates/uni-common/src/core/schema.rs:2107
//!
//! `SchemaManager::rename_property` never validates `new_name`, so it bypasses
//! both guards that `add_property`/`declare_property` enforce:
//!   1. the reserved-storage-column guard (ext_id/overflow_json/eid/... which
//!      collide with internal Arrow columns and cause Lance "Duplicate field
//!      name" at flush time), and
//!   2. the leading-underscore rule.
//!
//! ADD PROPERTY rejects these names; RENAME PROPERTY silently accepts them.

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::sync::Arc;
use uni_common::DataType;
use uni_common::core::schema::SchemaManager;

/// Returns the manager together with its `TempDir` guard.
///
/// The guard has to travel with the manager: the store is rooted at
/// `dir.path()`, so dropping the `TempDir` here would delete the directory out
/// from under a live `SchemaManager`. This previously used
/// `std::mem::forget(dir)` to keep it alive, which worked but leaked the
/// directory **on disk** — it was never cleaned up, once per test run, forever.
/// miri flags exactly this (`error: memory leaked: … tempfile::TempDir`), which
/// is how it was found. Handing the guard back keeps the directory alive for as
/// long as it is needed and still deletes it at end of test.
async fn new_manager() -> (SchemaManager, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let path = ObjectStorePath::from("schema.json");
    let manager = SchemaManager::load_from_store(store, &path).await.unwrap();
    (manager, dir)
}

#[tokio::test]
async fn rename_property_bypasses_reserved_storage_column_guard() {
    let (manager, _dir) = new_manager().await;
    manager.add_label("Foo").unwrap();
    manager
        .add_property("Foo", "a", DataType::Int64, true)
        .unwrap();

    // Baseline: ADD PROPERTY with the reserved storage-column name is REJECTED.
    let add_err = manager
        .add_property("Foo", "ext_id", DataType::Int64, true)
        .expect_err("add_property('ext_id') must be rejected as reserved");
    assert!(
        add_err.to_string().contains("reserved"),
        "add_property error should mention 'reserved', got: {add_err}"
    );

    // FIXED (schema.rs): RENAME PROPERTY now validates `new_name` like ADD
    // PROPERTY, so renaming to the reserved storage-column name is rejected and
    // the schema is left untouched.
    let rename_err = manager
        .rename_property("Foo", "a", "ext_id")
        .expect_err("rename to reserved 'ext_id' must be rejected");
    assert!(
        rename_err.to_string().contains("reserved"),
        "rename error should mention 'reserved', got: {rename_err}"
    );
    let props = manager.schema();
    let foo = props.properties.get("Foo").unwrap();
    assert!(
        !foo.contains_key("ext_id"),
        "colliding user property 'ext_id' must NOT be installed by a rejected rename"
    );
    assert!(
        foo.contains_key("a"),
        "the original property 'a' must remain (validation runs before mutation)"
    );
}

#[tokio::test]
async fn rename_property_bypasses_leading_underscore_rule() {
    let (manager, _dir) = new_manager().await;
    manager.add_label("Foo").unwrap();
    manager
        .add_property("Foo", "a", DataType::Int64, true)
        .unwrap();

    // Baseline: ADD PROPERTY with a leading-underscore name is REJECTED.
    assert!(
        manager
            .add_property("Foo", "_vid", DataType::Int64, true)
            .is_err(),
        "add_property('_vid') must be rejected (leading underscore reserved)"
    );

    // FIXED (schema.rs): rename to a leading-underscore name is now rejected.
    let rename_result = manager.rename_property("Foo", "a", "_vid");
    assert!(
        rename_result.is_err(),
        "rename to '_vid' must be rejected (leading underscore reserved), got {rename_result:?}"
    );
}
