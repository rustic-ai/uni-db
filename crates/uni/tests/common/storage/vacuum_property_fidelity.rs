// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `VACUUM` must not destroy the properties it compacts.
//!
//! `VACUUM` flushes and then runs semantic compaction
//! (`uni_query::query::executor::write::execute_vacuum`). Semantic vertex
//! compaction used to rebuild each property map from the **schema-declared**
//! properties alone, which structurally cannot include `ext_id` or
//! `overflow_json` — both are on the reserved-property list — so every
//! `ext_id` and every schemaless property was erased, and `_uid` was
//! recomputed from the truncated map.
//!
//! The pre-existing `VACUUM` coverage
//! (`runtime/admin_features_test.rs`) runs the statement and records
//! "implicitly verifies no error". That is exactly the shape of assertion this
//! defect survived: the statement succeeded, and nothing looked at what it did.
//!
//! `ext_id` is deliberately not asserted here: it is a system column, not a
//! queryable Cypher property, so `n.ext_id` is `Null` both before and after
//! `VACUUM` and an assertion on it would fail for a reason unrelated to this
//! defect. Its preservation is pinned at the storage level, where the column is
//! directly observable.
//!
//! The storage-level counterpart is
//! `uni-store/tests/common/storage/compaction_property_fidelity.rs`.

use anyhow::Result;
use uni_db::Uni;

// Rust guideline compliant

#[tokio::test]
async fn vacuum_preserves_schemaless_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Person (name STRING)").await?;
    // `nickname` is deliberately NOT declared: it lands in `overflow_json`.
    tx.execute("CREATE (:Person {name: 'Alice', ext_id: 'person-1', nickname: 'Al'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Precondition. Without it a post-VACUUM "still present" assertion would
    // pass vacuously on a fixture where the value was never written.
    let before = db
        .session()
        .query("MATCH (n:Person) RETURN n.nickname AS nickname")
        .await?;
    assert_eq!(before.len(), 1, "precondition: one Person exists");
    assert_eq!(
        before.rows()[0].get::<String>("nickname")?,
        "Al",
        "precondition: the schemaless property is readable before VACUUM"
    );

    let tx = db.session().tx().await?;
    tx.execute("VACUUM").await?;
    tx.commit().await?;

    let after = db
        .session()
        .query("MATCH (n:Person) RETURN n.nickname AS nickname, n.name AS name")
        .await?;
    assert_eq!(after.len(), 1, "VACUUM must not drop the vertex");
    assert_eq!(
        after.rows()[0].get::<String>("name")?,
        "Alice",
        "a declared property must survive VACUUM"
    );
    assert_eq!(
        after.rows()[0].get::<String>("nickname")?,
        "Al",
        "VACUUM erased a schemaless property stored in overflow_json"
    );

    Ok(())
}
