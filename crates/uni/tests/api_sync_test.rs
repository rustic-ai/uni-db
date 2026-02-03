// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, UniSync};

#[test]
fn test_sync_api() -> Result<()> {
    // 1. Initialize
    let db = UniSync::in_memory()?;

    // 2. Schema (Sync)
    db.schema()
        .label("User")
        .property("name", DataType::String)
        .property("age", DataType::Int32)
        .apply()?;

    // 3. Execute
    db.execute("CREATE (:User {name: 'Alice', age: 30})")?;
    db.execute("CREATE (:User {name: 'Bob', age: 25})")?;

    // 4. Query
    let result = db.query("MATCH (u:User) RETURN u.name, u.age ORDER BY u.age")?;
    assert_eq!(result.len(), 2);

    let row0 = &result.rows()[0];
    assert_eq!(row0.get::<String>("u.name")?, "Bob");
    assert_eq!(row0.get::<i32>("u.age")?, 25);

    // 5. Transaction
    let tx = db.begin()?;
    tx.execute("CREATE (:User {name: 'Charlie', age: 40})")?;
    tx.commit()?;

    let result = db.query("MATCH (u:User) WHERE u.name = 'Charlie' RETURN u.age")?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i32>("u.age")?, 40);

    Ok(())
}
