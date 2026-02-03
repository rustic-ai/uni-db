// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_session_variables() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create Schema
    db.execute("CREATE LABEL User (name STRING, tenant STRING)")
        .await?;

    db.execute("CREATE (n:User {name: 'Alice', tenant: 'A'})")
        .await?;
    db.execute("CREATE (n:User {name: 'Bob', tenant: 'B'})")
        .await?;
    db.flush().await?;

    // Create session
    let session = db.session().set("tenant_id", "A").build();

    // Query with session variable
    // $session.tenant_id should resolve to "A"
    let results = session
        .query("MATCH (n:User) WHERE n.tenant = $session.tenant_id RETURN n.name")
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("n.name")?, "Alice");

    Ok(())
}
