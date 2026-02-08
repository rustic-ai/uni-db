// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{Uni, Value};

#[tokio::test]
async fn test_string_functions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // STARTS WITH
    let res = db.query("RETURN 'hello' STARTS WITH 'he' AS r").await?;
    assert_eq!(res.rows()[0].get::<bool>("r")?, true);

    let res = db.query("RETURN 'hello' STARTS WITH 'lo' AS r").await?;
    assert_eq!(res.rows()[0].get::<bool>("r")?, false);

    // ENDS WITH
    let res = db.query("RETURN 'hello' ENDS WITH 'lo' AS r").await?;
    assert_eq!(res.rows()[0].get::<bool>("r")?, true);

    // CONTAINS
    let res = db.query("RETURN 'hello' CONTAINS 'ell' AS r").await?;
    assert_eq!(res.rows()[0].get::<bool>("r")?, true);

    // Mixed with List Comprehension (uses expr_compiler)
    // This forces usage of my new compile_binary_op logic because it's inside ListComprehension
    let res = db.query("RETURN [x IN ['abc', 'def'] WHERE x STARTS WITH 'a' | x] AS r").await?;
    
    // Should be ['abc']
    let r = res.rows()[0].value("r").unwrap();
    if let Value::List(l) = r {
        assert_eq!(l.len(), 1);
        assert_eq!(l[0], Value::String("abc".to_string()));
    } else {
        // Fallback might return null if LargeList bug persists, or empty list
        panic!("Expected list, got {:?}", r);
    }

    Ok(())
}
