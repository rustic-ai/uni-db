// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_traversal_label_filtering_bug() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. Setup Schema
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .label("Robot")
        .property("model", DataType::String)
        .edge_type("OWNS", &["Person"], &["Robot"])
        .apply()
        .await?;

    // 2. Insert Data
    // Create a Person 'Human' and a Robot 'Beep'
    db.execute("CREATE (p:Person {name: 'Human'})").await?;
    db.execute("CREATE (r:Robot {model: 'Beep'})").await?;

    // Connect them: Person -> OWNS -> Robot
    db.execute(
        "MATCH (p:Person {name: 'Human'}), (r:Robot {model: 'Beep'}) CREATE (p)-[:OWNS]->(r)",
    )
    .await?;

    // 3. Test Cases

    // Case A: MATCH (p:Person)-[:OWNS]->(x:Person)
    // Expectation: 0 rows, because the neighbor is a Robot, not a Person.
    let results_wrong_label = db
        .query("MATCH (p:Person)-[:OWNS]->(x:Person) RETURN x")
        .await?;

    // If the bug exists, this might return 1 row (the Robot), ignoring the :Person label on x.
    if !results_wrong_label.is_empty() {
        let row = results_wrong_label.rows()[0].clone();
        let val = row.get::<String>("x.model"); // Try to access a property that only exists on Robot
        println!(
            "Bug Reproduced! Expected 0 rows, got {}. First row: {:?}",
            results_wrong_label.len(),
            row
        );
        if let Ok(model) = val {
            println!(
                "Returned node appears to be the Robot with model: {}",
                model
            );
        }
    }
    assert_eq!(
        results_wrong_label.len(),
        0,
        "Expected 0 results for mismatched label traversal, but got {}",
        results_wrong_label.len()
    );

    // Case B: MATCH (p:Person)-[:OWNS]->(x:Robot)
    // Expectation: 1 row.
    let results_correct_label = db
        .query("MATCH (p:Person)-[:OWNS]->(x:Robot) RETURN x")
        .await?;
    assert_eq!(
        results_correct_label.len(),
        1,
        "Expected 1 result for correct label traversal"
    );

    // Case C: Variable Length Traversal
    // MATCH (p:Person)-[:OWNS*1..2]->(x:Person)
    // Expectation: 0 rows, because neighbor is Robot.
    let results_var_len = db
        .query("MATCH (p:Person)-[:OWNS*1..2]->(x:Person) RETURN x")
        .await?;
    assert_eq!(
        results_var_len.len(),
        0,
        "Expected 0 results for mismatched label var-len traversal"
    );

    // Case D: Variable Length Traversal (Correct Label)
    let results_var_len_correct = db
        .query("MATCH (p:Person)-[:OWNS*1..2]->(x:Robot) RETURN x")
        .await?;
    assert_eq!(
        results_var_len_correct.len(),
        1,
        "Expected 1 result for correct label var-len traversal"
    );

    Ok(())
}
