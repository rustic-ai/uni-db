// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression test for BUG-5: Locy parameters must propagate into ASSUME
//! forks so that parameterized rules re-evaluate correctly in hypothetical
//! states.

use anyhow::Result;
use uni_db::Uni;

/// The rows of an ASSUME whose body's only command is a QUERY.
fn assume_query_rows(
    result: &uni_db::locy::CommandResult,
) -> &Vec<std::collections::HashMap<String, uni_db::Value>> {
    match result {
        uni_db::locy::CommandResult::Assume { results, .. } => match results.as_slice() {
            [uni_db::locy::CommandResult::Query(rows)] => rows,
            other => panic!("expected the ASSUME body's one QUERY result, got: {other:?}"),
        },
        other => panic!("expected an Assume result, got: {other:?}"),
    }
}

/// Parameters bound via `.param()` must be available inside ASSUME mutations.
#[tokio::test]
async fn test_assume_mutation_uses_params() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Seed data: two agents
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Agent {name: 'alice', score: 10}), (:Agent {name: 'bob', score: 20})")
        .await?;
    tx.commit().await?;

    // ASSUME mutation references $target_name — should resolve from params.
    // The mutation boosts alice's score, then the rule re-evaluates.
    let result = db
        .session()
        .locy_with(
            "CREATE RULE high_scorers AS \
             MATCH (a:Agent) WHERE a.score > 15 YIELD KEY a \n\
             ASSUME { MATCH (a:Agent {name: $target_name}) SET a.score = 99 } \
             THEN QUERY high_scorers RETURN a.name AS name",
        )
        .param("target_name", "alice")
        .run()
        .await?;

    // In the hypothetical state, alice has score=99 so she should appear
    // alongside bob (who already has score=20).
    let rows = assume_query_rows(&result.command_results[0]);

    let names: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            r.get("name")
                .map(|v| v.to_string().trim_matches('"').to_string())
        })
        .collect();

    assert!(
        names.contains(&"alice".to_string()),
        "alice should be a high_scorer after ASSUME boost, got: {names:?}"
    );
    assert!(
        names.contains(&"bob".to_string()),
        "bob should still be a high_scorer, got: {names:?}"
    );

    Ok(())
}

/// Parameters bound via `.param()` must be available inside ASSUME rule
/// re-evaluation (not just the mutation itself).
#[tokio::test]
async fn test_assume_rule_reevaluation_uses_params() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Episode {agent_id: 'alice', action: 'deploy'}), \
         (:Episode {agent_id: 'alice', action: 'test'}), \
         (:Episode {agent_id: 'bob', action: 'deploy'})",
    )
    .await?;
    tx.commit().await?;

    // The rule uses $agent_id to filter episodes. In re-evaluation after
    // ASSUME, the same $agent_id must be resolved.
    let result = db
        .session()
        .locy_with(
            "CREATE RULE agent_episodes AS \
             MATCH (e:Episode) WHERE e.agent_id = $agent_id YIELD KEY e \n\
             ASSUME { MATCH (e:Episode {agent_id: $agent_id, action: 'deploy'}) \
                      SET e.action = 'rollback' } \
             THEN QUERY agent_episodes RETURN e.action AS action",
        )
        .param("agent_id", "alice")
        .run()
        .await?;

    let rows = assume_query_rows(&result.command_results[0]);

    // Alice had 2 episodes. After ASSUME, 'deploy' became 'rollback'.
    let actions: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            r.get("action")
                .map(|v| v.to_string().trim_matches('"').to_string())
        })
        .collect();

    assert_eq!(
        actions.len(),
        2,
        "should see both alice episodes, got: {actions:?}"
    );
    assert!(
        actions.contains(&"rollback".to_string()),
        "deploy should have been changed to rollback in ASSUME, got: {actions:?}"
    );
    assert!(
        actions.contains(&"test".to_string()),
        "test episode should be unchanged, got: {actions:?}"
    );

    Ok(())
}
