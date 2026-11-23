// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `USE` resolves modules whose rules were registered with `rules().register`.
//!
//! A program declaring `MODULE a.b` registers its rules as `a.b.<rule>`. A later
//! program's `USE a.b` (all rules) or `USE a.b { r }` (selected rules) makes the
//! short names resolve to them. `USE` sees exactly the rules visible to the
//! registry the program runs against, and the module map survives a restart
//! because it is rebuilt from the persisted sources.

use anyhow::Result;
use tempfile::tempdir;
use uni_db::{Uni, locy::CommandResult};

const MODULE_PROGRAM: &str = "MODULE a.b \
     CREATE RULE adult AS MATCH (p:Person) WHERE p.age >= 18 YIELD KEY p \
     CREATE RULE minor AS MATCH (p:Person) WHERE p.age < 18 YIELD KEY p";

async fn seed(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Ada', age: 36}), (:Person {name: 'Kid', age: 9})")
        .await?;
    tx.commit().await?;
    Ok(())
}

/// The names the program's one `QUERY` returned, sorted.
fn names(result: &uni_db::locy::LocyResult) -> Vec<String> {
    let rows = match result.command_results.last().expect("no command results") {
        CommandResult::Query(rows) => rows,
        other => panic!("expected a QUERY result, got {other:?}"),
    };
    let mut names: Vec<String> = rows
        .iter()
        .map(|row| match row.get("name") {
            Some(uni_db::Value::String(s)) => s.clone(),
            other => panic!("expected a name, got {other:?}"),
        })
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn glob_use_resolves_a_registered_module() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db).await?;
    db.rules().register(MODULE_PROGRAM).await?;

    let result = db
        .session()
        .locy(
            "USE a.b \
             CREATE RULE grown AS MATCH (p:Person) WHERE p IS adult YIELD KEY p \
             QUERY grown RETURN p.name AS name",
        )
        .await?;
    assert_eq!(names(&result), vec!["Ada".to_string()]);

    // An imported rule can be queried by its short name too.
    let result = db
        .session()
        .locy("USE a.b QUERY minor RETURN p.name AS name")
        .await?;
    assert_eq!(names(&result), vec!["Kid".to_string()]);
    Ok(())
}

#[tokio::test]
async fn selective_use_imports_only_the_listed_rules() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db).await?;
    db.rules().register(MODULE_PROGRAM).await?;

    let result = db
        .session()
        .locy(
            "USE a.b { adult } \
             CREATE RULE grown AS MATCH (p:Person) WHERE p IS adult YIELD KEY p \
             QUERY grown RETURN p.name AS name",
        )
        .await?;
    assert_eq!(names(&result), vec!["Ada".to_string()]);

    // `minor` was not imported, so its short name does not resolve.
    let err = db
        .session()
        .locy("USE a.b { adult } QUERY minor RETURN p.name AS name")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("minor"),
        "an unimported rule must not resolve, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn unknown_imports_are_reported() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.rules().register(MODULE_PROGRAM).await?;

    let err = db
        .session()
        .locy("USE a.b { nope } QUERY adult RETURN p.name AS name")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("nope") && err.to_string().contains("a.b"),
        "an unknown imported rule must name the rule and module, got: {err}"
    );

    let err = db
        .session()
        .locy("USE x.y QUERY adult RETURN p.name AS name")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("x.y"),
        "an unknown module must be named, got: {err}"
    );
    Ok(())
}

#[tokio::test]
async fn a_registered_program_can_use_an_earlier_module() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db).await?;
    db.rules().register(MODULE_PROGRAM).await?;
    db.rules()
        .register(
            "MODULE c \
             USE a.b \
             CREATE RULE named_adult AS MATCH (p:Person) WHERE p IS adult YIELD KEY p",
        )
        .await?;

    let result = db
        .session()
        .locy("USE c QUERY named_adult RETURN p.name AS name")
        .await?;
    assert_eq!(names(&result), vec!["Ada".to_string()]);
    Ok(())
}

#[tokio::test]
async fn session_registered_modules_are_visible_to_that_session() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db).await?;
    let session = db.session();
    session.rules().register(MODULE_PROGRAM).await?;

    let result = session
        .locy("USE a.b QUERY adult RETURN p.name AS name")
        .await?;
    assert_eq!(names(&result), vec!["Ada".to_string()]);

    // Another session does not see a session-level registration.
    assert!(
        db.session()
            .locy("USE a.b QUERY adult RETURN p.name AS name")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn modules_survive_restart_and_clear() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).build().await?;
        seed(&db).await?;
        db.rules().register(MODULE_PROGRAM).await?;
        db.flush().await?;
    }
    let db = Uni::open(&path).build().await?;
    let result = db
        .session()
        .locy("USE a.b QUERY adult RETURN p.name AS name")
        .await?;
    assert_eq!(names(&result), vec!["Ada".to_string()]);

    db.rules().clear().await?;
    let err = db
        .session()
        .locy("USE a.b QUERY adult RETURN p.name AS name")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("a.b"),
        "a cleared module must no longer resolve, got: {err}"
    );
    Ok(())
}
