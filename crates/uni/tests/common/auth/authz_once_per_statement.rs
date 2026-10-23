// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Every Cypher entry point consults the `AuthzPolicy` chain exactly once per
//! statement.
//!
//! `Transaction::execute` authorized the statement itself and then delegated
//! to `Transaction::query`, which authorized it again, so a policy saw two
//! identical checks for one `tx.execute(..)` — an audit policy logged every
//! write twice, and a rate-limiting or counting policy charged it twice. The
//! opposite failure, an entry point with no check at all, is the bypass the
//! earlier builder and prepared-statement fixes closed; this suite pins both
//! directions for every entry point.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(authz_once_per_statement)'

// Rust guideline compliant

use std::sync::{Arc, Mutex};

use anyhow::Result;
use uni_db::Uni;
use uni_plugin::traits::connector::{
    Action, AuthzError, AuthzPolicy, Decision, Principal, Resource,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

/// One `AuthzPolicy::check` call: the action verb and the resource path.
type Check = (String, String);

/// Allows everything and records each check it is asked to make.
struct Recorder {
    checks: Arc<Mutex<Vec<Check>>>,
}

impl AuthzPolicy for Recorder {
    fn check(
        &self,
        _principal: &Principal,
        action: &Action,
        resource: &Resource,
    ) -> Result<Decision, AuthzError> {
        self.checks
            .lock()
            .unwrap()
            .push((action.verb.clone(), resource.path.clone()));
        Ok(Decision::Allow)
    }
}

/// Register a [`Recorder`] on `db` and return its log.
fn register(db: &Uni) -> Result<Arc<Mutex<Vec<Check>>>> {
    let checks = Arc::new(Mutex::new(Vec::new()));
    let caps = CapabilitySet::from_iter_of([Capability::Authz]);
    let mut r = PluginRegistrar::new(
        PluginId::new("test-authz-recorder"),
        &caps,
        db.plugin_registry(),
    );
    r.authz_policy(Arc::new(Recorder {
        checks: Arc::clone(&checks),
    }))?;
    r.commit_to_registry()?;
    Ok(checks)
}

/// Take and clear the recorded checks.
fn drain(checks: &Arc<Mutex<Vec<Check>>>) -> Vec<Check> {
    std::mem::take(&mut *checks.lock().unwrap())
}

/// Assert the log holds exactly one check, for `cypher`, under `verb`.
fn assert_once(checks: &Arc<Mutex<Vec<Check>>>, entry: &str, cypher: &str, verb: &str) {
    let got = drain(checks);
    assert_eq!(
        got,
        vec![(verb.to_owned(), cypher.to_owned())],
        "{entry}: expected exactly one `{verb}` check for the statement"
    );
}

const WRITE: &str = "MATCH (a:Item)-[:R]->(b:Item) CREATE (a)-[:LINKED]->(b)";
const READ: &str = "MATCH (a:Item) RETURN count(a) AS n";
const PARAM_WRITE: &str = "CREATE (:Item {v: $v})";
const PARAM_READ: &str = "MATCH (a:Item) WHERE a.v = $v RETURN a.v AS v";

/// The reported case, plus the other transaction entry points.
#[tokio::test]
async fn authz_once_per_statement_transaction_entry_points() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    session
        .run("CREATE (:Item {v: 1})-[:R]->(:Item {v: 2})")
        .await?;
    let checks = register(&db)?;

    let tx = session.tx().await?;
    tx.execute(WRITE).await?;
    assert_once(&checks, "tx.execute(write)", WRITE, "write");
    tx.execute(READ).await?;
    assert_once(&checks, "tx.execute(read)", READ, "read");
    tx.query(WRITE).await?;
    assert_once(&checks, "tx.query(write)", WRITE, "write");
    tx.query(READ).await?;
    assert_once(&checks, "tx.query(read)", READ, "read");

    tx.execute_with(PARAM_WRITE).param("v", 3i64).run().await?;
    assert_once(&checks, "tx.execute_with.run", PARAM_WRITE, "write");
    tx.execute_with(PARAM_WRITE)
        .param("v", 4i64)
        .profile()
        .await?;
    assert_once(&checks, "tx.execute_with.profile", PARAM_WRITE, "write");
    tx.query_with(PARAM_WRITE)
        .param("v", 5i64)
        .execute()
        .await?;
    assert_once(&checks, "tx.query_with.execute", PARAM_WRITE, "write");
    tx.query_with(PARAM_READ)
        .param("v", 1i64)
        .fetch_all()
        .await?;
    assert_once(&checks, "tx.query_with.fetch_all", PARAM_READ, "read");
    tx.query_with(PARAM_READ)
        .param("v", 1i64)
        .fetch_one()
        .await?;
    assert_once(&checks, "tx.query_with.fetch_one", PARAM_READ, "read");
    tx.query_with(PARAM_READ).param("v", 1i64).profile().await?;
    assert_once(&checks, "tx.query_with.profile", PARAM_READ, "read");
    tx.query_with(PARAM_READ)
        .param("v", 1i64)
        .cursor()
        .await?
        .collect_remaining()
        .await?;
    assert_once(&checks, "tx.query_with.cursor", PARAM_READ, "read");

    let prepared = tx.prepare(PARAM_WRITE).await?;
    assert_eq!(
        drain(&checks),
        Vec::<Check>::new(),
        "tx.prepare checks at execution"
    );
    prepared.execute(&[("v", 6i64.into())]).await?;
    assert_once(&checks, "tx.prepare.execute", PARAM_WRITE, "write");
    prepared.bind().param("v", 7i64).execute().await?;
    assert_once(&checks, "tx.prepare.bind.execute", PARAM_WRITE, "write");
    let prepared = tx.prepare(PARAM_READ).await?;
    prepared.execute(&[("v", 1i64.into())]).await?;
    assert_once(&checks, "tx.prepare(read).execute", PARAM_READ, "read");

    tx.commit().await?;
    assert_eq!(
        drain(&checks),
        Vec::<Check>::new(),
        "commit is not a statement"
    );
    Ok(())
}

/// The session entry points: reads, the auto-commit `run`, builders and
/// prepared statements.
#[tokio::test]
async fn authz_once_per_statement_session_entry_points() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    session
        .run("CREATE (:Item {v: 1})-[:R]->(:Item {v: 2})")
        .await?;
    let checks = register(&db)?;

    session.query(READ).await?;
    assert_once(&checks, "session.query", READ, "read");
    session.run(READ).await?;
    assert_once(&checks, "session.run(read)", READ, "read");
    session.run(WRITE).await?;
    assert_once(&checks, "session.run(write)", WRITE, "write");

    session
        .query_with(PARAM_READ)
        .param("v", 1i64)
        .fetch_all()
        .await?;
    assert_once(&checks, "session.query_with.fetch_all", PARAM_READ, "read");
    session
        .query_with(PARAM_READ)
        .param("v", 1i64)
        .fetch_one()
        .await?;
    assert_once(&checks, "session.query_with.fetch_one", PARAM_READ, "read");
    session
        .query_with(PARAM_READ)
        .param("v", 1i64)
        .profile()
        .await?;
    assert_once(&checks, "session.query_with.profile", PARAM_READ, "read");
    session
        .query_with(PARAM_READ)
        .param("v", 1i64)
        .explain()
        .await?;
    assert_once(&checks, "session.query_with.explain", PARAM_READ, "read");
    session
        .query_with(PARAM_READ)
        .param("v", 1i64)
        .cursor()
        .await?
        .collect_remaining()
        .await?;
    assert_once(&checks, "session.query_with.cursor", PARAM_READ, "read");

    let prepared = session.prepare(PARAM_READ).await?;
    assert_eq!(
        drain(&checks),
        Vec::<Check>::new(),
        "session.prepare checks at execution"
    );
    prepared.execute(&[("v", 1i64.into())]).await?;
    assert_once(&checks, "session.prepare.execute", PARAM_READ, "read");
    prepared.bind().param("v", 2i64).execute().await?;
    assert_once(&checks, "session.prepare.bind.execute", PARAM_READ, "read");
    Ok(())
}
