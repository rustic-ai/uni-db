// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Locy evaluation consults the `AuthzPolicy` chain.
//!
//! Every Cypher entry point authorized its statement, but no Locy entry point
//! did: with a deny-all policy registered, `session.locy`, `tx.locy`, the
//! builders and prepared programs all ran, a `tx.locy` Cypher command wrote,
//! `tx.locy("CALL uni.admin.snapshot.create(..)")` created a snapshot, and
//! `tx.apply` replayed a `DerivedFactSet` — and the policy recorded no check.
//!
//! A program is now authorized before it runs, with the verbs Cypher uses:
//! `read` for what it evaluates (rules, goal queries, read commands), the
//! write verb for each command that writes (`write` for a Cypher write or a
//! `DERIVE`, `dbms` for an administrative procedure, `schema` for a DDL
//! one). The resource path is the program text. `tx.apply` authorizes each
//! replayed statement as a write.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(locy_authz)'

// Rust guideline compliant

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use uni_db::{Uni, UniError};
use uni_plugin::traits::connector::{
    Action, AuthzError, AuthzPolicy, Decision, Principal, Resource,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

/// One authorization request as the policy saw it.
#[derive(Clone, Debug)]
struct Check {
    verb: String,
    path: String,
    labels: Vec<String>,
    rel_types: Vec<String>,
}

/// Records every request; denies all of them while `deny` is set.
#[derive(Default)]
struct Recorder {
    deny: AtomicBool,
    seen: Mutex<Vec<Check>>,
}

impl Recorder {
    fn take(&self) -> Vec<Check> {
        std::mem::take(&mut *self.seen.lock().unwrap())
    }

    fn take_verbs(&self) -> Vec<String> {
        self.take().into_iter().map(|c| c.verb).collect()
    }
}

impl AuthzPolicy for Recorder {
    fn check(&self, _p: &Principal, a: &Action, r: &Resource) -> Result<Decision, AuthzError> {
        self.seen.lock().unwrap().push(Check {
            verb: a.verb.clone(),
            path: r.path.clone(),
            labels: r.labels.clone(),
            rel_types: r.rel_types.clone(),
        });
        if self.deny.load(Ordering::SeqCst) {
            Ok(Decision::Deny {
                reason: "locy_authz denies everything".into(),
            })
        } else {
            Ok(Decision::Allow)
        }
    }
}

fn register(db: &Uni) -> Result<Arc<Recorder>> {
    let policy = Arc::new(Recorder::default());
    let caps = CapabilitySet::from_iter_of([Capability::Authz]);
    let mut r = PluginRegistrar::new(PluginId::new("locy-authz"), &caps, db.plugin_registry());
    r.authz_policy(policy.clone())?;
    r.commit_to_registry()?;
    Ok(policy)
}

const READ: &str = "CREATE RULE items AS MATCH (i:Item) YIELD KEY i";
const DERIVE: &str = "CREATE RULE link AS MATCH (a:Item)-[:R]->(b:Item) \
                      DERIVE (a)-[:LINKED]->(b)\nDERIVE link";
const WRITE: &str = "CREATE (:Item {v: 9})";
const ADMIN: &str = "CALL uni.admin.snapshot.create('via-locy')";
const DDL: &str = "CALL uni.schema.createLabel('ViaLocy', {})";

async fn store(path: &str) -> Result<Uni> {
    let db = Uni::open(path).build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Item {v: 1})-[:R]->(:Item {v: 2})")
        .await?;
    tx.commit().await?;
    Ok(db)
}

/// (items, LINKED edges, snapshots, `ViaLocy` label exists).
async fn state(db: &Uni) -> Result<(i64, i64, usize, bool)> {
    let s = db.session();
    let items = s.query("MATCH (i:Item) RETURN count(i) AS n").await?.rows()[0].get("n")?;
    let links = s
        .query("MATCH ()-[r:LINKED]->() RETURN count(r) AS n")
        .await?
        .rows()[0]
        .get("n")?;
    Ok((
        items,
        links,
        db.list_snapshots().await?.len(),
        db.label_exists("ViaLocy").await?,
    ))
}

fn is_denied<T>(r: &uni_db::Result<T>) -> bool {
    matches!(r, Err(UniError::AuthorizationDenied { .. }))
}

/// A deny-all policy refuses every Locy entry point, and nothing the
/// programs would have done happens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_authz_deny_all_refuses_every_entry_point() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = store(dir.path().to_str().context("utf-8")?).await?;
    let policy = register(&db)?;
    let derived = db
        .session()
        .locy(DERIVE)
        .await?
        .derived()
        .cloned()
        .context("DERIVE produced a fact set")?;
    let before = state(&db).await?;
    policy.take();
    policy.deny.store(true, Ordering::SeqCst);

    let s = db.session();
    let mut outcomes: Vec<(&str, bool)> = Vec::new();
    outcomes.push(("session.locy", is_denied(&s.locy(READ).await)));
    outcomes.push((
        "session.locy_with.run",
        is_denied(&s.locy_with(READ).run().await),
    ));
    outcomes.push((
        "session.locy_with.profile",
        is_denied(&s.locy_with(READ).profile().await),
    ));
    outcomes.push((
        "session.prepare_locy.execute",
        is_denied(&s.prepare_locy(READ).await?.execute(&[]).await),
    ));
    outcomes.push(("session.locy admin", is_denied(&s.locy(ADMIN).await)));

    let tx = s.tx().await?;
    outcomes.push(("tx.locy", is_denied(&tx.locy(READ).await)));
    outcomes.push((
        "tx.locy_with.run",
        is_denied(&tx.locy_with(READ).run().await),
    ));
    outcomes.push((
        "tx.locy_with.profile",
        is_denied(&tx.locy_with(READ).profile().await),
    ));
    outcomes.push((
        "tx.prepare_locy.execute",
        is_denied(&tx.prepare_locy(READ).await?.execute(&[]).await),
    ));
    outcomes.push(("tx.locy write", is_denied(&tx.locy(WRITE).await)));
    outcomes.push(("tx.locy derive", is_denied(&tx.locy(DERIVE).await)));
    outcomes.push(("tx.locy admin", is_denied(&tx.locy(ADMIN).await)));
    outcomes.push(("tx.locy ddl", is_denied(&tx.locy(DDL).await)));
    outcomes.push(("tx.apply", is_denied(&tx.apply(derived).await)));
    let refused_checks = policy.take().len();
    // Nothing was buffered, so the commit itself is a no-op.
    tx.commit().await?;

    policy.deny.store(false, Ordering::SeqCst);
    let after = state(&db).await?;
    let ran: Vec<&str> = outcomes
        .iter()
        .filter(|(_, denied)| !denied)
        .map(|(name, _)| *name)
        .collect();
    assert!(ran.is_empty(), "not refused by a deny-all policy: {ran:?}");
    assert!(
        refused_checks >= outcomes.len(),
        "{refused_checks} checks for {} refused calls",
        outcomes.len()
    );
    assert_eq!(after, before, "(items, links, snapshots, ViaLocy label)");
    Ok(())
}

/// The verbs a program is authorized under, and the resource it names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_authz_verbs_follow_the_program() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = store(dir.path().to_str().context("utf-8")?).await?;
    let policy = register(&db)?;
    let s = db.session();

    s.locy(READ).await?;
    let checks = policy.take();
    assert_eq!(
        checks.iter().map(|c| c.verb.as_str()).collect::<Vec<_>>(),
        ["read"],
        "session read-only program: {checks:?}"
    );
    assert_eq!(checks[0].path, READ, "resource path is the program text");
    assert_eq!(checks[0].labels, ["Item"], "labels the rule reads");

    let derived = s
        .locy(DERIVE)
        .await?
        .derived()
        .cloned()
        .context("DERIVE produced a fact set")?;
    let checks = policy.take();
    assert_eq!(
        checks.iter().map(|c| c.verb.as_str()).collect::<Vec<_>>(),
        ["read", "write"],
        "session DERIVE program: {checks:?}"
    );
    assert_eq!(
        checks[1].rel_types,
        ["LINKED", "R"],
        "the edge DERIVE writes and the one it matches"
    );

    let tx = s.tx().await?;
    // Applied first: a DerivedFactSet is stale once the transaction writes.
    tx.apply(derived).await?;
    let applied = policy.take();
    assert!(
        !applied.is_empty()
            && applied
                .iter()
                .all(|c| c.verb == "write" && c.path == "<locy-apply>"),
        "tx.apply replays its statements as writes: {applied:?}"
    );
    for (program, expected) in [
        (READ, vec!["read"]),
        (WRITE, vec!["write"]),
        (DERIVE, vec!["read", "write"]),
        (ADMIN, vec!["dbms"]),
        (DDL, vec!["schema"]),
    ] {
        tx.locy(program).await?;
        let verbs = policy.take_verbs();
        assert_eq!(verbs, expected, "tx.locy({program})");
    }
    tx.commit().await?;

    // The administrative procedure ran, as it does through tx.execute.
    let (_, _, snapshots, via_locy) = state(&db).await?;
    assert_eq!((snapshots, via_locy), (1, true));
    Ok(())
}

/// The `write` request of a `DERIVE` names what the rule matches as well as
/// what it creates — as the `write` request of the equivalent Cypher
/// statement does — so a policy keyed on a label cannot be bypassed by
/// deriving instead of writing. Labels reached through an `IS` reference
/// count, and `DERIVE MERGE` names the `MERGED_WITH` edge it writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locy_authz_derive_names_what_it_writes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = store(dir.path().to_str().context("utf-8")?).await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Secret {v: 3})").await?;
    tx.commit().await?;
    let policy = register(&db)?;
    let s = db.session();

    let tx = s.tx().await?;
    tx.execute("MATCH (a:Item)-[:R]->(b:Item) CREATE (a)-[:LINKED]->(b)")
        .await?;
    tx.rollback();
    let cypher = policy.take();
    assert!(!cypher.is_empty(), "the Cypher write was authorized");

    s.locy(DERIVE).await?;
    let derive = policy.take();
    let write = &derive[1];
    assert_eq!(write.verb, "write", "{derive:?}");
    assert_eq!(
        (&write.labels, &write.rel_types),
        (&cypher[0].labels, &cypher[0].rel_types),
        "DERIVE names what the equivalent Cypher write names"
    );

    s.locy(
        "CREATE RULE secret AS MATCH (x:Secret) YIELD KEY x\n\
         CREATE RULE leak AS MATCH (b:Item) WHERE a IS secret \
         DERIVE (a)-[:LINKED]->(b)\nDERIVE leak",
    )
    .await?;
    let checks = policy.take();
    let write = checks.iter().find(|c| c.verb == "write").context("write")?;
    assert!(
        write.labels.iter().any(|l| l == "Secret"),
        "a label bound through IS: {checks:?}"
    );

    s.locy("CREATE RULE same AS MATCH (a:Item)-[:R]->(b:Item) DERIVE MERGE a, b\nDERIVE same")
        .await?;
    let checks = policy.take();
    let write = checks.iter().find(|c| c.verb == "write").context("write")?;
    assert_eq!(
        (&write.labels, &write.rel_types),
        (
            &vec!["Item".to_owned()],
            &vec!["MERGED_WITH".to_owned(), "R".to_owned()]
        ),
        "DERIVE MERGE: {checks:?}"
    );
    let tx = s.tx().await?;
    tx.locy("CREATE RULE same AS MATCH (a:Item)-[:R]->(b:Item) DERIVE MERGE a, b\nDERIVE same")
        .await?;
    tx.commit().await?;
    let merged: i64 = s
        .query("MATCH (:Item)-[m:MERGED_WITH]->(:Item) RETURN count(m) AS n")
        .await?
        .rows()[0]
        .get("n")?;
    assert_eq!(merged, 1, "DERIVE MERGE writes the edge it names");
    Ok(())
}
