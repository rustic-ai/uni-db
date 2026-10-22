// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mutating built-in procedures are not reads.
//!
//! The built-in `uni.admin.*` and `uni.schema.create*`/`drop*` procedures are
//! dispatched by the executor by name and are not in the plugin registry. The
//! read-only oracle (`validate_read_only_with`) asked only the registry for a
//! procedure's mode, so it took every one of them for a read: `Session::query`
//! (documented read-only) ran `uni.admin.snapshot.restore()`, `compact()`,
//! `snapshot.create()`, `verifyEdgeEndpoints()` and the DDL procedures, and the
//! authorization action for them was `read`, in `Session::query`,
//! `Session::run` and `Transaction::query` alike. A session prepared query
//! did not consult procedure modes at all.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(admin_procedure_read_only)'

// Rust guideline compliant

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use uni_db::{DataType, QueryResult, Uni, UniError};
use uni_plugin::traits::connector::{
    Action, AuthzError, AuthzPolicy, Decision, Principal, Resource,
};
use uni_plugin::{Capability, CapabilitySet, PluginId, PluginRegistrar};

/// Every mutating built-in procedure, as a complete statement.
const MUTATING: [&str; 6] = [
    "CALL uni.admin.compact()",
    "CALL uni.admin.snapshot.create('probe')",
    "CALL uni.admin.snapshot.restore('SNAP')",
    "CALL uni.admin.verifyEdgeEndpoints()",
    "CALL uni.schema.createLabel('Probe', {})",
    "CALL uni.schema.dropLabel('Person')",
];

/// Built-in procedures that only read; a read-only context keeps running them.
const READING: [&str; 2] = [
    "CALL uni.admin.compactionStatus() YIELD l1_runs RETURN l1_runs",
    "CALL uni.admin.snapshot.list() YIELD snapshot_id RETURN snapshot_id",
];

/// Records the verb of every authorization request and allows it.
#[derive(Default)]
struct RecordVerbs {
    seen: Mutex<Vec<(String, String)>>,
}

impl AuthzPolicy for RecordVerbs {
    fn check(
        &self,
        _principal: &Principal,
        action: &Action,
        resource: &Resource,
    ) -> Result<Decision, AuthzError> {
        self.seen
            .lock()
            .unwrap()
            .push((action.verb.clone(), resource.path.clone()));
        Ok(Decision::Allow)
    }
}

impl RecordVerbs {
    /// Verbs recorded for `cypher`, in order, then cleared.
    fn take(&self, cypher: &str) -> Vec<String> {
        let mut seen = self.seen.lock().unwrap();
        let verbs = seen
            .iter()
            .filter(|(_, p)| p == cypher)
            .map(|(v, _)| v.clone())
            .collect();
        seen.clear();
        verbs
    }
}

fn register_recorder(db: &Uni) -> Result<Arc<RecordVerbs>> {
    let policy = Arc::new(RecordVerbs::default());
    let caps = CapabilitySet::from_iter_of([Capability::Authz]);
    let mut r = PluginRegistrar::new(
        PluginId::new("test-record-verbs"),
        &caps,
        db.plugin_registry(),
    );
    r.authz_policy(policy.clone())?;
    r.commit_to_registry()?;
    Ok(policy)
}

/// A persistent store holding one `:Person`, a snapshot of it, then a
/// second `:Person` — so a restore is observable as the count going 2 → 1.
async fn store(path: &str) -> Result<(Uni, String)> {
    let db = Uni::open(path.to_string()).build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})").await?;
    tx.commit().await?;
    db.flush().await?;
    let snap = db.create_snapshot("alice-only").await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Bob'})").await?;
    tx.commit().await?;
    db.flush().await?;
    Ok((db, snap))
}

async fn persons(db: &Uni) -> Result<i64> {
    let res = db
        .session()
        .query("MATCH (n:Person) RETURN count(n) AS n")
        .await?;
    Ok(res.rows()[0].get::<i64>("n")?)
}

fn with_snap(stmt: &str, snap: &str) -> String {
    stmt.replace("SNAP", snap)
}

/// `true` when `r` is the read-only refusal `Session::query` raises.
fn is_read_only_refusal(r: &uni_db::Result<QueryResult>) -> bool {
    matches!(r, Err(UniError::Query { message, .. }) if message.contains("read-only"))
}

/// Every read-only Cypher entry point of a live, writable database refuses
/// every mutating built-in procedure with the read-only error — and the store
/// is untouched afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_procedure_read_only_session_entry_points_refuse() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?;
    let (db, snap) = store(path).await?;
    let snapshots_before = db.list_snapshots().await?.len();
    let session = db.session();
    let mut ran = Vec::new();
    for stmt in MUTATING {
        let stmt = with_snap(stmt, &snap);
        let entry_points: [(&str, uni_db::Result<QueryResult>); 3] = [
            ("query", session.query(&stmt).await),
            ("query_with", session.query_with(&stmt).fetch_all().await),
            (
                "prepare",
                match session.prepare(&stmt).await {
                    Ok(p) => p.execute(&[]).await,
                    Err(e) => Err(e),
                },
            ),
        ];
        for (entry, r) in entry_points {
            if !is_read_only_refusal(&r) {
                ran.push(format!("{entry}: {stmt} -> {:?}", r.map(|q| q.len())));
            }
        }
    }
    assert!(
        ran.is_empty(),
        "not refused as read-only:\n{}",
        ran.join("\n")
    );
    assert_eq!(persons(&db).await?, 2, "no restore happened");
    assert_eq!(db.list_snapshots().await?.len(), snapshots_before);
    for stmt in READING {
        session.query(stmt).await?;
    }
    Ok(())
}

/// A pinned session and a read-only handle refuse them too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_procedure_read_only_pinned_and_read_only_handle_refuse() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?;
    let (db, snap) = store(path).await?;
    let mut pinned = db.session();
    pinned.pin_to_version(&snap).await?;
    let mut ran = Vec::new();
    for stmt in MUTATING {
        let stmt = with_snap(stmt, &snap);
        let r = pinned.query(&stmt).await;
        if !is_read_only_refusal(&r) {
            ran.push(format!("pinned query: {stmt} -> {:?}", r.map(|q| q.len())));
        }
        let r = pinned.run(stmt.clone()).await;
        if !matches!(r, Err(UniError::ReadOnly { .. })) {
            ran.push(format!("pinned run: {stmt} -> {:?}", r.map(|q| q.len())));
        }
    }
    assert_eq!(persons(&db).await?, 2);
    db.shutdown().await?;

    let ro = Uni::open(path.to_string()).read_only().build().await?;
    let session = ro.session();
    for stmt in MUTATING {
        let stmt = with_snap(stmt, &snap);
        let r = session.query(&stmt).await;
        if !is_read_only_refusal(&r) {
            ran.push(format!(
                "read-only handle query: {stmt} -> {:?}",
                r.map(|q| q.len())
            ));
        }
        let r = session.run(stmt.clone()).await;
        if !matches!(r, Err(UniError::ReadOnly { .. })) {
            ran.push(format!(
                "read-only handle run: {stmt} -> {:?}",
                r.map(|q| q.len())
            ));
        }
    }
    for stmt in READING {
        session.query(stmt).await?;
    }
    ro.shutdown().await?;
    // Nothing either view ran reached the store.
    let db = Uni::open(path.to_string()).build().await?;
    let catalog = (
        db.label_exists("Person").await?,
        db.label_exists("Probe").await?,
    );
    assert_eq!(
        catalog,
        (true, false),
        "catalog changed through a read-only view"
    );
    assert!(ran.is_empty(), "not refused:\n{}", ran.join("\n"));
    Ok(())
}

/// A session-level Locy program is evaluated, not committed: its writes come
/// back for `tx.apply`. A Cypher command calling a mutating built-in has no
/// deferred form, so the program refuses it rather than running it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_procedure_read_only_session_locy_refuses() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?;
    let (db, snap) = store(path).await?;
    let snapshots_before = db.list_snapshots().await?.len();
    let session = db.session();
    let mut ran = Vec::new();
    for stmt in MUTATING {
        let stmt = with_snap(stmt, &snap);
        let r = session.locy(&stmt).await;
        let refused = matches!(&r, Err(UniError::Query { message, .. })
            if message.contains("cannot run in a session-level Locy program"));
        if !refused {
            ran.push(format!("{stmt} -> {:?}", r.map(|_| ())));
        }
    }
    let catalog = (
        db.label_exists("Person").await?,
        db.label_exists("Probe").await?,
    );
    assert!(
        ran.is_empty(),
        "ran in a session Locy program:\n{}",
        ran.join("\n")
    );
    assert_eq!(db.list_snapshots().await?.len(), snapshots_before);
    assert_eq!(catalog, (true, false));
    assert_eq!(persons(&db).await?, 2);
    Ok(())
}

/// `Session::run` is the entry point that runs them: the administrative
/// ones outside a transaction (a restore refuses inside one), authorized as
/// `dbms`; the DDL ones as `schema`. Never as `read`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_procedure_read_only_run_executes_with_admin_verb() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?;
    let (db, snap) = store(path).await?;
    let policy = register_recorder(&db)?;
    let session = db.session();

    let restore = with_snap("CALL uni.admin.snapshot.restore('SNAP')", &snap);
    session.run(restore.clone()).await?;
    assert_eq!(persons(&db).await?, 1, "restore rewound to the snapshot");
    assert_eq!(policy.take(&restore), ["dbms"]);

    for stmt in [
        "CALL uni.admin.compact()",
        "CALL uni.admin.snapshot.create('probe')",
        "CALL uni.admin.verifyEdgeEndpoints()",
    ] {
        session.run(stmt).await?;
        assert_eq!(policy.take(stmt), ["dbms"], "{stmt}");
    }
    let ddl = "CALL uni.schema.createLabel('Probe', {})";
    session.run(ddl).await?;
    let verbs = policy.take(ddl);
    assert!(
        !verbs.is_empty() && verbs.iter().all(|v| v == "schema"),
        "{ddl}: {verbs:?}"
    );
    for stmt in READING {
        session.run(stmt).await?;
        assert_eq!(policy.take(stmt), ["read"], "{stmt}");
    }
    Ok(())
}

/// Inside a transaction, `query` and `execute` authorize them as `dbms` /
/// `schema` rather than `read`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_procedure_read_only_transaction_verbs() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?;
    let (db, _snap) = store(path).await?;
    let policy = register_recorder(&db)?;
    let session = db.session();
    for (stmt, verb) in [
        ("CALL uni.admin.compact()", "dbms"),
        ("CALL uni.admin.snapshot.create('in-tx')", "dbms"),
        ("CALL uni.schema.createLabel('InTx', {})", "schema"),
    ] {
        let tx = session.tx().await?;
        let _ = tx.query(stmt).await;
        let q = policy.take(stmt);
        let _ = tx.execute(stmt).await;
        let e = policy.take(stmt);
        tx.rollback();
        assert!(
            !q.is_empty() && q.iter().all(|v| v == verb),
            "tx.query {stmt}: {q:?}"
        );
        assert!(
            !e.is_empty() && e.iter().all(|v| v == verb),
            "tx.execute {stmt}: {e:?}"
        );
    }
    Ok(())
}

/// A time-travel statement is read-only; a mutating procedure in it is
/// refused before the pinned view runs it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_procedure_read_only_time_travel_refuses() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().context("utf-8")?;
    let (db, snap) = store(path).await?;
    let session = db.session();
    let mut ran = Vec::new();
    for stmt in MUTATING {
        let stmt = format!("{} VERSION AS OF '{snap}'", with_snap(stmt, &snap));
        let r = session.query(&stmt).await;
        if r.is_ok() {
            ran.push(format!("{stmt} -> {:?}", r.map(|q| q.len())));
        }
    }
    assert!(ran.is_empty(), "ran under time travel:\n{}", ran.join("\n"));
    assert_eq!(persons(&db).await?, 2);
    Ok(())
}

/// A session prepared query classifies procedures as `Session::query` does:
/// a registered `WRITE` procedure is refused at prepare time. It used the
/// registry-blind oracle, so it prepared, and its execution wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_procedure_read_only_prepared_refuses_registered_write_procedure() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let mut caps = CapabilitySet::new();
    caps.insert(Capability::ProcedureWrites);
    let admin = Arc::new(Principal {
        id: "admin".to_owned(),
        groups: vec!["admin".to_owned()],
        capabilities: caps,
    });
    let session = db.session().with_principal(admin);
    let tx = session.tx().await?;
    tx.execute(
        "CALL uni.plugin.declareProcedure('t.mark', 'CREATE (:Marker)', 'WRITE', '[]', '[]')",
    )
    .await?;
    tx.commit().await?;
    let call = "CALL t.mark() YIELD row_json RETURN row_json";
    assert!(
        is_read_only_refusal(&session.query(call).await),
        "Session::query refuses it"
    );
    let prepared = session.prepare(call).await;
    let outcome = match prepared {
        Ok(p) => Err(format!(
            "prepared; execute -> {:?}",
            p.execute(&[]).await.map(|q| q.len())
        )),
        Err(UniError::Query { message, .. }) if message.contains("read-only") => Ok(()),
        Err(e) => Err(format!("unexpected error {e:?}")),
    };
    let markers = db
        .session()
        .query("MATCH (m:Marker) RETURN count(m) AS n")
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    assert_eq!(outcome, Ok(()), "markers written: {markers}");
    assert_eq!(markers, 0);
    Ok(())
}
