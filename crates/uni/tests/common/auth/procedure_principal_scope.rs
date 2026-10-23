// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A procedure sees the caller's principal on every entry point that can run it.
//!
//! Procedure capability gates (for example `Capability::ProcedureWrites` on
//! `uni.plugin.declareProcedure ... WRITE`) read the principal from the
//! `CURRENT_PRINCIPAL` task-local, which each API boundary must install
//! around execution. `Transaction::query`/`execute` and the session read paths
//! did; `Transaction::locy`, the transaction Locy builder, the session Locy
//! `profile`, prepared Locy programs, prepared Cypher queries and the
//! transaction Cypher builders did not — a procedure called from them saw no
//! principal, so a capability-gated procedure was refused for a principal that
//! held the capability.
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration -E 'test(procedure_principal_scope)'

// Rust guideline compliant

use std::sync::{Arc, Mutex};

use anyhow::Result;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use uni_db::{Session, Uni};
use uni_plugin::adapter_common::batch_builder::batch_into_stream;
use uni_plugin::traits::connector::Principal;
use uni_plugin::traits::procedure::{
    ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::{Capability, CapabilitySet, FnError, PluginId, PluginRegistrar, QName};

/// Principal ids seen by [`WhoAmI`], `None` for "no principal in scope".
type Seen = Arc<Mutex<Vec<Option<String>>>>;

/// `whoami.probe()` — records the principal in its context and yields it.
struct WhoAmI {
    seen: Seen,
    signature: ProcedureSignature,
}

impl ProcedurePlugin for WhoAmI {
    fn signature(&self) -> &ProcedureSignature {
        &self.signature
    }

    fn invoke(
        &self,
        ctx: ProcedureContext<'_>,
        _args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        let who = ctx.principal.map(|p| p.id.clone());
        self.seen.lock().unwrap().push(who.clone());
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("who", DataType::Utf8, true)]));
        let arr = Arc::new(StringArray::from(vec![who])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(schema, vec![arr])
            .map_err(|e| FnError::new(0x600, format!("whoami.probe: {e}")))?;
        Ok(batch_into_stream(batch))
    }
}

/// A database with `whoami.probe()` registered, a session authenticated as
/// `alice`, and the log of principals the procedure has seen.
async fn setup() -> Result<(Uni, Session, Seen)> {
    let db = Uni::in_memory().build().await?;
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let caps = CapabilitySet::from_iter_of([Capability::Procedure]);
    let mut r = PluginRegistrar::new(PluginId::new("whoami"), &caps, db.plugin_registry());
    r.procedure(
        QName::new("whoami", "probe"),
        ProcedureSignature {
            args: vec![],
            yields: vec![Field::new("who", DataType::Utf8, true)],
            mode: ProcedureMode::Read,
            side_effects: uni_plugin::SideEffects::ReadOnly,
            retry_contract: None,
            batch_input: None,
            docs: "Yields the calling principal's id.".to_owned(),
        },
        Arc::new(WhoAmI {
            seen: Arc::clone(&seen),
            signature: ProcedureSignature {
                args: vec![],
                yields: vec![Field::new("who", DataType::Utf8, true)],
                mode: ProcedureMode::Read,
                side_effects: uni_plugin::SideEffects::ReadOnly,
                retry_contract: None,
                batch_input: None,
                docs: String::new(),
            },
        }),
    )?;
    r.commit_to_registry()?;
    let session = db.session().with_principal(Arc::new(Principal {
        id: "alice".to_owned(),
        groups: vec![],
        capabilities: CapabilitySet::default(),
    }));
    Ok((db, session, seen))
}

/// Assert the procedure ran exactly once since the last check and saw `alice`.
fn assert_saw_alice(seen: &Seen, entry: &str) {
    let got = std::mem::take(&mut *seen.lock().unwrap());
    assert_eq!(
        got,
        vec![Some("alice".to_owned())],
        "{entry}: the procedure must run once and see the caller's principal"
    );
}

const CALL: &str = "CALL whoami.probe() YIELD who RETURN who";

/// Locy entry points, session and transaction, including prepared programs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_principal_scope_locy_entry_points() -> Result<()> {
    let (_db, session, seen) = setup().await?;

    session.locy(CALL).await?;
    assert_saw_alice(&seen, "session.locy");
    session.locy_with(CALL).run().await?;
    assert_saw_alice(&seen, "session.locy_with.run");
    session.locy_with(CALL).profile().await?;
    assert_saw_alice(&seen, "session.locy_with.profile");
    session.prepare_locy(CALL).await?.execute(&[]).await?;
    assert_saw_alice(&seen, "session.prepare_locy.execute");

    let tx = session.tx().await?;
    tx.locy(CALL).await?;
    assert_saw_alice(&seen, "tx.locy");
    tx.locy_with(CALL).run().await?;
    assert_saw_alice(&seen, "tx.locy_with.run");
    tx.locy_with(CALL).profile().await?;
    assert_saw_alice(&seen, "tx.locy_with.profile");
    tx.prepare_locy(CALL).await?.execute(&[]).await?;
    assert_saw_alice(&seen, "tx.prepare_locy.execute");
    tx.rollback();
    Ok(())
}

/// Cypher entry points, session and transaction, including builders, cursors
/// and prepared statements.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn procedure_principal_scope_cypher_entry_points() -> Result<()> {
    let (_db, session, seen) = setup().await?;

    session.query(CALL).await?;
    assert_saw_alice(&seen, "session.query");
    session.run(CALL).await?;
    assert_saw_alice(&seen, "session.run");
    session.query_with(CALL).fetch_all().await?;
    assert_saw_alice(&seen, "session.query_with.fetch_all");
    session.query_with(CALL).profile().await?;
    assert_saw_alice(&seen, "session.query_with.profile");
    session
        .query_with(CALL)
        .cursor()
        .await?
        .collect_remaining()
        .await?;
    assert_saw_alice(&seen, "session.query_with.cursor");
    session.prepare(CALL).await?.execute(&[]).await?;
    assert_saw_alice(&seen, "session.prepare.execute");

    let tx = session.tx().await?;
    tx.query(CALL).await?;
    assert_saw_alice(&seen, "tx.query");
    tx.execute(CALL).await?;
    assert_saw_alice(&seen, "tx.execute");
    tx.query_with(CALL).fetch_all().await?;
    assert_saw_alice(&seen, "tx.query_with.fetch_all");
    tx.query_with(CALL).execute().await?;
    assert_saw_alice(&seen, "tx.query_with.execute");
    tx.query_with(CALL).profile().await?;
    assert_saw_alice(&seen, "tx.query_with.profile");
    tx.query_with(CALL)
        .cursor()
        .await?
        .collect_remaining()
        .await?;
    assert_saw_alice(&seen, "tx.query_with.cursor");
    tx.execute_with(CALL).run().await?;
    assert_saw_alice(&seen, "tx.execute_with.run");
    tx.execute_with(CALL).profile().await?;
    assert_saw_alice(&seen, "tx.execute_with.profile");
    tx.prepare(CALL).await?.execute(&[]).await?;
    assert_saw_alice(&seen, "tx.prepare.execute");
    tx.rollback();
    Ok(())
}
