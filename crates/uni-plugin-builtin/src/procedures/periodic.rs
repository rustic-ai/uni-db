// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `uni.periodic.*` procedures — Cypher-facing wrappers over the
//! host's [`SchedulerControl`].
//!
//! These procedures translate Cypher `CALL` sites to method calls on
//! the host's [`uni_plugin::scheduler::SchedulerControl`] trait
//! object. The host (`uni-db`) constructs an `Arc<dyn SchedulerControl>`
//! pointing at its `SchedulerHost::scheduler()` and passes it to
//! [`register_into`] so the procedures' invocations land on the live
//! scheduler.
//!
//! ## Procedures
//!
//! - `uni.periodic.schedule(qname, kind, schedule_arg)` — register a
//!   job. `kind` is `"periodic"` (Periodic) | `"cron"` (Cron) |
//!   `"manual"` (Manual). `schedule_arg` is the interval in seconds for
//!   `periodic`, a cron expression (5- or 6-field) for `cron`, and
//!   ignored for `manual`.
//! - `uni.periodic.cancel(qname)` — cancel a job; yields `true` if the
//!   job was found.
//! - `uni.periodic.list()` — yield one row per known job with `qname`,
//!   `status`, `consecutive_failures`.
//! - `uni.periodic.submit(cypher)` — synchronously run one write-mode
//!   Cypher body through the scheduler.
//! - `uni.periodic.iterate(query, mutating_query, options_json)` —
//!   APOC-style batched-update (v1: single-pass).
//! - `uni.periodic.commit()` — sync sentinel; v1 no-op.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use uni_plugin::adapter_common::batch_builder::batch_into_stream;
use uni_plugin::scheduler::SchedulerControl;
use uni_plugin::traits::background::Schedule;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName};

/// The six `uni.periodic.*` procedures, dispatched through a single
/// [`ProcedurePlugin`] impl.
///
/// Per-variant `ProcedureSignature`s are constructed lazily and cached
/// in process-global `OnceLock`s so the trait's `signature()` accessor
/// can hand out a stable reference.
#[derive(Clone, Copy, Debug)]
pub enum PeriodicProc {
    /// `uni.periodic.schedule(qname, kind, schedule_arg)`.
    Schedule,
    /// `uni.periodic.cancel(qname)`.
    Cancel,
    /// `uni.periodic.list()`.
    List,
    /// `uni.periodic.submit(cypher)`.
    Submit,
    /// `uni.periodic.iterate(query, mutating_query, options_json)`.
    Iterate,
    /// `uni.periodic.commit()`.
    Commit,
}

/// Concrete [`ProcedurePlugin`] that pairs a [`PeriodicProc`] variant
/// with the live [`SchedulerControl`] handle.
#[derive(Debug)]
pub struct PeriodicProcPlugin {
    proc: PeriodicProc,
    scheduler: Arc<dyn SchedulerControl>,
}

impl PeriodicProc {
    /// Every periodic procedure, in registration order.
    pub const ALL: &'static [Self] = &[
        Self::Schedule,
        Self::Cancel,
        Self::List,
        Self::Submit,
        Self::Iterate,
        Self::Commit,
    ];

    /// Qualified procedure name.
    #[must_use]
    pub fn qname(&self) -> QName {
        let local = match self {
            Self::Schedule => "periodic.schedule",
            Self::Cancel => "periodic.cancel",
            Self::List => "periodic.list",
            Self::Submit => "periodic.submit",
            Self::Iterate => "periodic.iterate",
            Self::Commit => "periodic.commit",
        };
        QName::new("uni", local)
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::Schedule => ProcedureSignature {
                args: vec![
                    NamedArgType {
                        name: smol_str::SmolStr::new("qname"),
                        ty: ArgType::Primitive(DataType::Utf8),
                        default: None,
                        doc: "Fully-qualified job id (e.g. `uni.system.ttl_sweep`).".to_owned(),
                    },
                    NamedArgType {
                        name: smol_str::SmolStr::new("kind"),
                        ty: ArgType::Primitive(DataType::Utf8),
                        default: None,
                        doc: "`\"periodic\"`, `\"cron\"`, or `\"manual\"`.".to_owned(),
                    },
                    NamedArgType {
                        name: smol_str::SmolStr::new("schedule_arg"),
                        ty: ArgType::Primitive(DataType::Utf8),
                        default: None,
                        doc: "Interval seconds for `periodic`, cron expression for `cron`, ignored for `manual`.".to_owned(),
                    },
                ],
                yields: vec![Field::new("registered", DataType::Boolean, false)],
                mode: ProcedureMode::Write,
                side_effects: uni_plugin::SideEffects::Writes,
                retry_contract: None,
                batch_input: None,
                docs: "Register a background job to fire on the given schedule.".to_owned(),
            },
            Self::Cancel => ProcedureSignature {
                args: vec![NamedArgType {
                    name: smol_str::SmolStr::new("qname"),
                    ty: ArgType::Primitive(DataType::Utf8),
                    default: None,
                    doc: "Fully-qualified job id to cancel.".to_owned(),
                }],
                yields: vec![Field::new("cancelled", DataType::Boolean, false)],
                mode: ProcedureMode::Write,
                side_effects: uni_plugin::SideEffects::Writes,
                retry_contract: None,
                batch_input: None,
                docs: "Cancel a previously-scheduled job.".to_owned(),
            },
            Self::List => ProcedureSignature {
                args: vec![],
                yields: vec![
                    Field::new("qname", DataType::Utf8, false),
                    Field::new("status", DataType::Utf8, false),
                    Field::new("consecutive_failures", DataType::UInt32, false),
                ],
                mode: ProcedureMode::Read,
                side_effects: uni_plugin::SideEffects::ReadOnly,
                retry_contract: None,
                batch_input: None,
                docs: "List every known background job with its current status.".to_owned(),
            },
            Self::Submit => ProcedureSignature {
                args: vec![NamedArgType {
                    name: smol_str::SmolStr::new("cypher"),
                    ty: ArgType::Primitive(DataType::Utf8),
                    default: None,
                    doc: "Write-mode Cypher body to run once, as soon as possible.".to_owned(),
                }],
                yields: vec![Field::new("submitted", DataType::Boolean, false)],
                mode: ProcedureMode::Write,
                side_effects: uni_plugin::SideEffects::Writes,
                retry_contract: None,
                batch_input: None,
                docs: "Submit a one-shot Cypher body for synchronous write-mode execution.".to_owned(),
            },
            Self::Iterate => ProcedureSignature {
                args: vec![
                    NamedArgType {
                        name: smol_str::SmolStr::new("query"),
                        ty: ArgType::Primitive(DataType::Utf8),
                        default: None,
                        doc: "Driver Cypher query (read-mode) returning rows to mutate.".to_owned(),
                    },
                    NamedArgType {
                        name: smol_str::SmolStr::new("mutating_query"),
                        ty: ArgType::Primitive(DataType::Utf8),
                        default: None,
                        doc: "Mutating Cypher body, invoked once per batch.".to_owned(),
                    },
                    NamedArgType {
                        name: smol_str::SmolStr::new("options_json"),
                        ty: ArgType::Primitive(DataType::Utf8),
                        default: None,
                        doc: "JSON options blob, e.g. `{\"batchSize\": 1000}`. v1: sequential only."
                            .to_owned(),
                    },
                ],
                yields: vec![Field::new("batches", DataType::UInt32, false)],
                mode: ProcedureMode::Write,
                side_effects: uni_plugin::SideEffects::Writes,
                retry_contract: None,
                batch_input: None,
                docs: "APOC-style batched-update pattern (sequential v1).".to_owned(),
            },
            Self::Commit => ProcedureSignature {
                args: vec![],
                yields: vec![Field::new("committed", DataType::Boolean, false)],
                mode: ProcedureMode::Read,
                side_effects: uni_plugin::SideEffects::ReadOnly,
                retry_contract: None,
                batch_input: None,
                docs: "Force the scheduler persistence backend to flush its checkpoint buffer."
                    .to_owned(),
            },
        }
    }

    fn signature_cached(&self) -> &'static ProcedureSignature {
        // One `OnceLock` per variant so each can hand back a `'static`
        // reference without re-allocating on every call.
        static SCHEDULE: OnceLock<ProcedureSignature> = OnceLock::new();
        static CANCEL: OnceLock<ProcedureSignature> = OnceLock::new();
        static LIST: OnceLock<ProcedureSignature> = OnceLock::new();
        static SUBMIT: OnceLock<ProcedureSignature> = OnceLock::new();
        static ITERATE: OnceLock<ProcedureSignature> = OnceLock::new();
        static COMMIT: OnceLock<ProcedureSignature> = OnceLock::new();
        match self {
            Self::Schedule => SCHEDULE.get_or_init(|| self.build_signature()),
            Self::Cancel => CANCEL.get_or_init(|| self.build_signature()),
            Self::List => LIST.get_or_init(|| self.build_signature()),
            Self::Submit => SUBMIT.get_or_init(|| self.build_signature()),
            Self::Iterate => ITERATE.get_or_init(|| self.build_signature()),
            Self::Commit => COMMIT.get_or_init(|| self.build_signature()),
        }
    }
}

impl PeriodicProcPlugin {
    /// Construct with a variant and a handle to the live scheduler.
    #[must_use]
    pub fn new(proc: PeriodicProc, scheduler: Arc<dyn SchedulerControl>) -> Self {
        Self { proc, scheduler }
    }
}

impl ProcedurePlugin for PeriodicProcPlugin {
    fn signature(&self) -> &ProcedureSignature {
        self.proc.signature_cached()
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        match self.proc {
            PeriodicProc::Schedule => schedule_invoke(&*self.scheduler, args),
            PeriodicProc::Cancel => cancel_invoke(&*self.scheduler, args),
            PeriodicProc::List => list_invoke(&*self.scheduler),
            PeriodicProc::Submit => submit_invoke(&*self.scheduler, args),
            PeriodicProc::Iterate => iterate_invoke(&*self.scheduler, args),
            PeriodicProc::Commit => commit_invoke(&*self.scheduler),
        }
    }
}

/// Register `uni.periodic.*` procedures into `r`.
///
/// Pass `scheduler` so the procedures' `invoke()` bodies can reach the
/// live scheduler.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if any qname is
/// already taken.
pub fn register_into(
    r: &mut PluginRegistrar<'_>,
    scheduler: Arc<dyn SchedulerControl>,
) -> Result<(), PluginError> {
    for proc in PeriodicProc::ALL {
        r.procedure(
            proc.qname(),
            proc.signature_cached().clone(),
            Arc::new(PeriodicProcPlugin::new(*proc, Arc::clone(&scheduler))),
        )?;
    }
    Ok(())
}

fn schedule_invoke(
    scheduler: &dyn SchedulerControl,
    args: &[ColumnarValue],
) -> Result<SendableRecordBatchStream, FnError> {
    let qname_str = extract_utf8(args, 0, "qname")?;
    let kind = extract_utf8(args, 1, "kind")?;
    let schedule_arg = extract_utf8(args, 2, "schedule_arg")?;

    let id = QName::parse(&qname_str).map_err(|e| {
        FnError::new(
            0xB30,
            format!("uni.periodic.schedule: bad qname `{qname_str}`: {e}"),
        )
    })?;

    let schedule = match kind.as_str() {
        "manual" => Schedule::Manual,
        "periodic" => {
            let secs: u64 = schedule_arg.parse().map_err(|e| {
                FnError::new(
                    0xB31,
                    format!("uni.periodic.schedule: bad interval_secs `{schedule_arg}`: {e}"),
                )
            })?;
            Schedule::Periodic(Duration::from_secs(secs))
        }
        "cron" => {
            // Validate the expression now so a malformed schedule
            // is caught at registration rather than at first tick.
            let _: cron::Schedule = schedule_arg.parse().map_err(|e| {
                FnError::new(
                    0xB33,
                    format!("uni.periodic.schedule: bad cron expression `{schedule_arg}`: {e}"),
                )
            })?;
            Schedule::Cron(smol_str::SmolStr::new(&schedule_arg))
        }
        other => {
            return Err(FnError::new(
                0xB32,
                format!(
                    "uni.periodic.schedule: unknown kind `{other}` (expected `manual`, `periodic`, or `cron`)"
                ),
            ));
        }
    };

    // #233: reporting `registered: true` unconditionally hid a failed
    // persist, so the caller was told the job was registered and it vanished
    // at the next restart.
    scheduler.add_scheduled_job(id, schedule)?;

    single_bool("registered", true)
}

fn cancel_invoke(
    scheduler: &dyn SchedulerControl,
    args: &[ColumnarValue],
) -> Result<SendableRecordBatchStream, FnError> {
    let qname_str = extract_utf8(args, 0, "qname")?;
    let id = QName::parse(&qname_str).map_err(|e| {
        // Renumbered from 0xB33 (which now exclusively means "bad
        // cron expression" in schedule_invoke) to 0xB37.
        FnError::new(
            0xB37,
            format!("uni.periodic.cancel: bad qname `{qname_str}`: {e}"),
        )
    })?;
    // #233: a persistence failure used to be invisible here, so `cancelled`
    // read `true` while the job resurrected on the next restart.
    let cancelled = scheduler.cancel(&id)?;
    single_bool("cancelled", cancelled)
}

fn list_invoke(scheduler: &dyn SchedulerControl) -> Result<SendableRecordBatchStream, FnError> {
    let records = scheduler.list();
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("qname", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("consecutive_failures", DataType::UInt32, false),
    ]));
    let qnames: Vec<String> = records.iter().map(|r| r.id.to_string()).collect();
    let statuses: Vec<String> = records.iter().map(|r| format!("{:?}", r.status)).collect();
    let failures: Vec<u32> = records.iter().map(|r| r.consecutive_failures).collect();
    let cols: Vec<Arc<dyn Array>> = vec![
        Arc::new(StringArray::from(qnames)),
        Arc::new(StringArray::from(statuses)),
        Arc::new(UInt32Array::from(failures)),
    ];
    let batch = RecordBatch::try_new(schema, cols).map_err(|e| {
        FnError::new(
            0xB34,
            format!("uni.periodic.list: failed to build batch: {e}"),
        )
    })?;
    Ok(batch_into_stream(batch))
}

fn submit_invoke(
    scheduler: &dyn SchedulerControl,
    args: &[ColumnarValue],
) -> Result<SendableRecordBatchStream, FnError> {
    let cypher = extract_utf8(args, 0, "cypher")?;
    scheduler.submit_cypher(&cypher)?;
    single_bool("submitted", true)
}

fn iterate_invoke(
    scheduler: &dyn SchedulerControl,
    args: &[ColumnarValue],
) -> Result<SendableRecordBatchStream, FnError> {
    let _query = extract_utf8(args, 0, "query")?;
    let mutating_query = extract_utf8(args, 1, "mutating_query")?;
    let _options_json = extract_utf8(args, 2, "options_json")?;
    // v1: single-pass — execute the mutating query once. The
    // driver-query loop is a v2 follow-up since it needs
    // read-Cypher access that isn't yet on `SchedulerControl`.
    scheduler.submit_cypher(&mutating_query)?;
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "batches",
        DataType::UInt32,
        false,
    )]));
    let arr = Arc::new(UInt32Array::from(vec![1_u32])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema, vec![arr])
        .map_err(|e| FnError::new(0xB36, format!("uni.periodic.iterate: build batch: {e}")))?;
    Ok(batch_into_stream(batch))
}

fn commit_invoke(scheduler: &dyn SchedulerControl) -> Result<SendableRecordBatchStream, FnError> {
    // Drive the persistence backend's checkpoint flush. For backends
    // that write through every event (the system-label backend), this
    // is a cheap confirmation; buffered backends override the default
    // and synchronously fsync.
    scheduler.flush_checkpoint()?;
    single_bool("committed", true)
}

fn extract_utf8(args: &[ColumnarValue], idx: usize, field: &str) -> Result<String, FnError> {
    super::extract_utf8_arg(args, idx, "uni.periodic.*", field)
}

fn single_bool(name: &str, value: bool) -> Result<SendableRecordBatchStream, FnError> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        name,
        DataType::Boolean,
        false,
    )]));
    let arr = Arc::new(BooleanArray::from(vec![value])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema, vec![arr])
        .map_err(|e| FnError::new(0xB35, format!("uni.periodic.*: failed to build batch: {e}")))?;
    Ok(batch_into_stream(batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;
    use parking_lot::Mutex;
    use uni_plugin::scheduler::SchedulerJobRecord;

    /// A minimal `SchedulerControl` impl that records calls into a
    /// vector — used to verify the procedure dispatchers call through
    /// without invoking the host scheduler driver.
    #[derive(Debug, Default)]
    struct RecordingScheduler {
        added: Mutex<Vec<(QName, Schedule)>>,
        cancelled: Mutex<Vec<QName>>,
        list_records: Mutex<Vec<SchedulerJobRecord>>,
        submitted_cyphers: Mutex<Vec<String>>,
        flush_calls: std::sync::atomic::AtomicUsize,
    }

    impl SchedulerControl for RecordingScheduler {
        fn add_scheduled_job(&self, id: QName, schedule: Schedule) -> Result<(), FnError> {
            self.added.lock().push((id, schedule));
            Ok(())
        }

        fn cancel(&self, id: &QName) -> Result<bool, FnError> {
            self.cancelled.lock().push(id.clone());
            Ok(true)
        }

        fn list(&self) -> Vec<SchedulerJobRecord> {
            self.list_records.lock().clone()
        }

        fn submit_cypher(&self, cypher: &str) -> Result<(), FnError> {
            self.submitted_cyphers.lock().push(cypher.to_owned());
            Ok(())
        }

        fn flush_checkpoint(&self) -> Result<(), FnError> {
            self.flush_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    fn utf8(s: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_owned())))
    }

    fn plugin(proc: PeriodicProc, rec: Arc<RecordingScheduler>) -> PeriodicProcPlugin {
        PeriodicProcPlugin::new(proc, rec)
    }

    #[tokio::test]
    async fn schedule_periodic_dispatches_to_scheduler() {
        let rec = Arc::new(RecordingScheduler::default());
        let p = plugin(PeriodicProc::Schedule, rec.clone());
        let args = vec![utf8("myorg.ticker"), utf8("periodic"), utf8("30")];
        let _ = p
            .invoke(ProcedureContext::default(), &args)
            .expect("invoke");
        let added = rec.added.lock();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].0.to_string(), "myorg.ticker");
        assert!(matches!(added[0].1, Schedule::Periodic(d) if d == Duration::from_secs(30)));
    }

    #[tokio::test]
    async fn schedule_cron_dispatches_to_scheduler() {
        let rec = Arc::new(RecordingScheduler::default());
        let p = plugin(PeriodicProc::Schedule, rec.clone());
        // 6-field cron: sec min hour dom mon dow — every 5 minutes.
        let args = vec![utf8("myorg.cron_job"), utf8("cron"), utf8("0 */5 * * * *")];
        let _ = p
            .invoke(ProcedureContext::default(), &args)
            .expect("invoke");
        let added = rec.added.lock();
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].0.to_string(), "myorg.cron_job");
        assert!(matches!(&added[0].1, Schedule::Cron(s) if s.as_str() == "0 */5 * * * *"));
    }

    #[tokio::test]
    async fn schedule_cron_rejects_bad_expression() {
        let rec = Arc::new(RecordingScheduler::default());
        let p = plugin(PeriodicProc::Schedule, rec.clone());
        let args = vec![utf8("myorg.bad_cron"), utf8("cron"), utf8("not a cron")];
        match p.invoke(ProcedureContext::default(), &args) {
            Ok(_) => panic!("malformed cron must error"),
            Err(e) => assert!(e.message.contains("bad cron expression")),
        }
        assert!(rec.added.lock().is_empty());
    }

    #[tokio::test]
    async fn cancel_dispatches_to_scheduler() {
        let rec = Arc::new(RecordingScheduler::default());
        let p = plugin(PeriodicProc::Cancel, rec.clone());
        let args = vec![utf8("myorg.ticker")];
        let _ = p
            .invoke(ProcedureContext::default(), &args)
            .expect("invoke");
        let cancelled = rec.cancelled.lock();
        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0].to_string(), "myorg.ticker");
    }

    #[test]
    fn schedule_signature_declares_write_mode() {
        let s = PeriodicProc::Schedule.build_signature();
        assert_eq!(s.mode, ProcedureMode::Write);
        assert_eq!(s.args.len(), 3);
    }

    #[test]
    fn list_signature_is_read_mode_with_three_yields() {
        let s = PeriodicProc::List.build_signature();
        assert_eq!(s.mode, ProcedureMode::Read);
        assert_eq!(s.yields.len(), 3);
    }

    #[tokio::test]
    async fn submit_dispatches_cypher_to_scheduler() {
        let rec = Arc::new(RecordingScheduler::default());
        let p = plugin(PeriodicProc::Submit, rec.clone());
        let args = vec![utf8("CREATE (:Job {at: timestamp()})")];
        let _ = p
            .invoke(ProcedureContext::default(), &args)
            .expect("invoke");
        let cy = rec.submitted_cyphers.lock();
        assert_eq!(cy.len(), 1);
        assert!(cy[0].contains("CREATE (:Job"));
    }

    #[tokio::test]
    async fn iterate_v1_executes_mutating_query_once() {
        let rec = Arc::new(RecordingScheduler::default());
        let p = plugin(PeriodicProc::Iterate, rec.clone());
        let args = vec![
            utf8("MATCH (n) WHERE n.stale RETURN n"),
            utf8("MATCH (n) WHERE n.stale SET n.stale = false"),
            utf8("{\"batchSize\": 1000}"),
        ];
        let _ = p
            .invoke(ProcedureContext::default(), &args)
            .expect("invoke");
        let cy = rec.submitted_cyphers.lock();
        // v1: only mutating_query is submitted (driver-loop is v2).
        assert_eq!(cy.len(), 1);
        assert!(cy[0].contains("SET n.stale"));
    }

    #[tokio::test]
    async fn commit_dispatches_flush_checkpoint() {
        let rec = Arc::new(RecordingScheduler::default());
        let p = plugin(PeriodicProc::Commit, rec.clone());
        let _ = p.invoke(ProcedureContext::default(), &[]).expect("invoke");
        // Commit must drive the persistence backend's checkpoint flush
        // exactly once and must not touch the dispatch surfaces.
        assert_eq!(rec.flush_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(rec.submitted_cyphers.lock().is_empty());
        assert!(rec.added.lock().is_empty());
    }

    #[test]
    fn submit_signature_takes_one_cypher_arg() {
        let s = PeriodicProc::Submit.build_signature();
        assert_eq!(s.args.len(), 1);
        assert_eq!(s.args[0].name.as_str(), "cypher");
        assert_eq!(s.mode, ProcedureMode::Write);
    }

    #[test]
    fn iterate_signature_takes_three_args() {
        let s = PeriodicProc::Iterate.build_signature();
        assert_eq!(s.args.len(), 3);
        assert_eq!(s.mode, ProcedureMode::Write);
    }

    #[test]
    fn commit_signature_is_read_mode_no_args() {
        let s = PeriodicProc::Commit.build_signature();
        assert_eq!(s.args.len(), 0);
        assert_eq!(s.mode, ProcedureMode::Read);
    }
}
