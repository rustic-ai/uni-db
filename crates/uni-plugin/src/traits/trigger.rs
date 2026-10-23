//! Fine-grained triggers — APOC `apoc.trigger.*` analogue.

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use smol_str::SmolStr;

use crate::errors::FnError;
use crate::traits::procedure::ProcedureHost;

/// A fine-grained mutation trigger.
pub trait TriggerPlugin: Send + Sync {
    /// Subscription describing which events this trigger receives.
    fn subscription(&self) -> &TriggerSubscription;

    /// Fire the trigger with a batch of matching mutation events.
    ///
    /// # Threading policy
    ///
    /// `fire` is synchronous; the host wraps it differently depending
    /// on the subscription's [`FireMode`]:
    ///
    /// - [`FireMode::Synchronous`] — invoked inline on the transaction
    ///   commit path, on a `tokio::task::spawn_blocking` worker
    ///   thread. Returning [`TriggerOutcome::Reject`] aborts the
    ///   transaction; long-running work blocks the committer, so keep
    ///   the body tight.
    /// - [`FireMode::Async`] — fires off a separate `spawn_blocking`
    ///   task after the transaction commits; cannot reject (the
    ///   transaction has already landed). Failures are logged but do
    ///   not roll back.
    /// - [`FireMode::EventualConsistency`] — batched via the
    ///   `BackgroundJobProvider` machinery; the same blocking-worker
    ///   contract from [`crate::traits::background::BackgroundJobProvider::execute`] applies.
    ///
    /// In every mode the body must not call `block_on` against the
    /// host runtime; panics are caught at the dispatcher boundary.
    ///
    /// See `docs/PLUGIN_THREADING.md` for the long-form rationale.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the fire cannot complete. For `Synchronous`
    /// triggers this aborts the surrounding transaction.
    fn fire(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationBatch,
    ) -> Result<TriggerOutcome, FnError>;

    /// Re-fire after a [`TriggerOutcome::Defer`] previously returned.
    ///
    /// The host's deferral queue invokes this with the original
    /// `payload` once the `delay` has elapsed. The default
    /// implementation delegates back to [`Self::fire`] with the
    /// original [`MutationBatch`] — existing trigger plugins keep
    /// working without changes. Plugins that need access to the
    /// `payload` (e.g., to resume a long-running aggregation) override
    /// this method.
    ///
    /// Returning [`TriggerOutcome::Defer`] from `on_deferred` re-queues
    /// the item with `attempt + 1`, capped at the host's
    /// `DEFER_MAX_ATTEMPTS`.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] when the deferred fire cannot complete.
    /// The error is logged at warn and the item is dropped.
    fn on_deferred(
        &self,
        ctx: TriggerContext<'_>,
        events: &MutationBatch,
        _payload: &str,
    ) -> Result<TriggerOutcome, FnError> {
        self.fire(ctx, events)
    }
}

/// Selectors describing the events this trigger subscribes to.
#[derive(Clone, Debug)]
pub struct TriggerSubscription {
    /// Phase in the mutation lifecycle.
    pub phase: TriggerPhase,
    /// Event-kind bitmask (`NodeCreate | NodeUpdate | EdgeDelete | ...`).
    pub events: TriggerEventMask,
    /// Optional label allow-list; `None` means all labels.
    ///
    /// Bare label names (`"Customer"`). The host also accepts the Cypher
    /// form (`":Customer"`) and strips the leading colon when it builds its
    /// routes, so both name the same label.
    pub labels: Option<Vec<SmolStr>>,
    /// Optional edge-type allow-list; bare names, with a leading `:`
    /// accepted as for [`Self::labels`].
    pub edge_types: Option<Vec<SmolStr>>,
    /// Optional property allow-list — for `*Update` events, restrict to
    /// updates touching these properties.
    pub properties: Option<Vec<SmolStr>>,
    /// Cypher boolean expression evaluated per event (parsed by host).
    pub predicate_source: Option<String>,
    /// Firing mode (Sync / Async / Eventual).
    pub fire_mode: FireMode,
    /// Markdown docs.
    pub docs: String,
}

/// Lifecycle phase a trigger subscribes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum TriggerPhase {
    /// Before the mutation is applied — may reject.
    BeforeMutation,
    /// After the mutation is applied, in the same transaction.
    AfterMutation,
    /// Before transaction commit — may reject.
    BeforeCommit,
    /// After transaction commit; cannot reject.
    AfterCommit,
}

/// Bitmask of event kinds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TriggerEventMask(pub u32);

impl TriggerEventMask {
    /// Node creation.
    pub const NODE_CREATE: Self = Self(1 << 0);
    /// Node update.
    pub const NODE_UPDATE: Self = Self(1 << 1);
    /// Node deletion.
    pub const NODE_DELETE: Self = Self(1 << 2);
    /// Edge creation.
    pub const EDGE_CREATE: Self = Self(1 << 3);
    /// Edge update.
    pub const EDGE_UPDATE: Self = Self(1 << 4);
    /// Edge deletion.
    pub const EDGE_DELETE: Self = Self(1 << 5);
    /// Property change (covered by Node/Edge Update — independent bit for
    /// finer-grained matching).
    pub const PROPERTY_CHANGE: Self = Self(1 << 6);
    /// Label added.
    pub const LABEL_ADDED: Self = Self(1 << 7);
    /// Label removed.
    pub const LABEL_REMOVED: Self = Self(1 << 8);

    /// Combine two masks.
    #[must_use]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Check whether this mask is a superset of `other`.
    #[must_use]
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }
}

/// Firing mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FireMode {
    /// Synchronous — blocks the mutation; may reject.
    Synchronous,
    /// Fires after commit; cannot reject.
    Async,
    /// Eventually consistent — batched via `BackgroundJobProvider`.
    EventualConsistency,
}

/// Outcome returned by a trigger.
#[derive(Debug)]
#[non_exhaustive]
pub enum TriggerOutcome {
    /// Continue normally.
    Continue,
    /// Reject the surrounding mutation / transaction (valid only in
    /// `Before*` phases).
    Reject {
        /// Human-readable rejection reason.
        reason: String,
    },
    /// Defer this trigger's firing (e.g., for batched aggregation).
    Defer {
        /// Deferral metadata understood by the trigger implementation.
        until: TriggerDeferral,
    },
}

/// Deferral marker returned by [`TriggerOutcome::Defer`].
///
/// Carries an implementation-defined `payload` plus an optional
/// `delay` (FU-5). When `delay` is `None` the deferred item re-fires
/// on the next scheduler tick (legacy "any moment now" semantics);
/// when `Some(d)` the host's deferral queue waits at least `d` before
/// re-invoking the trigger.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TriggerDeferral {
    /// Implementation-defined payload — opaque to the host. Persisted
    /// across `Uni` restarts when the host's durable defer queue is
    /// enabled.
    pub payload: String,
    /// Wait at least this duration before re-firing. `None` means "as
    /// soon as the next tick fires" (~50–100 ms).
    pub delay: Option<std::time::Duration>,
}

impl TriggerDeferral {
    /// Construct a deferral with no delay.
    ///
    /// Use this when the trigger is simply asking "re-queue me for
    /// the next tick" — e.g., when an external prerequisite resource
    /// might become available at any moment.
    #[must_use]
    pub fn from_payload(payload: impl Into<String>) -> Self {
        Self {
            payload: payload.into(),
            delay: None,
        }
    }

    /// Construct a deferral with an explicit `delay`.
    ///
    /// The host's deferral queue waits at least `delay` before
    /// re-invoking [`TriggerPlugin::on_deferred`] (or, when the host
    /// has not adopted the `on_deferred` callback, [`TriggerPlugin::fire`]
    /// with the original [`MutationBatch`]).
    #[must_use]
    pub fn after(payload: impl Into<String>, delay: std::time::Duration) -> Self {
        Self {
            payload: payload.into(),
            delay: Some(delay),
        }
    }
}

/// Per-fire context.
///
/// # ABI note (3.0 breaking change)
///
/// Carries an **owned** optional [`ProcedureHost`] handle so a declared
/// (synthesized) trigger can reach the host's write-enabled inner-query
/// primitive from inside `fire`. The handle is owned (not borrowed)
/// because the after-commit async dispatch path moves the context into a
/// `'static` spawned task and rebuilds it there — a borrow could not
/// outlive the commit stack frame. Native trigger plugins that never
/// touch `host()` are unaffected (the field defaults to `None`).
#[non_exhaustive]
pub struct TriggerContext<'a> {
    /// Session identifier.
    pub session_id: &'a str,
    /// Transaction identifier.
    pub tx_id: u64,
    /// Owned host handle, threaded through the commit path so a
    /// declared trigger's Cypher action body can run against the same
    /// storage / writer the outer commit saw. `None` for native
    /// trigger plugins and for contexts built without a host.
    host: Option<Arc<dyn ProcedureHost>>,
}

impl std::fmt::Debug for TriggerContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TriggerContext")
            .field("session_id", &self.session_id)
            .field("tx_id", &self.tx_id)
            .field("host", &self.host.as_ref().map(|_| "<ProcedureHost>"))
            .finish()
    }
}

impl<'a> TriggerContext<'a> {
    /// Construct a fresh context with no host handle. The struct is
    /// `#[non_exhaustive]` so external callers can't use struct-literal
    /// syntax; this constructor is the supported path. Future fields
    /// ship via `with_*` builder methods to preserve API compatibility.
    #[must_use]
    pub fn new(session_id: &'a str, tx_id: u64) -> Self {
        Self {
            session_id,
            tx_id,
            host: None,
        }
    }

    /// Attach an owned host handle to this context.
    ///
    /// The commit-path dispatcher threads a write-enabled host through so
    /// a declared trigger's `fire` can downcast it (via
    /// [`ProcedureHost::as_any`]) and run its stored Cypher action body.
    #[must_use]
    pub fn with_host(mut self, host: Arc<dyn ProcedureHost>) -> Self {
        self.host = Some(host);
        self
    }

    /// Borrow the attached host handle, when one was threaded in.
    #[must_use]
    pub fn host(&self) -> Option<&Arc<dyn ProcedureHost>> {
        self.host.as_ref()
    }
}

/// Batch of mutation events delivered to a trigger.
///
/// The batch's `RecordBatch` schema is host-defined and stable:
/// `event_kind | vid_or_eid | label | property | old_value | new_value | …`.
#[derive(Clone, Debug)]
pub struct MutationBatch {
    /// The events as a typed columnar batch.
    pub events: Arc<RecordBatch>,
}
