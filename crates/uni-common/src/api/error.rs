// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum UniError {
    #[error("Database not found: {path}")]
    NotFound { path: PathBuf },

    #[error("Schema error: {message}")]
    Schema { message: String },

    #[error("Parse error: {message}")]
    Parse {
        message: String,
        position: Option<usize>,
        line: Option<usize>,
        column: Option<usize>,
        context: Option<String>,
    },

    #[error("Query error: {message}")]
    Query {
        message: String,
        query: Option<String>,
    },

    #[error("Transaction error: {message}")]
    Transaction { message: String },

    #[error("Transaction conflict: {message}")]
    TransactionConflict { message: String },

    #[error("Transaction already completed")]
    TransactionAlreadyCompleted,

    /// A previous statement in this transaction failed, marking it rollback-only.
    ///
    /// Once any statement returns an error, the transaction is poisoned: it has
    /// possibly half-applied rows in its private buffer, so it is no longer
    /// committable (Neo4j-style rollback-only semantics). All further statements
    /// and `commit()` are rejected with this error; only `rollback()` (or drop)
    /// succeeds, discarding the partial writes. Start a fresh transaction to
    /// retry.
    #[error(
        "Transaction is rollback-only: a previous statement failed; the transaction can no longer be committed and must be rolled back"
    )]
    TransactionRollbackOnly,

    /// Operation not supported on read-only database
    #[error("Operation '{operation}' not supported on read-only database")]
    ReadOnly { operation: String },

    /// Label not found in schema
    #[error("Label '{label}' not found in schema")]
    LabelNotFound { label: String },

    /// Edge type not found in schema
    #[error("Edge type '{edge_type}' not found in schema")]
    EdgeTypeNotFound { edge_type: String },

    /// Property not found on node/edge
    #[error("Property '{property}' not found on {entity_type} with label '{label}'")]
    PropertyNotFound {
        property: String,
        entity_type: String, // "node" or "edge"
        label: String,
    },

    /// Index not found
    #[error("Index '{index}' not found")]
    IndexNotFound { index: String },

    /// Snapshot not found
    #[error("Snapshot '{snapshot_id}' not found")]
    SnapshotNotFound { snapshot_id: String },

    /// A query or Locy evaluation was refused for exceeding a memory budget.
    ///
    /// Covers the per-query operator pool (`max_query_memory`), the
    /// result-size estimate, and a Locy relation's `max_derived_bytes`.
    /// `message` keeps the refusing layer's own text — which operator or rule
    /// asked, and for how much — since the budget alone does not locate it.
    #[error("Query exceeded memory limit of {limit_bytes} bytes: {message}")]
    MemoryLimitExceeded { limit_bytes: usize, message: String },

    #[error("Database is locked by another process")]
    DatabaseLocked,

    #[error("Operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    /// A Locy program stopped before reaching its least fixed point because it
    /// exceeded its wall-clock `timeout` or its `max_iterations` cap.
    ///
    /// This is the default outcome of an over-budget evaluation: partial results
    /// are *not* returned silently. The boxed [`LocyIncomplete`] carries the
    /// diagnostics (which rules were skipped, which complement rules are now
    /// unsound, how far evaluation got). The partial facts themselves are not
    /// embedded here — to recover them, re-run with `allow_partial` set, which
    /// returns `Ok` with the partial result instead of this error.
    #[error("Locy evaluation incomplete: {detail}")]
    LocyIncomplete { detail: Box<LocyIncomplete> },

    /// A GraphCompute invocation stopped before producing a complete result
    /// because it exceeded its wall-clock deadline, its convergence-iteration
    /// cap, or its native-work budget.
    ///
    /// Like [`UniError::LocyIncomplete`], this is the *default* outcome of an
    /// over-budget run: partial output is never returned silently (GraphCompute
    /// proposal §5.2). The boxed [`GraphComputeIncomplete`] distinguishes
    /// `Timeout` (too slow) from `IterationLimit` (did not converge) from
    /// `Exhausted` (hit the native-work meter), so a caller can pick the right
    /// remedy. To recover an anytime partial result, re-run with `allow_partial`.
    #[error("GraphCompute invocation incomplete: {detail}")]
    GraphComputeIncomplete { detail: Box<GraphComputeIncomplete> },

    #[error("Type error: expected {expected}, got {actual}")]
    Type { expected: String, actual: String },

    #[error("Constraint violation: {message}")]
    Constraint { message: String },

    /// A transaction was aborted at commit because a concurrent transaction
    /// committed a conflicting write since this transaction began (optimistic
    /// concurrency control). The transaction may be safely retried.
    #[error("Serialization conflict: {message}")]
    SerializationConflict { message: String },

    /// A transaction was aborted at commit because a concurrent transaction
    /// committed a row with the same unique key (serializable MERGE). The
    /// transaction may be safely retried, which will observe the existing row.
    #[error("Constraint conflict: {message}")]
    ConstraintConflict { message: String },

    #[error("Storage error: {message}")]
    Storage {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),

    #[error("Invalid identifier '{name}': {reason}")]
    InvalidIdentifier { name: String, reason: String },

    #[error("Label '{label}' already exists")]
    LabelAlreadyExists { label: String },

    #[error("Edge type '{edge_type}' already exists")]
    EdgeTypeAlreadyExists { edge_type: String },

    #[error("Permission denied: {action}")]
    PermissionDenied { action: String },

    #[error("Argument '{arg}' is invalid: {message}")]
    InvalidArgument { arg: String, message: String },

    /// Write context (transaction, bulk writer, or appender) is already active on session.
    #[error("A write context is already active on session '{session_id}'")]
    WriteContextAlreadyActive {
        session_id: String,
        hint: &'static str,
    },

    /// Transaction commit timed out waiting for the global writer lock.
    #[error("Transaction '{tx_id}' commit timed out")]
    CommitTimeout { tx_id: String, hint: &'static str },

    /// A `FOR UPDATE` pessimistic row lock could not be acquired within the
    /// deadline — the holder is another live transaction (contention or a
    /// lock-ordering deadlock). Unlike a plain [`UniError::Timeout`] (a slow
    /// operation that would just time out again), this is transient: a fresh
    /// transaction can retry and win the lock once the holder releases it, so
    /// it is classified retriable. See `is_retriable`.
    #[error("FOR UPDATE lock acquisition timed out after {timeout_ms}ms")]
    LockTimeout { timeout_ms: u64 },

    /// Transaction exceeded its deadline.
    #[error("Transaction '{tx_id}' expired")]
    TransactionExpired { tx_id: String, hint: &'static str },

    /// Operation was cancelled via a cancellation token.
    #[error("Operation cancelled")]
    Cancelled,

    /// Derived facts are stale relative to the current database version.
    #[error("Derived facts are stale: version gap is {version_gap}")]
    StaleDerivedFacts { version_gap: u64 },

    /// A Locy rule conflict was detected during transaction commit rule promotion.
    #[error("Rule conflict: rule '{rule_name}' conflicts during promotion")]
    RuleConflict { rule_name: String },

    /// A session hook rejected the operation.
    #[error("Hook rejected: {message}")]
    HookRejected { message: String },

    /// A synchronous trigger returned `TriggerOutcome::Reject` (or `Err`)
    /// during a `BeforeMutation` / `BeforeCommit` phase, aborting commit.
    #[error("Trigger '{trigger}' rejected commit: {reason}")]
    TriggerRejected { trigger: String, reason: String },

    /// Authentication failed (M5i). Raised when
    /// `Uni::session_with_credentials` cannot find a matching
    /// `AuthProvider` or the matched provider rejects the credentials.
    #[error("Authentication failed: {reason}")]
    AuthenticationFailed {
        /// Human-readable failure reason.
        reason: String,
    },

    /// An `AuthzPolicy::check` returned `Decision::Deny` for the
    /// current principal (M5i).
    #[error("Authorization denied: {reason}")]
    AuthorizationDenied {
        /// Reason from the deciding policy.
        reason: String,
    },

    /// A write was attempted against an ephemeral (transient, in-query)
    /// node or edge — i.e. one whose `Vid` / `Eid` has the
    /// `EPHEMERAL_BIT` set. Ephemeral entities are return-only
    /// projections; SET / DELETE / MERGE against them must fail before
    /// they reach storage (M5g / proposal §4.13.1).
    #[error("Cannot mutate ephemeral {kind} {id}: ephemeral entities are return-only")]
    EphemeralWriteAttempt {
        /// `"node"` or `"edge"`.
        kind: &'static str,
        /// Transient id (bottom 63 bits) for diagnostic output.
        id: u64,
    },

    /// Fork with the given name does not exist in the registry.
    #[error("Fork '{name}' not found")]
    ForkNotFound { name: String },

    /// `session.fork(name).new_()` was called against an existing fork.
    #[error("Fork '{name}' already exists")]
    ForkAlreadyExists { name: String },

    /// The fork name is empty, all-whitespace, too long, or contains
    /// control characters. Names flow into the registry key and on-disk
    /// catalog, so they are validated before any state is created.
    #[error("Invalid fork name: {reason}")]
    ForkNameInvalid { reason: String },

    /// Phase-1 gate: writes through `forked_session.tx()` are blocked
    /// until Phase 2 lands. Reads, `locy()`, and admin paths work.
    #[error(
        "Writes on a forked session are not yet supported (Phase 2); reads, locy, and admin paths work"
    )]
    ForkWritesNotYetSupported,

    /// Drop refused because forked sessions are still alive on the fork.
    #[error("Fork '{name}' is held by {holder_count} live session(s); drop refused")]
    ForkInUse { name: String, holder_count: usize },

    /// Drop refused because a transaction has uncommitted mutations on the
    /// fork. Commit or roll back the transaction first, then retry drop.
    #[error("Fork '{name}' has uncommitted transaction state; commit or rollback first")]
    ForkInflightTx { name: String },

    /// Drop refused because the fork has pending async flushes that did
    /// not drain within `UniConfig::drop_fork_drain_timeout`. Either retry
    /// later (the streams will eventually complete) or raise the timeout.
    #[error("Fork '{name}' has pending flushes that did not drain within timeout")]
    PendingFlushTimeout { name: String },

    /// Registry on disk is malformed (corrupt JSON, missing required field, etc.).
    #[error("Fork registry is corrupt: {message}")]
    ForkCorruptRegistry { message: String },

    /// Drop refused because this fork has nested children. Use
    /// `drop_fork_cascade` to remove the whole subtree, or drop the
    /// children individually first.
    #[error(
        "Fork '{name}' has nested children {children:?}; use drop_fork_cascade or drop them first"
    )]
    ForkHasChildren { name: String, children: Vec<String> },

    /// `drop_fork_cascade` refused because at least one fork in the
    /// subtree has live sessions or in-flight transactions. No branch
    /// has been deleted yet — the cascade is atomic at the validation
    /// step. Resolve the blockers and retry.
    #[error("Fork subtree cannot be dropped: {blockers:?}")]
    ForkSubtreeInUse { blockers: Vec<String> },

    /// `Session::fork(name)` refused because the configured `max_forks`
    /// budget is at capacity. Drop existing forks (or wait for the
    /// sweeper to reap expired ones) and retry. Counts include Active,
    /// Pending, and Tombstoned entries.
    #[error("Fork budget exceeded: {current}/{max} forks; drop one or raise UniConfig::max_forks")]
    ForkBudgetExceeded { current: usize, max: usize },

    /// 2PC step on a fork lifecycle operation failed.
    ///
    /// `stage` names the step (`registry_pending`, `create_branch`,
    /// `registry_active`, `tombstone`, `delete_branch`, `registry_clear`,
    /// `backend_unsupported`, `recovery`) so recovery and humans can
    /// triage without parsing prose.
    #[error("Fork '{name}' lifecycle failed at stage '{stage}': {source}")]
    ForkLifecycle {
        name: String,
        stage: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl UniError {
    /// Returns `true` when retrying the failed operation from scratch may succeed.
    ///
    /// Distinguishes transient contention failures — optimistic-concurrency
    /// aborts and lock/commit timeouts, which a fresh transaction can win — from
    /// deterministic failures (bad query, schema or type violation) that would
    /// fail identically on retry. This is the signal
    /// [`Session::transact_with_retry`](../../../uni_db/api/session/struct.Session.html)
    /// uses to decide whether to re-run a transaction closure.
    ///
    /// `TransactionExpired` is deliberately *not* retriable here: a fresh
    /// transaction gets a new deadline, but the helper treats deadline expiry as
    /// a caller-set budget, not a contention signal. A plain `Timeout` is
    /// likewise *not* retriable — re-running the same slow operation would just
    /// time out again; only `CommitTimeout` (lock contention at the commit point)
    /// and `LockTimeout` (a contended `FOR UPDATE` row lock / deadlock) signal
    /// retriable contention.
    ///
    /// # Examples
    /// ```
    /// use uni_common::UniError;
    ///
    /// assert!(UniError::SerializationConflict { message: "lost update".into() }.is_retriable());
    /// assert!(!UniError::Schema { message: "no such label".into() }.is_retriable());
    /// ```
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            UniError::SerializationConflict { .. }
                | UniError::ConstraintConflict { .. }
                | UniError::TransactionConflict { .. }
                | UniError::CommitTimeout { .. }
                | UniError::LockTimeout { .. }
        )
    }
}

pub type Result<T> = std::result::Result<T, UniError>;

/// Why a Locy evaluation stopped before reaching its least fixed point.
///
/// A wall-clock timeout and a non-convergence failure are both *incomplete*
/// outcomes, but they call for different remedies (raise the timeout / fix a
/// slow rule vs. raise `max_iterations` / fix a non-monotone rule), so they are
/// reported distinctly rather than collapsed into one flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocyIncompleteReason {
    /// The wall-clock `timeout` budget was exhausted mid-evaluation.
    Timeout,
    /// A recursive stratum hit `max_iterations` without converging.
    IterationLimit,
}

impl LocyIncompleteReason {
    /// Returns a stable machine-readable tag (`"timeout"` / `"iteration_limit"`).
    ///
    /// Used as the discriminator surfaced to non-Rust callers (e.g. the Python
    /// bindings), where matching on a Rust enum is not available.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LocyIncompleteReason::Timeout => "timeout",
            LocyIncompleteReason::IterationLimit => "iteration_limit",
        }
    }
}

impl std::fmt::Display for LocyIncompleteReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Diagnostics describing a Locy evaluation that stopped before completing.
///
/// Returned (boxed) inside [`UniError::LocyIncomplete`] when a program exceeds
/// its time or iteration budget, and also attached to a `LocyResult` when the
/// caller opts into partial results. The rule lists exist so a caller can tell
/// "not evaluated" apart from "genuinely empty": any rule named in
/// `incomplete_rules` or `skipped_rules` may be missing facts purely because
/// evaluation was cut short, so a zero-row count for it is not authoritative.
///
/// # Examples
/// ```
/// use uni_common::{LocyIncomplete, LocyIncompleteReason};
///
/// let detail = LocyIncomplete {
///     reason: LocyIncompleteReason::Timeout,
///     elapsed_ms: 305_000,
///     limit_ms: 300_000,
///     max_iterations: 1000,
///     completed_strata: 2,
///     total_strata: 4,
///     incomplete_rules: vec!["upstream_reaches".into()],
///     skipped_rules: vec!["healthy_assets".into()],
///     complement_rules_affected: vec!["healthy_assets".into()],
/// };
/// assert!(detail.to_string().contains("timeout"));
/// assert!(detail.to_string().contains("UNSOUND"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocyIncomplete {
    /// Why evaluation stopped.
    pub reason: LocyIncompleteReason,
    /// Wall-clock time elapsed when evaluation was cut short, in milliseconds.
    pub elapsed_ms: u64,
    /// The configured wall-clock `timeout`, in milliseconds.
    pub limit_ms: u64,
    /// The configured `max_iterations` cap for recursive strata.
    pub max_iterations: usize,
    /// Number of strata fully evaluated before the cutoff.
    pub completed_strata: usize,
    /// Total number of strata in the program.
    pub total_strata: usize,
    /// Rules in the stratum that was interrupted mid-evaluation. Their facts may
    /// be a partial fixpoint rather than the least fixed point.
    pub incomplete_rules: Vec<String>,
    /// Rules in strata that were never reached. They derived no facts solely
    /// because evaluation stopped first, not because their result is empty.
    pub skipped_rules: Vec<String>,
    /// Subset of the incomplete/skipped rules that use an `IS NOT` complement.
    /// Stratified negation over a partial relation is unsound, so these results
    /// must not be trusted at all — surfaced separately for emphasis.
    pub complement_rules_affected: Vec<String>,
}

impl std::fmt::Display for LocyIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{reason} after {elapsed_ms}ms (limit {limit_ms}ms, max_iterations {max_iters}); \
             evaluated {done}/{total} strata, {n_incomplete} rule(s) incomplete, \
             {n_skipped} rule(s) skipped",
            reason = self.reason,
            elapsed_ms = self.elapsed_ms,
            limit_ms = self.limit_ms,
            max_iters = self.max_iterations,
            done = self.completed_strata,
            total = self.total_strata,
            n_incomplete = self.incomplete_rules.len(),
            n_skipped = self.skipped_rules.len(),
        )?;
        if !self.complement_rules_affected.is_empty() {
            write!(
                f,
                "; UNSOUND complement rule(s) affected: {:?}",
                self.complement_rules_affected
            )?;
        }
        Ok(())
    }
}

/// Why a GraphCompute invocation stopped before producing a complete result.
///
/// The three outcomes call for different remedies — raise the deadline, raise
/// the iteration cap, or raise the native-work budget — so they are reported
/// distinctly rather than collapsed into a single "incomplete" flag
/// (GraphCompute proposal §5.2, error codes `0x865`–`0x867`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum GraphComputeIncompleteReason {
    /// The native-work budget (a multiple of `|E|` plus an absolute ceiling)
    /// was drained by kernel work before the algorithm finished. Maps to `0x865`.
    Exhausted,
    /// A convergence loop reached its superstep/iteration cap without settling.
    /// Maps to `0x866`.
    IterationLimit,
    /// The wall-clock deadline elapsed mid-invocation. Maps to `0x867`.
    Timeout,
}

impl GraphComputeIncompleteReason {
    /// Returns a stable machine-readable tag for non-Rust callers.
    ///
    /// One of `"exhausted"`, `"iteration_limit"`, or `"timeout"` — surfaced to
    /// callers (e.g. the Python bindings) that cannot match on a Rust enum.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            GraphComputeIncompleteReason::Exhausted => "exhausted",
            GraphComputeIncompleteReason::IterationLimit => "iteration_limit",
            GraphComputeIncompleteReason::Timeout => "timeout",
        }
    }

    /// Returns the GraphCompute error code (`0x865`–`0x867`) for this reason.
    ///
    /// Lets the loader shims map an incomplete outcome onto the pinned error
    /// block (proposal §12) without re-deriving the mapping per loader.
    #[must_use]
    pub fn error_code(self) -> u32 {
        match self {
            GraphComputeIncompleteReason::Exhausted => 0x865,
            GraphComputeIncompleteReason::IterationLimit => 0x866,
            GraphComputeIncompleteReason::Timeout => 0x867,
        }
    }

    /// Maps a GraphCompute error code (`0x865`–`0x867`) back to its reason.
    ///
    /// The inverse of [`error_code`](Self::error_code): the provider boundary
    /// receives a typed `FnError` carrying one of these codes and reconstructs
    /// the reason for the user-visible [`UniError::GraphComputeIncomplete`].
    /// Any other code is not an incomplete outcome and yields `None`.
    #[must_use]
    pub fn from_error_code(code: u32) -> Option<Self> {
        match code {
            0x865 => Some(GraphComputeIncompleteReason::Exhausted),
            0x866 => Some(GraphComputeIncompleteReason::IterationLimit),
            0x867 => Some(GraphComputeIncompleteReason::Timeout),
            _ => None,
        }
    }
}

impl std::fmt::Display for GraphComputeIncompleteReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Diagnostics describing a GraphCompute invocation that stopped early.
///
/// Returned (boxed) inside [`UniError::GraphComputeIncomplete`] when an
/// invocation exceeds its native-work budget, iteration cap, or wall-clock
/// deadline. The counters let a caller tell "too slow" apart from "did not
/// converge" apart from "did too much work", and size a retry accordingly.
///
/// # Examples
/// ```
/// use uni_common::{GraphComputeIncomplete, GraphComputeIncompleteReason};
///
/// let detail = GraphComputeIncomplete {
///     reason: GraphComputeIncompleteReason::Exhausted,
///     algorithm: "guest.ppr".into(),
///     elapsed_ms: 120,
///     iterations: 7,
///     work_charged: 1_000_000_000,
///     work_budget: 1_000_000_000,
/// };
/// assert!(detail.to_string().contains("exhausted"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GraphComputeIncomplete {
    /// Why the invocation stopped.
    pub reason: GraphComputeIncompleteReason,
    /// Qualified name of the algorithm being run, for the diagnostic message.
    pub algorithm: String,
    /// Wall-clock time elapsed when the invocation was cut short, in milliseconds.
    pub elapsed_ms: u64,
    /// Number of guest control-loop iterations completed before the cutoff.
    pub iterations: u64,
    /// Native work units charged when the invocation stopped (`0` if untracked).
    pub work_charged: u64,
    /// The configured native-work budget in the same units as `work_charged`.
    pub work_budget: u64,
}

impl std::fmt::Display for GraphComputeIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{reason} in algorithm `{algo}` after {elapsed_ms}ms, {iters} iteration(s); \
             charged {charged}/{budget} native-work units",
            reason = self.reason,
            algo = self.algorithm,
            elapsed_ms = self.elapsed_ms,
            iters = self.iterations,
            charged = self.work_charged,
            budget = self.work_budget,
        )
    }
}

/// Stable marker prefixing a serialized [`GraphComputeIncomplete`] in an error
/// message, so the structured reason survives the DataFusion error channel.
///
/// The GraphCompute `CALL` runs through generic DataFusion machinery whose only
/// egress is a stringified error (there is no bespoke execution node to carry an
/// out-of-band slot, as Locy uses for `LocyIncomplete`). The provider serializes
/// the diagnostics behind this tag; the query API boundary
/// ([`GraphComputeIncomplete::from_tagged_message`]) recovers them into the typed
/// [`UniError::GraphComputeIncomplete`]. This mirrors the codebase's existing
/// message-classification boundary (e.g. `TypeError:`, `ConstraintVerificationFailed:`).
pub const GRAPH_COMPUTE_INCOMPLETE_TAG: &str = "GraphComputeIncomplete:";

impl GraphComputeIncomplete {
    /// Serializes this diagnostic behind [`GRAPH_COMPUTE_INCOMPLETE_TAG`].
    ///
    /// The provider boundary returns the result as a `DataFusionError` message;
    /// the query API recovers the struct with
    /// [`from_tagged_message`](Self::from_tagged_message). Serialization cannot
    /// fail for this plain-data struct, so a marshalling error degrades to the
    /// bare tag rather than panicking.
    #[must_use]
    pub fn to_tagged_message(&self) -> String {
        let json = serde_json::to_string(self).unwrap_or_default();
        format!("{GRAPH_COMPUTE_INCOMPLETE_TAG}{json}")
    }

    /// Recovers a [`GraphComputeIncomplete`] from a tagged error message.
    ///
    /// Locates [`GRAPH_COMPUTE_INCOMPLETE_TAG`] anywhere in `msg` (upstream
    /// layers prepend context such as `Algorithm 'x': `) and deserializes the
    /// single JSON object that follows, ignoring any trailing text a later
    /// error-wrapping layer may append. Returns `None` when the tag is absent or
    /// the payload does not parse.
    #[must_use]
    pub fn from_tagged_message(msg: &str) -> Option<Self> {
        let start = msg.find(GRAPH_COMPUTE_INCOMPLETE_TAG)? + GRAPH_COMPUTE_INCOMPLETE_TAG.len();
        let payload = &msg[start..];
        // A streaming deserializer reads exactly one JSON value and stops, so any
        // suffix appended by an outer error wrapper is harmless.
        let mut stream =
            serde_json::Deserializer::from_str(payload).into_iter::<GraphComputeIncomplete>();
        stream.next()?.ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_compute_incomplete_tag_round_trips() {
        let original = GraphComputeIncomplete {
            reason: GraphComputeIncompleteReason::IterationLimit,
            // A qname with dots/quotes must survive the round trip intact.
            algorithm: "guest.author's.ppr".into(),
            elapsed_ms: 42,
            iterations: 200,
            work_charged: 1_234,
            work_budget: 5_678,
        };
        let tagged = original.to_tagged_message();
        assert!(tagged.starts_with(GRAPH_COMPUTE_INCOMPLETE_TAG));

        // Recovered verbatim even when an outer layer wraps the message with a
        // prefix and a trailing suffix (as the DataFusion channel does).
        let wrapped = format!("Algorithm 'x': {tagged}\ncaused by: stream closed");
        let recovered =
            GraphComputeIncomplete::from_tagged_message(&wrapped).expect("tag must be recovered");
        assert_eq!(recovered, original);
    }

    #[test]
    fn graph_compute_incomplete_reason_code_round_trips() {
        for reason in [
            GraphComputeIncompleteReason::Exhausted,
            GraphComputeIncompleteReason::IterationLimit,
            GraphComputeIncompleteReason::Timeout,
        ] {
            assert_eq!(
                GraphComputeIncompleteReason::from_error_code(reason.error_code()),
                Some(reason)
            );
        }
        // Codes outside the 0x865-0x867 incomplete block are not incomplete.
        assert_eq!(GraphComputeIncompleteReason::from_error_code(0x860), None);
    }

    #[test]
    fn untagged_message_yields_no_incomplete() {
        assert!(GraphComputeIncomplete::from_tagged_message("Execution error: boom").is_none());
    }

    #[test]
    fn retriable_errors_are_contention_failures() {
        let s = String::new;
        let retriable = [
            UniError::SerializationConflict { message: s() },
            UniError::ConstraintConflict { message: s() },
            UniError::TransactionConflict { message: s() },
            UniError::CommitTimeout {
                tx_id: s(),
                hint: "",
            },
            // A contended FOR UPDATE row lock / deadlock clears when the holder
            // releases; a fresh transaction can retry and win it.
            UniError::LockTimeout { timeout_ms: 10_000 },
        ];
        for e in &retriable {
            assert!(e.is_retriable(), "{e:?} should be retriable");
        }
    }

    #[test]
    fn deterministic_errors_are_not_retriable() {
        let s = String::new;
        let terminal = [
            UniError::Parse {
                message: s(),
                position: None,
                line: None,
                column: None,
                context: None,
            },
            UniError::Query {
                message: s(),
                query: None,
            },
            UniError::Schema { message: s() },
            UniError::Constraint { message: s() },
            UniError::InvalidArgument {
                arg: s(),
                message: s(),
            },
            // A caller-set deadline is not a contention signal.
            UniError::TransactionExpired {
                tx_id: s(),
                hint: "",
            },
            // Re-running the same slow operation would just time out again.
            UniError::Timeout { timeout_ms: 1 },
        ];
        for e in &terminal {
            assert!(!e.is_retriable(), "{e:?} should not be retriable");
        }
    }
}
