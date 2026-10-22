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

    /// Snapshot not found.
    ///
    /// The display carries a tag and the JSON-encoded id, so the typed error
    /// survives the executor's string boundary (see
    /// [`UniError::snapshot_not_found_from_message`]).
    #[error(
        "{SNAPSHOT_NOT_FOUND_TAG}{}: Snapshot '{snapshot_id}' not found",
        serde_json::to_string(snapshot_id).unwrap_or_default()
    )]
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

    /// A `=~` match needed more backtracking steps than the limit allows.
    ///
    /// Patterns with look-around, backreferences, atomic groups, possessive
    /// quantifiers and a few other constructs (`\b`, `\X`, `\b{g}`, `(?c)`)
    /// run on a backtracking matcher, which can take exponential
    /// time on some inputs. It stops after `limit` steps
    /// (`UniConfig::regex_backtrack_limit`) instead of running away; Java has
    /// no such limit, so the query fails where Neo4j would keep searching.
    #[error(
        "Regex backtrack limit of {limit} steps exceeded by pattern '{pattern}'; \
         simplify the pattern or raise UniConfig::regex_backtrack_limit"
    )]
    RegexBacktrackLimit { pattern: String, limit: usize },

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

    /// A bulk load is **committed** but `commit` could not finish publishing
    /// it in this process.
    ///
    /// The load passed its commit point (its durable publish marker), so it
    /// is decided: a background task retries the publish, with backoff, until
    /// it completes or the database shuts down, and otherwise the next open
    /// completes it, exactly once. Until then none of it is visible to readers
    /// of this database handle. **Do not retry the load** -- a retry would
    /// apply it twice. `load_id` is the load's id (`BulkWriter::load_id`);
    /// `Uni::bulk_load_applied` (or `Session::bulk_load_applied` on a fork)
    /// answers `true` for it.
    #[error(
        "Bulk load '{load_id}' is committed but its publish could not be finished by the \
         commit ({message}); it is completed in the background, or on the next open -- do \
         NOT retry it"
    )]
    BulkCommitIncomplete {
        /// The committed load's id.
        load_id: String,
        /// Why the in-process publish could not be finished.
        message: String,
    },

    /// Whether a bulk load reached its commit point is unknown: writing its
    /// publish marker failed in a way that may still have written it, and
    /// reading it back did not settle the question.
    ///
    /// The staged rows are kept. If the marker exists the next open completes
    /// the load; otherwise nothing of it is ever visible. Reopen the database,
    /// then check `Uni::bulk_load_applied(load_id)` before deciding to retry.
    #[error(
        "Bulk load '{load_id}': whether it committed is unknown ({message}); reopen the \
         database and check bulk_load_applied before retrying"
    )]
    BulkCommitOutcomeUnknown {
        /// The load's id.
        load_id: String,
        /// The marker write failure, and why reading it back did not decide.
        message: String,
    },

    /// The storage backend cannot stage a bulk load's rows invisibly until
    /// commit, so it cannot run one.
    ///
    /// A bulk load's isolation, abort and crash recovery all rest on staging
    /// (`StorageBackend::stages_invisibly`): without it every write would be
    /// visible as it lands, and the only undo would roll each table back by
    /// version, discarding whatever other writers committed to it meanwhile.
    /// Refused when the writer is built, before anything is written. Every
    /// Lance-backed database, primary or fork, stages.
    #[error("Bulk loading is not supported here: {reason}")]
    BulkLoadUnsupported {
        /// Why the backend cannot run a bulk load.
        reason: String,
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
    /// `backend_unsupported`, `recovery`, `restore_race`) so recovery and
    /// humans can triage without parsing prose. `restore_race` means a
    /// snapshot restore ran while the fork was being created; the create was
    /// rolled back and can be retried.
    #[error("Fork '{name}' lifecycle failed at stage '{stage}': {source}")]
    ForkLifecycle {
        name: String,
        stage: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A statement references parameters that the caller did not supply.
    ///
    /// Raised for reads and writes alike, before any row is written, so a
    /// missing `$x` can no longer be stored as `NULL`. `names` lists the
    /// parameters by name, without the `$`. openCypher's `ParameterMissing`;
    /// Neo4j's `Neo.ClientError.Statement.ParameterMissing`.
    #[error("{PARAMETER_MISSING_TAG}{}", names.join(", "))]
    ParameterMissing { names: Vec<String> },

    /// The operation cannot run in a transaction that holds uncommitted writes.
    ///
    /// Raised by operations whose effect is not transactional -- such as
    /// `uni.graph.project`, whose projection catalog is shared by every session
    /// and is not undone by a rollback -- and which would otherwise publish the
    /// transaction's uncommitted writes. Commit or roll back first, or run the
    /// operation outside the transaction.
    #[error(
        "'{operation}' cannot run in a transaction with uncommitted writes: {reason}; \
         commit or roll back first"
    )]
    UncommittedWritesInTransaction { operation: String, reason: String },

    /// Writes are stopped: the uncompacted flush generations reached the
    /// write throttle's hard limit while background compaction is disabled,
    /// so nothing would ever lower the count and a wait could only end in a
    /// timeout. Run `CALL uni.admin.compact()` (or `Uni::compact`) to resume
    /// writes, or enable compaction. Not retriable as is.
    #[error(
        "{WRITE_STALLED_TAG}l1_runs={l1_runs} hard_limit={hard_limit}: writes are stopped \
         because {l1_runs} uncompacted flush generations reached the hard limit of \
         {hard_limit} and background compaction is disabled; run CALL uni.admin.compact() \
         to resume writes"
    )]
    WriteStalled { l1_runs: usize, hard_limit: usize },

    /// A relationship's endpoint does not carry a label its declared edge type
    /// allows.
    ///
    /// An edge type declared with source or target labels accepts an endpoint
    /// only when the endpoint carries at least one of them. Raised when an
    /// edge is written (CREATE, MERGE, bulk insert, COPY FROM, fork promote)
    /// and when a `REMOVE` of a label would leave an existing edge violating
    /// its declaration. `endpoint` is `"source"` or `"target"`; `labels` are
    /// the endpoint's labels, `allowed` the declared ones.
    #[error(
        "{EDGE_ENDPOINT_TAG}{edge_type}|{endpoint}|{}|{}: edge type '{edge_type}' requires \
         its {endpoint} to carry one of [{}], but it carries [{}]",
        labels.join(","), allowed.join(","), allowed.join(", "), labels.join(", ")
    )]
    EdgeEndpointViolation {
        edge_type: String,
        endpoint: String,
        labels: Vec<String>,
        allowed: Vec<String>,
    },

    /// A property write (`SET` / `REMOVE`) on a relationship named endpoints
    /// or a type other than the stored ones.
    ///
    /// A relationship's endpoints and type are fixed when it is created; a
    /// property write rewrites its row but cannot move it. Raised when the
    /// relationship value a `SET` / `REMOVE` acts on -- a map carrying `_eid`,
    /// `_src`, `_dst` and `_type`, typically -- disagrees with the stored edge
    /// `eid`. `stored` and `requested` read `(src)-[:TYPE]->(dst)`, with vertex
    /// ids and the type name (`#<id>` when the schema cannot name it).
    #[error(
        "{EDGE_IDENTITY_TAG}{eid}|{stored}|{requested}: relationship {eid} is {stored}; a \
         property write cannot change its endpoints or type to {requested}"
    )]
    EdgeIdentityChange {
        eid: u64,
        stored: String,
        requested: String,
    },

    /// `restore_snapshot` refused because the database has forks.
    ///
    /// A restore rewinds the primary tables and the UID index that primary
    /// and every fork share, but not the forks' branches: a fork would keep
    /// data cut from the pre-restore primary and lose the UID entries it
    /// registered after the snapshot. So a restore runs only once every fork
    /// is fully dropped. `forks` names each fork the registry still holds,
    /// whatever its state (active, being created, or being dropped), sorted.
    /// Drop them with `drop_fork` (or `drop_fork_cascade` for a fork with
    /// children) and retry.
    #[error(
        "{RESTORE_BLOCKED_BY_FORKS_TAG}{}: restore_snapshot refused: the database has \
         forks [{}], and a restore does not rewind fork branches; drop them first \
         (drop_fork, or drop_fork_cascade for a fork with children)",
        serde_json::to_string(forks).unwrap_or_default(), forks.join(", ")
    )]
    RestoreBlockedByForks { forks: Vec<String> },
}

/// Display prefix of [`UniError::RestoreBlockedByForks`] (see
/// [`UniError::restore_blocked_by_forks_from_message`]).
pub const RESTORE_BLOCKED_BY_FORKS_TAG: &str = "RestoreBlockedByForks: ";

/// Display prefix of [`UniError::SnapshotNotFound`] (see
/// [`UniError::snapshot_not_found_from_message`]).
pub const SNAPSHOT_NOT_FOUND_TAG: &str = "SnapshotNotFound: ";

/// Display prefix of [`UniError::WriteStalled`], so the typed error survives
/// the string boundary between the executor and the API layer (see
/// [`UniError::write_stalled_from_message`]).
pub const WRITE_STALLED_TAG: &str = "WriteStalled: ";

/// Display prefix of [`UniError::EdgeEndpointViolation`] (see
/// [`UniError::edge_endpoint_from_message`]).
pub const EDGE_ENDPOINT_TAG: &str = "EdgeEndpointViolation: ";

/// Display prefix of [`UniError::EdgeIdentityChange`] (see
/// [`UniError::edge_identity_from_message`]).
pub const EDGE_IDENTITY_TAG: &str = "EdgeIdentityChange: ";

/// Display prefix of [`UniError::ParameterMissing`], also used verbatim by the
/// query layers so the typed error survives their string boundary (see
/// [`UniError::parameter_missing_from_message`]).
pub const PARAMETER_MISSING_TAG: &str =
    "ParameterMissing: MissingParameter - Expected parameter(s): ";

/// Tag carried in the message of an [`UniError::UncommittedWritesInTransaction`]
/// refusal raised below the API layer.
pub const UNCOMMITTED_WRITES_TAG: &str = "UncommittedWritesInTransaction:";

/// Tag carried in the reason of a reserved-property-name rejection, so the
/// typed error survives the string boundary between the executor and the API
/// layer (see [`UniError::reserved_property_from_message`]).
pub const RESERVED_PROPERTY_TAG: &str = "ReservedPropertyName";

impl UniError {
    /// The rejection of a write whose property is named after a storage column.
    #[must_use]
    pub fn reserved_property_name(name: &str) -> Self {
        Self::InvalidIdentifier {
            name: name.to_string(),
            reason: format!(
                "{RESERVED_PROPERTY_TAG}: the property name is reserved by the storage layer; \
                 please choose a different name"
            ),
        }
    }

    /// Recovers a [`Self::reserved_property_name`] rejection from its display
    /// text, wherever it sits in `msg` (upstream layers may prefix it).
    #[must_use]
    pub fn reserved_property_from_message(msg: &str) -> Option<Self> {
        const PREFIX: &str = "Invalid identifier '";
        let tail = &msg[msg.find(PREFIX)? + PREFIX.len()..];
        let (name, rest) = tail.split_once("': ")?;
        rest.starts_with(RESERVED_PROPERTY_TAG)
            .then(|| Self::reserved_property_name(name))
    }

    /// The message the query layers raise for missing parameters.
    ///
    /// Parsed back into [`Self::ParameterMissing`] by
    /// [`Self::parameter_missing_from_message`].
    ///
    /// # Examples
    /// ```
    /// use uni_common::UniError;
    ///
    /// let msg = UniError::parameter_missing_message(["x", "y"]);
    /// assert!(matches!(
    ///     UniError::parameter_missing_from_message(&msg),
    ///     Some(UniError::ParameterMissing { names }) if names == ["x", "y"]
    /// ));
    /// ```
    #[must_use]
    pub fn parameter_missing_message<I, S>(names: I) -> String
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let names: Vec<String> = names.into_iter().map(|n| n.as_ref().to_string()).collect();
        format!("{PARAMETER_MISSING_TAG}{}", names.join(", "))
    }

    /// Recovers [`Self::ParameterMissing`] from a message produced by
    /// [`Self::parameter_missing_message`], wherever it sits in `msg` (upstream
    /// layers may prefix it, or append context after a `:`).
    #[must_use]
    pub fn parameter_missing_from_message(msg: &str) -> Option<Self> {
        let tail = &msg[msg.find(PARAMETER_MISSING_TAG)? + PARAMETER_MISSING_TAG.len()..];
        let names: Vec<String> = tail
            .split(", ")
            .map_while(|part| {
                let name: String = part
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                (!name.is_empty()).then_some(name)
            })
            .collect();
        (!names.is_empty()).then_some(Self::ParameterMissing { names })
    }

    /// The tagged message an operation below the API layer raises when it
    /// refuses to run over uncommitted transaction writes.
    #[must_use]
    pub fn uncommitted_writes_message(operation: &str, reason: &str) -> String {
        format!("{UNCOMMITTED_WRITES_TAG}{operation}|{reason}")
    }

    /// Recovers [`Self::UncommittedWritesInTransaction`] from a message built by
    /// [`Self::uncommitted_writes_message`], wherever it sits in `msg`.
    #[must_use]
    pub fn uncommitted_writes_from_message(msg: &str) -> Option<Self> {
        let tail = &msg[msg.find(UNCOMMITTED_WRITES_TAG)? + UNCOMMITTED_WRITES_TAG.len()..];
        let (operation, reason) = tail.split_once('|')?;
        Some(Self::UncommittedWritesInTransaction {
            operation: operation.to_string(),
            reason: reason.to_string(),
        })
    }

    /// Recovers [`Self::WriteStalled`] from its display text, wherever it sits
    /// in `msg` (upstream layers may prefix it).
    #[must_use]
    pub fn write_stalled_from_message(msg: &str) -> Option<Self> {
        let tail = &msg[msg.find(WRITE_STALLED_TAG)? + WRITE_STALLED_TAG.len()..];
        let tail = tail.strip_prefix("l1_runs=")?;
        let (runs, tail) = tail.split_once(' ')?;
        let tail = tail.strip_prefix("hard_limit=")?;
        let (hard, _) = tail.split_once(':')?;
        Some(Self::WriteStalled {
            l1_runs: runs.parse().ok()?,
            hard_limit: hard.parse().ok()?,
        })
    }

    /// Recovers [`Self::SnapshotNotFound`] from its display text, wherever it
    /// sits in `msg`.
    ///
    /// # Examples
    /// ```
    /// use uni_common::UniError;
    ///
    /// let err = UniError::SnapshotNotFound { snapshot_id: "it's: 1".into() };
    /// let back = UniError::snapshot_not_found_from_message(&format!("ctx: {err}"));
    /// assert!(matches!(
    ///     back,
    ///     Some(UniError::SnapshotNotFound { snapshot_id }) if snapshot_id == "it's: 1"
    /// ));
    /// ```
    #[must_use]
    pub fn snapshot_not_found_from_message(msg: &str) -> Option<Self> {
        let tail = &msg[msg.find(SNAPSHOT_NOT_FOUND_TAG)? + SNAPSHOT_NOT_FOUND_TAG.len()..];
        // The id is a JSON string: it may hold `'` or `: `.
        let snapshot_id: String = serde_json::Deserializer::from_str(tail)
            .into_iter::<String>()
            .next()?
            .ok()?;
        Some(Self::SnapshotNotFound { snapshot_id })
    }

    /// Recovers [`Self::EdgeEndpointViolation`] from its display text,
    /// wherever it sits in `msg`.
    #[must_use]
    pub fn edge_endpoint_from_message(msg: &str) -> Option<Self> {
        let tail = &msg[msg.find(EDGE_ENDPOINT_TAG)? + EDGE_ENDPOINT_TAG.len()..];
        let (head, _) = tail.split_once(": ")?;
        let mut parts = head.split('|');
        let edge_type = parts.next()?.to_string();
        let endpoint = parts.next()?.to_string();
        let split = |s: &str| -> Vec<String> {
            s.split(',')
                .filter(|l| !l.is_empty())
                .map(str::to_string)
                .collect()
        };
        let labels = split(parts.next()?);
        let allowed = split(parts.next()?);
        Some(Self::EdgeEndpointViolation {
            edge_type,
            endpoint,
            labels,
            allowed,
        })
    }

    /// Recovers [`Self::EdgeIdentityChange`] from its display text,
    /// wherever it sits in `msg`.
    ///
    /// # Examples
    /// ```
    /// use uni_common::UniError;
    ///
    /// let err = UniError::EdgeIdentityChange {
    ///     eid: 7,
    ///     stored: "(1)-[:R]->(2)".into(),
    ///     requested: "(3)-[:R]->(2)".into(),
    /// };
    /// let back = UniError::edge_identity_from_message(&format!("ctx: {err}"));
    /// assert!(matches!(back, Some(UniError::EdgeIdentityChange { eid: 7, .. })));
    /// ```
    #[must_use]
    pub fn edge_identity_from_message(msg: &str) -> Option<Self> {
        let tail = &msg[msg.find(EDGE_IDENTITY_TAG)? + EDGE_IDENTITY_TAG.len()..];
        let (head, _) = tail.split_once(": ")?;
        let mut parts = head.splitn(3, '|');
        let eid = parts.next()?.parse().ok()?;
        let stored = parts.next()?.to_string();
        let requested = parts.next()?.to_string();
        Some(Self::EdgeIdentityChange {
            eid,
            stored,
            requested,
        })
    }

    /// Recovers [`Self::RestoreBlockedByForks`] from its display text,
    /// wherever it sits in `msg`.
    ///
    /// # Examples
    /// ```
    /// use uni_common::UniError;
    ///
    /// let err = UniError::RestoreBlockedByForks {
    ///     forks: vec!["a, b: c".into(), "d".into()],
    /// };
    /// let back = UniError::restore_blocked_by_forks_from_message(&format!("ctx: {err}"));
    /// assert!(matches!(
    ///     back,
    ///     Some(UniError::RestoreBlockedByForks { forks }) if forks == ["a, b: c", "d"]
    /// ));
    /// ```
    #[must_use]
    pub fn restore_blocked_by_forks_from_message(msg: &str) -> Option<Self> {
        let tail =
            &msg[msg.find(RESTORE_BLOCKED_BY_FORKS_TAG)? + RESTORE_BLOCKED_BY_FORKS_TAG.len()..];
        // The names are a JSON array: a fork name may hold `,` or `: `.
        let forks: Vec<String> = serde_json::Deserializer::from_str(tail)
            .into_iter::<Vec<String>>()
            .next()?
            .ok()?;
        (!forks.is_empty()).then_some(Self::RestoreBlockedByForks { forks })
    }

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
    fn write_stalled_round_trips_through_its_message() {
        let e = UniError::WriteStalled {
            l1_runs: 32,
            hard_limit: 32,
        };
        let msg = format!("Query error: CREATE failed: {e}");
        assert!(msg.contains("uni.admin.compact()"));
        assert!(matches!(
            UniError::write_stalled_from_message(&msg),
            Some(UniError::WriteStalled {
                l1_runs: 32,
                hard_limit: 32
            })
        ));
        assert!(UniError::write_stalled_from_message("unrelated").is_none());
    }

    #[test]
    fn edge_endpoint_violation_round_trips_through_its_message() {
        let e = UniError::EdgeEndpointViolation {
            edge_type: "R".into(),
            endpoint: "target".into(),
            labels: vec!["A".into(), "C".into()],
            allowed: vec!["B".into()],
        };
        let msg = format!("Execution error: CREATE failed: {e}");
        match UniError::edge_endpoint_from_message(&msg) {
            Some(UniError::EdgeEndpointViolation {
                edge_type,
                endpoint,
                labels,
                allowed,
            }) => {
                assert_eq!(edge_type, "R");
                assert_eq!(endpoint, "target");
                assert_eq!(labels, vec!["A".to_string(), "C".to_string()]);
                assert_eq!(allowed, vec!["B".to_string()]);
            }
            other => panic!("{other:?}"),
        }
        let unlabeled = UniError::EdgeEndpointViolation {
            edge_type: "R".into(),
            endpoint: "source".into(),
            labels: vec![],
            allowed: vec!["A".into()],
        };
        match UniError::edge_endpoint_from_message(&unlabeled.to_string()) {
            Some(UniError::EdgeEndpointViolation { labels, .. }) => assert!(labels.is_empty()),
            other => panic!("{other:?}"),
        }
    }

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
