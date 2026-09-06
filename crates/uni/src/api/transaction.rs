// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Transaction — the explicit write scope.
//!
//! Transactions provide ACID guarantees for multi-statement writes.
//! Changes are isolated until commit.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use metrics;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, warn};
use uuid::Uuid;

use crate::api::UniInner;
use crate::api::impl_locy::{self, LocyRuleRegistry};
use crate::api::session::Session;
use crate::api::triggers::{MutationEvents, TriggerRouter, tx_id_to_u64};
use uni_common::{Result, UniError};
use uni_locy::DerivedFactSet;
use uni_plugin::traits::trigger::TriggerContext;

use crate::api::locy_result::LocyResult;
use uni_query::{ExecuteResult, ProfileOutput, QueryCursor, QueryResult, Row, Value};

/// Snapshot of L0 mutation state, used for before/after comparison in execute operations.
struct L0Snapshot {
    mutation_count: usize,
    mutation_stats: uni_store::runtime::l0::MutationStats,
}

/// Await `fut`, bounded by `timeout` when the builder set one.
///
/// A `None` timeout awaits unbounded; an elapsed deadline surfaces as
/// [`UniError::Timeout`] rather than a `tokio` `Elapsed`.
async fn with_optional_timeout<T>(
    timeout: Option<Duration>,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    let Some(t) = timeout else {
        return fut.await;
    };
    let started = std::time::Instant::now();
    let out = tokio::time::timeout(t, fut)
        .await
        .map_err(|_| UniError::Timeout {
            timeout_ms: t.as_millis() as u64,
        })??;
    // `tokio::time::timeout` polls the inner future before arming its timer, so
    // a statement that finishes inside a single poll returns `Ok` no matter how
    // small the budget — the timer never gets a chance to fire. Comparing the
    // elapsed time closes that hole, which is why the session terminals carry
    // the same check; without it `.timeout()` was silently inert on every
    // transaction statement fast enough not to yield.
    if started.elapsed() > t {
        return Err(UniError::Timeout {
            timeout_ms: t.as_millis() as u64,
        });
    }
    Ok(out)
}

/// Transaction isolation level.
///
/// Uses commit-time serialization: `tx()` allocates a private L0 buffer
/// without acquiring the writer lock; the writer lock is only acquired
/// at commit time for WAL + merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum IsolationLevel {
    /// Serialized isolation with begin-time writer lock.
    #[default]
    Serialized,
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IsolationLevel::Serialized => write!(f, "Serialized"),
        }
    }
}

// `CommitResult` / `RulePromotionError` are pure value types; they live in
// `uni-plugin-host` (shared with the hooks engine) and are re-exported here so
// `uni_db::api::transaction::CommitResult` and `uni_db::CommitResult` stay
// stable.
pub use uni_plugin_host::commit_result::{CommitResult, RulePromotionError};

/// A database transaction — the explicit write scope.
///
/// Transactions provide ACID guarantees for multiple operations.
/// Changes are isolated until [`commit()`](Self::commit).
///
/// # Concurrency
///
/// Uses commit-time serialization: each transaction owns a private L0 buffer.
/// `tx()` only takes a reader lock (to snapshot the version); the writer lock
/// is acquired briefly per-mutation and once at commit for WAL + merge.
/// Multiple transactions can coexist; isolation is provided by private L0 buffers.
///
/// # Drop Behavior
///
/// If dropped without calling `commit()` or `rollback()`, the private L0 is
/// simply discarded (no writer lock needed) and a warning is logged if dirty.
pub struct Transaction {
    pub(crate) db: Arc<UniInner>,
    /// Private L0 buffer — mutations within this transaction are routed here.
    pub(crate) tx_l0: Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
    /// Per-transaction VID/EID reservoir. Bulk-reserves from the global
    /// `IdAllocator` and hands out IDs without re-locking — amortizes the
    /// allocator's `tokio::Mutex` across the tx's mutations.
    pub(crate) id_reservoir: Arc<uni_store::runtime::TxIdReservoir>,
    /// Session-level write guard (set false on complete)
    session_write_guard: Arc<std::sync::atomic::AtomicBool>,
    /// Session's rule registry (for rule promotion on commit)
    session_rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    /// Transaction-scoped rule registry
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    /// Session's metrics counters (for commit/rollback tracking)
    session_metrics: Arc<crate::api::session::SessionMetricsInner>,
    completed: bool,
    /// Rollback-only poison flag (bug #15 / Neo4j-style atomicity).
    ///
    /// Set to `true` the first time any statement execution returns an `Err`.
    /// Because statements run through `&self` entry points (`execute`, `query`,
    /// `apply`, the builders) while `completed` is only flipped by the
    /// `mut self` lifecycle methods, this needs interior mutability. An
    /// `AtomicBool` (not a `Cell`) is required: a `&Transaction` is held across
    /// `.await` points inside `Send` async fns, so the struct must stay `Sync` —
    /// `Cell` is not `Sync`. Once set, every further statement and `commit()` is
    /// rejected with [`UniError::TransactionRollbackOnly`]; only `rollback()`/drop
    /// succeed, discarding the half-applied private L0.
    rollback_only: AtomicBool,
    id: String,
    /// Session ID (for commit notifications and hooks).
    session_id: String,
    start_time: Instant,
    started_at_version: u64,
    /// Optional deadline for the transaction.
    deadline: Option<Instant>,
    /// Child cancellation token derived from the session's parent token.
    cancellation_token: CancellationToken,
    /// Hooks inherited from the session.
    hooks: Vec<Arc<dyn crate::api::hooks::SessionHook>>, // Flattened from session's HashMap
    /// Principal inherited from the session (if any). Drives the
    /// M6a.3 write/schema/dbms authz consultation in `Self::execute`.
    principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
    /// `FOR UPDATE` pessimistic lock guards, held from MATCH until the
    /// transaction ends (dropped on commit/rollback → locks released).
    ///
    /// Always present; only populated when `UniConfig::ssi_enabled` is `true`.
    for_update_guards: parking_lot::Mutex<Vec<tokio::sync::OwnedMutexGuard<()>>>,
    /// Row keys this transaction already holds a `FOR UPDATE` lock on, so a
    /// repeated match on the same key does not self-deadlock.
    ///
    /// Always present; only populated when `UniConfig::ssi_enabled` is `true`.
    for_update_held: parking_lot::Mutex<std::collections::HashSet<Vec<u8>>>,
    /// Pinned L0 snapshot for snapshot-isolated reads (Component C1). Captured at
    /// begin (after `occ_read_seq`), threaded into reads via the executor.
    /// Released (`take`n) at the start of commit/rollback — **before** the
    /// commit-time freeze check — so a transaction's own pin never makes its own
    /// commit freeze; only a *concurrent* reader's pin does. `None` after release
    /// (or on an abandoned tx, dropped via `Arc`).
    ///
    /// Behind a `Mutex` for interior mutability: a `FOR UPDATE` acquisition on a
    /// still-fresh transaction RE-PINS this to lock-acquisition time so the
    /// locked read sees the latest committed value (see `acquire_for_update_locks`).
    ///
    /// Always present; only set to `Some` when `UniConfig::ssi_enabled` is `true`.
    /// Shared so a `tx.prepare(...)` [`PreparedQuery`] can read the *live*
    /// snapshot at execute time (it is pinned lazily on first freeze, so a
    /// value captured at prepare time could be stale).
    snapshot: Arc<parking_lot::Mutex<Option<uni_store::runtime::SnapshotView>>>,
    /// Ephemeral ("scratch") transaction (G8/E2): writes go to a private L0 over
    /// a pinned read base with an in-memory id allocator, and `commit()` is
    /// refused — the writes are always discarded on drop. This is the cheap
    /// write-isolated per-rollout fork (no Lance branch / registry / WAL).
    ephemeral: bool,
}

/// Classify a Cypher payload as `"write"`, `"schema"`, or `"dbms"` for
/// the purposes of `AuthzPolicy` consultation.
///
/// This is a deliberately shallow classifier: it scans the first
/// keyword(s) of the trimmed, uppercased payload. Real parser-driven
/// classification is a follow-up — for M6a.3 the only requirement is
/// that the verb crossing the policy boundary is *meaningful* (so a
/// policy that wants to reject `CREATE INDEX` while allowing `CREATE
/// (n)` can do so).
///
/// Recognition:
/// - `CREATE INDEX` / `DROP INDEX` / `CREATE LABEL` / `CREATE
///   CONSTRAINT` / `DROP CONSTRAINT` → `"schema"`.
/// - `CREATE USER` / `DROP USER` / `GRANT` / `REVOKE` / `SHOW
///   USERS` / `ALTER USER` → `"dbms"`.
/// - Everything else routed through `Transaction::execute` → `"write"`.
fn classify_verb(cypher: &str) -> &'static str {
    let s = cypher.trim_start();
    // Take up to 32 chars and uppercase for a cheap prefix match.
    //
    // Iterate by `char`, not by byte. `s.len()` is a BYTE length, so slicing
    // `s[..s.len().min(32)]` panicked whenever byte 32 landed inside a
    // multi-byte codepoint — i.e. on any query with non-ASCII text near the
    // start, such as `CREATE (:Thing)-[:\`CONNAÎT\`]->(:Thing)`.
    let prefix: String = s.chars().take(32).flat_map(char::to_uppercase).collect();
    let p = prefix.as_str();

    if p.starts_with("CREATE INDEX")
        || p.starts_with("DROP INDEX")
        || p.starts_with("CREATE LABEL")
        || p.starts_with("DROP LABEL")
        || p.starts_with("CREATE CONSTRAINT")
        || p.starts_with("DROP CONSTRAINT")
    {
        return "schema";
    }
    if p.starts_with("CREATE USER")
        || p.starts_with("DROP USER")
        || p.starts_with("ALTER USER")
        || p.starts_with("GRANT ")
        || p.starts_with("REVOKE ")
        || p.starts_with("SHOW USERS")
        || p.starts_with("SHOW ROLES")
    {
        return "dbms";
    }
    "write"
}

impl Transaction {
    pub(crate) async fn new(session: &Session) -> Result<Self> {
        Self::new_with_options(session, None, IsolationLevel::default(), false).await
    }

    /// Begin an ephemeral ("scratch") transaction (G8/E2): write-isolated over a
    /// pinned read base with an in-memory id allocator, `commit()` refused, all
    /// writes discarded on drop.
    pub(crate) async fn new_scratch(session: &Session) -> Result<Self> {
        Self::new_with_options(session, None, IsolationLevel::default(), true).await
    }

    pub(crate) async fn new_with_options(
        session: &Session,
        timeout: Option<Duration>,
        _isolation: IsolationLevel,
        ephemeral: bool,
    ) -> Result<Self> {
        // Ensure no other write context is active on this session
        if session
            .active_write_guard()
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(UniError::WriteContextAlreadyActive {
                session_id: session.id().to_string(),
                hint: "Only one Transaction, BulkWriter, or Appender can be active per Session at a time. Commit or rollback the active one first, or create a separate Session for concurrent writes.",
            });
        }

        // Panic safety: if anything between the compare_exchange above and the
        // Transaction construction below panics, this scopeguard ensures the
        // write guard is cleared so the Session isn't permanently locked.
        // Once the Transaction is successfully constructed, we forget the guard —
        // Transaction's Drop impl takes over cleanup responsibility.
        let write_guard_cleanup = scopeguard::guard(session.active_write_guard().clone(), |g| {
            g.store(false, Ordering::SeqCst);
        });

        let db = session.db().clone();
        let writer_lock = db.writer.clone().ok_or_else(|| {
            // No need to manually clear — scopeguard handles it on early return
            UniError::ReadOnly {
                operation: "start_transaction".to_string(),
            }
        })?;

        // READ lock only — create a private L0 buffer without blocking other writers.
        // This is the key commit-time serialization change: no writer WRITE lock
        // is taken at transaction begin; it's deferred to commit().
        let (started_at_version, tx_l0, id_reservoir) = {
            let writer: &uni_store::Writer = writer_lock.as_ref();
            let l0 = writer.create_transaction_l0();
            let version = l0.read().current_version;
            let batch = db.config.tx_id_reservoir_batch;
            // A scratch tx draws ids from a throwaway in-memory allocator seeded
            // ABOVE the primary's live HWM, so its vids/eids can't collide with
            // pinned base rows and the global `id_allocator.json` is never
            // advanced or rewritten (G8/E2). A normal tx uses the global
            // allocator.
            let reservoir = if ephemeral {
                let (vid_hwm, eid_hwm) = writer.allocator.current_hwm().await;
                let scratch_alloc =
                    uni_store::runtime::id_allocator::IdAllocator::in_memory_seeded(
                        vid_hwm,
                        eid_hwm,
                        batch as u64,
                    )
                    .await
                    .map_err(UniError::Internal)?;
                Arc::new(uni_store::runtime::TxIdReservoir::new(
                    Arc::new(scratch_alloc),
                    batch,
                ))
            } else {
                Arc::new(uni_store::runtime::TxIdReservoir::new(
                    writer.allocator.clone(),
                    batch,
                ))
            };
            (version, l0, reservoir)
        };

        // Component C1: pin the L0 snapshot AFTER `create_transaction_l0` stamped
        // `occ_read_seq`, so the snapshot reflects state >= read_seq — any commit
        // newer than read_seq is then caught by OCC rather than silently read.
        // Only pinned when SSI is enabled; otherwise reads run against live L0.
        //
        // Component C2: pin the L1 tier as well — a StorageManager clone whose
        // scans filter to `_version <= started_at_version`, so a flush
        // completing mid-transaction cannot leak post-snapshot rows. One per
        // transaction (the pinned manager carries a fresh AdjacencyManager).
        // A scratch tx always pins (even with SSI off): its read base must be the
        // version-pinned `pinned_at_version` storage (which SHARES the adjacency
        // manager, so scratch edges written to tx_l0 are visible to traversal)
        // rather than live L0, giving stable read-your-writes isolation (G8/E2).
        let snapshot = if db.config.ssi_enabled || ephemeral {
            let writer: &uni_store::Writer = writer_lock.as_ref();
            let mut snap = writer.l0_manager.pin_snapshot();
            snap.pinned_storage = Some(Arc::new(
                db.storage.pinned_at_version(snap.started_at_version),
            ));
            Arc::new(parking_lot::Mutex::new(Some(snap)))
        } else {
            Arc::new(parking_lot::Mutex::new(None))
        };

        let id = Uuid::new_v4().to_string();
        info!(transaction_id = %id, "Transaction started");

        // Clone session's rule registry for transaction-scoped modifications
        let session_registry = session.rule_registry().read().unwrap().clone();

        let deadline = timeout.map(|d| Instant::now() + d);
        // Child token from session — cancelled when session.cancel() fires
        let cancellation_token = session.cancellation_token().child_token();

        let tx = Self {
            db,
            tx_l0,
            id_reservoir,
            session_write_guard: session.active_write_guard().clone(),
            session_rule_registry: session.rule_registry().clone(),
            rule_registry: Arc::new(std::sync::RwLock::new(session_registry)),
            session_metrics: session.metrics_inner.clone(),
            completed: false,
            rollback_only: AtomicBool::new(false),
            id,
            session_id: session.id().to_string(),
            start_time: Instant::now(),
            started_at_version,
            deadline,
            cancellation_token,
            hooks: session.hooks.values().cloned().collect(),
            principal: session.principal.clone(),
            for_update_guards: parking_lot::Mutex::new(Vec::new()),
            for_update_held: parking_lot::Mutex::new(std::collections::HashSet::new()),
            snapshot,
            ephemeral,
        };

        // Transaction constructed successfully — its Drop impl will clear the
        // write guard, so we disarm the scopeguard.
        std::mem::forget(write_guard_cleanup);

        // Phase 2 Day 11: count this transaction as in-flight on its
        // UniInner so `Uni::drop_fork` can surface the
        // forgot-to-commit case as `ForkInflightTx` rather than
        // proceeding silently. Decrement happens in `Transaction::drop`.
        tx.db.inflight_tx_count.fetch_add(1, Ordering::SeqCst);

        Ok(tx)
    }

    // ── Cypher Reads (sees shared DB + uncommitted writes) ────────────

    /// The transaction's pinned read snapshot — frozen L0 generations
    /// (Component C1) plus the version-pinned L1 storage (Component C2) —
    /// threaded into the executor.
    ///
    /// Returns `Some` for a read-write transaction begun under
    /// `UniConfig::ssi_enabled` (until commit/rollback `take`s it); `None` when
    /// SSI is disabled (nothing was pinned ⇒ live reads) or after release. A
    /// `None` is a safe no-op downstream.
    pub(crate) fn read_snapshot(&self) -> Option<uni_store::runtime::SnapshotView> {
        self.snapshot.lock().clone()
    }

    /// Execute a Cypher query within the transaction.
    /// Reads see the private L0 buffer (uncommitted writes).
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.mark_on_err(self.query_inner(cypher).await)
    }

    /// Inner body of [`Self::query`]; [`Self::query`] wraps its result in
    /// `mark_on_err` so any failure poisons the transaction (bug #15).
    async fn query_inner(&self, cypher: &str) -> Result<QueryResult> {
        self.check_completed()?;
        // Authorization: `query_inner` is the execution choke point for BOTH
        // reads and writes — `Session::run` routes writes here (via `tx.query`)
        // to preserve trailing `RETURN` rows, and direct `tx.query` calls also
        // land here. Only `tx.execute`/the parameterized builders authorized
        // before, so a write run through `tx.query` bypassed the `AuthzPolicy`
        // chain. Authorize here with a read-aware verb (a pure read must not be
        // mis-classified as a write). Gated on a registered policy so the common
        // no-policy path skips the extra parse.
        if !self.db.plugin_registry.authz_policies().is_empty() {
            self.authorize(cypher, self.authz_verb(cypher))?;
        }
        if self.db.config.ssi_enabled {
            self.acquire_for_update_locks(cypher, &HashMap::new())
                .await?;
        } else if cypher.to_ascii_lowercase().contains("for update") {
            // FOR UPDATE is a no-op when SSI is disabled — surface it loudly so a
            // runtime misconfiguration does not silently drop pessimistic locking
            // (a far easier mistake than a compile-time opt-out). Structured field
            // for filtering; message template per M-LOG-STRUCTURED.
            tracing::warn!(
                ssi_enabled = false,
                "FOR UPDATE ignored: ssi_enabled is false, so no row locks are acquired and \
                 concurrent writers are not serialized — enable SSI or guard the RMW externally"
            );
        }
        // FU-1: thread the transaction's principal into the executor
        // scope so procedure capability gates (e.g. ProcedureWrites
        // for `declareProcedure WRITE`) see the authenticated user.
        let principal = self.principal.clone();
        let fut = self.db.execute_internal_with_tx_l0(
            cypher,
            HashMap::new(),
            self.tx_l0.clone(),
            Some(self.id_reservoir.clone()),
            self.read_snapshot(),
            self.cancel_scope(None),
        );
        uni_query::maybe_scope_with_principal(principal, fut).await
    }

    /// Acquires `FOR UPDATE` pessimistic row locks for the matched keyed nodes,
    /// holding them until the transaction ends. Only single keyed-node matches
    /// are locked (the RMW use case); other `FOR UPDATE` patterns log a warning.
    ///
    /// # Errors
    /// Returns [`UniError::LockTimeout`] (retriable) if a lock cannot be acquired
    /// within the bound — a likely deadlock or long-held lock. `transact_with_retry`
    /// will re-run the closure, which re-acquires from scratch and can win the lock
    /// once the holder releases.
    ///
    /// Only ever called when `UniConfig::ssi_enabled` is `true` (see `query`).
    async fn acquire_for_update_locks(
        &self,
        cypher: &str,
        params: &HashMap<String, uni_common::Value>,
    ) -> Result<()> {
        // Cheap gate: only parse-for-locks when the hint is present. The full
        // query parse/plan still happens downstream in execute_internal.
        if !cypher.to_ascii_lowercase().contains("for update") {
            return Ok(());
        }
        let Ok(ast) = uni_cypher::parse(cypher) else {
            // Let execute_internal surface the parse error uniformly.
            return Ok(());
        };
        let collected = crate::api::for_update::collect_for_update_keys(&ast, params);
        if collected.unsupported {
            tracing::warn!(
                "FOR UPDATE applied to an unsupported pattern (only single keyed nodes are \
                 locked); that lock hint is ignored"
            );
        }
        let writer = self.db.writer.as_ref().ok_or(UniError::ReadOnly {
            operation: "for_update".to_string(),
        })?;
        let mut keys = collected.keys;
        keys.sort();
        keys.dedup();
        let mut acquired_any = false;
        for key in keys {
            if self.for_update_held.lock().contains(&key) {
                continue;
            }
            let handle = writer.row_lock_handle(&key);
            // Bounded wait so a deadlock surfaces as a retriable LockTimeout
            // rather than hanging the transaction. (A plain Timeout would not be
            // retriable; a contended row lock genuinely clears when the holder
            // commits/rolls back, so the retry helper should re-attempt it.)
            let guard =
                tokio::time::timeout(std::time::Duration::from_secs(10), handle.lock_owned())
                    .await
                    .map_err(|_| UniError::LockTimeout { timeout_ms: 10_000 })?;
            self.for_update_held.lock().insert(key);
            self.for_update_guards.lock().push(guard);
            acquired_any = true;
        }

        // Read-latest under the lock. If this acquisition actually took a lock and
        // the transaction is still FRESH (no reads, no writes yet), re-pin the
        // snapshot and re-stamp the OCC read sequence to NOW. The locked read then
        // sees the latest committed value, so a `FOR UPDATE` read-modify-write
        // commits WITHOUT a retry: the lock keeps the row stable, while a
        // non-FOR-UPDATE writer that commits the row after this new baseline is
        // still caught by OCC (correct). Fresh-only because a single per-tx
        // `occ_read_seq` cannot keep earlier reads at their begin basis — a tx that
        // read before `FOR UPDATE` keeps today's mutex+retry behaviour.
        if acquired_any && !self.is_dirty() {
            let read_set_empty = self
                .tx_l0
                .read()
                .occ_read_set
                .as_ref()
                .is_none_or(|rs| rs.lock().is_empty());
            if read_set_empty {
                // Order mirrors transaction begin: advance `occ_read_seq` first,
                // then pin, so the snapshot always reflects state >= read_seq (a
                // racing commit causes at most a spurious retry, never a missed
                // conflict / lost update). The C2 L1 pin is rebuilt at the new
                // baseline alongside the L0 pin.
                self.tx_l0.write().occ_read_seq = writer.current_commit_sequence();
                let mut snap = writer.l0_manager.pin_snapshot();
                snap.pinned_storage = Some(Arc::new(
                    self.db.storage.pinned_at_version(snap.started_at_version),
                ));
                *self.snapshot.lock() = Some(snap);
            }
        }
        Ok(())
    }

    /// Execute a Cypher query with parameters.
    pub fn query_with(&self, cypher: &str) -> TxQueryBuilder<'_> {
        TxQueryBuilder {
            tx: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            cancellation_token: None,
            timeout: None,
        }
    }

    // ── Cypher Writes ─────────────────────────────────────────────────

    /// Execute a Cypher mutation within the transaction.
    /// Mutation count is read from the private L0 (not the global writer).
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn execute(&self, cypher: &str) -> Result<ExecuteResult> {
        self.mark_on_err(self.execute_inner(cypher).await)
    }

    /// Inner body of [`Self::execute`]; [`Self::execute`] wraps its result in
    /// `mark_on_err` so any failure (authorization or statement execution)
    /// poisons the transaction (bug #15).
    async fn execute_inner(&self, cypher: &str) -> Result<ExecuteResult> {
        self.check_completed()?;
        self.authorize(cypher, classify_verb(cypher))?;
        let before = self.snapshot_l0();
        let result = self.query_inner(cypher).await?;
        let after = self.snapshot_l0();
        Ok(Self::compute_execute_result(&before, &after, &result))
    }

    /// Consult the plugin registry's `AuthzPolicy` chain for an action
    /// inside this transaction. Mirrors `Session::authorize` but reads
    /// the principal off the transaction (cloned from the session at
    /// `Transaction::new` time) so a long-lived transaction can't be
    /// retroactively escalated by a session re-auth.
    fn authorize(&self, cypher: &str, verb: &str) -> Result<()> {
        crate::api::session::authorize_query(&self.db, self.principal.as_deref(), cypher, verb)
    }

    /// Classify the authorization verb for a statement executed via
    /// [`Self::query_inner`].
    ///
    /// A statement with no write/schema/dbms clause authorizes under `"read"`;
    /// otherwise it uses [`classify_verb`]. Mirrors `Session::run`'s read-only
    /// oracle (consulting the db-level plugin registry for procedure modes — a
    /// transaction has no session-scoped registry) so a write executed through
    /// `tx.query` is authorized under the correct verb rather than mis-denying a
    /// read as a write.
    fn authz_verb(&self, cypher: &str) -> &'static str {
        let proc_is_write = |name: &str| {
            use uni_plugin::traits::procedure::ProcedureMode;
            uni_plugin::QName::parse(name)
                .ok()
                .and_then(|q| self.db.plugin_registry.procedure(&q))
                .is_some_and(|e| !matches!(e.signature.mode, ProcedureMode::Read))
        };
        match uni_cypher::parse(cypher) {
            Ok(ast) if uni_query::validate_read_only_with(&ast, &proc_is_write).is_ok() => "read",
            _ => classify_verb(cypher),
        }
    }

    /// Per-statement guards shared by the typed `execute`/`query` entry points
    /// and their parameterized builders: authorization and `FOR UPDATE` lock
    /// acquisition **with the statement's actual parameters**.
    ///
    /// The builders previously called `execute_internal_with_tx_l0` directly,
    /// skipping both — so a parameterized `tx.query(cypher, params)` carrying
    /// `FOR UPDATE` silently took no row locks, and an `AuthzPolicy` was
    /// bypassed by parameterizing the statement. Centralizing the guards keeps a
    /// new builder method from re-opening either gap.
    ///
    /// # Errors
    /// Returns an authorization error, or [`UniError::LockTimeout`] if a
    /// `FOR UPDATE` lock cannot be acquired.
    async fn run_exec_guards(&self, cypher: &str, params: &HashMap<String, Value>) -> Result<()> {
        self.authorize(cypher, classify_verb(cypher))?;
        if self.db.config.ssi_enabled {
            self.acquire_for_update_locks(cypher, params).await?;
        } else if cypher.to_ascii_lowercase().contains("for update") {
            tracing::warn!(
                ssi_enabled = false,
                "FOR UPDATE ignored: ssi_enabled is false, so no row locks are acquired and \
                 concurrent writers are not serialized — enable SSI or guard the RMW externally"
            );
        }
        Ok(())
    }

    /// Execute a mutation with parameters using a builder.
    ///
    /// Returns an [`ExecuteBuilder`] that provides `.param()` chaining
    /// and a `.run()` method that returns [`ExecuteResult`].
    pub fn execute_with(&self, cypher: &str) -> ExecuteBuilder<'_> {
        ExecuteBuilder {
            tx: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout: None,
            cancellation_token: None,
        }
    }

    // ── DerivedFactSet Application ─────────────────────────────────────

    /// Apply a `DerivedFactSet` (from a session-level DERIVE) to this transaction.
    ///
    /// Replays the collected Cypher mutation ASTs against the transaction's
    /// private L0 buffer.
    ///
    /// **Freshness is required by default**: if any commit happened between
    /// DERIVE evaluation and this apply (version gap > 0), this returns
    /// [`UniError::StaleDerivedFacts`]. Session-level derivation reads are
    /// not part of this transaction's OCC read-set, so this version check is
    /// the only thing standing between a concurrent base-data change and a
    /// silently stale derivation. Opt out with
    /// [`Transaction::apply_with`] + [`ApplyBuilder::allow_stale`] or bound
    /// the gap with [`ApplyBuilder::max_version_gap`].
    #[instrument(skip(self, derived), fields(transaction_id = %self.id))]
    pub async fn apply(&self, derived: DerivedFactSet) -> Result<ApplyResult> {
        self.apply_internal(derived, false, None).await
    }

    /// Start building an apply operation with staleness controls.
    pub fn apply_with(&self, derived: DerivedFactSet) -> ApplyBuilder<'_> {
        ApplyBuilder {
            tx: self,
            derived,
            allow_stale: false,
            max_version_gap: None,
        }
    }

    async fn apply_internal(
        &self,
        derived: DerivedFactSet,
        allow_stale: bool,
        max_gap: Option<u64>,
    ) -> Result<ApplyResult> {
        self.check_completed()?;
        let current_version = self.tx_l0.read().current_version;
        let version_gap = current_version.saturating_sub(derived.evaluated_at_version);

        // Fresh-by-default: unless the caller explicitly opted into
        // staleness, any commit between DERIVE evaluation and apply rejects
        // the apply — the derivation may be based on data that no longer
        // exists (architecture review §2.4).
        //
        // This is a pre-flight precondition check that runs BEFORE any
        // mutation is replayed, so a `StaleDerivedFacts` rejection writes
        // nothing to `tx_l0`. It must NOT poison the transaction (it is not a
        // half-applied statement): the tx stays usable so a caller can re-apply
        // with a wider gap. Only the replay loop below — which can leave
        // half-applied rows — is wrapped in `mark_on_err` (bug #15).
        if !allow_stale {
            let max = max_gap.unwrap_or(0);
            if version_gap > max {
                return Err(UniError::StaleDerivedFacts { version_gap });
            }
        }
        if version_gap > 0 {
            info!(
                transaction_id = %self.id,
                version_gap,
                "Applying DerivedFactSet with version gap"
            );
        }

        // From here on a failure can leave a partially-replayed `DerivedFactSet`
        // in `tx_l0`, so poison the transaction on any error (bug #15).
        self.mark_on_err(
            Self::replay_facts(
                &self.db,
                &self.tx_l0,
                derived,
                version_gap,
                self.cancel_scope(None),
            )
            .await,
        )
    }

    /// Replays a `DerivedFactSet`'s mutation queries into `tx_l0`.
    ///
    /// Factored out of [`Self::apply_internal`] so the caller can wrap exactly
    /// this — the only part that can leave half-applied rows — in `mark_on_err`,
    /// while the pre-flight staleness guard remains a clean (non-poisoning)
    /// early return.
    async fn replay_facts(
        db: &Arc<UniInner>,
        tx_l0: &Arc<parking_lot::RwLock<uni_store::runtime::l0::L0Buffer>>,
        derived: DerivedFactSet,
        version_gap: u64,
        cancel: crate::api::impl_query::CancelScope,
    ) -> Result<ApplyResult> {
        let mut facts_applied = 0;
        for query in derived.mutation_queries {
            db.execute_ast_internal_with_tx_l0(
                query,
                "<locy-apply>",
                HashMap::new(),
                db.config.clone(),
                tx_l0.clone(),
                cancel.clone(),
            )
            .await?;
            facts_applied += 1;
        }

        Ok(ApplyResult {
            facts_applied,
            version_gap,
        })
    }

    // ── Bulk Insert (admin convenience) ─────────────────────────────

    /// Bulk insert vertices for a given label within this transaction.
    ///
    /// Mutations are written to the transaction's private L0 and become
    /// visible on commit. Returns the allocated VIDs in input order.
    ///
    /// For vertices carrying more than one label, see
    /// [`Self::bulk_insert_vertices_labeled`].
    #[instrument(skip(self, properties_list), fields(transaction_id = %self.id))]
    pub async fn bulk_insert_vertices(
        &self,
        label: &str,
        properties_list: Vec<uni_common::Properties>,
    ) -> Result<Vec<uni_common::core::id::Vid>> {
        self.bulk_insert_vertices_labeled(&[label], properties_list)
            .await
    }

    /// Bulk insert vertices carrying **one or more** labels.
    ///
    /// The storage layer has always stored a vertex's labels as a set
    /// (`Writer::insert_vertices_batch` takes `Vec<String>`), but until this
    /// existed the only public bulk path hardcoded a single label — so
    /// multi-label vertices could be stored by the engine and not created by a
    /// caller. Data whose nodes are genuinely multi-labelled (an LDBC `Place`
    /// that is also a `City`) had to drop labels at load time, and a later
    /// `MATCH (p:Place)` would return nothing at all, with no error.
    ///
    /// Mutations are written to the transaction's private L0 and become visible
    /// on commit. Returns the allocated VIDs in input order.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::LabelNotFound`] if any label is undeclared, or
    /// [`UniError::Schema`] if `labels` is empty — a vertex with no label cannot
    /// be matched by any pattern, so it is rejected rather than stored.
    #[instrument(skip(self, properties_list), fields(transaction_id = %self.id))]
    pub async fn bulk_insert_vertices_labeled(
        &self,
        labels: &[&str],
        properties_list: Vec<uni_common::Properties>,
    ) -> Result<Vec<uni_common::core::id::Vid>> {
        self.check_completed()?;
        if labels.is_empty() {
            return Err(UniError::Schema {
                message: "bulk_insert_vertices_labeled requires at least one label; a vertex                           with no label cannot be matched by any pattern"
                    .to_string(),
            });
        }
        let schema = self.db.schema.schema();
        for label in labels {
            schema
                .labels
                .get(*label)
                .ok_or_else(|| UniError::LabelNotFound {
                    label: (*label).to_string(),
                })?;
        }
        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "bulk_insert_vertices".to_string(),
        })?;
        let writer: &uni_store::Writer = writer_lock.as_ref();
        if properties_list.is_empty() {
            return Ok(Vec::new());
        }
        let vids = writer
            .allocate_vids(properties_list.len())
            .await
            .map_err(UniError::Internal)?;
        // Route mutations through the transaction's private L0.
        let result = writer
            .insert_vertices_batch(
                vids.clone(),
                properties_list,
                labels.iter().map(|l| (*l).to_string()).collect(),
                Some(&self.tx_l0),
            )
            .await
            .map_err(UniError::Internal);
        // A partially-applied batch leaves rows in `tx_l0`; poison the tx so it
        // cannot commit (bug #15).
        self.mark_on_err(result)?;
        Ok(vids)
    }

    /// Overwrite an existing vertex's properties in place, keeping its VID.
    ///
    /// Full-row replace via the writer, routed through this transaction's
    /// private L0. Used by fork promote's ext_id-keyed upsert path; the
    /// vertex must already exist on primary (the caller resolved its VID).
    pub async fn update_vertex_properties(
        &self,
        label: &str,
        vid: uni_common::core::id::Vid,
        props: uni_common::Properties,
    ) -> Result<()> {
        self.check_completed()?;
        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "update_vertex_properties".to_string(),
        })?;
        let writer: &uni_store::Writer = writer_lock.as_ref();
        let result = writer
            .insert_vertex_with_labels(vid, props, &[label.to_string()], Some(&self.tx_l0))
            .await
            .map(|_| ())
            .map_err(UniError::Internal);
        self.mark_on_err(result)
    }

    /// Soft-delete an existing vertex by VID within this transaction.
    ///
    /// Routed through the transaction's private L0 so it commits atomically
    /// with the upserts/inserts; used by fork promote's delete-promotion
    /// pass. The `label` scopes the tombstone's flush to the right table.
    pub async fn delete_vertex_by_vid(
        &self,
        label: &str,
        vid: uni_common::core::id::Vid,
    ) -> Result<()> {
        self.check_completed()?;
        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "delete_vertex_by_vid".to_string(),
        })?;
        let writer: &uni_store::Writer = writer_lock.as_ref();
        let result = writer
            .delete_vertex(vid, Some(vec![label.to_string()]), Some(&self.tx_l0))
            .await
            .map_err(UniError::Internal);
        self.mark_on_err(result)
    }

    /// Bulk insert edges for a given edge type within this transaction.
    ///
    /// Mutations are written to the transaction's private L0 and become
    /// visible on commit.
    #[instrument(skip(self, edges), fields(transaction_id = %self.id))]
    pub async fn bulk_insert_edges(
        &self,
        edge_type: &str,
        edges: Vec<(
            uni_common::core::id::Vid,
            uni_common::core::id::Vid,
            uni_common::Properties,
        )>,
    ) -> Result<()> {
        self.check_completed()?;
        let schema = self.db.schema.schema();
        let edge_meta =
            schema
                .edge_types
                .get(edge_type)
                .ok_or_else(|| UniError::EdgeTypeNotFound {
                    edge_type: edge_type.to_string(),
                })?;
        let type_id = edge_meta.id;
        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "bulk_insert_edges".to_string(),
        })?;
        let writer: &uni_store::Writer = writer_lock.as_ref();
        // Pre-allocate all EIDs in one IdAllocator mutex acquisition.
        let eids = writer
            .allocate_eids(edges.len())
            .await
            .map_err(UniError::Internal)?;
        // Route mutations through the transaction's private L0.
        let result: Result<()> = async {
            for ((src_vid, dst_vid, props), eid) in edges.into_iter().zip(eids) {
                writer
                    .insert_edge(
                        src_vid,
                        dst_vid,
                        type_id,
                        eid,
                        props,
                        Some(edge_type.to_string()),
                        Some(&self.tx_l0),
                    )
                    .await
                    .map_err(UniError::Internal)?;
            }
            Ok(())
        }
        .await;
        // A partially-applied batch leaves rows in `tx_l0`; poison the tx so it
        // cannot commit (bug #15).
        self.mark_on_err(result)
    }

    // ── Bulk Writer / Appender ────────────────────────────────────────

    /// Create a bulk writer builder for efficient data loading within this transaction.
    ///
    /// The bulk writer writes directly to storage (bypassing the L0 buffer).
    /// The Transaction's write guard ensures mutual exclusion — the BulkWriter
    /// does not manage the guard itself.
    pub fn bulk_writer(&self) -> crate::api::bulk::BulkWriterBuilder {
        crate::api::bulk::BulkWriterBuilder::new_unguarded(self.db.bulk_backend())
    }

    /// Create a streaming appender for row-by-row data loading within this transaction.
    ///
    /// The appender writes directly to storage (bypassing the L0 buffer).
    /// The Transaction's write guard ensures mutual exclusion.
    pub fn appender(&self, label: &str) -> crate::api::appender::AppenderBuilder {
        crate::api::appender::AppenderBuilder::new_from_tx(self.db.bulk_backend(), label)
    }

    // ── Locy Evaluation ───────────────────────────────────────────────

    /// Evaluate a Locy program within the transaction.
    ///
    /// DERIVE commands auto-apply to the transaction's write buffer.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn locy(&self, program: &str) -> Result<LocyResult> {
        self.check_completed()?;
        // Create a LocyEngine directly from UniInner (which sees tx L0).
        // Transaction path: auto-apply DERIVE mutations to the private L0.
        let engine = impl_locy::LocyEngine {
            db: &self.db,
            tx_l0_override: Some(self.tx_l0.clone()),
            locy_l0: Some(self.tx_l0.clone()),
            collect_derive: false,
            read_snapshot: self.read_snapshot(),
            cancel: self.cancel_scope(None),
        };
        engine.evaluate(program).await
    }

    /// Evaluate a Locy program with parameters using a builder.
    pub fn locy_with(&self, program: &str) -> crate::api::locy_builder::TxLocyBuilder<'_> {
        crate::api::locy_builder::TxLocyBuilder::new(self, program)
    }

    // ── Prepared Statements ──────────────────────────────────────────

    /// Prepare a Cypher query for repeated execution within this transaction.
    ///
    /// The query is parsed and planned once; subsequent executions skip those
    /// phases. If the schema changes, the prepared query auto-replans.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn prepare(&self, cypher: &str) -> Result<crate::api::prepared::PreparedQuery> {
        self.check_completed()?;
        // Bind the prepared query to this transaction's private L0, id
        // reservoir, and (live) read snapshot, so its reads see the tx's
        // uncommitted writes and its writes land in `tx_l0` — undone by
        // `rollback()` rather than leaking into main L0.
        let binding = crate::api::prepared::PreparedTxBinding {
            tx_l0: self.tx_l0.clone(),
            id_reservoir: self.id_reservoir.clone(),
            snapshot: self.snapshot.clone(),
        };
        let guards = crate::api::prepared::PreparedGuards::for_transaction(
            self.principal.clone(),
            self.hooks.clone(),
            self.session_id.clone(),
            classify_verb(cypher).to_string(),
        );
        crate::api::prepared::PreparedQuery::new_tx_bound(
            self.db.clone(),
            cypher,
            binding,
            guards,
            self.cancel_scope(None),
        )
        .await
    }

    /// Prepare a Locy program for repeated evaluation within this transaction.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub async fn prepare_locy(&self, program: &str) -> Result<crate::api::prepared::PreparedLocy> {
        self.check_completed()?;
        crate::api::prepared::PreparedLocy::new(
            self.db.clone(),
            self.rule_registry.clone(),
            program,
        )
    }

    // ── Rule Management ───────────────────────────────────────────────

    /// Access the transaction-scoped rule registry.
    /// On commit, new rules are promoted to the session (best-effort).
    pub fn rules(&self) -> super::rule_registry::RuleRegistry<'_> {
        super::rule_registry::RuleRegistry::new(&self.rule_registry)
            .with_plugin_registry(&self.db.plugin_registry)
    }

    // ── Lifecycle ─────────────────────────────────────────────────────

    /// Commit the transaction.
    ///
    /// Persists all changes made during the transaction. Returns a
    /// [`CommitResult`] with commit metadata.
    #[instrument(skip(self), fields(transaction_id = %self.id, duration_ms), level = "info")]
    pub async fn commit(mut self) -> Result<CommitResult> {
        self.check_completed()?;

        // G8/E2: a scratch transaction can never be committed — refuse before any
        // writer lock or L0 merge, so its writes are always discarded on drop.
        if self.ephemeral {
            return Err(UniError::ReadOnly {
                operation: "commit a scratch transaction (its writes are discarded on drop; use session.tx() to persist)".to_string(),
            });
        }

        let writer_lock = self.db.writer.as_ref().ok_or_else(|| UniError::ReadOnly {
            operation: "commit".to_string(),
        })?;

        // Read mutation count from the private L0 (no lock needed)
        let mutations = self.tx_l0.read().mutation_count;

        // Run before-commit hooks BEFORE acquiring writer lock (rejection point)
        if !self.hooks.is_empty() {
            let ctx = crate::api::hooks::CommitHookContext {
                session_id: self.session_id.clone(),
                tx_id: self.id.clone(),
                mutation_count: mutations,
            };
            for hook in &self.hooks {
                hook.before_commit(&ctx)?;
            }
        }

        // M5e — registry phased hooks also see `before_commit` so a
        // `BuiltinHookPlugin`-installed legacy hook fires through the same
        // path as a directly-registered phased hook. Reject short-circuits
        // the same way the legacy chain above does.
        {
            let registry_hooks = self.db.plugin_registry.hooks();
            if !registry_hooks.is_empty() {
                let ctx = uni_plugin::traits::hook::CommitContext::new(&self.session_id);
                for hook in registry_hooks.iter() {
                    use uni_plugin::errors::HookOutcome;
                    match hook.before_commit(&ctx) {
                        HookOutcome::Continue => {}
                        HookOutcome::Reject { reason } => {
                            return Err(UniError::HookRejected { message: reason });
                        }
                        _ => {}
                    }
                }
            }
        }

        // Build trigger router once per commit. `is_empty()` lets us
        // short-circuit the per-row event extraction when no triggers
        // are registered (zero overhead on the hot path).
        let trigger_router = TriggerRouter::from_registry_with_queue(
            &self.db.plugin_registry,
            Some(Arc::clone(&self.db.defer_queue)),
            Arc::clone(&self.db.ec_queue),
        )?;

        // M11 FU-4: CDC subscribers also need per-row mutation events,
        // so the extraction must run if *either* triggers or CDC
        // outputs are registered. When neither is registered we skip
        // the L0 probe + scan entirely (zero-cost hot path).
        let cdc_active = !self.db.plugin_registry.cdc_outputs_is_empty();
        let need_events = !trigger_router.is_empty() || cdc_active;

        // For the trigger / CDC path, capture an `L0Manager` handle
        // ahead of the L0 read scope so `PreExistingProbe` can scan
        // committed L0 + pending-flush L0s for the VIDs/EIDs about to
        // be committed. The handle is cheap (Arc clone).
        let l0_manager_for_probe = if need_events {
            if let Some(wl) = self.db.writer.as_ref() {
                Some(Arc::clone(&wl.l0_manager))
            } else {
                None
            }
        } else {
            None
        };

        // Snapshot labels and edge types from L0 (sync read, short
        // lock scope). Also build the L0-chain portion of the probe
        // here while we already hold the lock — `from_l0_chain` is
        // sync.
        let (labels_affected, edge_types_affected, mut probe_opt) = {
            let l0 = self.tx_l0.read();
            let labels: Vec<String> = l0
                .vertex_labels
                .values()
                .flatten()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            let edge_types: Vec<String> = l0
                .edge_types
                .values()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            let probe = l0_manager_for_probe
                .as_ref()
                .map(|l0m| crate::api::triggers::PreExistingProbe::from_l0_chain(l0m, &l0));
            (labels, edge_types, probe)
        };

        // Extend the probe with an L1 storage existence scan for VIDs
        // not found in the L0 chain. The candidate snapshot is taken
        // under a short sync lock; the async probe runs outside the
        // lock to keep the L0 buffer available to other readers and
        // to satisfy the Send bound on the surrounding async fn.
        if let Some(ref mut probe) = probe_opt {
            let candidates = {
                let l0 = self.tx_l0.read();
                probe.pending_l1_candidates(&l0)
            };
            if !candidates.is_empty() {
                probe.extend_with_l1(candidates, &self.db.storage).await;
            }
        }

        // Build trigger events under a second short read of tx_l0
        // — the tx is single-threaded, so its contents are stable
        // between the snapshot above and now. Includes the CDC path:
        // when triggers aren't registered but CDC subscribers are, we
        // still extract events (with no property bag — the trigger
        // predicate gate is the only consumer of `properties_referenced`).
        let trigger_events = if need_events {
            let l0 = self.tx_l0.read();
            let props_referenced = trigger_router.properties_referenced();
            Some(MutationEvents::from_l0_with_probe(
                &l0,
                probe_opt.as_ref(),
                &props_referenced,
            ))
        } else {
            None
        };

        // Trigger BeforeMutation / BeforeCommit dispatch — runs after
        // hook before_commit so legacy hooks see the rejection state
        // they already expect, and before the writer lock so a reject
        // is cheap (no lock acquired yet).
        if let Some(ref events) = trigger_events {
            let tx_id_u64 = tx_id_to_u64(&self.id);
            let ctx = TriggerContext::new(&self.session_id, tx_id_u64);
            trigger_router.dispatch_before(ctx, events)?;
        }

        // Component C1: release this transaction's OWN snapshot pin before the
        // commit runs, so the commit-time freeze (`is_current_pinned`) only fires
        // when ANOTHER live transaction pins this generation. Without this, the
        // tx's own pin would force a needless deep clone of the main L0 on every
        // commit. Read-your-writes is unaffected (it uses the live `tx_l0`).
        self.snapshot.lock().take();

        // Bound ONLY the `flush_lock` acquisition (contention with another
        // in-progress commit) by `commit_timeout` — passed INTO the writer rather
        // than wrapping the whole future in `tokio::time::timeout`. Wrapping the
        // whole future would cancel it past the durable point (WAL flush) — e.g.
        // during the inline post-commit L0→L1 flush — and return a retriable
        // `CommitTimeout` for a transaction that is already durable and visible,
        // which a retry would double-apply. The writer surfaces `CommitTimeout`
        // with an empty `tx_id` on a lock-acquisition timeout; we fill it in.
        let (wal_lsn, _flush_pending) = writer_lock
            .commit_transaction_l0_with_lock_timeout(
                self.tx_l0.clone(),
                Some(self.db.config.commit_timeout),
            )
            .await
            // Preserve typed commit errors (e.g. SSI `SerializationConflict` /
            // `ConstraintConflict`) so callers can detect and retry them, instead
            // of flattening every error into `Internal`.
            .map_err(|e| match e.downcast::<UniError>() {
                Ok(UniError::CommitTimeout { hint, .. }) => UniError::CommitTimeout {
                    tx_id: self.id.clone(),
                    hint,
                },
                Ok(typed) => typed,
                Err(other) => UniError::Internal(other),
            })?;
        // _flush_pending is true when async_flush_enabled and the coordinator
        // accepted a flush submission. Nothing to do here — the spawned stream
        // task drives the pipeline to completion independently.
        let writer: &uni_store::Writer = writer_lock.as_ref();
        // Update cached metrics atomics while we still hold the writer lock
        {
            let l0 = writer.l0_manager.get_current();
            let l0_guard = l0.read();
            self.db
                .cached_l0_mutation_count
                .store(l0_guard.mutation_count, Ordering::Relaxed);
            self.db
                .cached_l0_estimated_size
                .store(l0_guard.estimated_size, Ordering::Relaxed);
        }
        self.db.cached_wal_lsn.store(wal_lsn, Ordering::Relaxed);
        let version = writer.l0_manager.get_current().read().current_version;

        self.completed = true;

        // Persist a schemaless edge type minted while this transaction ran.
        // `get_or_assign_edge_type_id` interns the name → id mapping on a
        // synchronous per-row path and so cannot save; this is the first
        // asynchronous point after the write is durable. Without it the
        // registry dies with the process and every id-keyed read path answers
        // from an empty one after a reopen — an untyped `MATCH (a)-[e]-(b)`
        // finds nothing, a pattern comprehension yields `[]`, and an edge's
        // `edge_type` comes back blank (rustic-ai/uni-db#253).
        //
        // Skipped for a forked session: a fork's `SchemaManager` shares
        // primary's `path`, so saving it would overwrite primary's catalog
        // with the fork's merged view. Fork-local additions persist through
        // `ForkRegistryHandle::update_schema_overlay` instead.
        //
        // A failure here cannot fail the commit — the transaction is already
        // durable, and returning an error would invite a retry that
        // double-applies it. The manager stays dirty, so the next commit
        // retries the write rather than dropping it silently.
        if self.db.storage.fork_scope().is_none()
            && let Err(e) = self.db.storage.schema_manager().save_if_dirty().await
        {
            tracing::error!(
                error = %e,
                "failed to persist the schema after commit; \
                 a newly interned edge type will be lost if the process exits \
                 before a later commit retries the write"
            );
        }

        let duration = self.start_time.elapsed();
        tracing::Span::current().record("duration_ms", duration.as_millis());
        metrics::histogram!("uni_transaction_duration_seconds").record(duration.as_secs_f64());
        metrics::counter!("uni_transaction_commits_total").increment(1);

        // Best-effort rule promotion: promote new rules from tx → session
        let mut rule_promotion_errors = Vec::new();
        let rules_promoted = {
            match (
                self.rule_registry.read(),
                self.session_rule_registry.write(),
            ) {
                (Ok(tx_reg), Ok(mut session_reg)) => {
                    let new_names: Vec<String> = tx_reg
                        .rules
                        .keys()
                        .filter(|n| !session_reg.rules.contains_key(*n))
                        .cloned()
                        .collect();
                    let promoted = new_names.len();
                    if promoted > 0 {
                        // Promote the SOURCES that own the new rules and rebuild
                        // the session registry from the combined sources, so the
                        // promoted rules get their STRATA too. Copying only the
                        // rules map left them stratum-less, so they never
                        // evaluated (and a later rebuild dropped them). Fall back
                        // to a rules-only copy if any promoted rule lacks a
                        // tracked source (rebuild would otherwise drop it).
                        let mut combined = session_reg.sources.clone();
                        for src in &tx_reg.sources {
                            let owns_new = src.rule_names.iter().any(|r| new_names.contains(r));
                            let already = combined.iter().any(|s| s.source == src.source);
                            if owns_new && !already {
                                combined.push(src.clone());
                            }
                        }
                        let rebuilt = super::impl_locy::rebuild_registry_from_sources(
                            &combined,
                            &self.db.plugin_registry,
                        );
                        let preserved_all = |r: &super::impl_locy::LocyRuleRegistry| {
                            new_names.iter().all(|n| r.rules.contains_key(n))
                                && session_reg.rules.keys().all(|n| r.rules.contains_key(n))
                        };
                        match rebuilt {
                            Ok(r) if preserved_all(&r) => *session_reg = r,
                            _ => {
                                for (name, rule) in &tx_reg.rules {
                                    session_reg
                                        .rules
                                        .entry(name.clone())
                                        .or_insert_with(|| rule.clone());
                                }
                                rule_promotion_errors.push(RulePromotionError {
                                    rule_text: "<promotion rebuild>".into(),
                                    error: "promoted rules lacked tracked sources; \
                                            strata not rebuilt"
                                        .into(),
                                });
                            }
                        }
                    }
                    promoted
                }
                (Err(e), _) => {
                    rule_promotion_errors.push(RulePromotionError {
                        rule_text: "<all>".into(),
                        error: format!("tx rule registry lock poisoned: {e}"),
                    });
                    0
                }
                (_, Err(e)) => {
                    rule_promotion_errors.push(RulePromotionError {
                        rule_text: "<all>".into(),
                        error: format!("session rule registry lock poisoned: {e}"),
                    });
                    0
                }
            }
        };

        // Release write guard
        self.session_write_guard.store(false, Ordering::SeqCst);

        // Increment session-level commit counter
        self.session_metrics
            .transactions_committed
            .fetch_add(1, Ordering::Relaxed);
        self.db.total_commits.fetch_add(1, Ordering::Relaxed);

        let commit_result = CommitResult {
            mutations_committed: mutations,
            rules_promoted,
            version,
            started_at_version: self.started_at_version,
            wal_lsn,
            duration,
            rule_promotion_errors,
        };

        // Broadcast commit notification (ignore send error — no receivers is fine).
        // M11 FU-4: when CDC providers are registered, materialize the
        // canonical mutation-event batch onto the notification so
        // `CdcRuntime` can hand subscribers actual rows (not an empty
        // batch). Triggers consumed `trigger_events` synchronously
        // above, so reusing it here is free.
        let mutations_batch = if cdc_active {
            trigger_events
                .as_ref()
                .and_then(|e| e.materialize_all())
                .map(Arc::new)
        } else {
            None
        };
        let notif = crate::api::notifications::CommitNotification {
            version,
            mutation_count: mutations,
            labels_affected,
            edge_types_affected,
            rules_promoted,
            timestamp: chrono::Utc::now(),
            tx_id: self.id.clone(),
            session_id: self.session_id.clone(),
            causal_version: self.started_at_version,
            mutations: mutations_batch,
        };
        let _ = self.db.commit_tx.send(Arc::new(notif));

        // Run after-commit hooks (infallible — panics caught and logged)
        if !self.hooks.is_empty() {
            let ctx = crate::api::hooks::CommitHookContext {
                session_id: self.session_id.clone(),
                tx_id: self.id.clone(),
                mutation_count: mutations,
            };
            for hook in &self.hooks {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.after_commit(&ctx, &commit_result);
                }));
                if let Err(e) = result {
                    tracing::error!("after_commit hook panicked: {:?}", e);
                }
            }
        }

        // M5e — registry phased hooks also see `after_commit`. A
        // `BuiltinHookPlugin`-installed legacy hook is dispatched through
        // the `LegacyHookAdapter` (which mirrors the slim
        // `PluginCommitResult` into the legacy `CommitResult` shape).
        {
            let registry_hooks = self.db.plugin_registry.hooks();
            if !registry_hooks.is_empty() {
                let plugin_commit_result = uni_plugin::traits::hook::PluginCommitResult {
                    mutations: commit_result.mutations_committed as u64,
                    version: commit_result.version,
                    wal_lsn: commit_result.wal_lsn,
                    duration: commit_result.duration,
                };
                let ctx = uni_plugin::traits::hook::CommitContext::new(&self.session_id)
                    .with_commit_result(&plugin_commit_result);
                for hook in registry_hooks.iter() {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        hook.after_commit(&ctx);
                    }));
                    if let Err(e) = result {
                        tracing::error!("registry after_commit hook panicked: {:?}", e);
                    }
                }
            }
        }

        // Trigger AfterMutation / AfterCommit dispatch. Synchronous
        // triggers run inline (panics caught); Async / Eventual fire
        // on the tokio runtime so the commit returns immediately.
        if let Some(events) = trigger_events {
            let tx_id_u64 = tx_id_to_u64(&self.id);
            let mut ctx = TriggerContext::new(&self.session_id, tx_id_u64);
            // WS-A — attach a write-enabled host so a declared
            // (synthesized) trigger's after-commit Cypher action can run
            // against the just-committed graph state. Built ONCE per
            // commit and only when triggers are registered. The L0
            // context uses the writer's post-commit `current` buffer so
            // the action sees the data this transaction just committed;
            // `execute_inner_query(Write)` writes land directly in main
            // L0 + WAL (durable, immediately visible) rather than through
            // a nested transaction, so no re-entrant commit is produced.
            if !trigger_router.is_empty()
                && let Some(writer_arc) = self.db.writer.as_ref()
            {
                let l0_ctx = uni_query::query::df_graph::L0Context {
                    current_l0: Some(writer_arc.l0_manager.get_current()),
                    // Data is now in main L0; the tx's private buffer is
                    // excluded to avoid double-vision on read.
                    transaction_l0: None,
                    pending_flush_l0s: writer_arc.l0_manager.get_pending_flush(),
                };
                let host: Arc<dyn uni_plugin::traits::procedure::ProcedureHost> = Arc::new(
                    uni_query::query::executor::procedure_host::QueryProcedureHost::from_commit_parts(
                        Arc::clone(&self.db.storage),
                        Arc::clone(&self.db.properties),
                        Some(Arc::clone(&self.db.procedure_registry)),
                        self.db.xervo_runtime.clone(),
                        l0_ctx,
                        // Top-level commit runs declared-trigger actions
                        // at depth 0; the synthetic-trigger re-entrancy
                        // guard caps the chain.
                        0,
                    )
                    .with_writer(Arc::clone(writer_arc)),
                );
                ctx = ctx.with_host(host);
            }
            let runtime = tokio::runtime::Handle::current();
            trigger_router.dispatch_after(ctx, &events, &runtime);
        }

        info!("Transaction committed");

        Ok(commit_result)
    }

    /// Rollback the transaction, discarding all changes.
    ///
    /// No writer lock needed — the private L0 is simply dropped. This method
    /// is infallible and synchronous. If the transaction is already completed,
    /// this is a silent no-op (idempotent).
    pub fn rollback(mut self) {
        if self.completed {
            return;
        }
        self.completed = true;

        // Component C1: release the snapshot pin promptly (symmetric with commit).
        self.snapshot.lock().take();

        // Release write guard
        self.session_write_guard.store(false, Ordering::SeqCst);

        let duration = self.start_time.elapsed();
        metrics::histogram!("uni_transaction_duration_seconds").record(duration.as_secs_f64());
        metrics::counter!("uni_transaction_rollbacks_total").increment(1);

        // Increment session-level rollback counter
        self.session_metrics
            .transactions_rolled_back
            .fetch_add(1, Ordering::Relaxed);

        info!("Transaction rolled back");
    }

    /// Check if the transaction has uncommitted changes.
    pub fn is_dirty(&self) -> bool {
        self.tx_l0.read().mutation_count > 0
    }

    /// Get the transaction ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Database version when this transaction was started.
    pub fn started_at_version(&self) -> u64 {
        self.started_at_version
    }

    /// Cancel all in-flight queries in this transaction.
    #[instrument(skip(self), fields(transaction_id = %self.id))]
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Get a clone of this transaction's cancellation token.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// The cancellation scope statements in this transaction run under.
    ///
    /// The transaction's token is a child of its session's, so cancelling the
    /// session cascades here. Every terminal passes one of these down, which is
    /// what makes [`Self::cancel`] mean anything — the child token created at
    /// construction was previously never handed to an executor, so "cancel all
    /// in-flight queries in this transaction" cancelled nothing.
    pub(crate) fn cancel_scope(
        &self,
        caller: Option<CancellationToken>,
    ) -> crate::api::impl_query::CancelScope {
        crate::api::impl_query::CancelScope::new(Some(self.cancellation_token.clone()), caller)
    }

    /// Run a parameterized mutation against this transaction's private L0 and
    /// report the affected-row counters derived from the before/after L0 diff.
    ///
    /// Shared by the `ExecuteBuilder::run` and `TxQueryBuilder::execute`
    /// terminals, which differ only in which builder holds the inputs. The
    /// caller is responsible for the `mark_on_err` poisoning wrap.
    pub(crate) async fn run_tx_mutation(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
        cancellation_token: Option<CancellationToken>,
        timeout: Option<Duration>,
    ) -> Result<ExecuteResult> {
        self.check_completed()?;
        self.run_exec_guards(cypher, &params).await?;
        let before = self.snapshot_l0();
        let cancel = self.cancel_scope(cancellation_token);
        let fut = self.db.execute_internal_with_tx_l0(
            cypher,
            params,
            self.tx_l0.clone(),
            Some(self.id_reservoir.clone()),
            self.read_snapshot(),
            cancel,
        );
        let result = with_optional_timeout(timeout, fut).await?;
        let after = self.snapshot_l0();
        Ok(Self::compute_execute_result(&before, &after, &result))
    }

    /// Snapshot the current L0 mutation count and stats for before/after comparison.
    fn snapshot_l0(&self) -> L0Snapshot {
        let l0 = self.tx_l0.read();
        L0Snapshot {
            mutation_count: l0.mutation_count,
            mutation_stats: l0.mutation_stats.clone(),
        }
    }

    /// Compute an `ExecuteResult` by comparing L0 snapshots before and after a query.
    fn compute_execute_result(
        before: &L0Snapshot,
        after: &L0Snapshot,
        result: &QueryResult,
    ) -> ExecuteResult {
        let affected_rows = if result.is_empty() {
            after.mutation_count.saturating_sub(before.mutation_count)
        } else {
            result.len()
        };
        let diff = after.mutation_stats.diff(&before.mutation_stats);
        ExecuteResult::with_details(affected_rows, &diff, result.metrics().clone())
    }

    /// Marks the transaction rollback-only.
    ///
    /// Idempotent: once poisoned by a failed statement, the transaction stays
    /// rollback-only for the rest of its life.
    fn set_rollback_only(&self) {
        self.rollback_only.store(true, Ordering::SeqCst);
    }

    /// Poisons the transaction (rollback-only) whenever a statement returns
    /// `Err`, then passes the result through unchanged.
    ///
    /// Applied at every `&self` statement-execution return seam (`execute`,
    /// `query`, `apply`, and the parameterized builders) so that ANY statement
    /// error — constraint violation, parse error, mid-`UNWIND` failure with rows
    /// already in `tx_l0` — leaves the transaction non-committable (bug #15).
    fn mark_on_err<T>(&self, r: Result<T>) -> Result<T> {
        if r.is_err() {
            self.set_rollback_only();
        }
        r
    }

    fn check_completed(&self) -> Result<()> {
        if self.completed {
            return Err(UniError::TransactionAlreadyCompleted);
        }
        // A statement that previously failed poisoned the transaction: reject
        // all further statements AND commit() so half-applied rows in `tx_l0`
        // can never be persisted. `rollback()` does not route through here, so
        // it still succeeds and discards the private L0 (Neo4j-style).
        if self.rollback_only.load(Ordering::SeqCst) {
            return Err(UniError::TransactionRollbackOnly);
        }
        if let Some(deadline) = self.deadline
            && Instant::now() > deadline
        {
            return Err(UniError::TransactionExpired {
                tx_id: self.id.clone(),
                hint: "Transaction exceeded its timeout. All operations are rejected. The transaction will auto-rollback on drop.",
            });
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.completed {
            if self.is_dirty() {
                warn!(
                    transaction_id = %self.id,
                    "Transaction dropped with uncommitted writes — discarding private L0"
                );
            }
            // No writer lock needed — the private L0 drops with the Transaction.
            // Release write guard
            self.session_write_guard.store(false, Ordering::SeqCst);
        }
        // Release any FOR UPDATE pessimistic locks and prune the now-unreferenced
        // lock-map entries (otherwise the map grows once per distinct locked key
        // for the life of the database). Order matters: drop the guards FIRST so
        // the `Arc` strong count reflects only the map before we prune.
        // (When SSI is disabled the guard vec and held set are always empty, so
        // this is a cheap no-op.)
        {
            self.for_update_guards.lock().clear();
            let keys: Vec<Vec<u8>> = self.for_update_held.lock().drain().collect();
            if !keys.is_empty()
                && let Some(writer) = self.db.writer.as_ref()
            {
                writer.release_for_update_locks(&keys);
            }
        }
        // Phase 2 Day 11: pair with the increment in `new_with_options`.
        // Always runs, regardless of commit/rollback/silent-drop.
        self.db.inflight_tx_count.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Builder for parameterized mutations within a transaction.
///
/// Created by [`Transaction::execute_with()`]. Chain `.param()` calls to bind
/// parameters, then call `.run()` to execute and get an [`ExecuteResult`].
pub struct ExecuteBuilder<'a> {
    tx: &'a Transaction,
    cypher: String,
    params: HashMap<String, Value>,
    timeout: Option<Duration>,
    /// Mirrors [`TxQueryBuilder`]. Without it a caller could attach a token to
    /// a read on this transaction but not to a write, and the mutation
    /// terminal would silently ignore one.
    cancellation_token: Option<CancellationToken>,
}

impl<'a> ExecuteBuilder<'a> {
    /// Attach a cancellation token for cooperative cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Bind a parameter to the mutation.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.params.insert(k.to_string(), v);
        }
        self
    }

    /// Set maximum execution time for this mutation.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Execute the mutation and return affected row count with detailed stats.
    pub async fn run(self) -> Result<ExecuteResult> {
        let tx = self.tx;
        tx.mark_on_err(self.run_inner().await)
    }

    /// Inner body of [`Self::run`]; the public method wraps the result in
    /// `mark_on_err` so any failure poisons the transaction (bug #15).
    async fn run_inner(self) -> Result<ExecuteResult> {
        self.tx
            .run_tx_mutation(
                &self.cypher,
                self.params,
                self.cancellation_token,
                self.timeout,
            )
            .await
    }

    /// Execute the mutation with profiling, returning the [`ExecuteResult`]
    /// (mutation counters from the tx's private L0) together with the
    /// [`ProfileOutput`] (per-operator timings and memory).
    ///
    /// Mirrors `Session::query(cypher).profile()` for the write path:
    /// the mutation is routed through the transaction's private L0
    /// (commit-time serialization), so writes are only visible after
    /// `tx.commit().await?`. The captured profile includes both the
    /// DataFusion runtime stats and the DDL/admin aggregate stat where
    /// applicable.
    pub async fn profile(self) -> Result<(ExecuteResult, ProfileOutput)> {
        let tx = self.tx;
        tx.mark_on_err(self.profile_inner().await)
    }

    /// Inner body of [`Self::profile`]; the public method wraps the result in
    /// `mark_on_err` so any failure poisons the transaction (bug #15).
    async fn profile_inner(self) -> Result<(ExecuteResult, ProfileOutput)> {
        self.tx.check_completed()?;
        self.tx.run_exec_guards(&self.cypher, &self.params).await?;
        let before = self.tx.snapshot_l0();
        let fut = self.tx.db.profile_internal_with_tx_l0(
            &self.cypher,
            self.params,
            self.tx.tx_l0.clone(),
            Some(self.tx.id_reservoir.clone()),
            self.tx.read_snapshot(),
        );
        let (result, profile) = with_optional_timeout(self.timeout, fut).await?;
        let after = self.tx.snapshot_l0();
        Ok((
            Transaction::compute_execute_result(&before, &after, &result),
            profile,
        ))
    }
}

/// Builder for parameterized queries within a transaction.
pub struct TxQueryBuilder<'a> {
    tx: &'a Transaction,
    cypher: String,
    params: HashMap<String, Value>,
    cancellation_token: Option<CancellationToken>,
    timeout: Option<Duration>,
}

impl<'a> TxQueryBuilder<'a> {
    /// Bind a parameter to the mutation.
    pub fn param(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.params.insert(name.to_string(), value.into());
        self
    }

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Set maximum execution time for this query.
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Execute the mutation and return affected row count with detailed stats.
    pub async fn execute(self) -> Result<ExecuteResult> {
        let tx = self.tx;
        tx.mark_on_err(self.execute_inner().await)
    }

    /// Inner body of [`Self::execute`]; the public method wraps the result in
    /// `mark_on_err` so any failure poisons the transaction (bug #15).
    async fn execute_inner(self) -> Result<ExecuteResult> {
        self.tx
            .run_tx_mutation(
                &self.cypher,
                self.params,
                self.cancellation_token,
                self.timeout,
            )
            .await
    }

    /// Execute as a query and return rows.
    pub async fn fetch_all(self) -> Result<QueryResult> {
        let tx = self.tx;
        tx.mark_on_err(self.fetch_all_inner().await)
    }

    /// Inner body of [`Self::fetch_all`]; the public method wraps the result in
    /// `mark_on_err` so any failure poisons the transaction (bug #15).
    async fn fetch_all_inner(self) -> Result<QueryResult> {
        self.tx.check_completed()?;
        self.tx.run_exec_guards(&self.cypher, &self.params).await?;
        let cancel = self.tx.cancel_scope(self.cancellation_token);
        let fut = self.tx.db.execute_internal_with_tx_l0(
            &self.cypher,
            self.params,
            self.tx.tx_l0.clone(),
            Some(self.tx.id_reservoir.clone()),
            self.tx.read_snapshot(),
            cancel,
        );
        with_optional_timeout(self.timeout, fut).await
    }

    /// Execute the query and return the first row, or `None` if empty.
    pub async fn fetch_one(self) -> Result<Option<Row>> {
        let result = self.fetch_all().await?;
        Ok(result.into_rows().into_iter().next())
    }

    /// Execute the query and return a cursor for streaming results.
    pub async fn cursor(self) -> Result<QueryCursor> {
        let tx = self.tx;
        tx.mark_on_err(self.cursor_inner().await)
    }

    /// Inner body of [`Self::cursor`]; the public method wraps the result in
    /// `mark_on_err` so any failure poisons the transaction (bug #15).
    async fn cursor_inner(self) -> Result<QueryCursor> {
        self.tx.check_completed()?;
        self.tx.run_exec_guards(&self.cypher, &self.params).await?;

        // `execute`/`fetch_all` apply `.timeout()` by wrapping their future in
        // `tokio::time::timeout`. A cursor returns a stream that outlives this
        // call, so the ceiling has to travel with the stream instead: it goes
        // into the config the cursor guard reads. The token likewise has to be
        // handed down rather than awaited here — until now `cursor_inner`
        // dropped both on the floor, so a transaction cursor ran unbounded and
        // uncancellable while the builder accepted the options.
        let mut config = self.tx.db.config.clone();
        if let Some(t) = self.timeout {
            config.query_timeout = t;
        }

        self.tx
            .db
            .execute_cursor_internal_with_tx_l0(
                &self.cypher,
                self.params,
                self.tx.tx_l0.clone(),
                config,
                self.tx.cancel_scope(self.cancellation_token),
            )
            .await
    }
}

/// Result of applying a `DerivedFactSet` to a transaction.
#[derive(Debug)]
pub struct ApplyResult {
    /// Number of mutation queries replayed.
    pub facts_applied: usize,
    /// Number of versions that committed between DERIVE evaluation and apply.
    /// 0 means the data was fresh.
    pub version_gap: u64,
}

/// Builder for applying a `DerivedFactSet` with staleness controls.
///
/// The default is **fresh-required** (version gap must be 0); see
/// [`Transaction::apply`] for the rationale.
pub struct ApplyBuilder<'a> {
    tx: &'a Transaction,
    derived: DerivedFactSet,
    allow_stale: bool,
    max_version_gap: Option<u64>,
}

impl<'a> ApplyBuilder<'a> {
    /// Require that no commits occurred between DERIVE evaluation and apply.
    /// Returns `StaleDerivedFacts` if the version gap is > 0.
    ///
    /// This is the default since 2.0.7; the method is kept so existing
    /// callers stay valid and intent stays explicit.
    pub fn require_fresh(mut self) -> Self {
        self.allow_stale = false;
        self.max_version_gap = None;
        self
    }

    /// Apply regardless of how many commits happened since the DERIVE was
    /// evaluated. The derivation may be based on data that has since
    /// changed; a gap > 0 is logged at info level.
    pub fn allow_stale(mut self) -> Self {
        self.allow_stale = true;
        self
    }

    /// Allow up to `n` versions of gap between evaluation and apply.
    /// Returns `StaleDerivedFacts` if the gap exceeds `n`.
    pub fn max_version_gap(mut self, n: u64) -> Self {
        self.max_version_gap = Some(n);
        self
    }

    /// Execute the apply operation.
    pub async fn run(self) -> Result<ApplyResult> {
        self.tx
            .apply_internal(self.derived, self.allow_stale, self.max_version_gap)
            .await
    }
}

/// Write-side host for the `uni-fork` promote engine.
#[async_trait::async_trait]
impl uni_fork::ForkPromoteSink for Transaction {
    async fn bulk_insert_vertices(
        &self,
        label: &str,
        rows: Vec<uni_common::Properties>,
    ) -> Result<Vec<uni_common::core::id::Vid>> {
        Transaction::bulk_insert_vertices(self, label, rows).await
    }

    async fn bulk_insert_edges(
        &self,
        edge_type: &str,
        edges: Vec<(
            uni_common::core::id::Vid,
            uni_common::core::id::Vid,
            uni_common::Properties,
        )>,
    ) -> Result<()> {
        Transaction::bulk_insert_edges(self, edge_type, edges).await
    }

    async fn update_vertex_properties(
        &self,
        label: &str,
        vid: uni_common::core::id::Vid,
        props: uni_common::Properties,
    ) -> Result<()> {
        Transaction::update_vertex_properties(self, label, vid, props).await
    }

    async fn delete_vertex(&self, label: &str, vid: uni_common::core::id::Vid) -> Result<()> {
        Transaction::delete_vertex_by_vid(self, label, vid).await
    }
}

#[cfg(test)]
mod classify_verb_tests {
    use super::classify_verb;

    /// `classify_verb` sliced `s[..s.len().min(32)]` — a **byte** length, while
    /// its comment said "up to 32 chars". Any statement whose 32nd byte landed
    /// inside a multi-byte codepoint panicked, and because every write goes
    /// through here the panic crossed the pyo3 boundary as an abort rather than
    /// an exception.
    ///
    /// The sweep below necessarily places a multi-byte character across the
    /// byte-32 boundary at some padding length.
    #[test]
    fn classify_verb_does_not_panic_on_multibyte_boundaries() {
        // Sweep the padding rather than computing the offset by hand: some
        // length in this range necessarily puts the two-byte `Î` across byte
        // 32. (An earlier draft of this test guessed one offset, landed the
        // codepoint at byte 35, and passed against the unfixed code.)
        for pad in 0..40 {
            let cypher = format!("CREATE (:T)-[:`{}Î`]->(:T)", "x".repeat(pad));
            assert_eq!(
                classify_verb(&cypher),
                "write",
                "panicked or misclassified at pad={pad}"
            );
        }
    }

    /// Non-ASCII must not change the classification of a leading keyword.
    #[test]
    fn classify_verb_still_recognises_keywords() {
        assert_eq!(classify_verb("  CREATE INDEX idx ON :T(x)"), "schema");
        assert_eq!(classify_verb("DROP CONSTRAINT c"), "schema");
        assert_eq!(classify_verb("CREATE (:Café {n: 'Ünter'})"), "write");
    }

    /// A statement shorter than the prefix window is unaffected.
    #[test]
    fn classify_verb_handles_short_input() {
        assert_eq!(classify_verb("CREATE (:T)"), "write");
        assert_eq!(classify_verb(""), "write");
    }
}
