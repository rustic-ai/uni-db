// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Commit notifications — reactive awareness of database changes.
//!
//! Sessions can watch for commits via `session.watch()` or `session.watch_with()`
//! to receive filtered `CommitNotification` events.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use tokio::sync::broadcast;

/// Describes a committed transaction's effects.
#[derive(Debug, Clone)]
pub struct CommitNotification {
    /// Database version after commit.
    pub version: u64,
    /// Number of mutations in the committed transaction.
    pub mutation_count: usize,
    /// Vertex labels that were affected by the commit.
    pub labels_affected: Vec<String>,
    /// Edge types that were affected by the commit.
    pub edge_types_affected: Vec<String>,
    /// Number of Locy rules promoted from the transaction.
    pub rules_promoted: usize,
    /// Timestamp of the commit.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Transaction ID.
    pub tx_id: String,
    /// Session ID that committed the transaction.
    pub session_id: String,
    /// Database version when the transaction started (for causal ordering).
    pub causal_version: u64,
    /// Per-row mutation events for this commit, in the canonical
    /// `event_row_schema` shape (`event_kind`,
    /// `vid_or_eid`, `label`, `property`, `old_value`, `new_value`,
    /// `properties_new`, `properties_old`).
    ///
    /// `Some` only when at least one [`CdcOutputProvider`] is
    /// registered at commit time — the empty-registry hot path
    /// broadcasts `None` so the trigger / watch surface pays no
    /// extraction cost when CDC is unused. The `CdcRuntime` consumes
    /// this field directly; user-facing `session.watch()` consumers
    /// ignore it.
    ///
    /// [`CdcOutputProvider`]: uni_plugin::traits::cdc::CdcOutputProvider
    pub mutations: Option<Arc<RecordBatch>>,
    /// True when this commit's mutation batch could not be materialized.
    ///
    /// Distinct from `mutations: None`, which means "zero rows, or no CDC
    /// provider was registered". #233 Tier 1: the two were indistinguishable,
    /// so a materialization failure was delivered as an EMPTY batch carrying
    /// the commit's real LSN range and the stream then checkpointed past it —
    /// exactly the silent feed gap the `halted` flag exists to prevent.
    /// `CdcRuntime` halts a stream rather than delivering when this is set.
    pub mutations_failed: bool,
    /// Commits this stream lost, immediately before this one, because the
    /// consumer fell behind the broadcast channel.
    ///
    /// `0` on every notification of a stream that has kept up, which is the
    /// normal case. A non-zero value means the broadcaster evicted that many
    /// older commits before this one was delivered; they cannot be redelivered.
    ///
    /// **Filtered skips never appear here.** `WatchBuilder`'s label, edge-type,
    /// `exclude_session` and `debounce` filters legitimately drop commits, and
    /// this counter is set only from the broadcast lag path — so a version jump
    /// caused by a filter cannot be mistaken for loss, and a filtered stream
    /// stays quiet.
    ///
    /// #233: without this, involuntary loss was indistinguishable from
    /// voluntary filtering. A consumer doing idempotent work (invalidate a
    /// cache and re-read) is unaffected by lag — the broadcaster evicts the
    /// OLDEST entries, so the newest commit always arrives and the re-read
    /// picks up whatever the lost commits did. A consumer doing per-commit
    /// non-idempotent work (an audit mirror, a counter) is silently wrong, and
    /// had no way to find out. If you need a contiguous feed, use the CDC
    /// surface, which guarantees it and halts on a gap rather than continuing.
    pub dropped_before: u64,
}

/// An async stream of commit notifications with optional filtering.
pub struct CommitStream {
    rx: broadcast::Receiver<Arc<CommitNotification>>,
    label_filter: Option<HashSet<String>>,
    edge_type_filter: Option<HashSet<String>>,
    exclude_session: Option<String>,
    debounce: Option<Duration>,
    last_emitted: Option<Instant>,
    /// Lag-dropped commits not yet reported to the consumer.
    ///
    /// Accumulated from `RecvError::Lagged` and drained onto the next
    /// notification that survives the filters, so the count reaches the
    /// consumer attached to a real delivery (#233).
    pending_dropped: u64,
}

impl CommitStream {
    /// Wait for the next matching commit notification.
    ///
    /// Returns `None` if the broadcast channel is closed (database dropped).
    /// Skips notifications that don't match filters or are within the debounce window.
    ///
    /// # Delivery
    ///
    /// This is a **best-effort** notification stream, not a change feed. The
    /// broadcast channel is bounded, and a consumer that falls behind loses
    /// the oldest commits — it always still receives the newest. That is
    /// deliberate and correct for the intended use, invalidate-and-re-read:
    /// the final notification always arrives, so a re-read picks up whatever
    /// the lost commits did. `debounce` drops notifications for the same
    /// reason.
    ///
    /// Any loss is reported on [`CommitNotification::dropped_before`]. A
    /// consumer doing non-idempotent per-commit work must check it.
    ///
    /// **If you need a contiguous feed, use the CDC surface instead**
    /// (`CdcOutputProvider`): it guarantees contiguity, checkpoints, and halts
    /// on a gap rather than silently continuing past one.
    pub async fn next(&mut self) -> Option<CommitNotification> {
        loop {
            match self.rx.recv().await {
                Ok(notif) => {
                    // Apply exclude_session filter
                    if self
                        .exclude_session
                        .as_ref()
                        .is_some_and(|excluded| notif.session_id == *excluded)
                    {
                        continue;
                    }

                    // Apply label filter
                    if self.label_filter.as_ref().is_some_and(|labels| {
                        !notif.labels_affected.iter().any(|l| labels.contains(l))
                    }) {
                        continue;
                    }

                    // Apply edge type filter
                    if self.edge_type_filter.as_ref().is_some_and(|types| {
                        !notif.edge_types_affected.iter().any(|t| types.contains(t))
                    }) {
                        continue;
                    }

                    // Apply debounce
                    if let Some(debounce) = self.debounce {
                        if self
                            .last_emitted
                            .is_some_and(|last| last.elapsed() < debounce)
                        {
                            continue;
                        }
                        self.last_emitted = Some(Instant::now());
                    }

                    // Attach any lag accumulated since the last delivery, then
                    // reset. Doing it here rather than in the `Lagged` arm is
                    // what keeps filtered skips out of the count (#233).
                    let mut out = (*notif).clone();
                    out.dropped_before = std::mem::take(&mut self.pending_dropped);
                    return Some(out);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        dropped = n,
                        "CommitStream lagged; the dropped commits cannot be redelivered and \
                         are reported on the next notification's `dropped_before`",
                    );
                    self.pending_dropped = self.pending_dropped.saturating_add(n);
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return None;
                }
            }
        }
    }
}

/// Builder for creating a filtered [`CommitStream`].
pub struct WatchBuilder {
    rx: broadcast::Receiver<Arc<CommitNotification>>,
    label_filter: Option<HashSet<String>>,
    edge_type_filter: Option<HashSet<String>>,
    exclude_session: Option<String>,
    debounce: Option<Duration>,
}

impl WatchBuilder {
    pub fn new(rx: broadcast::Receiver<Arc<CommitNotification>>) -> Self {
        Self {
            rx,
            label_filter: None,
            edge_type_filter: None,
            exclude_session: None,
            debounce: None,
        }
    }

    /// Only receive notifications that affect the given labels.
    pub fn labels(mut self, labels: &[&str]) -> Self {
        self.label_filter = Some(labels.iter().map(|s| (*s).to_owned()).collect());
        self
    }

    /// Only receive notifications that affect the given edge types.
    pub fn edge_types(mut self, types: &[&str]) -> Self {
        self.edge_type_filter = Some(types.iter().map(|s| (*s).to_owned()).collect());
        self
    }

    /// Collapse notifications within the given interval.
    pub fn debounce(mut self, interval: Duration) -> Self {
        self.debounce = Some(interval);
        self
    }

    /// Exclude notifications from the given session ID.
    pub fn exclude_session(mut self, session_id: &str) -> Self {
        self.exclude_session = Some(session_id.to_owned());
        self
    }

    /// Build the commit stream with the configured filters.
    pub fn build(self) -> CommitStream {
        CommitStream {
            rx: self.rx,
            label_filter: self.label_filter,
            edge_type_filter: self.edge_type_filter,
            exclude_session: self.exclude_session,
            debounce: self.debounce,
            last_emitted: None,
            pending_dropped: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notif(version: u64) -> Arc<CommitNotification> {
        Arc::new(CommitNotification {
            version,
            mutation_count: 1,
            labels_affected: vec!["Person".to_owned()],
            edge_types_affected: vec![],
            rules_promoted: 0,
            timestamp: chrono::Utc::now(),
            tx_id: format!("tx-{version}"),
            session_id: "s1".to_owned(),
            causal_version: version.saturating_sub(1),
            mutations: None,
            mutations_failed: false,
            dropped_before: 0,
        })
    }

    /// A consumer that falls behind must be told how much it lost.
    ///
    /// #233. The `Lagged` arm warned and continued, so the next notification
    /// arrived indistinguishable from a contiguous one. A consumer doing
    /// idempotent work is fine either way — the broadcaster evicts the OLDEST,
    /// so the newest commit always lands — but one doing non-idempotent
    /// per-commit work (an audit mirror, a counter) was silently wrong with no
    /// way to find out.
    #[tokio::test]
    async fn a_lagging_consumer_is_told_what_it_lost() {
        let (tx, rx) = broadcast::channel::<Arc<CommitNotification>>(4);
        let mut stream = WatchBuilder::new(rx).build();

        // Overrun the 4-slot channel without reading: 1..=6 sent, 1 and 2 evicted.
        for v in 1..=6 {
            tx.send(notif(v)).expect("receiver is alive");
        }

        let first = stream.next().await.expect("a notification is delivered");
        assert_eq!(
            first.dropped_before, 2,
            "the two evicted commits must be reported on the first delivery"
        );
        assert_eq!(
            first.version, 3,
            "delivery resumes at the oldest surviving commit"
        );

        // The count is per-gap, not cumulative: a caught-up stream reports 0.
        let second = stream.next().await.expect("second notification");
        assert_eq!(
            second.dropped_before, 0,
            "a stream that has caught up must not keep reporting an old gap"
        );
        assert_eq!(second.version, 4);
    }

    /// A filtered skip is not a loss, and must not be counted as one.
    ///
    /// The same loop `continue`s for label filters, so a naive gap signal
    /// would fire on every filtered stream. `dropped_before` is set only from
    /// the broadcast lag path and only rides out on a delivered notification,
    /// so filtering stays silent by construction.
    #[tokio::test]
    async fn a_filtered_skip_is_not_reported_as_loss() {
        let (tx, rx) = broadcast::channel::<Arc<CommitNotification>>(16);
        let mut stream = WatchBuilder::new(rx).labels(&["Person"]).build();

        // Three commits the filter rejects, then one it accepts.
        for v in 1..=3 {
            let mut n = (*notif(v)).clone();
            n.labels_affected = vec!["Widget".to_owned()];
            tx.send(Arc::new(n)).expect("receiver is alive");
        }
        tx.send(notif(4)).expect("receiver is alive");

        let got = stream
            .next()
            .await
            .expect("the matching commit is delivered");
        assert_eq!(
            got.version, 4,
            "control: the filter skipped the three Widget commits"
        );
        assert_eq!(
            got.dropped_before, 0,
            "a filter skip is deliberate and must not be reported as lost commits"
        );
    }
}
