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
}

/// An async stream of commit notifications with optional filtering.
pub struct CommitStream {
    rx: broadcast::Receiver<Arc<CommitNotification>>,
    label_filter: Option<HashSet<String>>,
    edge_type_filter: Option<HashSet<String>>,
    exclude_session: Option<String>,
    debounce: Option<Duration>,
    last_emitted: Option<Instant>,
}

impl CommitStream {
    /// Wait for the next matching commit notification.
    ///
    /// Returns `None` if the broadcast channel is closed (database dropped).
    /// Skips notifications that don't match filters or are within the debounce window.
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

                    return Some((*notif).clone());
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("CommitStream lagged by {} notifications", n);
                    // Continue receiving — we just lost some older notifications
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
        }
    }
}
