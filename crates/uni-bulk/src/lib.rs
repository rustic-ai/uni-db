// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! High-throughput bulk ingestion engine for uni-db.
//!
//! This crate provides the standalone write path that powers
//! `uni_db::Transaction::bulk_writer()` and `appender()`:
//!
//! - [`BulkWriter`] / [`BulkWriterBuilder`] — buffered, index-deferred bulk
//!   loading of vertices and edges with constraint validation, automatic
//!   checkpointing, async or blocking index rebuild, and version-based
//!   abort/rollback.
//! - [`StreamingAppender`] / [`AppenderBuilder`] — an ergonomic row-by-row
//!   append API for a single label, layered over [`BulkWriter`].
//!
//! The engine takes its storage/schema/writer handles via [`BulkBackend`],
//! a plain dependency-injection bundle constructed by the uni-db driver. No
//! trait indirection is used on the hot batch-write path.
//!
//! The shutdown coordinator ([`uni_plugin_host::shutdown::ShutdownHandle`])
//! lives in `uni-plugin-host`; the async index-rebuild path subscribes to it
//! via [`BulkBackend::shutdown`].

pub mod appender;
pub mod bulk;
mod endpoint_check;
pub mod events;
pub mod flush_intent;
#[cfg(test)]
mod tests;

pub use appender::{AppenderBuilder, StreamingAppender};
pub use bulk::{
    BulkBackend, BulkConfig, BulkPhase, BulkProgress, BulkStats, BulkWriter, BulkWriterBuilder,
    EdgeData, IntoArrow, RebuildManagerSource, record_batch_to_property_maps,
};
pub use events::{
    BULK_EVENT_CHUNK_ROWS, BulkEventSink, BulkWriteKind, EventDelivery, LoadEventReader,
    VisibleLoad,
};
pub use flush_intent::{
    RecoveredLoad, check_recovered_bulk_endpoints, finish_recovered_load_events,
    recover_interrupted_bulk_load,
};
