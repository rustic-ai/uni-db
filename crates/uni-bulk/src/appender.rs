// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Streaming appender — row-by-row data loading for a single label.
//!
//! Wraps `BulkWriter` to provide an ergonomic, buffered append API for
//! loading large volumes of vertices into a single label.

use std::collections::HashMap;

use uni_common::{Result, UniError, Value};

use crate::bulk::{BulkBackend, BulkStats, BulkWriter, BulkWriterBuilder};

/// Builder for creating a [`StreamingAppender`].
pub struct AppenderBuilder {
    backend: BulkBackend,
    label: String,
    batch_size: usize,
    defer_vector_indexes: bool,
    max_buffer_size_bytes: Option<usize>,
}

impl AppenderBuilder {
    /// Create an appender builder for use within a Transaction.
    ///
    /// The Transaction already holds the session write guard, so the appender
    /// uses the unguarded bulk-writer path and does not acquire or release a
    /// guard of its own.
    pub fn new_from_tx(backend: BulkBackend, label: &str) -> Self {
        Self {
            backend,
            label: label.to_string(),
            batch_size: 5000,
            defer_vector_indexes: true,
            max_buffer_size_bytes: None,
        }
    }

    /// Set the number of rows to buffer before auto-flushing to the bulk writer.
    ///
    /// Default: 5000.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set whether to defer vector index building until commit.
    ///
    /// Default: `true`.
    pub fn defer_vector_indexes(mut self, defer: bool) -> Self {
        self.defer_vector_indexes = defer;
        self
    }

    /// Set the maximum buffer size in bytes before triggering a checkpoint.
    ///
    /// Default: 1 GB (from BulkWriter defaults).
    pub fn max_buffer_size_bytes(mut self, size: usize) -> Self {
        self.max_buffer_size_bytes = Some(size);
        self
    }

    /// Build the streaming appender.
    ///
    /// The owning Transaction already holds the session write guard, so the
    /// appender layers over an unguarded [`BulkWriter`].
    pub fn build(self) -> Result<StreamingAppender> {
        let mut bulk_builder = BulkWriterBuilder::new_unguarded(self.backend)
            .batch_size(self.batch_size)
            .defer_vector_indexes(self.defer_vector_indexes);
        if let Some(max_buf) = self.max_buffer_size_bytes {
            bulk_builder = bulk_builder.max_buffer_size_bytes(max_buf);
        }
        let writer = bulk_builder.build()?;

        Ok(StreamingAppender {
            writer: Some(writer),
            label: self.label,
            batch_size: self.batch_size,
            buffer: Vec::with_capacity(self.batch_size),
        })
    }
}

/// A streaming appender for buffered, single-label data loading.
///
/// Rows are buffered internally and flushed to the underlying `BulkWriter`
/// when the buffer reaches `batch_size`. Call [`finish()`](Self::finish) to
/// flush remaining rows and commit.
///
/// The appender is always created from a Transaction, which owns the session
/// write guard for the appender's lifetime; the appender itself acquires no
/// guard.
pub struct StreamingAppender {
    writer: Option<BulkWriter>,
    label: String,
    batch_size: usize,
    buffer: Vec<HashMap<String, Value>>,
}

impl StreamingAppender {
    /// Append a single row of properties.
    ///
    /// The row is buffered internally. When the buffer reaches `batch_size`,
    /// it is automatically flushed to the underlying bulk writer.
    pub async fn append(&mut self, properties: impl Into<HashMap<String, Value>>) -> Result<()> {
        self.buffer.push(properties.into());
        if self.buffer.len() >= self.batch_size {
            self.flush_buffer().await?;
        }
        Ok(())
    }

    /// Append an Arrow `RecordBatch` of rows.
    ///
    /// Each row in the batch is converted to a property map and buffered.
    /// Columns in the batch become property keys; values are converted from
    /// Arrow types to Uni [`Value`]s via `arrow_to_value`.
    pub async fn write_batch(&mut self, batch: &arrow_array::RecordBatch) -> Result<()> {
        for props in crate::bulk::record_batch_to_property_maps(batch)? {
            self.buffer.push(props);
            if self.buffer.len() >= self.batch_size {
                self.flush_buffer().await?;
            }
        }
        Ok(())
    }

    /// Flush all buffered rows and commit the bulk writer.
    ///
    /// Consumes the appender. Returns statistics about the loading operation.
    pub async fn finish(mut self) -> Result<BulkStats> {
        self.flush_buffer().await?;
        let writer = self
            .writer
            .take()
            .ok_or_else(|| UniError::Internal(anyhow::anyhow!("Appender already finished")))?;
        let stats = writer.commit().await.map_err(UniError::Internal)?;
        Ok(stats)
    }

    /// Abort the appender without committing.
    ///
    /// Consumes the appender. Discards all buffered rows and rolls back any
    /// batches that were already flushed to storage, so no partially loaded
    /// rows survive the abort.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying bulk writer fails to roll back its
    /// flushed tables.
    pub async fn abort(mut self) -> Result<()> {
        self.buffer.clear();
        if let Some(writer) = self.writer.take() {
            // BulkWriter::abort rolls back / drops any flushed tables so that
            // previously flushed batches do not survive the abort.
            writer.abort().await.map_err(UniError::Internal)?;
        }
        Ok(())
    }

    /// Get the number of rows currently buffered (not yet flushed).
    pub fn buffered_count(&self) -> usize {
        self.buffer.len()
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let rows = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.batch_size));
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| UniError::Internal(anyhow::anyhow!("Appender already finished")))?;
        writer
            .insert_vertices(&self.label, rows)
            .await
            .map_err(UniError::Internal)?;
        Ok(())
    }
}
