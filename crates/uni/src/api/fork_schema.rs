// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Fork-local schema mutation API.
//!
//! `Session::fork_schema()` returns a [`ForkSchemaBuilder`] that adds
//! labels and edge types to the fork's *overlay* — the
//! `SchemaDelta` persisted under `catalog/fork_schemas/{fork_id}.json`
//! — and to the fork's in-memory `SchemaManager` so subsequent writes
//! on the fork see the addition immediately. Primary's
//! `catalog/schema.json` is **never** touched.
//!
//! This is the strict-schema-mode counterpart to the schemaless
//! on-the-fly dataset/branch creation in
//! `uni_store::backend::BranchedBackend`: under
//! `UniConfig.strict_schema = true`, an unknown label is rejected
//! before reaching the backend, so the only way for a fork to
//! introduce its own labels without leaking into primary is to grow
//! the overlay.

// Rust guideline compliant

use std::sync::Arc;

use uni_common::core::schema::{EdgeTypeMeta, LabelMeta};
use uni_common::{Result, UniError};

use super::session::Session;

/// Pending change accumulated by [`ForkSchemaBuilder`].
enum ForkSchemaChange {
    AddLabel {
        name: String,
        description: Option<String>,
    },
    AddEdgeType {
        name: String,
        from_labels: Vec<String>,
        to_labels: Vec<String>,
        description: Option<String>,
    },
}

/// Builder for fork-local schema additions.
///
/// Built by [`Session::fork_schema`]. Drop without `.apply()` to
/// discard pending changes; once `.apply()` is awaited the changes
/// land both in the fork's in-memory `SchemaManager` and in the
/// persisted overlay file.
///
/// # Errors
///
/// `apply()` returns:
/// - `UniError::InvalidArgument` if called on a non-forked session
///   (this builder is only valid via `Session::fork_schema()`).
/// - `UniError::Schema` for in-memory `SchemaManager` rejections
///   (e.g. duplicate label).
/// - `UniError::ForkLifecycle { stage: "overlay_persist", .. }` if
///   the overlay PUT to `catalog/fork_schemas/{fork_id}.json` fails.
#[must_use = "fork-schema builders do nothing until .apply() is awaited"]
pub struct ForkSchemaBuilder<'a> {
    session: &'a Session,
    pending: Vec<ForkSchemaChange>,
}

impl<'a> ForkSchemaBuilder<'a> {
    pub(crate) fn new(session: &'a Session) -> Self {
        Self {
            session,
            pending: Vec::new(),
        }
    }

    /// Begin a label addition.
    pub fn label(self, name: &str) -> ForkLabelBuilder<'a> {
        ForkLabelBuilder {
            builder: self,
            name: name.to_string(),
            description: None,
        }
    }

    /// Begin an edge-type addition.
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> ForkEdgeTypeBuilder<'a> {
        ForkEdgeTypeBuilder {
            builder: self,
            name: name.to_string(),
            from_labels: from.iter().map(|s| s.to_string()).collect(),
            to_labels: to.iter().map(|s| s.to_string()).collect(),
            description: None,
        }
    }

    /// Apply all accumulated changes atomically. Each change is
    /// persisted to the overlay before the next is staged in memory,
    /// so a mid-batch failure leaves the fork in a consistent state
    /// (the partial additions before the failure remain, both
    /// in-memory and on disk).
    pub async fn apply(self) -> Result<()> {
        let scope = self
            .session
            .fork_scope()
            .ok_or_else(|| UniError::InvalidArgument {
                arg: "session".into(),
                message:
                    "fork_schema() requires a forked session; use db.schema() for primary schema changes"
                        .into(),
            })?;
        let manager = self.session.db().schema.clone();
        for change in self.pending {
            match change {
                ForkSchemaChange::AddLabel { name, description } => {
                    // Mutate the fork's in-memory SchemaManager. This
                    // does NOT call save() — that would write to
                    // primary's catalog/schema.json. The fork's
                    // SchemaManager shares primary's path field but
                    // we never call .save() on the fork side, by
                    // contract.
                    manager
                        .add_label_with_desc(&name, description.clone())
                        .map_err(|e| UniError::Schema {
                            message: e.to_string(),
                        })?;
                    let meta = manager.schema().labels.get(&name).cloned().ok_or_else(|| {
                        UniError::Internal(anyhow::anyhow!(
                            "fork_schema: label {name} not visible in fork SchemaManager after add"
                        ))
                    })?;
                    // Vertices the fork (or primary, before the fork point)
                    // created under this label schemalessly live only in the
                    // main table; copy them into the label's table on the
                    // fork's branch, or label-scoped reads lose them (#304).
                    // Append-only, so primary is untouched. Done before the
                    // overlay persists: the overlay is the fork's only durable
                    // schema record, so a crash before it leaves the label
                    // undeclared and its rows still read from the main table,
                    // and a re-declaration backfills again.
                    self.session
                        .db()
                        .backfill_declared_label(&name)
                        .await
                        .map_err(UniError::Internal)?;
                    persist_label(&scope, name.clone(), meta).await?;
                }
                ForkSchemaChange::AddEdgeType {
                    name,
                    from_labels,
                    to_labels,
                    description,
                } => {
                    manager
                        .add_edge_type_with_desc(
                            &name,
                            from_labels.clone(),
                            to_labels.clone(),
                            description.clone(),
                        )
                        .map_err(|e| UniError::Schema {
                            message: e.to_string(),
                        })?;
                    let meta = manager
                        .schema()
                        .edge_types
                        .get(&name)
                        .cloned()
                        .ok_or_else(|| UniError::Internal(anyhow::anyhow!(
                            "fork_schema: edge type {name} not visible in fork SchemaManager after add"
                        )))?;
                    persist_edge_type(&scope, name.clone(), meta).await?;
                }
            }
        }
        Ok(())
    }
}

async fn persist_label(
    scope: &Arc<uni_store::fork::ForkScope>,
    name: String,
    meta: LabelMeta,
) -> Result<()> {
    scope
        .add_label_to_overlay(name, meta)
        .await
        .map_err(|e| UniError::ForkLifecycle {
            name: format!("<fork:{}>", scope.fork_id()),
            stage: "overlay_persist",
            source: e.into(),
        })
}

async fn persist_edge_type(
    scope: &Arc<uni_store::fork::ForkScope>,
    name: String,
    meta: EdgeTypeMeta,
) -> Result<()> {
    scope
        .add_edge_type_to_overlay(name, meta)
        .await
        .map_err(|e| UniError::ForkLifecycle {
            name: format!("<fork:{}>", scope.fork_id()),
            stage: "overlay_persist",
            source: e.into(),
        })
}

#[must_use = "builders do nothing until .done() or .apply() is awaited"]
pub struct ForkLabelBuilder<'a> {
    builder: ForkSchemaBuilder<'a>,
    name: String,
    description: Option<String>,
}

impl<'a> ForkLabelBuilder<'a> {
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    fn finish(mut self) -> ForkSchemaBuilder<'a> {
        self.builder.pending.push(ForkSchemaChange::AddLabel {
            name: self.name,
            description: self.description,
        });
        self.builder
    }

    /// Continue with another label.
    pub fn label(self, name: &str) -> ForkLabelBuilder<'a> {
        self.finish().label(name)
    }

    /// Continue with an edge type.
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> ForkEdgeTypeBuilder<'a> {
        self.finish().edge_type(name, from, to)
    }

    /// Apply this label (and any prior pending changes).
    pub async fn apply(self) -> Result<()> {
        self.finish().apply().await
    }
}

#[must_use = "builders do nothing until .done() or .apply() is awaited"]
pub struct ForkEdgeTypeBuilder<'a> {
    builder: ForkSchemaBuilder<'a>,
    name: String,
    from_labels: Vec<String>,
    to_labels: Vec<String>,
    description: Option<String>,
}

impl<'a> ForkEdgeTypeBuilder<'a> {
    pub fn description(mut self, desc: &str) -> Self {
        self.description = Some(desc.to_string());
        self
    }

    fn finish(mut self) -> ForkSchemaBuilder<'a> {
        self.builder.pending.push(ForkSchemaChange::AddEdgeType {
            name: self.name,
            from_labels: self.from_labels,
            to_labels: self.to_labels,
            description: self.description,
        });
        self.builder
    }

    /// Continue with a label.
    pub fn label(self, name: &str) -> ForkLabelBuilder<'a> {
        self.finish().label(name)
    }

    /// Continue with another edge type.
    pub fn edge_type(self, name: &str, from: &[&str], to: &[&str]) -> ForkEdgeTypeBuilder<'a> {
        self.finish().edge_type(name, from, to)
    }

    /// Apply this edge type (and any prior pending changes).
    pub async fn apply(self) -> Result<()> {
        self.finish().apply().await
    }
}
