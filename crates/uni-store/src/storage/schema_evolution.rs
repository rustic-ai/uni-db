// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Reconcile a write batch's schema against the table it is about to hit.
//!
//! uni builds a flush batch's Arrow schema from the *declared catalog
//! properties* — [`VertexDataset::get_arrow_schema`] and its delta twin read
//! `schema.properties` and never consult the dataset. Declaring a property on
//! a label that already has flushed data therefore produces a batch carrying a
//! column the dataset has never had, which Lance rejects:
//!
//! ```text
//! Append with different schema: fields did not match,
//! missing=[], unexpected=[added_later]
//! ```
//!
//! The rejection happens in the flush's *stream* phase, so the rotated L0 is
//! never completed or truncated: it stays on `pending_flush`, nothing
//! re-flushes it, and the automatic path logs the failure as
//! `"(non-critical)"`. The label is wedged permanently and survives reopen.
//! That is issue #249.
//!
//! Dropping a property is the mirror case. Lance tolerates a column *missing*
//! from an appended batch only when the **stored** field is nullable
//! (`allow_missing_if_nullable && expected_field.nullable`), and
//! `SchemaBuilder::property` defaults to `NOT NULL` — so a dropped property
//! wedges the table with `missing=[p], unexpected=[]`.
//!
//! [`ensure_table_accepts`] closes both by widening the stored schema before
//! the write. It is deliberately placed in the two shared write helpers in
//! [`crate::storage::manager`] rather than at the DDL sites, because the
//! backend there is polymorphic: the same call covers primary, forks, the
//! fork branch-creation race, and fork-local schema overlays that no
//! primary-side pass can see.
//!
//! [`VertexDataset::get_arrow_schema`]: crate::storage::vertex::VertexDataset::get_arrow_schema

use std::sync::Arc;

use arrow_schema::{Field, Schema as ArrowSchema};

use crate::backend::StorageBackend;

/// Whether the batch being written carries every column of its table.
///
/// This is the difference between an `Append` — built from the full declared
/// schema, so a column absent from it really was dropped — and a
/// `MergeInsert` source, which deliberately carries only the touched columns
/// (a tombstone batch). Relaxing nullability on the strength of a
/// partial batch would silently drop `NOT NULL` from every untouched column in
/// the table on the first `SET`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchCoverage {
    /// Every column of the table is present; an absent column means a drop.
    Full,
    /// Only the touched columns are present; absences mean nothing.
    Partial,
}

/// Table-name prefixes whose schema is derived from declared properties.
///
/// Everything else — the `vertices` / `edges` main tables, `adjacency_*`, and
/// every index dataset — has a hardcoded field list that takes no `&Schema`,
/// so it can never legitimately drift and must never be widened.
const PROPERTY_TABLE_PREFIXES: [&str; 2] = ["vertices_", "deltas_"];

/// Columns that belong to the storage layout rather than to a user property.
///
/// A user property cannot start with `_` (rejected by `declare_property` and,
/// since the rename-bypass fix, by `rename_property`), so the underscore rule
/// covers most of these; the rest are named explicitly.
const SYSTEM_COLUMNS: [&str; 6] = ["ext_id", "overflow_json", "src_vid", "dst_vid", "eid", "op"];

fn is_system_column(name: &str) -> bool {
    name.starts_with('_') || SYSTEM_COLUMNS.contains(&name)
}

fn is_property_table(table_name: &str) -> bool {
    PROPERTY_TABLE_PREFIXES
        .iter()
        .any(|p| table_name.starts_with(p))
}

/// Widen `table_name`'s stored schema so it accepts `batch_schema`.
///
/// Widening-only, and only for the drift the catalog can legitimately
/// produce. A no-op — cheap, one cached schema lookup — whenever the table
/// already accepts the batch, which is every write on a store that has not had
/// a property declared since its last flush.
///
/// # Errors
///
/// - a **system** column is missing from a full batch, or present in the batch
///   and absent from the table. Either means the table is corrupt or written
///   by an incompatible layout, and silently synthesising a NULL column would
///   convert that into a wrong answer;
/// - a property column exists with a different type (a re-declaration that
///   needs a real migration);
/// - the widening itself fails.
pub async fn ensure_table_accepts(
    backend: &dyn StorageBackend,
    table_name: &str,
    batch_schema: &ArrowSchema,
    coverage: BatchCoverage,
) -> anyhow::Result<()> {
    if !is_property_table(table_name) {
        return Ok(());
    }
    // Absent table: the write itself creates it from this very schema.
    let Some(stored) = backend.get_table_schema(table_name).await? else {
        return Ok(());
    };

    let mut add: Vec<Field> = Vec::new();
    for field in batch_schema.fields() {
        if stored.fields().iter().any(|f| f.name() == field.name()) {
            continue;
        }
        if is_system_column(field.name()) {
            anyhow::bail!(
                "table '{}' has no system column '{}' that the write batch carries. \
                 This is not property drift — the table was written by an incompatible \
                 storage layout, and creating the column would hide that rather than \
                 report it.",
                table_name,
                field.name()
            );
        }
        add.push(field.as_ref().clone());
    }

    let mut relax_nullable: Vec<String> = Vec::new();
    if coverage == BatchCoverage::Full {
        for field in stored.fields() {
            if field.is_nullable() || batch_schema.column_with_name(field.name()).is_some() {
                continue;
            }
            if is_system_column(field.name()) {
                anyhow::bail!(
                    "write batch for '{}' omits the required system column '{}'. \
                     Refusing to relax it to nullable: a missing system column is a \
                     layout error, not a dropped property.",
                    table_name,
                    field.name()
                );
            }
            relax_nullable.push(field.name().clone());
        }
    }

    if add.is_empty() && relax_nullable.is_empty() {
        return Ok(());
    }

    // Loud on purpose. The widening is correct, but it also means the catalog
    // and the dataset had drifted — and if that ever happens for a reason
    // other than a property declaration, this line is the only evidence.
    tracing::warn!(
        table = table_name,
        added = ?add.iter().map(|f| f.name()).collect::<Vec<_>>(),
        relaxed = ?relax_nullable,
        "widening stored schema to accept a write batch (issue #249)"
    );
    metrics::counter!(
        "uni_schema_reconcile_columns_added_total",
        "table" => table_name.to_string()
    )
    .increment((add.len() + relax_nullable.len()) as u64);

    backend
        .evolve_table_schema(table_name, &add, &relax_nullable)
        .await
}

/// Move a newly declared property's values out of the `overflow_json` blob and
/// into its typed column.
///
/// An **undeclared** property is stored in the per-row `overflow_json` blob.
/// The projected read path prefers a typed column when one exists and falls
/// back to the blob only when it does not — so materialising an all-NULL
/// column for a property that already has schemaless data makes
/// `RETURN n.p` start answering NULL while `RETURN properties(n)`, which
/// coalesces through `build_all_props_column_for_schema_scan`, still answers
/// with the real value. Two read paths, two answers, no error.
///
/// That is why widening alone is not enough: it would trade #249's loud
/// failure for a silent wrong answer.
///
/// Returns the number of values moved. Zero means nothing was rewritten — the
/// common declare-then-ingest case costs one scan and no commit.
///
/// # Ordering and safety
///
/// - The per-table write lock is held across **both** the scan and the
///   replace. `replace_table_atomic` overwrites the whole table, so a flush
///   landing in between would be silently dropped — durable loss of committed
///   data (the issue-#96 shape). `write` takes the same lock internally.
/// - The merge is **typed-wins**: a row whose typed column already holds a
///   value keeps it, and only NULLs are filled from the blob. A write can
///   legitimately land a real value before the lock is acquired, and
///   "overflow wins" would clobber it. This mirrors the declared-column-wins
///   rule the property builder already documents.
/// - The key is **stripped** from the blob. Leaving a shadow copy behind means
///   a later `SET n.p = NULL` reads back the stale blob value through the
///   `_all_props` coalesce — the value would come back from the dead.
///
/// # Errors
///
/// Propagates a corrupt overflow payload, and any scan or replace failure.
pub async fn backfill_property_from_overflow(
    backend: &dyn StorageBackend,
    table_name: &str,
    prop: &str,
    data_type: &uni_common::core::schema::DataType,
) -> anyhow::Result<usize> {
    use arrow_array::{Array, LargeBinaryArray, RecordBatch};
    use uni_common::Value;
    use uni_common::cypher_value_codec as codec;

    use crate::backend::types::ScanRequest;
    use crate::storage::arrow_convert::PropertyExtractor;

    if !backend.table_exists(table_name).await? {
        return Ok(0);
    }

    // Held across scan *and* replace. See the note above.
    let _guard = backend.lock_table_for_write(table_name).await;

    let batches = backend.scan(ScanRequest::all(table_name)).await?;
    if batches.is_empty() {
        return Ok(0);
    }
    let batch_schema = batches[0].schema();
    if batch_schema.column_with_name(prop).is_none()
        || batch_schema.column_with_name("overflow_json").is_none()
    {
        return Ok(0);
    }
    let prop_idx = batch_schema.index_of(prop)?;
    let overflow_idx = batch_schema.index_of("overflow_json")?;

    let mut moved = 0usize;
    let mut rebuilt: Vec<RecordBatch> = Vec::with_capacity(batches.len());

    for batch in &batches {
        let rows = batch.num_rows();
        let overflow = batch
            .column(overflow_idx)
            .as_any()
            .downcast_ref::<LargeBinaryArray>();

        let mut from_blob: Vec<Option<Value>> = Vec::with_capacity(rows);
        let mut new_overflow = arrow_array::builder::LargeBinaryBuilder::new();

        for row in 0..rows {
            let raw =
                crate::runtime::columnar_scan::extract_from_overflow_blob(overflow, row, prop)?;
            match raw {
                Some(bytes) => {
                    // In the form a write of the declared property stores.
                    // `check_declared_values` refused the declaration if any
                    // value could not be, so a failure here is a value that
                    // arrived since; refuse rather than store it as NULL.
                    let value = crate::storage::declared_values::coerce(
                        prop,
                        data_type,
                        codec::decode(&bytes)?,
                    )
                    .map_err(|e| anyhow::anyhow!("cannot promote '{prop}' in {table_name}: {e}"))?;
                    from_blob.push(Some(value));
                    moved += 1;
                    // Re-encode the blob without the promoted key.
                    let arr = overflow.expect("a raw entry implies a blob");
                    match codec::decode(arr.value(row))? {
                        Value::Map(mut m) => {
                            m.remove(prop);
                            if m.is_empty() {
                                new_overflow.append_null();
                            } else {
                                new_overflow.append_value(codec::encode(&Value::Map(m)));
                            }
                        }
                        // Not a map: leave it untouched rather than guess.
                        _ => new_overflow.append_value(arr.value(row)),
                    }
                }
                None => {
                    from_blob.push(None);
                    match overflow {
                        Some(a) if !a.is_null(row) => new_overflow.append_value(a.value(row)),
                        _ => new_overflow.append_null(),
                    }
                }
            }
        }

        let no_deletes = vec![false; rows];
        let blob_column =
            PropertyExtractor::new(prop, data_type)
                .build_column(rows, &no_deletes, |i| from_blob[i].as_ref())?;

        // Typed-wins: keep every non-NULL stored value, fill the NULLs.
        let existing = batch.column(prop_idx);
        let keep_existing = arrow::compute::is_not_null(existing.as_ref())?;
        let merged = arrow::compute::kernels::zip::zip(&keep_existing, existing, &blob_column)?;

        let mut columns = batch.columns().to_vec();
        columns[prop_idx] = merged;
        columns[overflow_idx] = Arc::new(new_overflow.finish());
        rebuilt.push(RecordBatch::try_new(batch_schema.clone(), columns)?);
    }

    if moved == 0 {
        // Nothing to promote: skip the rewrite entirely. This is the ordinary
        // declare-a-brand-new-property case.
        return Ok(0);
    }

    tracing::info!(
        table = table_name,
        property = prop,
        values = moved,
        "promoting a schemaless property into its typed column (issue #249)"
    );
    backend
        .replace_table_atomic(table_name, rebuilt, batch_schema)
        .await?;
    Ok(moved)
}

/// Convenience wrapper taking an `Arc`'d schema, as the write helpers hold.
pub async fn ensure_table_accepts_batch(
    backend: &dyn StorageBackend,
    table_name: &str,
    batch_schema: &Arc<ArrowSchema>,
    coverage: BatchCoverage,
) -> anyhow::Result<()> {
    ensure_table_accepts(backend, table_name, batch_schema.as_ref(), coverage).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    #[test]
    fn only_property_tables_are_widened() {
        assert!(is_property_table("vertices_Person"));
        assert!(is_property_table("deltas_KNOWS_fwd"));
        // The main tables carry `props_json`, not per-property columns, and
        // their schemas are hardcoded.
        assert!(!is_property_table("vertices"));
        assert!(!is_property_table("edges"));
        assert!(!is_property_table("adjacency_KNOWS_fwd"));
    }

    #[test]
    fn system_columns_are_recognised_on_both_layouts() {
        for c in [
            "_vid",
            "_uid",
            "_deleted",
            "_version",
            "_labels",
            "_created_at",
            "_updated_at",
            "ext_id",
            "overflow_json",
            "src_vid",
            "dst_vid",
            "eid",
            "op",
        ] {
            assert!(is_system_column(c), "{c} should be a system column");
        }
        assert!(!is_system_column("name"));
        assert!(!is_system_column("added_later"));
    }

    /// The distinction that keeps a `SET` from stripping `NOT NULL` off every
    /// untouched column: a partial batch's absences carry no information.
    #[test]
    fn partial_coverage_never_relaxes() {
        let stored = ArrowSchema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("num", DataType::Int64, false),
        ]);
        let partial = ArrowSchema::new(vec![
            Field::new("_vid", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        // `num` is absent and NOT NULL, but the batch is partial.
        let relax: Vec<_> = stored
            .fields()
            .iter()
            .filter(|f| {
                !f.is_nullable()
                    && partial.column_with_name(f.name()).is_none()
                    && !is_system_column(f.name())
            })
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(relax, vec!["num".to_string()]);
        // …which `ensure_table_accepts` only acts on under `Full`.
        assert_eq!(BatchCoverage::Partial, BatchCoverage::Partial);
    }
}
