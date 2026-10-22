// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Helper for building Arrow property columns from row-based data.

use crate::storage::arrow_convert::PropertyExtractor;
use anyhow::Result;
use arrow_array::ArrayRef;
use arrow_array::builder::LargeBinaryBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::{Properties, Schema, Value};

/// Per-label vertex column that holds a vertex's string `ext_id`.
///
/// `ext_id` is a reserved property name, so it is never a declared column:
/// readers that assemble a vertex's undeclared properties from
/// `overflow_json` must also read this column, because the flush moves a
/// string `ext_id` here and out of the blob. See
/// [`stored_in_ext_id_column`] and [`restore_ext_id_column`].
pub const EXT_ID_COLUMN: &str = "ext_id";

/// Whether a vertex property is stored in the per-label `ext_id` column.
///
/// Only a string fits the `Utf8` column. Any other `ext_id` value stays in
/// `overflow_json`; excluding it from both dropped it at flush.
pub fn stored_in_ext_id_column(key: &str, value: &Value) -> bool {
    key == EXT_ID_COLUMN && matches!(value, Value::String(_))
}

/// Puts a row's `ext_id` column value back into its property map.
///
/// The inverse of the flush's [`stored_in_ext_id_column`] split, for every
/// reader of a per-label vertex table. A no-op when `batch` has no `ext_id`
/// column (an edge table, or a projection that did not request it) or the
/// row's value is null. The column wins over a blob entry for the same key:
/// the flush never writes both for one row.
pub fn restore_ext_id_column(batch: &arrow_array::RecordBatch, row: usize, props: &mut Properties) {
    use arrow_array::Array;
    if let Some(col) = batch
        .column_by_name(EXT_ID_COLUMN)
        .and_then(|c| c.as_any().downcast_ref::<arrow_array::StringArray>())
        && !col.is_null(row)
    {
        props.insert(
            EXT_ID_COLUMN.to_string(),
            Value::String(col.value(row).to_string()),
        );
    }
}

/// Merge decoded `overflow_json` properties into a row's already-extracted
/// declared properties, letting the **declared column win** a key collision.
///
/// The blob holds schemaless properties, and every writer excludes declared
/// keys from it (`build_overflow_json_column`), so in normal operation the two
/// key sets are disjoint and this is a plain union. A collision means the row
/// carries residue from before the property was declared, and the typed column
/// is by construction the later write -- so the column is the newer value.
///
/// Three call sites used to inline this with two different rules: the row-wise
/// reader kept the column, while the columnar reader and semantic compaction
/// let the blob overwrite it -- and compaction *persists* what it picks. No
/// writer can currently produce a colliding row, so the disagreement was latent
/// rather than live, which is exactly why it is worth stating once.
///
/// `PropertyManager::merge_overflow_into_props` still applies this rule inline:
/// it filters the blob down to the requested keys on the same pass, and routing
/// it through here would cost a `HashMap` per row on a hot read path.
pub fn merge_overflow_into(dest: &mut Properties, overflow: HashMap<String, Value>) {
    for (key, value) in overflow {
        dest.entry(key).or_insert(value);
    }
}

/// Builds property columns for a specific label/edge_type using the Schema.
pub struct PropertyColumnBuilder<'a> {
    schema: &'a Schema,
    label: &'a str,
    len: usize,
    deleted: Option<&'a [bool]>,
}

impl<'a> PropertyColumnBuilder<'a> {
    pub fn new(schema: &'a Schema, label: &'a str, len: usize) -> Self {
        Self {
            schema,
            label,
            len,
            deleted: None,
        }
    }

    pub fn with_deleted(mut self, deleted: &'a [bool]) -> Self {
        self.deleted = Some(deleted);
        self
    }

    pub fn build<F>(self, get_row_props: F) -> Result<Vec<ArrayRef>>
    where
        F: Fn(usize) -> &'a Properties,
    {
        let mut columns = Vec::new();

        if let Some(props) = self.schema.properties.get(self.label) {
            let mut sorted_props: Vec<_> = props.iter().collect();
            sorted_props.sort_by_key(|(name, _)| *name);

            let default_deleted = vec![false; self.len];
            let deleted = self.deleted.unwrap_or(&default_deleted);

            for (name, meta) in sorted_props {
                let extractor = PropertyExtractor::new(name, &meta.r#type);
                let column =
                    extractor.build_column(self.len, deleted, |i| get_row_props(i).get(name))?;
                let column = if meta.nullable {
                    column
                } else {
                    fill_deleted_nulls(column, deleted)?
                };
                columns.push(column);
            }
        }

        Ok(columns)
    }
}

/// `column` with the nulls of deleted rows replaced by an all-zero value of
/// its type (0, empty string, empty list, a struct of such).
///
/// A tombstone row carries no properties, but a declared `NOT NULL` column
/// must still hold a value in it: the table refuses a null there. Several
/// builders write a typed placeholder for a deleted row; list, map and
/// sparse-vector columns write a null, so a delete of a vertex or edge with
/// such a column failed to flush. Nulls in live rows are kept, so a missing
/// `NOT NULL` value is still refused.
fn fill_deleted_nulls(column: ArrayRef, deleted: &[bool]) -> Result<ArrayRef> {
    use arrow_array::{Array, BooleanArray};
    if column.null_count() == 0 {
        return Ok(column);
    }
    let keep: BooleanArray = (0..column.len())
        .map(|i| Some(!(deleted.get(i).copied().unwrap_or(false) && column.is_null(i))))
        .collect();
    if keep.true_count() == column.len() {
        return Ok(column);
    }
    let zeroed = arrow_array::make_array(zeroed_data(column.data_type(), column.len())?);
    Ok(arrow::compute::kernels::zip::zip(&keep, &column, &zeroed)?)
}

/// `len` all-zero, non-null values of `data_type`: [`ArrayData::new_null`]'s
/// zeroed buffers with every validity bitmap dropped, recursively.
///
/// [`ArrayData::new_null`]: arrow::array::ArrayData::new_null
fn zeroed_data(data_type: &arrow_schema::DataType, len: usize) -> Result<arrow::array::ArrayData> {
    fn strip(data: arrow::array::ArrayData) -> Result<arrow::array::ArrayData> {
        let children = data
            .child_data()
            .iter()
            .cloned()
            .map(strip)
            .collect::<Result<Vec<_>>>()?;
        Ok(data
            .into_builder()
            .nulls(None)
            .child_data(children)
            .build()?)
    }
    strip(arrow::array::ArrayData::new_null(data_type, len))
}

/// Builds an `overflow_json` column (LargeBinary) for properties not defined in the schema.
///
/// Properties present in the schema are stored as typed columns; remaining properties
/// are serialized into a JSONB binary blob per row. Rows with no overflow properties
/// produce a null entry.
///
/// # Arguments
/// * `len` - Number of rows
/// * `label_or_type` - Label (for vertices) or edge type name used to look up schema properties
/// * `schema` - The database schema
/// * `get_row_props` - Closure that returns the full property map for a given row index
/// * `stored_elsewhere` - Entries kept out of the blob because a dedicated
///   column holds them (vertices pass [`stored_in_ext_id_column`])
pub fn build_overflow_json_column<'a, F, S>(
    len: usize,
    label_or_type: &str,
    schema: &Schema,
    get_row_props: F,
    stored_elsewhere: S,
) -> Result<ArrayRef>
where
    F: Fn(usize) -> &'a Properties,
    S: Fn(&str, &Value) -> bool,
{
    let schema_props = schema.properties.get(label_or_type);
    let mut builder = LargeBinaryBuilder::new();

    for i in 0..len {
        let props = get_row_props(i);
        let mut overflow_props = HashMap::new();

        for (key, value) in props.iter() {
            if stored_elsewhere(key, value) {
                continue;
            }
            if !schema_props.is_some_and(|sp| sp.contains_key(key)) {
                overflow_props.insert(key.clone(), value.clone());
            }
        }

        if overflow_props.is_empty() {
            builder.append_null();
        } else {
            // Encode directly via the CypherValue codec. Routing through
            // `serde_json::to_value` would use `Value`'s untagged Serialize and
            // turn a temporal into its tagged-struct form (`{"Date": {..}}`),
            // which then round-trips back as a `Value::Map`, losing the type.
            let jsonb = uni_common::cypher_value_codec::encode(&Value::Map(overflow_props));
            builder.append_value(&jsonb);
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deleted row of a `NOT NULL` list, map, sparse-vector or
    /// multi-vector column gets a non-null zero value; a live row's null is
    /// kept, so a missing value is still refused.
    #[test]
    fn deleted_rows_fill_not_null_columns_and_live_nulls_stay() {
        use arrow_array::{Array, RecordBatch};
        use uni_common::core::schema::DataType;

        let types = [
            DataType::List(Box::new(DataType::Vector { dimensions: 2 })),
            DataType::SparseVector { dimensions: 8 },
            DataType::List(Box::new(DataType::String)),
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Int64)),
            DataType::Bytes,
            DataType::Vector { dimensions: 2 },
        ];
        for dt in types {
            let column = PropertyExtractor::new("p", &dt)
                .build_column(2, &[false, true], |_| None)
                .unwrap();
            let filled = fill_deleted_nulls(column, &[false, true]).unwrap();
            assert!(filled.is_null(0), "{dt:?}: a live row's null is kept");
            assert!(filled.is_valid(1), "{dt:?}: a deleted row gets a value");
            let tombstone = filled.slice(1, 1);
            let field = arrow_schema::Field::new("p", tombstone.data_type().clone(), false);
            RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![field])),
                vec![tombstone],
            )
            .unwrap_or_else(|e| panic!("{dt:?}: tombstone row is not a valid NOT NULL value: {e}"));
        }
    }

    /// The declared column wins a key collision; schemaless keys still merge.
    ///
    /// Both readers and semantic compaction share this rule, and compaction
    /// *persists* whichever value it picks, so the two former behaviours were
    /// not merely inconsistent -- one of them wrote the older value back to
    /// disk. Nothing pinned either.
    #[test]
    fn overflow_merge_keeps_the_declared_value() {
        let mut dest = Properties::from([
            ("score".to_string(), Value::Int(2)),
            ("name".to_string(), Value::String("declared".into())),
        ]);
        let overflow = HashMap::from([
            ("score".to_string(), Value::Int(1)),
            ("nickname".to_string(), Value::String("schemaless".into())),
        ]);

        merge_overflow_into(&mut dest, overflow);

        assert_eq!(dest.get("score"), Some(&Value::Int(2)), "column wins");
        assert_eq!(
            dest.get("name"),
            Some(&Value::String("declared".into())),
            "untouched declared keys survive"
        );
        assert_eq!(
            dest.get("nickname"),
            Some(&Value::String("schemaless".into())),
            "schemaless keys merge in"
        );
        assert_eq!(dest.len(), 3);
    }
}
