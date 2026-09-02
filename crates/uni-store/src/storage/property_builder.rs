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
                columns.push(column);
            }
        }

        Ok(columns)
    }
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
/// * `skip_keys` - Additional property keys to exclude (e.g., `"ext_id"` for vertices)
pub fn build_overflow_json_column<'a, F>(
    len: usize,
    label_or_type: &str,
    schema: &Schema,
    get_row_props: F,
    skip_keys: &[&str],
) -> Result<ArrayRef>
where
    F: Fn(usize) -> &'a Properties,
{
    let schema_props = schema.properties.get(label_or_type);
    let mut builder = LargeBinaryBuilder::new();

    for i in 0..len {
        let props = get_row_props(i);
        let mut overflow_props = HashMap::new();

        for (key, value) in props.iter() {
            if skip_keys.contains(&key.as_str()) {
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
