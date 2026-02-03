// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Helper for building Arrow property columns from row-based data.

use crate::storage::arrow_convert::PropertyExtractor;
use anyhow::Result;
use arrow_array::ArrayRef;
use uni_common::{Properties, Schema};

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
