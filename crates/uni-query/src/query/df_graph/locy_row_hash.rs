// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `__locy_row_hash(col, ...)`: a fixed-width identity of a referenced derived
//! row, for a recursive FOLD / ALONG rule's derivation discriminators (#159).
//!
//! A derivation of such a rule is told apart from another by its clause, the
//! nodes and relationships its MATCH bound — and, since an extension of a fact
//! is a different derivation from an extension of another fact, by the
//! referenced fact itself. The last is this column: a hash of every column of
//! the referenced row, hidden discriminators included, so the identity chains
//! through the whole derivation at a fixed width. Without it two paths that
//! diverged below the first hop and carried equal values were one fact.

// Rust guideline compliant

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_schema::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::{ColumnarValue, DisplayAs, DisplayFormatType, PhysicalExpr};

/// The marker function name the Locy planner emits.
pub(crate) const ROW_HASH_FN: &str = "__locy_row_hash";

/// The identity's width: two independent 64-bit hashes.
pub(crate) const ROW_HASH_WIDTH: i32 = 16;

/// The identity column's type.
pub(crate) fn row_hash_type() -> DataType {
    DataType::FixedSizeBinary(ROW_HASH_WIDTH)
}

/// The identity `__locy_row_hash` gives row `row` of `columns`.
///
/// Exposed so a runtime component can recompute, from a rule's own facts, the
/// identity a clause body projected for a referenced fact: both sides hash the
/// same values in the same order with the same keys.
///
/// # Errors
///
/// Fails when a value cannot be read from a column.
pub(crate) fn row_hash(
    columns: &[arrow_array::ArrayRef],
    row: usize,
) -> datafusion::error::Result<[u8; ROW_HASH_WIDTH as usize]> {
    let mut low = std::collections::hash_map::DefaultHasher::new();
    let mut high = std::collections::hash_map::DefaultHasher::new();
    high.write_u8(0x5a);
    for column in columns {
        let value = ScalarValue::try_from_array(column, row)?;
        value.hash(&mut low);
        value.hash(&mut high);
    }
    let mut bytes = [0u8; ROW_HASH_WIDTH as usize];
    bytes[..8].copy_from_slice(&low.finish().to_le_bytes());
    bytes[8..].copy_from_slice(&high.finish().to_le_bytes());
    Ok(bytes)
}

/// Hashes its children's values row by row into a 16-byte identity.
///
/// Values are hashed through `ScalarValue`'s `Hash`, which is defined for every
/// type, so no column type is unsupported. The hashers have fixed keys, so the
/// identity is stable within a process — which is all a fixpoint needs.
#[derive(Debug, Eq)]
pub(crate) struct RowHashExpr {
    children: Vec<Arc<dyn PhysicalExpr>>,
}

impl RowHashExpr {
    pub(crate) fn new(children: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self { children }
    }
}

impl PartialEq for RowHashExpr {
    fn eq(&self, other: &Self) -> bool {
        self.children.len() == other.children.len()
            && self
                .children
                .iter()
                .zip(&other.children)
                .all(|(a, b)| a.eq(b))
    }
}

impl Hash for RowHashExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for c in &self.children {
            c.hash(state);
        }
    }
}

impl std::fmt::Display for RowHashExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{ROW_HASH_FN}(")?;
        for (i, c) in self.children.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{c}")?;
        }
        write!(f, ")")
    }
}

impl DisplayAs for RowHashExpr {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl PartialEq<dyn PhysicalExpr> for RowHashExpr {
    fn eq(&self, other: &dyn PhysicalExpr) -> bool {
        other.downcast_ref::<Self>().is_some_and(|o| self == o)
    }
}

impl PhysicalExpr for RowHashExpr {
    fn data_type(&self, _input_schema: &Schema) -> datafusion::error::Result<DataType> {
        Ok(row_hash_type())
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        // Never NULL itself, but the column it fills is: a sibling clause
        // that has no such identity projects a typed NULL into it, and the
        // clauses' batches are unioned under the first one's schema.
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> datafusion::error::Result<ColumnarValue> {
        let rows = batch.num_rows();
        let columns = self
            .children
            .iter()
            .map(|c| c.evaluate(batch)?.into_array(rows))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let mut out = FixedSizeBinaryBuilder::with_capacity(rows, ROW_HASH_WIDTH);
        for row in 0..rows {
            out.append_value(row_hash(&columns, row)?)?;
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(children)))
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}
