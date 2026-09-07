//! Built-in per-label `Storage` reference implementation.
//!
//! [`MemoryStorage`] is a real in-memory [`Storage`] implementation used
//! by the conformance suite and by [`super::storage_table_provider`] to
//! back a native label from plugin storage. [`LanceFilterPushdown`] is the
//! public pushdown primitive for Lance-backed `TableProvider`s.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use parking_lot::RwLock;
use uni_plugin::FnError;
use uni_plugin::traits::storage::{Storage, WriteHandle};

/// Evaluate `predicate` against every row of `batches` and return only
/// the rows that survive.
///
/// Uses DataFusion's [`DefaultPhysicalPlanner::create_physical_expr`]
/// to lower the logical [`Expr`] against the table schema, then
/// invokes `arrow::compute::filter_record_batch` per batch. Failures
/// in lowering (custom UDFs the planner cannot resolve, etc.) bubble
/// up as [`FnError`] `0x711` — the wire-format the plugin Storage
/// contract reserves for "predicate not encodable by this backend",
/// so callers fall back to a DataFusion `Filter` above the unfiltered
/// scan.
///
/// # Errors
///
/// Returns [`FnError`] code `0x711` for unencodable predicates, or
/// `0x713` for runtime evaluation failures (schema mismatch, type
/// errors at evaluation time).
fn apply_predicate_in_memory(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    predicate: &Expr,
) -> Result<Vec<RecordBatch>, FnError> {
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::common::DFSchema;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::create_physical_expr as build_physical_expr;
    use std::sync::Arc as StdArc;

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(|e| {
        FnError::new(
            0x711,
            format!("memory backend: schema lowering failed: {e}"),
        )
    })?;
    let exec_props = datafusion::execution::context::ExecutionProps::new();
    let physical: StdArc<dyn PhysicalExpr> =
        build_physical_expr(predicate, &df_schema, &exec_props).map_err(|e| {
            FnError::new(
                0x711,
                format!("memory backend: predicate not encodable (physical lowering failed): {e}"),
            )
        })?;

    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let evaluated = physical.evaluate(batch).map_err(|e| {
            FnError::new(0x713, format!("memory backend: predicate eval failed: {e}"))
        })?;
        let arr = evaluated.into_array(batch.num_rows()).map_err(|e| {
            FnError::new(
                0x713,
                format!("memory backend: predicate result-to-array failed: {e}"),
            )
        })?;
        let mask = arr
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or_else(|| {
                FnError::new(
                    0x713,
                    "memory backend: predicate did not produce BooleanArray".to_owned(),
                )
            })?;
        let kept = filter_record_batch(batch, mask).map_err(|e| {
            FnError::new(
                0x713,
                format!("memory backend: filter_record_batch failed: {e}"),
            )
        })?;
        if kept.num_rows() > 0 {
            out.push(kept);
        }
    }
    Ok(out)
}

/// In-memory `Storage` — keeps every batch per table in a `Vec`.
///
/// Real implementation: write appends a batch, read concatenates and
/// streams. Filter pushdown is honored on a best-effort basis: when a
/// predicate is supplied, [`MemoryStorage::read_batch`] evaluates it
/// against each batch via the standard DataFusion physical-expr
/// planner and returns the surviving rows. Expressions the planner
/// cannot lower (custom UDFs missing from the planner state, etc.)
/// surface as [`FnError`] `0x711` so callers can fall back to a
/// DataFusion-side `FilterExec` above an unfiltered scan — matching
/// the contract documented on [`Storage::read_batch`]. Schema is
/// derived from the first written batch.
#[derive(Debug)]
pub struct MemoryStorage {
    tables: RwLock<HashMap<String, Vec<RecordBatch>>>,
    next_handle: std::sync::atomic::AtomicU64,
}

impl MemoryStorage {
    /// Construct a fresh, empty in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            next_handle: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn read_batch(
        &self,
        table: &str,
        predicate: Option<&Expr>,
    ) -> Result<SendableRecordBatchStream, FnError> {
        let tables = self.tables.read();
        // #233 Tier 1: a missing table used to yield zero rows and an EMPTY
        // schema, indistinguishable from a table that exists and is empty —
        // so a typo'd or not-yet-created table read as "no data" rather than
        // "no such table".
        let Some(batches) = tables.get(table).cloned() else {
            return Err(FnError::new(
                0x712,
                format!("MemoryStorage: no such table `{table}`"),
            ));
        };
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));

        // Apply predicate at scan time. The plugin Storage contract says
        // a backend that cannot encode the predicate must surface
        // `FnError 0x711`; the in-memory backend trivially supports every
        // predicate DataFusion can lower against the table's schema, so
        // we route through the standard physical-expr planner and
        // surface `0x711` only for the lowering-failed branch.
        let filtered = match predicate {
            None => batches,
            Some(expr) => apply_predicate_in_memory(&schema, &batches, expr)?,
        };

        let owned_batches: Vec<datafusion::common::Result<RecordBatch>> =
            filtered.into_iter().map(Ok).collect();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(owned_batches),
        )))
    }
    async fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<WriteHandle, FnError> {
        self.tables
            .write()
            .entry(table.to_owned())
            .or_default()
            .push(batch.clone());
        let id = self
            .next_handle
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(WriteHandle { id })
    }
    async fn list_tables(&self) -> Result<Vec<String>, FnError> {
        Ok(self.tables.read().keys().cloned().collect())
    }
    async fn delete(&self, table: &str, _predicate: &Expr) -> Result<u64, FnError> {
        // M5a in-memory backend: predicate-less delete clears the table.
        // Real predicate evaluation will arrive when the plugin Storage
        // trait grows a predicate-encoding contract (deferred follow-up).
        let mut tables = self.tables.write();
        let count = tables
            .get(table)
            .map(|b| b.iter().map(|x| x.num_rows() as u64).sum::<u64>())
            .unwrap_or(0);
        tables.remove(table);
        Ok(count)
    }
    async fn schema(&self, table: &str) -> Option<SchemaRef> {
        self.tables
            .read()
            .get(table)
            .and_then(|v| v.first().map(|b| b.schema()))
    }
}

/// Encode a DataFusion [`Expr`] as a Lance-compatible SQL filter string.
///
/// Delegates to `datafusion::sql::unparser::expr_to_sql`, which renders the
/// expression as a [`sqlparser::ast::Expr`]; `Display` produces the textual
/// SQL Lance's `filter` parameter accepts.
///
/// # Errors
///
/// Returns [`FnError`] with code `0x711` when the expression contains a
/// shape the unparser can't render (custom UDFs, certain extension types,
/// etc.). Callers should fall back to a DataFusion `Filter` operator
/// above the unfiltered scan in that case.
#[cfg(feature = "lance-backend")]
fn expr_to_lance_filter(expr: &Expr) -> Result<String, FnError> {
    match datafusion::sql::unparser::expr_to_sql(expr) {
        Ok(sql_expr) => Ok(sql_expr.to_string()),
        Err(e) => Err(FnError::new(
            0x711,
            format!(
                "lance plugin adapter: predicate not encodable to Lance SQL \
                 (datafusion unparser: {e}); wrap in DataFusion Filter above"
            ),
        )),
    }
}

// ============================================================================
// M5h — Pushdown marker for Lance-backed table providers.
//
// `LanceFilterPushdown` is a public [`SupportsFilterPushdown`] impl that
// classifies filters via [`expr_to_lance_filter`]: predicates the
// DataFusion unparser can render to Lance-compatible SQL land in
// `fully_handled`; predicates that fail to render are omitted (left for
// a DataFusion `Filter` operator above the scan to enforce).
//
// Wrap it through [`super::PushdownAwareTable::with_filter`] when
// constructing a Lance-backed `TableProvider` so the optimizer's
// `PushdownNegotiationRule` can elide the redundant `Filter`:
//
// ```ignore
// let provider = PushdownAwareTable::with_filter(
//     my_lance_table_provider,
//     Arc::new(LanceFilterPushdown),
// );
// ctx.register_table("t", Arc::new(provider))?;
// ```
//
// Note: M5h does NOT today auto-wrap built-in `TableProvider`s — the
// plugin `Storage` layer (`Storage::read_batch`) is separate from
// DataFusion's `TableProvider`, and no built-in bridges between them
// (uni-query has its own physical execution path for graph rows). This
// marker is therefore the *public* pushdown primitive users / extension
// crates plug into when they construct their own Lance-backed
// `TableProvider`s; building the in-tree bridge is its own scope.
// ============================================================================

/// `SupportsFilterPushdown` marker for Lance-backed `TableProvider`s.
///
/// Delegates to `expr_to_lance_filter` (which uses
/// `datafusion::sql::unparser::expr_to_sql`) to determine which filters
/// Lance can encode in its `ScanRequest::with_filter`. Encodable filters
/// are reported as `fully_handled`; unencodable filters are omitted so
/// the optimizer keeps a verifying `Filter` operator above the scan.
///
/// Stateless and `Sync` — wrap it in `Arc` once and reuse across all
/// Lance-backed `TableProvider`s.
#[cfg(feature = "lance-backend")]
#[derive(Debug, Default)]
pub struct LanceFilterPushdown;

#[cfg(feature = "lance-backend")]
impl uni_plugin::traits::pushdown::SupportsFilterPushdown for LanceFilterPushdown {
    fn push_filters(&self, filters: &[Expr]) -> uni_plugin::traits::pushdown::FilterApplication {
        let fully_handled = filters
            .iter()
            .enumerate()
            .filter(|(_, expr)| expr_to_lance_filter(expr).is_ok())
            .map(|(idx, _)| idx)
            .collect();
        uni_plugin::traits::pushdown::FilterApplication {
            fully_handled,
            partially_handled: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_storage_list_tables_returns_empty() {
        let s = MemoryStorage::new();
        assert!(s.list_tables().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn memory_storage_write_then_list_includes_table() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let s = MemoryStorage::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
        )
        .unwrap();
        let h = s.write_batch("people", &batch).await.unwrap();
        assert!(h.id >= 1);

        let tables = s.list_tables().await.unwrap();
        assert_eq!(tables, vec!["people".to_owned()]);
        assert_eq!(s.schema("people").await.unwrap().as_ref(), schema.as_ref());
    }

    #[tokio::test]
    async fn memory_storage_delete_removes_table() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let s = MemoryStorage::new();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2]))]).unwrap();
        s.write_batch("t", &batch).await.unwrap();

        let predicate = Expr::Literal(datafusion::scalar::ScalarValue::Boolean(Some(true)), None);
        let deleted = s.delete("t", &predicate).await.unwrap();
        assert_eq!(deleted, 2);
        assert!(s.list_tables().await.unwrap().is_empty());
    }

    #[cfg(feature = "lance-backend")]
    #[tokio::test]
    async fn lance_predicate_pushdown_unsupported_returns_711() {
        // Direct exercise of the encoder. `ScalarValue::Binary(Some(_))`
        // has no SQL literal form so `datafusion::sql::unparser::expr_to_sql`
        // returns `not_impl_err`. We confirm the wrapper surfaces this as
        // `FnError 0x711` so callers can fall back to a DataFusion Filter
        // above the unfiltered scan.
        let predicate = Expr::Literal(
            datafusion::scalar::ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef])),
            None,
        );

        let err =
            expr_to_lance_filter(&predicate).expect_err("binary literal must fail the unparser");
        assert_eq!(err.code, 0x711, "expected 0x711, got {err:?}");
        assert!(
            err.message.contains("predicate not encodable"),
            "expected message to mention encoding failure, got {err:?}"
        );
    }

    #[cfg(feature = "lance-backend")]
    #[test]
    fn lance_filter_pushdown_classifies_encodable_and_unencodable() {
        use datafusion::common::Column;
        use datafusion::logical_expr::{BinaryExpr, Operator};
        use datafusion::scalar::ScalarValue;
        use uni_plugin::traits::pushdown::SupportsFilterPushdown;

        // Encodable: `col = 'foo'` round-trips through the SQL unparser
        // and lands in `fully_handled`.
        let encodable = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column(Column::from_name("col"))),
            Operator::Eq,
            Box::new(Expr::Literal(ScalarValue::Utf8(Some("foo".into())), None)),
        ));
        // Unencodable: a binary-literal predicate hits the same shape as
        // `lance_predicate_pushdown_unsupported_returns_711` above; the
        // unparser declines, so it must be omitted from `fully_handled`.
        let unencodable = Expr::Literal(
            ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef])),
            None,
        );

        let marker = LanceFilterPushdown;
        let filters = vec![encodable, unencodable];
        let app = marker.push_filters(&filters);
        assert_eq!(
            app.fully_handled,
            vec![0],
            "only the encodable predicate (index 0) should be fully_handled"
        );
        assert!(
            app.partially_handled.is_empty(),
            "LanceFilterPushdown never reports partial handling"
        );
    }
}
