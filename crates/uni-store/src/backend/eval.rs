// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Native evaluation of [`FilterExpr`] — no SQL, no DataFusion.
//!
//! This is the half of the structured-filter work that makes a non-Lance
//! backend possible. [`FilterExpr::to_sql`] lets a SQL-speaking backend keep
//! doing what it does; [`FilterExpr::eval`] lets one that speaks no SQL answer
//! the same predicate over Arrow rows directly.
//!
//! # Three-valued logic is not optional
//!
//! SQL comparisons against NULL yield NULL, and a row is kept only when the
//! predicate is *true*. A two-valued evaluator gets this backwards in a way
//! that silently returns extra rows: `col != 'x'` over a NULL `col` is NULL in
//! SQL (row excluded) but `Some(_) != Some("x")` in Rust (row included). So
//! [`FilterExpr::eval`] returns `Option<bool>`, `None` meaning unknown, and the
//! caller keeps a row only on `Ok(Some(true))`.

use std::borrow::Cow;
use std::cmp::Ordering;

use arrow_array::{Array, RecordBatch, cast::AsArray, types::*};
use arrow_schema::DataType;

use crate::backend::types::{CmpOp, FilterExpr, Scalar, StringMatchKind};

/// Why a [`FilterExpr`] could not be evaluated natively.
///
/// Every variant is a *refusal*, never a silent `false`. A filter the evaluator
/// does not understand must not quietly drop or admit rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvalError {
    /// The row has no such column.
    UnknownColumn(String),
    /// The evaluator has no defined semantics here — an unsupported Arrow type,
    /// a cross-domain comparison (string vs number), or a [`FilterExpr::Raw`]
    /// predicate this backend cannot parse.
    Unsupported(String),
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvalError::UnknownColumn(c) => write!(f, "no such column: {c}"),
            EvalError::Unsupported(why) => write!(f, "cannot evaluate filter: {why}"),
        }
    }
}

impl std::error::Error for EvalError {}

/// A single cell's value, borrowed from the underlying storage.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell<'a> {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    /// Borrowed straight from a string array when the cell is a top-level
    /// column; owned when it is an element of a list, whose backing
    /// `ArrayRef` is materialized per-call and does not outlive the read.
    Str(Cow<'a, str>),
    /// A list-typed cell, for [`FilterExpr::ArrayContains`].
    List(Vec<Cell<'a>>),
}

/// One row, addressable by column name.
///
/// Deliberately minimal: a backend implements this over whatever it already
/// has, and gets the engine's whole predicate vocabulary for free.
pub trait RowAccessor {
    /// Fetch a column's value.
    ///
    /// # Errors
    ///
    /// [`EvalError::UnknownColumn`] if absent, [`EvalError::Unsupported`] if
    /// the physical type has no [`Cell`] representation.
    fn column(&self, name: &str) -> Result<Cell<'_>, EvalError>;
}

/// A row of an Arrow [`RecordBatch`].
pub struct ArrowRow<'a> {
    batch: &'a RecordBatch,
    row: usize,
}

impl<'a> ArrowRow<'a> {
    /// Borrow row `row` of `batch`.
    pub fn new(batch: &'a RecordBatch, row: usize) -> Self {
        Self { batch, row }
    }
}

impl RowAccessor for ArrowRow<'_> {
    fn column(&self, name: &str) -> Result<Cell<'_>, EvalError> {
        let idx = self
            .batch
            .schema()
            .index_of(name)
            .map_err(|_| EvalError::UnknownColumn(name.to_string()))?;
        cell_at(self.batch.column(idx).as_ref(), self.row)
    }
}

/// Read one Arrow cell.
///
/// Unsupported physical types are an error rather than [`Cell::Null`]: a
/// timestamp column compared against a string literal must refuse, not report
/// "no match" (hazard 6 in the design — `value_to_lance` emits temporals as
/// strings and leans on backend coercion we do not reimplement here).
fn cell_at(array: &dyn Array, row: usize) -> Result<Cell<'_>, EvalError> {
    if array.is_null(row) {
        return Ok(Cell::Null);
    }
    Ok(match array.data_type() {
        DataType::Boolean => Cell::Bool(array.as_boolean().value(row)),
        DataType::Int8 => Cell::Int(array.as_primitive::<Int8Type>().value(row) as i64),
        DataType::Int16 => Cell::Int(array.as_primitive::<Int16Type>().value(row) as i64),
        DataType::Int32 => Cell::Int(array.as_primitive::<Int32Type>().value(row) as i64),
        DataType::Int64 => Cell::Int(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => Cell::UInt(array.as_primitive::<UInt8Type>().value(row) as u64),
        DataType::UInt16 => Cell::UInt(array.as_primitive::<UInt16Type>().value(row) as u64),
        DataType::UInt32 => Cell::UInt(array.as_primitive::<UInt32Type>().value(row) as u64),
        DataType::UInt64 => Cell::UInt(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => Cell::Float(array.as_primitive::<Float32Type>().value(row) as f64),
        DataType::Float64 => Cell::Float(array.as_primitive::<Float64Type>().value(row)),
        DataType::Utf8 => Cell::Str(Cow::Borrowed(array.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => Cell::Str(Cow::Borrowed(array.as_string::<i64>().value(row))),
        DataType::List(_) => {
            let inner = array.as_list::<i32>().value(row);
            Cell::List(collect_list(inner.as_ref())?)
        }
        DataType::LargeList(_) => {
            let inner = array.as_list::<i64>().value(row);
            Cell::List(collect_list(inner.as_ref())?)
        }
        other => {
            return Err(EvalError::Unsupported(format!(
                "no native evaluation for column type {other}"
            )));
        }
    })
}

/// Materialize a list cell's elements as owned values.
///
/// `ListArray::value` hands back an `ArrayRef` that does not outlive this
/// call, so element strings are cloned. That is why [`Cell::Str`] is a `Cow`:
/// the common top-level-column read still borrows.
fn collect_list(inner: &dyn Array) -> Result<Vec<Cell<'static>>, EvalError> {
    (0..inner.len())
        .map(|i| {
            Ok(match cell_at(inner, i)? {
                Cell::Null => Cell::Null,
                Cell::Bool(b) => Cell::Bool(b),
                Cell::Int(v) => Cell::Int(v),
                Cell::UInt(v) => Cell::UInt(v),
                Cell::Float(v) => Cell::Float(v),
                Cell::Str(s) => Cell::Str(Cow::Owned(s.into_owned())),
                Cell::List(_) => {
                    return Err(EvalError::Unsupported("nested lists".to_string()));
                }
            })
        })
        .collect()
}

impl FilterExpr {
    /// Evaluate this predicate against one row, in SQL's three-valued logic.
    ///
    /// Returns `Some(true)` / `Some(false)` for a determinate answer and `None`
    /// for SQL NULL ("unknown"). **Keep a row only on `Ok(Some(true))`** —
    /// treating `None` as either boolean is how NULL-handling bugs get in.
    ///
    /// # Errors
    ///
    /// [`EvalError`] when the predicate references a missing column, touches a
    /// type with no native semantics, or contains a [`FilterExpr::Raw`] this
    /// backend cannot parse. Failing loudly is deliberate: a `Raw` treated as
    /// "match all" leaks rows the caller asked to exclude.
    pub fn eval(&self, row: &dyn RowAccessor) -> Result<Option<bool>, EvalError> {
        match self {
            FilterExpr::Literal(b) => Ok(Some(*b)),
            FilterExpr::And(parts) => {
                // Kleene AND: FALSE absorbs, so a determinate false short-circuits
                // even if a sibling is unknown.
                let mut unknown = false;
                for p in parts {
                    match p.eval(row)? {
                        Some(false) => return Ok(Some(false)),
                        Some(true) => {}
                        None => unknown = true,
                    }
                }
                Ok(if unknown { None } else { Some(true) })
            }
            FilterExpr::Or(parts) => {
                // Kleene OR: TRUE absorbs, mirroring `And`.
                let mut unknown = false;
                for p in parts {
                    match p.eval(row)? {
                        Some(true) => return Ok(Some(true)),
                        Some(false) => {}
                        None => unknown = true,
                    }
                }
                Ok(if unknown { None } else { Some(false) })
            }
            // `NOT NULL` is NULL — negation propagates unknown rather than
            // resolving it, which is what makes `NOT (x IN (…))` over a NULL
            // `x` exclude the row instead of admitting it.
            FilterExpr::Not(inner) => Ok(inner.eval(row)?.map(|b| !b)),
            FilterExpr::Compare { column, op, value } => compare(&row.column(column)?, *op, value),
            FilterExpr::In { column, values } => {
                // `x IN (a, b)` is `x = a OR x = b`: a hit wins over any NULL,
                // and only a miss-with-NULLs is unknown.
                let cell = row.column(column)?;
                let mut unknown = false;
                for v in values {
                    match compare(&cell, CmpOp::Eq, v)? {
                        Some(true) => return Ok(Some(true)),
                        Some(false) => {}
                        None => unknown = true,
                    }
                }
                Ok(if unknown { None } else { Some(false) })
            }
            FilterExpr::ArrayContains { column, value } => {
                let cell = row.column(column)?;
                list_contains(&cell, value)
            }
            FilterExpr::StringMatch {
                column,
                kind,
                pattern,
            } => match row.column(column)? {
                Cell::Null => Ok(None),
                // Exact substring semantics — no wildcard interpretation at all,
                // which is precisely what `to_sql` cannot promise.
                Cell::Str(s) => Ok(Some(match kind {
                    StringMatchKind::Contains => s.contains(pattern.as_str()),
                    StringMatchKind::StartsWith => s.starts_with(pattern.as_str()),
                    StringMatchKind::EndsWith => s.ends_with(pattern.as_str()),
                })),
                other => Err(EvalError::Unsupported(format!(
                    "string match on a non-string cell {other:?}"
                ))),
            },
            // Determinate by definition: "is this NULL" is never itself unknown.
            FilterExpr::IsNull(column) => Ok(Some(matches!(row.column(column)?, Cell::Null))),
            FilterExpr::IsNotNull(column) => Ok(Some(!matches!(row.column(column)?, Cell::Null))),
            FilterExpr::Raw(s) => Err(EvalError::Unsupported(format!(
                "raw backend predicate {s:?} has no native evaluation"
            ))),
        }
    }
}

/// `column <op> value` in three-valued logic.
fn compare(cell: &Cell<'_>, op: CmpOp, value: &Scalar) -> Result<Option<bool>, EvalError> {
    if matches!(cell, Cell::Null) || matches!(value, Scalar::Null) {
        return Ok(None);
    }
    Ok(order(cell, value)?.map(|o| match op {
        CmpOp::Eq => o == Ordering::Equal,
        CmpOp::NotEq => o != Ordering::Equal,
        CmpOp::Lt => o == Ordering::Less,
        CmpOp::LtEq => o != Ordering::Greater,
        CmpOp::Gt => o == Ordering::Greater,
        CmpOp::GtEq => o != Ordering::Less,
    }))
}

/// `array_contains(column, value)` in three-valued logic.
///
/// A NULL list is unknown. A present match wins over NULL elements; otherwise
/// NULL elements make the answer unknown — same shape as `IN`, which is what
/// the predicate means.
fn list_contains(cell: &Cell<'_>, value: &Scalar) -> Result<Option<bool>, EvalError> {
    let Cell::List(items) = cell else {
        if matches!(cell, Cell::Null) {
            return Ok(None);
        }
        return Err(EvalError::Unsupported(
            "array_contains on a non-list column".to_string(),
        ));
    };
    if matches!(value, Scalar::Null) {
        return Ok(None);
    }
    let mut unknown = false;
    for item in items {
        match compare(item, CmpOp::Eq, value)? {
            Some(true) => return Ok(Some(true)),
            Some(false) => {}
            None => unknown = true,
        }
    }
    Ok(if unknown { None } else { Some(false) })
}

/// Numeric domain for cross-type comparison.
#[derive(Debug, Clone, Copy)]
enum Num {
    I(i64),
    U(u64),
    F(f64),
}

fn cell_num(c: &Cell<'_>) -> Option<Num> {
    match c {
        Cell::Int(v) => Some(Num::I(*v)),
        Cell::UInt(v) => Some(Num::U(*v)),
        Cell::Float(v) => Some(Num::F(*v)),
        _ => None,
    }
}

fn scalar_num(s: &Scalar) -> Option<Num> {
    match s {
        Scalar::Int(v) => Some(Num::I(*v)),
        Scalar::UInt(v) => Some(Num::U(*v)),
        Scalar::Float(v) => Some(Num::F(*v)),
        _ => None,
    }
}

/// Order a cell against a scalar, or refuse.
///
/// `Ok(None)` is reserved for NaN — genuinely unordered — and never used for
/// "these types don't compare", which is an error.
fn order(cell: &Cell<'_>, value: &Scalar) -> Result<Option<Ordering>, EvalError> {
    match (cell, value) {
        (Cell::Bool(a), Scalar::Bool(b)) => Ok(Some(a.cmp(b))),
        (Cell::Str(a), Scalar::Str(b)) => Ok(Some(a.as_ref().cmp(b.as_str()))),
        _ => match (cell_num(cell), scalar_num(value)) {
            (Some(a), Some(b)) => Ok(cmp_num(a, b)),
            _ => Err(EvalError::Unsupported(format!(
                "cannot compare cell {cell:?} with literal {value:?}"
            ))),
        },
    }
}

/// Compare two numbers **exactly**, without a lossy common cast.
///
/// SQL says `1 = 1.0`, so mixed integer/float comparison must work — but the
/// obvious implementation (cast both to `f64`) is wrong above 2^53, and every
/// id column in the engine is `u64` with `Vid::INVALID == u64::MAX`. Each pair
/// is therefore compared in a domain that can represent both operands.
///
/// This is a deliberate, documented divergence from a backend that coerces
/// through `f64` first (DataFusion does): for integers beyond 2^53 compared
/// against a float literal, we answer exactly and it answers approximately. No
/// engine-generated filter mixes the domains — only a user filter could, and
/// those arrive as [`FilterExpr::Raw`], which this evaluator refuses outright.
fn cmp_num(a: Num, b: Num) -> Option<Ordering> {
    match (a, b) {
        (Num::I(x), Num::I(y)) => Some(x.cmp(&y)),
        (Num::U(x), Num::U(y)) => Some(x.cmp(&y)),
        (Num::I(x), Num::U(y)) => Some(if x < 0 {
            Ordering::Less
        } else {
            (x as u64).cmp(&y)
        }),
        (Num::U(x), Num::I(y)) => Some(if y < 0 {
            Ordering::Greater
        } else {
            x.cmp(&(y as u64))
        }),
        (Num::F(x), Num::F(y)) => x.partial_cmp(&y),
        (Num::I(x), Num::F(y)) => cmp_i64_f64(x, y),
        (Num::F(x), Num::I(y)) => cmp_i64_f64(y, x).map(Ordering::reverse),
        (Num::U(x), Num::F(y)) => cmp_u64_f64(x, y),
        (Num::F(x), Num::U(y)) => cmp_u64_f64(y, x).map(Ordering::reverse),
    }
}

/// 2^63 — the first `f64` above every `i64`, exactly representable.
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
/// 2^64 — the first `f64` above every `u64`, exactly representable.
const TWO_POW_64: f64 = 18_446_744_073_709_551_616.0;

fn cmp_i64_f64(i: i64, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f >= TWO_POW_63 {
        return Some(Ordering::Less);
    }
    if f < -TWO_POW_63 {
        return Some(Ordering::Greater);
    }
    // `floor` is exact and now in `i64` range, so the integer part compares
    // losslessly; a surviving fraction breaks the tie.
    let floor = f.floor();
    Some(match i.cmp(&(floor as i64)) {
        Ordering::Equal if f > floor => Ordering::Less,
        o => o,
    })
}

fn cmp_u64_f64(u: u64, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    if f < 0.0 {
        return Some(Ordering::Greater);
    }
    if f >= TWO_POW_64 {
        return Some(Ordering::Less);
    }
    let floor = f.floor();
    Some(match u.cmp(&(floor as u64)) {
        Ordering::Equal if f > floor => Ordering::Less,
        o => o,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::types::{CmpOp, FilterExpr, Scalar, StringMatchKind, ToSqlError};
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{
        BooleanArray, Float64Array, Int64Array, RecordBatchIterator, StringArray, UInt64Array,
    };
    use arrow_schema::{Field, Schema};
    use futures::TryStreamExt;
    use proptest::prelude::*;
    use std::sync::Arc;

    // Small, deliberately overlapping domains: filters must actually select a
    // proper subset for the comparison to mean anything. `NULL` appears in
    // every nullable column, and one string carries an apostrophe so every
    // generated case exercises the escape path in `scalar_to_sql`.
    const INTS: [Option<i64>; 6] = [Some(-2), Some(-1), Some(0), Some(1), Some(2), None];
    const UINTS: [Option<u64>; 5] = [Some(0), Some(1), Some(2), Some(3), None];
    const FLOATS: [Option<f64>; 5] = [Some(-1.5), Some(0.0), Some(1.0), Some(2.5), None];
    const STRS: [Option<&str>; 5] = [Some("a"), Some("b"), Some("it's"), Some("a%b_c"), None];
    const BOOLS: [Option<bool>; 3] = [Some(true), Some(false), None];
    const LABEL_SETS: [Option<&[&str]>; 5] = [
        Some(&["Person"]),
        Some(&["Person", "Admin"]),
        Some(&[]),
        Some(&["it's"]),
        None,
    ];

    const ROWS: usize = 120;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("rid", arrow_schema::DataType::UInt64, false),
            Field::new("i", arrow_schema::DataType::Int64, true),
            Field::new("u", arrow_schema::DataType::UInt64, true),
            Field::new("f", arrow_schema::DataType::Float64, true),
            Field::new("s", arrow_schema::DataType::Utf8, true),
            Field::new("b", arrow_schema::DataType::Boolean, true),
            Field::new(
                "labels",
                arrow_schema::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow_schema::DataType::Utf8,
                    true,
                ))),
                true,
            ),
        ]))
    }

    /// A deterministic batch whose columns advance at coprime strides, so the
    /// rows cover the cross-product of value domains without enumerating it.
    fn batch() -> RecordBatch {
        let mut labels = ListBuilder::new(StringBuilder::new());
        for r in 0..ROWS {
            match LABEL_SETS[r % LABEL_SETS.len()] {
                Some(items) => {
                    for it in items {
                        labels.values().append_value(it);
                    }
                    labels.append(true);
                }
                None => labels.append(false),
            }
        }
        let labels = labels.finish();
        // Re-declare the list field as the builder names it, so the written
        // schema and the declared one agree.
        let schema = Arc::new(Schema::new(vec![
            schema().field(0).clone(),
            schema().field(1).clone(),
            schema().field(2).clone(),
            schema().field(3).clone(),
            schema().field(4).clone(),
            schema().field(5).clone(),
            Field::new("labels", labels.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from_iter_values(0..ROWS as u64)),
                Arc::new(Int64Array::from_iter(
                    (0..ROWS).map(|r| INTS[r % INTS.len()]),
                )),
                Arc::new(UInt64Array::from_iter(
                    (0..ROWS).map(|r| UINTS[(r / 2) % UINTS.len()]),
                )),
                Arc::new(Float64Array::from_iter(
                    (0..ROWS).map(|r| FLOATS[(r / 3) % FLOATS.len()]),
                )),
                Arc::new(StringArray::from_iter(
                    (0..ROWS).map(|r| STRS[(r / 5) % STRS.len()]),
                )),
                Arc::new(BooleanArray::from_iter(
                    (0..ROWS).map(|r| BOOLS[(r / 7) % BOOLS.len()]),
                )),
                Arc::new(labels),
            ],
        )
        .expect("batch")
    }

    /// Row ids the native evaluator keeps — `Some(true)` only.
    fn eval_rids(expr: &FilterExpr, batch: &RecordBatch) -> Result<Vec<u64>, EvalError> {
        let rid = batch.column(0).as_primitive::<UInt64Type>();
        let mut out = Vec::new();
        for r in 0..batch.num_rows() {
            if expr.eval(&ArrowRow::new(batch, r))? == Some(true) {
                out.push(rid.value(r));
            }
        }
        Ok(out)
    }

    /// Row ids Lance keeps for the rendered SQL.
    async fn sql_rids(ds: &lance::Dataset, sql: &str) -> anyhow::Result<Vec<u64>> {
        let mut scanner = ds.scan();
        scanner.project(&["rid"])?;
        scanner.filter(sql)?;
        let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;
        let mut out: Vec<u64> = batches
            .iter()
            .flat_map(|b| {
                let a = b.column(0).as_primitive::<UInt64Type>();
                (0..b.num_rows()).map(|i| a.value(i)).collect::<Vec<_>>()
            })
            .collect();
        out.sort_unstable();
        Ok(out)
    }

    fn cmp_op() -> impl Strategy<Value = CmpOp> {
        prop_oneof![
            Just(CmpOp::Eq),
            Just(CmpOp::NotEq),
            Just(CmpOp::Lt),
            Just(CmpOp::LtEq),
            Just(CmpOp::Gt),
            Just(CmpOp::GtEq),
        ]
    }

    /// A `(column, scalar)` pair drawn from one type domain.
    ///
    /// Well-typed on purpose: cross-domain comparison is a *refusal* in the
    /// evaluator and a coercion in DataFusion, so it is pinned by explicit
    /// tests below rather than folded into the agreement property.
    fn typed_operand() -> impl Strategy<Value = (String, Scalar)> {
        prop_oneof![
            (-3i64..4).prop_map(|v| ("i".to_string(), Scalar::Int(v))),
            (0u64..5).prop_map(|v| ("u".to_string(), Scalar::UInt(v))),
            prop_oneof![
                Just(-1.5f64),
                Just(0.0),
                Just(1.0),
                Just(2.5),
                Just(3.0),
                Just(0.5)
            ]
            .prop_map(|v| ("f".to_string(), Scalar::Float(v))),
            prop_oneof![Just("a"), Just("b"), Just("it's"), Just("z")]
                .prop_map(|v| ("s".to_string(), Scalar::Str(v.to_string()))),
            any::<bool>().prop_map(|v| ("b".to_string(), Scalar::Bool(v))),
            Just(("i".to_string(), Scalar::Null)),
            Just(("s".to_string(), Scalar::Null)),
        ]
    }

    fn expr_strategy() -> impl Strategy<Value = FilterExpr> {
        let leaf = prop_oneof![
            8 => (typed_operand(), cmp_op())
                .prop_map(|((column, value), op)| FilterExpr::Compare { column, op, value }),
            3 => proptest::collection::vec(typed_operand(), 0..4).prop_map(|ops| {
                // `IN` needs one column; take the first operand's and keep only
                // the literals that belong to that domain.
                let column = ops
                    .first()
                    .map(|(c, _)| c.clone())
                    .unwrap_or_else(|| "i".to_string());
                let values = ops
                    .into_iter()
                    .filter(|(c, _)| *c == column)
                    .map(|(_, v)| v)
                    .collect();
                FilterExpr::In { column, values }
            }),
            3 => prop_oneof![Just("Person"), Just("Admin"), Just("it's"), Just("Ghost")]
                .prop_map(|v| FilterExpr::ArrayContains {
                    column: "labels".to_string(),
                    value: Scalar::Str(v.to_string()),
                }),
            // Wildcard-free patterns only: a pattern holding `%`/`_` is exactly
            // the case `to_sql` refuses, so it cannot participate in an
            // agreement property. It is pinned by `wildcard_pattern_*` below.
            3 => (
                prop_oneof![
                    Just(StringMatchKind::Contains),
                    Just(StringMatchKind::StartsWith),
                    Just(StringMatchKind::EndsWith),
                ],
                prop_oneof![Just("a"), Just("b"), Just("it"), Just("'"), Just("z")],
            )
                .prop_map(|(kind, pattern)| FilterExpr::StringMatch {
                    column: "s".to_string(),
                    kind,
                    pattern: pattern.to_string(),
                }),
            2 => prop_oneof![Just("i"), Just("s"), Just("b"), Just("labels")].prop_map(|c| {
                FilterExpr::IsNull(c.to_string())
            }),
            2 => prop_oneof![Just("u"), Just("f"), Just("s")].prop_map(|c| {
                FilterExpr::IsNotNull(c.to_string())
            }),
            1 => any::<bool>().prop_map(FilterExpr::Literal),
        ];
        leaf.prop_recursive(3, 16, 4, |inner| {
            prop_oneof![
                3 => proptest::collection::vec(inner.clone(), 0..4).prop_map(FilterExpr::And),
                3 => proptest::collection::vec(inner.clone(), 0..4).prop_map(FilterExpr::Or),
                1 => inner.prop_map(FilterExpr::negate),
            ]
        })
    }

    /// The phase's acceptance criterion: over randomly generated expressions,
    /// the native evaluator selects exactly the rows Lance's SQL filter does.
    ///
    /// Verified to bite, by mutation: making the Kleene `And` two-valued, and
    /// dropping the apostrophe escape in `scalar_to_sql`, both fail this test.
    /// The mixed `And`/`Or`/`Not` nesting is what exercises `to_sql`'s
    /// unconditional parenthesisation: dropping the `paren` call also fails
    /// this test, because `a OR b AND c` binds differently than the tree says.
    #[test]
    fn eval_agrees_with_lance_sql() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let tmp = tempfile::tempdir().expect("tempdir");
        let uri = tmp.path().join("t.lance").to_string_lossy().to_string();
        let batch = batch();
        let schema = batch.schema();
        rt.block_on(async {
            lance::Dataset::write(
                RecordBatchIterator::new(vec![Ok(batch.clone())], schema),
                &uri,
                None,
            )
            .await
            .expect("write");
        });
        let ds = rt.block_on(lance::Dataset::open(&uri)).expect("open");

        proptest!(ProptestConfig::with_cases(200), |(expr in expr_strategy())| {
            let sql = expr.to_sql().expect("renderable");
            let native = eval_rids(&expr, &batch).expect("evaluable");
            let lance = rt.block_on(sql_rids(&ds, &sql)).expect("scan");
            prop_assert_eq!(
                native, lance,
                "\nexpr: {:?}\nsql:  {}\n", expr, sql
            );
        });
    }

    // ---- explicit cases the plan calls out ----

    /// A NULL in an `IN` list makes a miss unknown, not false, so the
    /// predicate can never be FALSE. Rendering it literally as
    /// `i IN (NULL, ...)` let Lance's multi-`InList` rewrite collapse the
    /// unknown: `NOT ((i IN (NULL)) AND (i IN (0)))` selected every row
    /// instead of only the non-null, non-zero ones. Pinned as SQL text so the
    /// rendering cannot regress silently.
    #[test]
    fn in_list_with_null_renders_as_explicit_unknown() {
        let all_null = FilterExpr::In {
            column: "i".into(),
            values: vec![Scalar::Null],
        };
        assert_eq!(all_null.to_sql().unwrap(), "CAST(NULL AS BOOLEAN)");

        let mixed = FilterExpr::In {
            column: "i".into(),
            values: vec![Scalar::Int(0), Scalar::Null],
        };
        assert_eq!(
            mixed.to_sql().unwrap(),
            "(i IN (0) OR CAST(NULL AS BOOLEAN))"
        );

        // The common, NULL-free case keeps the plain rendering.
        let plain = FilterExpr::In {
            column: "i".into(),
            values: vec![Scalar::Int(0), Scalar::Int(1)],
        };
        assert_eq!(plain.to_sql().unwrap(), "i IN (0, 1)");
    }

    /// End-to-end for the shape the proptest shrank to: the native evaluator
    /// and Lance must select the same rows.
    #[test]
    fn not_and_of_two_in_lists_with_null_agrees_with_lance() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let tmp = tempfile::tempdir().expect("tempdir");
        let uri = tmp.path().join("t.lance").to_string_lossy().to_string();
        let b = batch();
        let schema = b.schema();
        rt.block_on(async {
            lance::Dataset::write(
                RecordBatchIterator::new(vec![Ok(b.clone())], schema),
                &uri,
                None,
            )
            .await
            .expect("write");
        });
        let ds = rt.block_on(lance::Dataset::open(&uri)).expect("open");

        let expr = FilterExpr::Not(Box::new(FilterExpr::And(vec![
            FilterExpr::In {
                column: "i".into(),
                values: vec![Scalar::Null],
            },
            FilterExpr::In {
                column: "i".into(),
                values: vec![Scalar::Int(0)],
            },
        ])));

        let native = eval_rids(&expr, &b).expect("evaluable");
        let lance = rt
            .block_on(sql_rids(&ds, &expr.to_sql().expect("renderable")))
            .expect("scan");
        assert_eq!(native, lance);

        // `i` cycles [-2, -1, 0, 1, 2, NULL]: the answer is every row whose
        // `i` is neither NULL nor 0, i.e. four of every six.
        assert_eq!(native.len(), 80, "expected the non-null, non-zero rows");
    }

    /// The same rewrite reached from the *column* side, with multi-value lists.
    ///
    /// #212: the fix above renders a single-candidate `IN` as `=`, which leaves
    /// no `InList` for Lance's multi-`InList`-on-one-column rewrite to pair. Two
    /// *multi*-value lists still render as two `InList`s, so this checks the
    /// hole that fix does not cover — every listed value is non-NULL, and the
    /// unknown comes from `f` being NULL on a third of the rows.
    #[test]
    fn not_and_of_two_multi_value_in_lists_agrees_with_lance() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let tmp = tempfile::tempdir().expect("tempdir");
        let uri = tmp.path().join("t.lance").to_string_lossy().to_string();
        let b = batch();
        let schema = b.schema();
        rt.block_on(async {
            lance::Dataset::write(
                RecordBatchIterator::new(vec![Ok(b.clone())], schema),
                &uri,
                None,
            )
            .await
            .expect("write");
        });
        let ds = rt.block_on(lance::Dataset::open(&uri)).expect("open");

        // `f` cycles [-1.5, 0.0, 1.0, 2.5, NULL], three rows each. The two sets
        // are disjoint, so the conjunction is FALSE wherever `f` is non-NULL and
        // unknown where it is NULL — and `NOT` keeps it unknown there.
        let expr = FilterExpr::Not(Box::new(FilterExpr::And(vec![
            FilterExpr::In {
                column: "f".into(),
                values: vec![Scalar::Float(-1.5), Scalar::Float(1.0)],
            },
            FilterExpr::In {
                column: "f".into(),
                values: vec![Scalar::Float(0.0), Scalar::Float(2.5)],
            },
        ])));

        let native = eval_rids(&expr, &b).expect("evaluable");
        let lance = rt
            .block_on(sql_rids(&ds, &expr.to_sql().expect("renderable")))
            .expect("scan");
        assert_eq!(native, lance, "sql: {}", expr.to_sql().expect("renderable"));

        // Every non-NULL row and no NULL row: four of every five values, three
        // rows each.
        assert_eq!(
            native.len(),
            ROWS * 4 / 5,
            "expected exactly the non-NULL rows"
        );
    }

    #[test]
    fn null_yields_unknown_not_false() {
        let b = batch();
        // `s != 'a'` must exclude NULL `s` rows, the classic two-valued bug.
        let expr = FilterExpr::Compare {
            column: "s".to_string(),
            op: CmpOp::NotEq,
            value: Scalar::Str("a".to_string()),
        };
        let kept = eval_rids(&expr, &b).unwrap();
        let s = b.column(4).as_string::<i32>();
        for r in 0..b.num_rows() {
            if s.is_null(r) {
                assert!(!kept.contains(&(r as u64)), "NULL row {r} must not survive");
            }
        }
        assert!(!kept.is_empty(), "non-NULL non-'a' rows must survive");

        // A NULL literal makes the whole conjunct unknown.
        let with_null = FilterExpr::all([
            FilterExpr::Compare {
                column: "i".to_string(),
                op: CmpOp::Eq,
                value: Scalar::Null,
            },
            FilterExpr::Literal(true),
        ]);
        assert!(eval_rids(&with_null, &b).unwrap().is_empty());
    }

    #[test]
    fn empty_in_is_false_and_renders() {
        let expr = FilterExpr::In {
            column: "i".to_string(),
            values: vec![],
        };
        assert_eq!(expr.to_sql().unwrap(), "false");
        assert!(eval_rids(&expr, &batch()).unwrap().is_empty());
    }

    #[test]
    fn in_with_null_is_unknown_only_on_miss() {
        let b = batch();
        let hit = FilterExpr::In {
            column: "i".to_string(),
            values: vec![Scalar::Int(1), Scalar::Null],
        };
        // A real match outranks the NULL.
        assert!(!eval_rids(&hit, &b).unwrap().is_empty());

        let miss = FilterExpr::In {
            column: "i".to_string(),
            values: vec![Scalar::Int(99), Scalar::Null],
        };
        assert!(eval_rids(&miss, &b).unwrap().is_empty());
    }

    #[test]
    fn mixed_numeric_compares_by_value_not_representation() {
        // SQL says 1 = 1.0. `Scalar`'s own `PartialEq` says otherwise, and
        // that split is deliberate — structural equality serves plan caching.
        assert_ne!(Scalar::Int(1), Scalar::Float(1.0));
        assert_eq!(cmp_num(Num::I(1), Num::F(1.0)), Some(Ordering::Equal));
        assert_eq!(cmp_num(Num::U(1), Num::F(1.5)), Some(Ordering::Less));
        assert_eq!(cmp_num(Num::I(-1), Num::U(0)), Some(Ordering::Less));
    }

    #[test]
    fn u64_sentinels_survive_the_int_domain() {
        // `Vid::INVALID` is `u64::MAX`; through an `i64` it would read as -1.
        assert_eq!(
            cmp_num(Num::U(u64::MAX), Num::I(-1)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            FilterExpr::version_at_most(u64::MAX).to_sql().unwrap(),
            "_version <= 18446744073709551615"
        );
    }

    #[test]
    fn large_integers_compare_exactly_against_floats() {
        // Above 2^53 a common `f64` cast loses the distinction; we do not.
        let big = (1u64 << 53) + 1;
        assert_eq!(
            cmp_num(Num::U(big), Num::F((1u64 << 53) as f64)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            cmp_num(Num::I(i64::MAX), Num::F(f64::MAX)),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp_num(Num::I(i64::MIN), Num::F(f64::NEG_INFINITY)),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn nan_is_unordered_so_every_comparison_is_unknown() {
        assert_eq!(cmp_num(Num::F(f64::NAN), Num::F(1.0)), None);
        assert_eq!(cmp_num(Num::I(1), Num::F(f64::NAN)), None);
        let unknown = compare(&Cell::Float(f64::NAN), CmpOp::Eq, &Scalar::Float(f64::NAN));
        assert_eq!(unknown.unwrap(), None);
    }

    #[test]
    fn raw_and_cross_domain_refuse_loudly() {
        let b = batch();
        let raw = FilterExpr::Raw("i > 0".to_string());
        assert!(matches!(
            raw.eval(&ArrowRow::new(&b, 0)),
            Err(EvalError::Unsupported(_))
        ));

        let crossed = FilterExpr::Compare {
            column: "i".to_string(),
            op: CmpOp::Eq,
            value: Scalar::Str("a".to_string()),
        };
        assert!(matches!(
            crossed.eval(&ArrowRow::new(&b, 0)),
            Err(EvalError::Unsupported(_))
        ));

        let missing = FilterExpr::Compare {
            column: "nope".to_string(),
            op: CmpOp::Eq,
            value: Scalar::Int(1),
        };
        assert!(matches!(
            missing.eval(&ArrowRow::new(&b, 0)),
            Err(EvalError::UnknownColumn(_))
        ));
    }

    #[test]
    fn apostrophes_round_trip_through_the_single_escaper() {
        let expr = FilterExpr::Compare {
            column: "s".to_string(),
            op: CmpOp::Eq,
            value: Scalar::Str("it's".to_string()),
        };
        assert_eq!(expr.to_sql().unwrap(), "s = 'it''s'");
        assert!(!eval_rids(&expr, &batch()).unwrap().is_empty());
    }

    #[test]
    fn wildcard_pattern_refuses_in_sql_but_evaluates_exactly() {
        let b = batch();
        // `a%b_c` is a literal row value; the pattern below is a literal `%b_`,
        // which LIKE would read as "anything, b, any single char".
        let expr = FilterExpr::StringMatch {
            column: "s".to_string(),
            kind: StringMatchKind::Contains,
            pattern: "%b_".to_string(),
        };
        assert!(matches!(expr.to_sql(), Err(ToSqlError::Unsupported(_))));

        // The native evaluator has no wildcard notion at all, so it answers.
        let kept = eval_rids(&expr, &b).unwrap();
        assert!(
            !kept.is_empty(),
            "the literal substring `%b_` occurs in `a%b_c` rows"
        );
        let s = b.column(4).as_string::<i32>();
        for r in 0..b.num_rows() {
            let expect = !s.is_null(r) && s.value(r).contains("%b_");
            assert_eq!(kept.contains(&(r as u64)), expect, "row {r}");
        }
    }

    #[test]
    fn sql_pushable_drops_an_or_whole_never_one_branch() {
        let bad = FilterExpr::StringMatch {
            column: "s".to_string(),
            kind: StringMatchKind::Contains,
            pattern: "%".to_string(),
        };
        let good = FilterExpr::equals("i", Scalar::Int(1));

        // An `And` sheds the unrenderable conjunct — a widening, which the
        // caller's residual re-narrows.
        let anded = FilterExpr::And(vec![good.clone(), bad.clone()]);
        assert_eq!(anded.sql_pushable(), good);

        // An `Or` must go whole. Keeping only `good` would NARROW the result
        // and silently lose the rows `bad` would have matched.
        let ored = FilterExpr::Or(vec![good.clone(), bad.clone()]);
        assert_eq!(ored.sql_pushable(), FilterExpr::Literal(true));

        // Nested: the bad `Or` is dropped, its sibling conjunct survives.
        let nested = FilterExpr::And(vec![good.clone(), ored]);
        assert_eq!(nested.sql_pushable(), good);

        // Fully renderable trees are returned untouched.
        let fine = FilterExpr::And(vec![good.clone(), FilterExpr::IsNull("s".to_string())]);
        assert_eq!(fine.sql_pushable(), fine);
    }

    #[test]
    fn scalar_from_value_excludes_null_so_producers_keep_bailing() {
        use uni_common::Value;
        assert_eq!(
            Scalar::from_value(&Value::String("x".into())),
            Some(Scalar::Str("x".into()))
        );
        assert_eq!(Scalar::from_value(&Value::Int(1)), Some(Scalar::Int(1)));
        assert_eq!(
            Scalar::from_value(&Value::Bool(true)),
            Some(Scalar::Bool(true))
        );
        // Null and the composite types must stay `None`: every caller reads that
        // as "cannot build this probe" and falls back to a general path.
        assert_eq!(Scalar::from_value(&Value::Null), None);
        assert_eq!(Scalar::from_value(&Value::List(vec![])), None);
    }

    /// A rendered `Compare` must actually narrow a real Lance scan.
    ///
    /// The regression this pins is not a bad-looking string — `"createdAt" >= 2`
    /// reads fine. Lance parses a double-quoted name as a **string literal**, so
    /// the clause became a data-independent constant matching every row. Only a
    /// scan proves the difference, which is why this test writes a dataset with
    /// a camelCase column rather than asserting on text.
    #[test]
    fn rendered_compare_narrows_a_real_scan() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let uri = tmp.path().join("c.lance").to_string_lossy().to_string();
        let schema = Arc::new(Schema::new(vec![
            Field::new("rid", arrow_schema::DataType::UInt64, false),
            Field::new("createdAt", arrow_schema::DataType::Int64, true),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(0..5u64)),
                Arc::new(Int64Array::from_iter_values(1..6i64)),
            ],
        )
        .unwrap();
        rt.block_on(async {
            lance::Dataset::write(RecordBatchIterator::new(vec![Ok(b)], schema), &uri, None)
                .await
                .unwrap();
        });
        let ds = rt.block_on(lance::Dataset::open(&uri)).unwrap();

        let range = FilterExpr::all([
            FilterExpr::compare("createdAt", CmpOp::GtEq, Scalar::Int(2)),
            FilterExpr::compare("createdAt", CmpOp::LtEq, Scalar::Int(4)),
        ]);
        let sql = range.to_sql().unwrap();
        assert!(!sql.contains('"'), "column must be bare: {sql}");
        assert_eq!(rt.block_on(sql_rids(&ds, &sql)).unwrap(), vec![1, 2, 3]);

        // What the old fused form did. `"createdAt"` is the string literal
        // `'createdAt'`, so the clause is a constant: which constant depends on
        // the operator (`>` matched every row in isolation, this two-sided form
        // matches none), but it never depends on the data.
        let quoted = "\"createdAt\" >= 2 AND \"createdAt\" <= 4";
        assert_eq!(
            rt.block_on(sql_rids(&ds, quoted)).unwrap(),
            Vec::<u64>::new(),
            "a double-quoted column is a string literal, not an identifier"
        );
    }
}
