// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Backend-agnostic types for storage operations.

use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::runtime::counters::QueryCounters;

/// Canonical column names the engine filters on.
///
/// These are the physical columns every table carries. Naming them here keeps
/// the strings greppable and stops each producer from spelling them itself.
pub mod cols {
    /// Soft-delete tombstone.
    pub const DELETED: &str = "_deleted";
    /// MVCC version stamp. A **data column**, not a dataset version — see
    /// [`super::FilterExpr::version_at_most`].
    pub const VERSION: &str = "_version";
}

/// A literal value in a [`FilterExpr`].
///
/// `UInt` is separate from `Int` on purpose: every id column in the engine is
/// `u64`, and `Vid::INVALID` is `u64::MAX`, which round-trips through `i64` as
/// `-1`. One narrowing conversion would turn a sentinel into a plausible-looking
/// negative id.
#[derive(Debug, Clone)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(String),
}

/// Structural equality, deliberately **not** SQL equality.
///
/// This compares representations: `Int(1) != Float(1.0)` and `Float(NaN) ==
/// Float(NaN)` (bitwise). SQL's rules — where `1 = 1.0` holds and `NaN`
/// compares to nothing — live in the evaluator's comparison, not here. Keeping
/// them apart means plan-cache lookups and dedup can use this impl without
/// inheriting three-valued logic.
impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Scalar::Null, Scalar::Null) => true,
            (Scalar::Bool(a), Scalar::Bool(b)) => a == b,
            (Scalar::Int(a), Scalar::Int(b)) => a == b,
            (Scalar::UInt(a), Scalar::UInt(b)) => a == b,
            (Scalar::Float(a), Scalar::Float(b)) => a.to_bits() == b.to_bits(),
            (Scalar::Str(a), Scalar::Str(b)) => a == b,
            _ => false,
        }
    }
}
impl Eq for Scalar {}

impl Scalar {
    /// Map a [`uni_common::Value`] onto a comparable literal.
    ///
    /// `None` for anything with no scalar equivalent — lists, maps, bytes,
    /// vectors, graph values, **and `Value::Null`**. Null is excluded
    /// deliberately: every existing producer treats a null key value as "this
    /// probe cannot be built" and falls back, so mapping it to [`Scalar::Null`]
    /// here would silently turn those bail-outs into `col = NULL`, which matches
    /// nothing. A caller that genuinely wants SQL NULL writes it itself.
    pub fn from_value(value: &uni_common::Value) -> Option<Scalar> {
        match value {
            uni_common::Value::String(s) => Some(Scalar::Str(s.clone())),
            uni_common::Value::Int(n) => Some(Scalar::Int(*n)),
            uni_common::Value::Float(f) => Some(Scalar::Float(*f)),
            uni_common::Value::Bool(b) => Some(Scalar::Bool(*b)),
            _ => None,
        }
    }
}

impl std::hash::Hash for Scalar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Scalar::Null => {}
            Scalar::Bool(b) => b.hash(state),
            Scalar::Int(i) => i.hash(state),
            Scalar::UInt(u) => u.hash(state),
            // Matches the bitwise `PartialEq` above.
            Scalar::Float(f) => f.to_bits().hash(state),
            Scalar::Str(s) => s.hash(state),
        }
    }
}

/// Comparison operator for [`FilterExpr::Compare`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    fn as_sql(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::NotEq => "!=",
            CmpOp::Lt => "<",
            CmpOp::LtEq => "<=",
            CmpOp::Gt => ">",
            CmpOp::GtEq => ">=",
        }
    }
}

/// Which end of a string a [`FilterExpr::StringMatch`] anchors to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StringMatchKind {
    Contains,
    StartsWith,
    EndsWith,
}

/// Why a [`FilterExpr`] could not be rendered to SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToSqlError {
    /// The dialect has no escape mechanism for this pattern.
    Unsupported(String),
}

impl std::fmt::Display for ToSqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToSqlError::Unsupported(why) => write!(f, "cannot render filter to SQL: {why}"),
        }
    }
}

impl std::error::Error for ToSqlError {}

/// Filter expression for backend queries.
///
/// Structured so a backend that cannot parse SQL can still evaluate the
/// engine's own predicates — MVCC visibility, id lookups, label membership.
/// [`FilterExpr::to_sql`] lowers it for backends that prefer text.
///
/// # Escaping and precedence live here
///
/// Producers hand over *data*, never syntax. [`Self::to_sql`] is the single
/// place quoting, escaping, and parenthesisation happen; anything that builds a
/// predicate by formatting a string is bypassing that guarantee.
///
/// # Column names are backtick-quoted, never double-quoted
///
/// Lance's filter dialect parses a double-quoted name as a **string literal**,
/// not a quoted identifier: `"rid" > 2` is `'rid' > 2`, a constant, and on
/// lance 7 it matched every row. Its identifier quote is the backtick, and
/// [`Self::to_sql`] backtick-quotes every column name, so a producer may hand over any name a column can carry — SQL
/// keywords (`order`, `select`, `null`, `true`), spaces, dashes, quotes,
/// mixed case, non-ASCII. Emitted bare, those were parse errors or, for `null`
/// and `true`, literals that made the predicate a silent constant. The one
/// name it cannot spell, one containing a backtick, is refused with
/// [`ToSqlError::Unsupported`], which [`Self::sql_pushable`] turns into "leave
/// it to the residual filter".
#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    /// A constant. `Literal(true)` matches every row, `Literal(false)` none.
    ///
    /// Replaces the old `None` variant: it composes (`Not(Literal(true))` is
    /// meaningful), it renders (`delete("true")` needs it), and it gives empty
    /// `And` / `In` a total canonical form instead of a special case.
    Literal(bool),
    /// Conjunction. Empty is [`Self::Literal`]`(true)`.
    And(Vec<FilterExpr>),
    /// Disjunction. Empty is [`Self::Literal`]`(false)`.
    Or(Vec<FilterExpr>),
    /// Negation, in three-valued logic: `NOT NULL` is NULL, not true.
    Not(Box<FilterExpr>),
    /// `column <op> value`.
    Compare {
        column: String,
        op: CmpOp,
        value: Scalar,
    },
    /// `column IN (values…)`. Empty is [`Self::Literal`]`(false)`.
    ///
    /// Every literal must match the column's physical type. Lance rejects a
    /// mismatch at plan time (`could not convert to literal of type 'Utf8'`) and
    /// **splitting a mixed list into an `OR` of same-typed lists does not help**
    /// — verified: `(s IN ('a')) OR (s IN (1))` fails exactly like
    /// `s IN ('a', 1)`, and even `i IN (1, 2.5)` fails against an `Int64`
    /// column. Type agreement is the producer's obligation, not the renderer's.
    In { column: String, values: Vec<Scalar> },
    /// List-typed column contains `value` (e.g. a vertex's `labels`).
    ArrayContains { column: String, value: Scalar },
    /// Substring / prefix / suffix match on a string column.
    ///
    /// Carries the raw `pattern` as **data**, not a pre-built `LIKE` string, so
    /// each backend picks its own encoding. That distinction is what lets a
    /// native evaluator answer a pattern containing `%` or `_` exactly while
    /// [`Self::to_sql`] declines it — the target dialect has no `ESCAPE`.
    StringMatch {
        column: String,
        kind: StringMatchKind,
        pattern: String,
    },
    /// `column IS NULL`. Determinate: never yields unknown.
    IsNull(String),
    /// `column IS NOT NULL`. Determinate: never yields unknown.
    IsNotNull(String),
    /// Backend-specific predicate text, passed through verbatim.
    ///
    /// This is **not** transitional. The user-facing `filter` argument on
    /// `uni.vector.query` and friends is opaque text that nothing in the engine
    /// parses, so it can only ever arrive as `Raw`.
    ///
    /// A backend that cannot evaluate `Raw` MUST fail loudly. Treating it as
    /// "match all" would silently return rows the caller asked to exclude,
    /// which on a filtered query is a data leak, not a degraded result.
    Raw(String),
}

impl Default for FilterExpr {
    fn default() -> Self {
        FilterExpr::Literal(true)
    }
}

/// Replace same-column `IN` conjuncts with a single `IN` over their intersection.
///
/// `col IN A AND col IN B` is exactly `col IN (A ∩ B)` in three-valued logic: on
/// a non-NULL column both say TRUE iff the value is in both sets, and on a NULL
/// column both are unknown. The fold is therefore semantics-preserving, and it
/// is also strictly more selective.
///
/// It exists for rendering, not for optimisation. Two `InList`s over one column
/// are what triggers the Lance rewrite that loses the unknown — the rewrite
/// folds a disjoint conjunction to FALSE, which is sound in two-valued logic and
/// wrong wherever the column is NULL, since the conjunction is unknown there.
/// `NOT` over that returned every row, NULLs included. Leaving one `InList` gives
/// the rewrite nothing to pair. See issue #212.
///
/// Only NULL-free lists fold. A list containing NULL already renders as
/// `(col IN (…) OR CAST(NULL AS BOOLEAN))` rather than a bare `InList`, so it
/// cannot feed the rewrite, and its unknown-on-miss semantics do not survive a
/// naive set intersection.
fn fold_same_column_ins(parts: &[FilterExpr]) -> Vec<FilterExpr> {
    use std::collections::HashMap;

    let foldable = |e: &FilterExpr| {
        matches!(e, FilterExpr::In { values, .. }
            if !values.is_empty() && !values.iter().any(|v| matches!(v, Scalar::Null)))
    };

    // Which columns appear in more than one foldable `IN`. Nothing else moves.
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for p in parts {
        if let FilterExpr::In { column, .. } = p
            && foldable(p)
        {
            *counts.entry(column.as_str()).or_default() += 1;
        }
    }
    if !counts.values().any(|n| *n > 1) {
        return parts.to_vec();
    }

    let mut out: Vec<FilterExpr> = Vec::with_capacity(parts.len());
    let mut emitted: Vec<&str> = Vec::new();
    for p in parts {
        let FilterExpr::In { column, .. } = p else {
            out.push(p.clone());
            continue;
        };
        if !foldable(p) || counts.get(column.as_str()).copied().unwrap_or(0) < 2 {
            out.push(p.clone());
            continue;
        }
        if emitted.contains(&column.as_str()) {
            continue; // already folded into the first occurrence
        }
        emitted.push(column.as_str());

        // Intersect every foldable `IN` on this column, order-preserving.
        let mut acc: Option<Vec<Scalar>> = None;
        for q in parts {
            if let FilterExpr::In { column: c, values } = q
                && c == column
                && foldable(q)
            {
                acc = Some(match acc {
                    None => values.clone(),
                    Some(prev) => prev
                        .into_iter()
                        .filter(|v| values.iter().any(|w| w == v))
                        .collect(),
                });
            }
        }
        let values = acc.unwrap_or_default();
        if values.is_empty() {
            // An empty intersection is FALSE on a non-NULL column and *unknown*
            // on a NULL one — not the plain `false` an empty `IN` renders as.
            // `col IS NULL AND unknown` gives exactly that: NULL where the
            // column is NULL, FALSE everywhere else.
            out.push(FilterExpr::And(vec![
                FilterExpr::IsNull(column.clone()),
                FilterExpr::In {
                    column: column.clone(),
                    values: vec![Scalar::Null],
                },
            ]));
        } else {
            out.push(FilterExpr::In {
                column: column.clone(),
                values,
            });
        }
    }
    out
}

impl FilterExpr {
    /// `_deleted = false` — the soft-delete visibility predicate.
    ///
    /// A constructor rather than a variant: a dedicated `NotDeleted` variant
    /// would invite a backend to implement it as something other than a
    /// predicate on the physical `_deleted` column.
    pub fn not_deleted() -> Self {
        FilterExpr::Compare {
            column: cols::DELETED.to_string(),
            op: CmpOp::Eq,
            value: Scalar::Bool(false),
        }
    }

    /// `_version <= v` — the MVCC snapshot bound.
    ///
    /// Note this is a predicate on a **data column**. It is emphatically not
    /// dataset time-travel; rows physically carry `_version`, and a backend
    /// that answered this by checking out an older dataset version would
    /// return a different set of rows.
    pub fn version_at_most(v: u64) -> Self {
        FilterExpr::Compare {
            column: cols::VERSION.to_string(),
            op: CmpOp::LtEq,
            value: Scalar::UInt(v),
        }
    }

    /// `column <op> value`.
    pub fn compare(column: impl Into<String>, op: CmpOp, value: Scalar) -> Self {
        FilterExpr::Compare {
            column: column.into(),
            op,
            value,
        }
    }

    /// `column = value`.
    pub fn equals(column: impl Into<String>, value: Scalar) -> Self {
        Self::compare(column, CmpOp::Eq, value)
    }

    /// `column IN (values…)`.
    pub fn one_of(column: impl Into<String>, values: impl IntoIterator<Item = Scalar>) -> Self {
        FilterExpr::In {
            column: column.into(),
            values: values.into_iter().collect(),
        }
    }

    /// List-typed `column` contains `value`.
    pub fn array_contains(column: impl Into<String>, value: Scalar) -> Self {
        FilterExpr::ArrayContains {
            column: column.into(),
            value,
        }
    }

    /// Conjoin, dropping trivially-true children and collapsing the empty case.
    pub fn all(parts: impl IntoIterator<Item = FilterExpr>) -> Self {
        let kept: Vec<_> = parts
            .into_iter()
            .filter(|p| !matches!(p, FilterExpr::Literal(true)))
            .collect();
        match kept.len() {
            0 => FilterExpr::Literal(true),
            1 => kept.into_iter().next().expect("len checked"),
            _ => FilterExpr::And(kept),
        }
    }

    /// Disjoin, dropping trivially-false children and collapsing the empty case.
    pub fn any_of(parts: impl IntoIterator<Item = FilterExpr>) -> Self {
        let kept: Vec<_> = parts
            .into_iter()
            .filter(|p| !matches!(p, FilterExpr::Literal(false)))
            .collect();
        match kept.len() {
            0 => FilterExpr::Literal(false),
            1 => kept.into_iter().next().expect("len checked"),
            _ => FilterExpr::Or(kept),
        }
    }

    /// Negate.
    pub fn negate(inner: FilterExpr) -> Self {
        FilterExpr::Not(Box::new(inner))
    }

    /// The largest weakening of this predicate that [`Self::to_sql`] can render.
    ///
    /// Drops conjuncts of a top-level [`Self::And`] that fail to render, leaving
    /// everything else intact; a node that cannot render and is not an `And`
    /// collapses to [`Self::Literal`]`(true)`.
    ///
    /// **Only sound where the caller re-applies the original predicate above the
    /// scan.** The result matches a *superset* of the intended rows. Dropping a
    /// conjunct of an `And` widens; that is why an `Or` is only ever dropped
    /// whole — discarding one branch of a disjunction would *narrow* the result
    /// and silently lose rows.
    pub fn sql_pushable(&self) -> FilterExpr {
        if self.to_sql().is_ok() {
            return self.clone();
        }
        match self {
            FilterExpr::And(parts) => FilterExpr::all(parts.iter().map(Self::sql_pushable)),
            _ => FilterExpr::Literal(true),
        }
    }

    /// Whether this matches every row, so callers can skip building a filtered
    /// path at all.
    pub fn is_trivially_true(&self) -> bool {
        match self {
            FilterExpr::Literal(b) => *b,
            FilterExpr::And(parts) => parts.iter().all(Self::is_trivially_true),
            FilterExpr::Or(parts) => parts.iter().any(Self::is_trivially_true),
            // Conservative: `Not(Literal(false))` is trivially true, but saying
            // "no" only costs an unnecessary filtered path, never correctness.
            _ => false,
        }
    }

    /// Render to SQL text for a backend that wants a predicate string.
    ///
    /// Every non-leaf child is parenthesised unconditionally. That is not
    /// cosmetic: composing predicates by string concatenation is only safe
    /// while no operand contains a lower-precedence operator, and nothing
    /// enforces that at a `format!` call site.
    ///
    /// # Errors
    ///
    /// [`ToSqlError::Unsupported`] when a construct has no representation in
    /// the target dialect.
    pub fn to_sql(&self) -> Result<String, ToSqlError> {
        match self {
            FilterExpr::Literal(true) => Ok("true".to_string()),
            FilterExpr::Literal(false) => Ok("false".to_string()),
            FilterExpr::And(parts) => {
                if parts.is_empty() {
                    return Ok("true".to_string());
                }
                let folded = fold_same_column_ins(parts);
                let rendered: Result<Vec<_>, _> =
                    folded.iter().map(|p| p.to_sql().map(paren)).collect();
                Ok(rendered?.join(" AND "))
            }
            FilterExpr::Or(parts) => {
                if parts.is_empty() {
                    return Ok("false".to_string());
                }
                let rendered: Result<Vec<_>, _> =
                    parts.iter().map(|p| p.to_sql().map(paren)).collect();
                Ok(rendered?.join(" OR "))
            }
            FilterExpr::Not(inner) => Ok(format!("NOT {}", paren(inner.to_sql()?))),
            FilterExpr::Compare {
                column,
                op,
                value: Scalar::Int(i64::MIN),
            } => respell_i64_min_compare(column, *op).to_sql(),
            FilterExpr::Compare { column, op, value } => Ok(format!(
                "{} {} {}",
                quote_ident(column)?,
                op.as_sql(),
                scalar_to_sql(value)
            )),
            FilterExpr::In { column, values } if values.contains(&Scalar::Int(i64::MIN)) => {
                // `i64::MIN` has no literal (see `respell_i64_min_compare`), so
                // it leaves the list as `col = MIN`, respelled, and the rest of
                // the list keeps its own rendering. `col IN (a, MIN)` and
                // `col IN (a) OR col = MIN` agree in three-valued logic.
                let rest: Vec<Scalar> = values
                    .iter()
                    .filter(|v| **v != Scalar::Int(i64::MIN))
                    .cloned()
                    .collect();
                let min = FilterExpr::compare(column.clone(), CmpOp::Eq, Scalar::Int(i64::MIN));
                if rest.is_empty() {
                    return min.to_sql();
                }
                FilterExpr::Or(vec![FilterExpr::one_of(column.clone(), rest), min]).to_sql()
            }
            FilterExpr::In { column, values } => {
                if values.is_empty() {
                    // `col IN ()` is a parse error in the target dialect, and
                    // the predicate is unsatisfiable anyway.
                    return Ok("false".to_string());
                }
                // A NULL in the list is not just another candidate value: it
                // makes a *miss* unknown rather than false, so the predicate can
                // be TRUE or NULL but never FALSE.
                //
                // Rendering it literally as `col IN (NULL, 0)` is not safe.
                // Lance evaluates a lone `col IN (NULL, ...)` correctly, but
                // once two `IN` predicates over the same column meet under a
                // `NOT` its optimizer rewrites them into a form that loses the
                // unknown: `NOT ((i IN (NULL)) AND (i IN (0)))` selected every
                // row instead of only the non-null, non-zero ones. Swapping
                // either side to `=` made it correct again, so the trigger is
                // the multi-`InList`-on-one-column rewrite, not our `NOT`.
                //
                // Emitting the unknown explicitly sidesteps that path entirely
                // and matches `eval`'s Kleene `In` exactly.
                let (nulls, non_nulls): (Vec<_>, Vec<_>) =
                    values.iter().partition(|v| matches!(v, Scalar::Null));
                if nulls.is_empty() {
                    // A single candidate renders as `=`, never as a one-element
                    // `IN`. The two are identical in three-valued logic — both
                    // are NULL on a NULL column — but only the `IN` form feeds
                    // the multi-`InList` rewrite described above, and that
                    // rewrite loses the unknown whether the NULL arrives in the
                    // list *or in the column*.
                    //
                    // The list-side hole was closed above. The column-side one
                    // was still open: with `f` NULL on some rows,
                    // `NOT ((f IN (1.0)) AND (f IN (2.5)))` returned every row,
                    // including the NULLs, because Lance folded the disjoint
                    // conjunction to FALSE — sound in two-valued logic, wrong
                    // when the column can be NULL, where the conjunction is
                    // unknown. Emitting `=` leaves no `InList` for the rewrite
                    // to pair. See issue #212.
                    if let [only] = non_nulls.as_slice() {
                        return Ok(format!(
                            "{} = {}",
                            quote_ident(column)?,
                            scalar_to_sql(only)
                        ));
                    }
                    let items: Vec<_> = non_nulls.iter().map(|v| scalar_to_sql(v)).collect();
                    return Ok(format!(
                        "{} IN ({})",
                        quote_ident(column)?,
                        items.join(", ")
                    ));
                }
                if non_nulls.is_empty() {
                    // Every candidate is NULL: the predicate is unknown for
                    // every row, whatever the column holds.
                    return Ok(SQL_UNKNOWN.to_string());
                }
                let items: Vec<_> = non_nulls.iter().map(|v| scalar_to_sql(v)).collect();
                Ok(format!(
                    "({} IN ({}) OR {})",
                    quote_ident(column)?,
                    items.join(", "),
                    SQL_UNKNOWN
                ))
            }
            FilterExpr::ArrayContains { column, value } => Ok(format!(
                "array_contains({}, {})",
                quote_ident(column)?,
                scalar_to_sql(value)
            )),
            FilterExpr::StringMatch {
                column,
                kind,
                pattern,
            } => {
                // Refusing here rather than at construction is the point of the
                // variant: the pattern is representable, this *dialect* just
                // cannot express it. A `%`/`_` inside the pattern would be read
                // as a wildcard, and there is no `ESCAPE` clause to disarm it.
                if pattern.contains('%') || pattern.contains('_') {
                    return Err(ToSqlError::Unsupported(format!(
                        "LIKE pattern {pattern:?} contains a SQL wildcard and this \
                         dialect has no ESCAPE clause"
                    )));
                }
                let escaped = pattern.replace('\'', "''");
                let column = quote_ident(column)?;
                Ok(match kind {
                    StringMatchKind::Contains => format!("{column} LIKE '%{escaped}%'"),
                    StringMatchKind::StartsWith => format!("{column} LIKE '{escaped}%'"),
                    StringMatchKind::EndsWith => format!("{column} LIKE '%{escaped}'"),
                })
            }
            FilterExpr::IsNull(column) => Ok(format!("{} IS NULL", quote_ident(column)?)),
            FilterExpr::IsNotNull(column) => Ok(format!("{} IS NOT NULL", quote_ident(column)?)),
            FilterExpr::Raw(s) => Ok(s.clone()),
        }
    }
}

/// `column op i64::MIN`, spelled without the literal `-9223372036854775808`.
///
/// The SQL dialect parses that literal as the negation of `9223372036854775808`,
/// which overflows `i64` and becomes a `Float64`; comparing it with an `Int64`
/// column then fails the whole scan (`could not convert to literal of type
/// 'Int64'`). Each operator is rewritten to an equivalent in three-valued logic
/// that names only `i64::MIN + 1` — NULL on a NULL column, the same truth
/// value everywhere else:
///
/// * `= MIN`, `<= MIN` → `< MIN + 1`;
/// * `<> MIN`, `> MIN` → `>= MIN + 1`;
/// * `>= MIN` → `IS NOT NULL OR unknown` (true, or NULL on a NULL column);
/// * `< MIN` → `IS NULL AND unknown` (false, or NULL on a NULL column).
fn respell_i64_min_compare(column: &str, op: CmpOp) -> FilterExpr {
    let next = Scalar::Int(i64::MIN + 1);
    let unknown = || FilterExpr::In {
        column: column.to_string(),
        values: vec![Scalar::Null],
    };
    match op {
        CmpOp::Eq | CmpOp::LtEq => FilterExpr::compare(column, CmpOp::Lt, next),
        CmpOp::NotEq | CmpOp::Gt => FilterExpr::compare(column, CmpOp::GtEq, next),
        CmpOp::GtEq => FilterExpr::Or(vec![FilterExpr::IsNotNull(column.to_string()), unknown()]),
        CmpOp::Lt => FilterExpr::And(vec![FilterExpr::IsNull(column.to_string()), unknown()]),
    }
}

/// Wrap in parentheses unless it is already a bare token.
fn paren(s: String) -> String {
    if s == "true" || s == "false" {
        s
    } else {
        format!("({s})")
    }
}

/// Render a column name as a backtick-quoted identifier.
///
/// Backticks are the only identifier quote the Lance filter dialect has: its
/// tokenizer treats `` ` `` as the delimited-identifier start, and its planner
/// reads a backtick-quoted name as an exact column reference. A double quote is
/// a **string literal** there, never an identifier.
///
/// Quoting is unconditional rather than "only when needed". A bare name is not
/// just a parse hazard (`order`, `my prop`, `has'sq`): `null` and `true` parse
/// as *literals* and silently turn the predicate into a constant, and a bare
/// name that misses the schema falls back to a case-insensitive match. Deciding
/// which names are safe bare would mean tracking the parser's keyword list;
/// quoting every name is exact for all of them.
///
/// # Errors
///
/// [`ToSqlError::Unsupported`] for a name containing a backtick. The SQL
/// tokenizer accepts the doubled-backtick escape, but Lance then resolves the
/// unescaped name as a *field path*, where a bare backtick is a syntax error
/// (`Column has`tick does not exist`). No spelling reaches such a column, so the
/// conjunct is refused and [`FilterExpr::sql_pushable`] leaves it to the
/// residual filter.
fn quote_ident(name: &str) -> Result<String, ToSqlError> {
    if name.contains('`') {
        return Err(ToSqlError::Unsupported(format!(
            "column name {name:?} contains a backtick, which the storage filter \
             dialect cannot reference"
        )));
    }
    Ok(format!("`{name}`"))
}

/// Render a literal. The single place string escaping happens.
/// SQL for a boolean whose value is unknown.
///
/// A bare `NULL` is untyped and the optimizer is free to fold it; the cast
/// pins it as a boolean so three-valued logic is preserved through `NOT` and
/// `AND`/`OR`.
const SQL_UNKNOWN: &str = "CAST(NULL AS BOOLEAN)";

fn scalar_to_sql(v: &Scalar) -> String {
    match v {
        Scalar::Null => "NULL".to_string(),
        Scalar::Bool(b) => b.to_string(),
        Scalar::Int(i) => i.to_string(),
        Scalar::UInt(u) => u.to_string(),
        Scalar::Float(f) => f.to_string(),
        // Doubling is the SQL-standard escape. Four hand-rolled copies of this
        // existed across the workspace; one of them was a live bug.
        Scalar::Str(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

/// Tunable knobs for a vector / multi-vector ANN query.
///
/// `Default` (all `None`) lets Lance pick its built-in defaults, i.e. the
/// behavior before these knobs existed. `nprobes` controls how many IVF
/// partitions are probed (higher = better recall, slower); `refine_factor` re-ranks
/// `refine_factor * k` index candidates with exact distances (recovers PQ error);
/// `ef` sets the HNSW search-time beam width (candidate list size).
#[derive(Debug, Clone, Copy, Default)]
pub struct VectorQueryOpts {
    /// Number of IVF partitions to probe. `None` = Lance default.
    pub nprobes: Option<usize>,
    /// Exact-distance re-rank factor over the candidate set. `None` = no refine.
    pub refine_factor: Option<u32>,
    /// HNSW search-time beam width (candidate list size). `None` = Lance default
    /// `1.5 * k`, which is too small for good recall on larger graphs; higher =
    /// better recall, slower.
    pub ef: Option<usize>,
}

/// Column projection for backend queries.
#[derive(Debug, Clone)]
pub enum ColumnProjection {
    /// Select specific columns by name.
    Columns(Vec<String>),
    /// Select all columns.
    All,
}

/// Scan request for table reads.
#[derive(Debug, Clone)]
pub struct ScanRequest {
    /// Table name to scan.
    pub table_name: String,
    /// Columns to project.
    pub columns: ColumnProjection,
    /// Filter expression.
    pub filter: FilterExpr,
    /// Maximum number of raw rows the backend returns.
    ///
    /// **Raw rows, not entities.** The cut happens inside the Lance scanner,
    /// below MVCC dedup and below the L0 merge, so it counts physical rows
    /// including superseded versions and tombstones. See [`Self::with_limit`]
    /// before setting this.
    pub limit: Option<usize>,
    /// Optional Lance branch to read from. `None` = primary (main) branch.
    ///
    /// Set by the storage manager when a session has fork scope active;
    /// see `crate::backend::lance_branch` for the underlying primitives.
    pub branch: Option<String>,
    /// Per-query execution counters, when this scan belongs to a query.
    ///
    /// The backend layer never sees a [`QueryContext`](crate::QueryContext) —
    /// `ScanRequest` is the only object that crosses into it — so this is how a
    /// count taken at the point a branch scan *executes* gets attributed back to
    /// the query that caused it. `None` for scans issued outside a query
    /// (compaction, recovery, index builds).
    pub counters: Option<Arc<QueryCounters>>,
}

impl ScanRequest {
    /// Create a scan request for all columns with no filter.
    pub fn all(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: ColumnProjection::All,
            filter: FilterExpr::Literal(true),
            limit: None,
            branch: None,
            counters: None,
        }
    }

    /// Builder: set columns.
    pub fn with_columns(mut self, columns: Vec<String>) -> Self {
        self.columns = ColumnProjection::Columns(columns);
        self
    }

    /// Builder: set filter.
    ///
    /// Takes a structured [`FilterExpr`] and nothing else. There is
    /// deliberately no `From<&str>` shim: opaque predicate text has to be
    /// spelled [`FilterExpr::Raw`] at the call site, so every place a backend
    /// is handed something only a SQL speaker can evaluate is greppable.
    pub fn with_filter(mut self, filter: FilterExpr) -> Self {
        self.filter = filter;
        self
    }

    /// Builder: cap the number of **raw rows** the backend returns.
    ///
    /// # This cannot implement a Cypher `LIMIT`
    ///
    /// The backend applies this with `scanner.limit(...)` after `project` and
    /// `filter` but *before* anything that reconciles versions. A labelled read
    /// is three stages and this is the first:
    ///
    /// ```text
    /// ScanRequest            raw rows, filtered only by `_version <= hwm`
    ///   -> mvcc_dedup_batch       sort (_vid ASC, _version DESC), first per vid
    ///   -> merge_lance_and_l0     concat the L0 batch, dedup again
    /// ```
    ///
    /// So a limit here truncates while every version of every vid is still
    /// present, and rows arrive in write order — meaning the cut keeps the
    /// *oldest* surviving version of a vid and discards the newest. Dedup then
    /// has only the stale row to choose from and returns it. No error, a
    /// well-formed wrong answer. `bugs::issue_239_scan_limit_precedes_mvcc_dedup`
    /// demonstrates exactly this: `limit(2)` returns a vertex with `n = 1`
    /// whose committed value is `100`.
    ///
    /// This class has shipped before. The only historical caller was
    /// `rebuild_vid_labels_index` with `.with_limit(100_000)`; every vertex past
    /// the cap was left out of the index, and because the traversal label filter
    /// *keeps* rows it cannot resolve, a `Post` predicate admitted `Comment`s at
    /// LDBC SF1 (#211).
    ///
    /// # When it is safe
    ///
    /// Only for a scan whose result is not interpreted as a set of current
    /// entities: sampling a table's physical shape, bounding a diagnostic read,
    /// or reading a table that is single-version by construction. If the caller
    /// would be upset by a superseded row, a deleted row, or fewer distinct
    /// entities than it asked for, this is the wrong tool.
    ///
    /// A sound Cypher `LIMIT` pushdown has to act **above** the merge, where a
    /// vid appears exactly once — see #239.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Builder: set the Lance branch to read from.
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Builder: set the Lance branch from an `Option`.
    pub fn with_optional_branch(mut self, branch: Option<String>) -> Self {
        self.branch = branch;
        self
    }

    /// Builder: attach the per-query counter set from an `Option`.
    ///
    /// Takes an `Option` so call sites can forward
    /// `ctx.and_then(|c| c.counters.clone())` without branching.
    ///
    /// Note what this does and does not buy, because the difference has cost
    /// time. Attaching counters makes a scan report *index* work — the
    /// execution-stats callback feeds `index_scans`, `index_comparisons` and
    /// `lance_iops`, and `count_branch_scan` records a fork-branch read. It
    /// does **not** make the scan report rows: `rows_scanned` and
    /// `storage_rows` are added by `columnar_scan::merge_lance_and_l0` from the
    /// batch it gets back, after this request has returned.
    ///
    /// A `count_storage_rows` used to sit beside `count_branch_scan`, adding
    /// both row counters, with no caller anywhere in the workspace. It read as
    /// the mechanism by which requests count rows, so switching a call site to
    /// a counted scan looked like it would start counting them, and it did not.
    /// Wiring it would have double-counted every Lance row against the site
    /// above. Removed rather than left as a false affordance; count rows where
    /// the batch is in hand.
    pub fn with_counters(mut self, counters: Option<Arc<QueryCounters>>) -> Self {
        self.counters = counters;
        self
    }

    /// Records that this scan executed against a fork branch, if counting is on.
    pub fn count_branch_scan(&self) {
        if let Some(c) = &self.counters {
            c.add_branch_scan();
        }
    }
}

/// Write mode for backend writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Append rows to existing data.
    Append,
    /// Replace all existing data (atomic overwrite).
    Overwrite,
}

/// Distance metric for vector search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance.
    L2,
    /// Cosine distance.
    Cosine,
    /// Dot product distance.
    Dot,
}

/// The buildable physical vector-index shapes and their tuning parameters.
///
/// A backend-agnostic mirror of the ANN index families a storage backend can
/// construct. The logical MUVERA type is resolved to its `inner` shape before
/// reaching the backend, so it never appears here. `num_partitions` is already
/// resolved to a concrete value (the logical `Option` default of "auto" is
/// mapped to a single partition by the caller, matching the prior behavior).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorIndexKind {
    /// Brute-force flat index (a single IVF partition).
    Flat,
    /// IVF with uncompressed vectors.
    IvfFlat { num_partitions: u32 },
    /// IVF with Product Quantization.
    IvfPq {
        num_partitions: u32,
        num_sub_vectors: u32,
        num_bits: u8,
    },
    /// IVF with Scalar Quantization.
    IvfSq { num_partitions: u32 },
    /// IVF with RabitQ Quantization. `num_bits` `None` keeps the backend default.
    IvfRq {
        num_partitions: u32,
        num_bits: Option<u8>,
    },
    /// IVF-HNSW without quantization (highest recall).
    HnswFlat {
        m: u32,
        ef_construction: u32,
        num_partitions: u32,
    },
    /// IVF-HNSW with Scalar Quantization.
    HnswSq {
        m: u32,
        ef_construction: u32,
        num_partitions: u32,
    },
    /// IVF-HNSW with Product Quantization.
    HnswPq {
        m: u32,
        ef_construction: u32,
        num_sub_vectors: u32,
        num_partitions: u32,
    },
}

/// Parameters for building a physical vector (ANN) index.
///
/// Pairs the distance metric with the index shape ([`VectorIndexKind`]). This is
/// the backend-agnostic input to
/// [`StorageBackend::create_vector_index`](crate::backend::StorageBackend::create_vector_index).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorIndexParams {
    /// Distance metric used to compare vectors.
    pub metric: DistanceMetric,
    /// The index shape and its tuning parameters.
    pub kind: VectorIndexKind,
}

/// Scalar index type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarIndexType {
    /// B-Tree index for range queries.
    BTree,
    /// Bitmap index for low-cardinality columns.
    Bitmap,
    /// Label list index for array columns.
    LabelList,
}

/// Index metadata.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Columns covered by the index.
    pub columns: Vec<String>,
    /// Index type description (backend-specific, e.g., "IVF_PQ", "BTree").
    pub index_type: String,
}

/// What a single [`StorageBackend::optimize_table`] call did.
///
/// [`StorageBackend::optimize_table`]: crate::backend::StorageBackend::optimize_table
///
/// Backend-neutral by construction: the Lance backend fills it from
/// `CompactionMetrics` and `RemovalStats`, and a backend that optimizes nothing
/// returns [`OptimizeReport::default`], which reads correctly as "ran, did
/// nothing". That distinction is the whole point — before this type existed,
/// `optimize_table` returned `Result<()>` and every number describing what
/// compaction actually did was discarded one layer below the struct that
/// reported it (#172).
///
/// An all-zero report is a **real reading**, not a missing one: Lance skips the
/// commit entirely when the compaction plan is empty, returning zeroed metrics.
/// The denominator that separates "nothing to do" from "no table was visited"
/// lives one level up, on [`CompactionStats::tables_optimized`].
///
/// [`CompactionStats::tables_optimized`]: crate::compaction::CompactionStats::tables_optimized
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OptimizeReport {
    /// Fragments merged away and rewritten.
    pub fragments_removed: usize,
    /// Fragments written in their place. Never more than `fragments_removed`
    /// for a compaction that made progress.
    pub fragments_added: usize,
    /// Data files merged away. Counts deletion files too, so it can exceed
    /// `fragments_removed` but never trails it.
    pub files_removed: usize,
    /// Data files written. Lance documents this as equal to the number of
    /// fragments added.
    pub files_added: usize,
    /// Bytes freed by pruning dataset versions older than the retention window.
    ///
    /// This is *reclaimed*, not "before minus after": it counts bytes deleted
    /// from disk by version cleanup, and does not account for space saved by
    /// re-encoding, which the storage layer does not report. It reads `0` for
    /// any database younger than the retention window — which is every
    /// short-lived one, including test fixtures.
    pub bytes_reclaimed: u64,
    /// Dataset versions pruned.
    pub old_versions_removed: u64,
}

impl OptimizeReport {
    /// Did this call find nothing to do?
    ///
    /// True only for a report that is zero in every field — which the storage
    /// layer produces for an empty compaction plan, and which is therefore a
    /// measurement rather than an absence.
    pub fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

impl std::ops::AddAssign for OptimizeReport {
    fn add_assign(&mut self, rhs: Self) {
        self.fragments_removed += rhs.fragments_removed;
        self.fragments_added += rhs.fragments_added;
        self.files_removed += rhs.files_removed;
        self.files_added += rhs.files_added;
        self.bytes_reclaimed += rhs.bytes_reclaimed;
        self.old_versions_removed += rhs.old_versions_removed;
    }
}

/// A full-text search's stored hits and the scores of documents not yet
/// stored, ranked together.
///
/// See [`StorageBackend::full_text_search_with_unstored`](super::traits::StorageBackend::full_text_search_with_unstored).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TextSearch {
    /// The stored hits, as [`StorageBackend::full_text_search`](super::traits::StorageBackend::full_text_search)
    /// returns them: the table's rows with a `_score` column, not
    /// necessarily sorted.
    pub stored: Vec<RecordBatch>,
    /// `(id, score)` for each matching unstored document.
    pub unstored: Vec<(u64, f32)>,
}

/// Occupancy and capacity of a storage backend's in-process caches.
///
/// Reported by [`crate::backend::StorageBackend::cache_usage`]. For Lance
/// these are the shared session's index and metadata caches, sized by
/// `UniConfig::lance_index_cache_bytes` and
/// `UniConfig::lance_metadata_cache_bytes`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendCacheUsage {
    /// Bytes currently held by the index cache.
    pub index_bytes: u64,
    /// Capacity the index cache evicts down to.
    pub index_capacity_bytes: u64,
    /// Bytes currently held by the metadata cache.
    pub metadata_bytes: u64,
    /// Capacity the metadata cache evicts down to.
    pub metadata_capacity_bytes: u64,
}
