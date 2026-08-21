// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Backend-agnostic types for storage operations.

use std::sync::Arc;

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
/// # Column names are emitted bare, and must stay that way
///
/// Lance's filter dialect parses a double-quoted name as a **string literal**,
/// not a quoted identifier: `"rid" > 2` matches every row (it is `'rid' > 2`,
/// a constant), and `"no_such_col" > 2` does not even error. So [`Self::to_sql`]
/// cannot defensively quote a column, and adding quotes to "harden" it would
/// silently turn every predicate into a constant. Checking that a column name is
/// a safe bare identifier is therefore the **producer's** job.
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
                let rendered: Result<Vec<_>, _> =
                    parts.iter().map(|p| p.to_sql().map(paren)).collect();
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
            FilterExpr::Compare { column, op, value } => Ok(format!(
                "{} {} {}",
                column,
                op.as_sql(),
                scalar_to_sql(value)
            )),
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
                    let items: Vec<_> = non_nulls.iter().map(|v| scalar_to_sql(v)).collect();
                    return Ok(format!("{} IN ({})", column, items.join(", ")));
                }
                if non_nulls.is_empty() {
                    // Every candidate is NULL: the predicate is unknown for
                    // every row, whatever the column holds.
                    return Ok(SQL_UNKNOWN.to_string());
                }
                let items: Vec<_> = non_nulls.iter().map(|v| scalar_to_sql(v)).collect();
                Ok(format!(
                    "({} IN ({}) OR {})",
                    column,
                    items.join(", "),
                    SQL_UNKNOWN
                ))
            }
            FilterExpr::ArrayContains { column, value } => Ok(format!(
                "array_contains({}, {})",
                column,
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
                Ok(match kind {
                    StringMatchKind::Contains => format!("{column} LIKE '%{escaped}%'"),
                    StringMatchKind::StartsWith => format!("{column} LIKE '{escaped}%'"),
                    StringMatchKind::EndsWith => format!("{column} LIKE '%{escaped}'"),
                })
            }
            FilterExpr::IsNull(column) => Ok(format!("{column} IS NULL")),
            FilterExpr::IsNotNull(column) => Ok(format!("{column} IS NOT NULL")),
            FilterExpr::Raw(s) => Ok(s.clone()),
        }
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
    /// Maximum number of rows to return.
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

    /// Builder: set limit.
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

    /// Records `n` rows produced by this scan, if counting is on.
    pub fn count_storage_rows(&self, n: usize) {
        if let Some(c) = &self.counters {
            c.add_storage_rows(n);
            c.add_rows_scanned(n);
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
