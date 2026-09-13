//! Locy aggregate and predicate plugins.
//!
//! Locy aggregates (used in `FOLD value AS X`) require `Semilattice`
//! metadata so the fixpoint engine can verify its monotonicity proofs
//! explicitly.
//!
//! Locy predicates evaluate to boolean (or fuzzy) columns and are the
//! surface neural predicates plug into.

use arrow_array::{Array, BooleanArray, Float64Array};
use arrow_schema::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::scalar::ScalarValue;

use crate::errors::FnError;
use crate::traits::scalar::ArgType;

/// Probability semiring selected for a `FOLD` evaluation.
///
/// Mirrors the host's `uni_locy::SemiringKind` minus the provenance-only
/// `TopKProofs` / `BddExact` variants, which are handled above the aggregate
/// trait by the executor. Plugin aggregates only ever see the two value-level
/// combinators: independence (`AddMult`) and Viterbi/fuzzy (`MaxMin`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FoldSemiring {
    /// Independence-mode probability: noisy-OR (`1 − ∏(1 − pᵢ)`) / product.
    #[default]
    AddMult,
    /// Viterbi / fuzzy-truth: max-disjunction / min-conjunction.
    MaxMin,
}

/// Per-fold evaluation context threaded into [`LocyAggState::ingest_indices`].
///
/// Carries the probability-domain policy (`strict`), the underflow guard
/// (`epsilon`, used by bounded-product log-space switching), and the active
/// [`FoldSemiring`]. Constructed once per `FOLD` execution and passed by
/// reference to every per-group ingest call.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FoldContext {
    /// When `true`, probability-domain aggregates error on inputs outside
    /// `[0, 1]` instead of clamping them with a warning.
    pub strict: bool,
    /// Underflow threshold: bounded-product switches to log-space once the
    /// running product drops below this value. `0.0` disables the switch.
    pub epsilon: f64,
    /// Active probability semiring for this fold.
    pub semiring: FoldSemiring,
}

impl Default for FoldContext {
    fn default() -> Self {
        Self {
            strict: false,
            epsilon: 0.0,
            semiring: FoldSemiring::AddMult,
        }
    }
}

/// A Locy aggregate plugin (`FOLD value AS plugin_name`).
///
/// The fixpoint engine uses `semilattice()` metadata to verify monotonicity
/// and prove termination; non-monotone aggregates are rejected at compile
/// time when used inside a recursive Locy clause.
///
/// `Debug` is a supertrait so `Arc<dyn LocyAggregate>` can sit inside
/// `#[derive(Debug)]` structs in the fixpoint engine. The 9 built-in
/// impls already `#[derive(Debug)]` their unit/struct types.
pub trait LocyAggregate: Send + Sync + std::fmt::Debug {
    /// Lattice properties used by the fixpoint engine.
    fn semilattice(&self) -> Semilattice;

    /// Declared output type for `FOLD` results.
    fn output_type(&self) -> DataType;

    /// Output type given the aggregate's *input* column type.
    ///
    /// Defaults to [`LocyAggregate::output_type`] (input-independent).
    /// Type-preserving aggregates (`MIN` / `MAX`) override this to return the
    /// input type so an `Int64` column folds to an `Int64` result rather than
    /// being widened to `Float64`.
    fn output_type_for_input(&self, _input: &DataType) -> DataType {
        self.output_type()
    }

    /// Construct a fresh per-grouping state.
    fn create(&self) -> Box<dyn LocyAggState>;

    /// Initial accumulator value for the row-level fast path used by
    /// the Locy fixpoint engine ([`MonotonicAggState`]). For numeric
    /// aggregates this is the identity element (`0` for SUM/COUNT/NOR,
    /// `1` for PROD, `+inf` for MIN, `-inf` for MAX). Returns `None`
    /// for aggregates that have no row-level fast path (`AVG`, `COLLECT`
    /// — these run outside the fast path).
    ///
    /// [`MonotonicAggState`]: ../../uni_query/query/df_graph/locy_fixpoint/struct.MonotonicAggState.html
    fn initial_accum_f64(&self) -> Option<f64> {
        None
    }

    /// Which way this aggregate's value moves as the fixpoint grows.
    ///
    /// Answers the question [`Semilattice::monotone_join`] cannot: that flag
    /// says *whether* the aggregate is monotone, never *which direction*. The
    /// two cannot be merged, because `MIN` and `MAX` are both monotone and share
    /// a single [`Semilattice::BOUNDED_MIN_MAX`] constant.
    ///
    /// Used by the Locy compiler to decide whether a `REQUIRE` threshold can
    /// participate in recursion (issue #265): a lower bound over a
    /// non-decreasing fold, or an upper bound over a non-increasing one, can
    /// only flip false to true as the fixpoint grows, so the constrained
    /// operator stays monotone and its least fixpoint exists. The reverse
    /// pairings oscillate and are rejected.
    ///
    /// Defaults to [`FoldDirection::Unknown`], which rejects `REQUIRE`. An
    /// aggregate that wants to support it must say so explicitly — declining by
    /// omission is the safe answer, and it keeps this addition backward
    /// compatible for existing implementors.
    fn direction(&self) -> FoldDirection {
        FoldDirection::Unknown
    }

    /// Row-level update step on a primitive `f64` accumulator.
    ///
    /// Returns the new accumulator value after folding `val` into `accum`.
    /// `strict` enables strict-mode probability-domain validation for
    /// `MNOR` / `MPROD` (inputs outside `[0, 1]` produce a `FnError`
    /// instead of being clamped with a warning).
    ///
    /// Default impl returns [`FnError::CODE_UNKNOWN_FUNCTION`] indicating
    /// the aggregate has no row-level fast path; the fixpoint engine
    /// must use the batch-shape [`LocyAggState::ingest`] path instead.
    ///
    /// # Errors
    ///
    /// - In strict mode with an out-of-domain value for a probabilistic
    ///   aggregate.
    /// - When the aggregate has no row-level path (default impl).
    fn update_step(&self, _accum: f64, _val: f64, _strict: bool) -> Result<f64, FnError> {
        Err(FnError::new(
            FnError::CODE_UNKNOWN_FUNCTION,
            "aggregate has no row-level update_step; use ingest()",
        ))
    }

    /// True if this aggregate operates on the probability domain `[0, 1]`.
    ///
    /// Used by the Locy fixpoint engine to trigger provenance tracking
    /// (shared-proof detection) when any rule's stratum has a
    /// probability-domain aggregate. Default `false`. Override `true`
    /// for `MNOR`, `MPROD`, and future probability-domain aggregates
    /// authored by users.
    fn is_probability_aggregate(&self) -> bool {
        false
    }

    /// True if this aggregate is the noisy-OR semiring (`1 − ∏(1 − pᵢ)`).
    ///
    /// Used by the fixpoint engine's `apply_post_fixpoint_chain` to
    /// select the per-row probability combination operator when
    /// multiple independent evidence sources are joined. Default
    /// `false`. Override `true` for `MNOR`.
    fn is_noisy_or(&self) -> bool {
        false
    }
}

/// Per-grouping state for a [`LocyAggregate`].
///
/// `'static` is required so the fixpoint engine can safely downcast
/// `&dyn LocyAggState` to the concrete state via [`LocyAggState::as_any`]
/// during `merge`. Implementations expose `as_any` with a one-liner
/// `fn as_any(&self) -> &dyn std::any::Any { self }`.
pub trait LocyAggState: Send + 'static {
    /// Return `&dyn Any` for safe downcasting in `merge` implementations.
    ///
    /// Default `impl` is `self`. Implementations should not override unless
    /// they need to expose a different concrete type than the implementor.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Ingest the rows at `indices` of `col` into the state under `cx`.
    ///
    /// This is the primitive the non-recursive `FOLD` executor calls once per
    /// key group: `col` is the whole fold-input column and `indices` selects
    /// the rows belonging to one group. [`FoldContext`] carries the
    /// strict-domain / epsilon / semiring policy. Implementations skip null
    /// rows. Built-in aggregates override this for byte-identical, context-aware
    /// folding; user aggregates may rely on the [`LocyAggState::ingest`] default
    /// if they do not need per-group dispatch.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if a value cannot be ingested (e.g., a strict
    /// probability-domain violation or an unexpected Arrow type).
    fn ingest_indices(
        &mut self,
        col: &dyn Array,
        indices: &[usize],
        cx: &FoldContext,
    ) -> Result<(), FnError>;

    /// Ingest every row of column `value_col` in `batch` into the state.
    ///
    /// Convenience wrapper over [`LocyAggState::ingest_indices`] across all
    /// rows with a default [`FoldContext`]. Kept for callers and tests that
    /// fold a whole batch as a single group.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the column cannot be ingested.
    fn ingest(&mut self, batch: &RecordBatch, value_col: usize) -> Result<(), FnError> {
        let indices: Vec<usize> = (0..batch.num_rows()).collect();
        self.ingest_indices(batch.column(value_col), &indices, &FoldContext::default())
    }

    /// Merge `other`'s state into `self`.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the states cannot be merged (e.g., type
    /// mismatch between aggregate instances).
    fn merge(&mut self, other: &dyn LocyAggState) -> Result<(), FnError>;

    /// Produce the final aggregated value.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the value cannot be finalized.
    fn finalize(&self) -> Result<ScalarValue, FnError>;

    /// Fixpoint shortcut: `true` once no further `ingest` can change state.
    ///
    /// `MAX` over a bounded domain returns `true` at the top; `SUM` never
    /// returns `true`. The fixpoint engine uses this to terminate early.
    fn is_at_top(&self) -> bool {
        false
    }
}

/// Which way an aggregate's value moves as the fixpoint grows.
///
/// Re-exported from `uni-common` because the Locy compiler must agree on it
/// and `uni-locy` does not depend on this crate. Deliberately not a field on
/// [`Semilattice`]: that struct is not `#[non_exhaustive]`, so a new field
/// would break every plugin constructing one, and `MIN` and `MAX` are
/// indistinguishable within it — both return [`Semilattice::BOUNDED_MIN_MAX`].
pub use uni_common::locy::FoldDirection;

/// Lattice properties of an aggregate.
///
/// Used by the Locy fixpoint engine to verify monotonicity and prove
/// termination. The flags are not independent: `monotone_join` typically
/// implies `commutative` and `associative`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Semilattice {
    /// `f(x, x) == x`. Idempotent aggregates can deduplicate inputs.
    pub idempotent: bool,
    /// `f(x, y) == f(y, x)`. Commutative aggregates are order-independent.
    pub commutative: bool,
    /// `f(f(x, y), z) == f(x, f(y, z))`. Associative aggregates can be
    /// partial-aggregated.
    pub associative: bool,
    /// `f` preserves or raises the partial order. Monotone aggregates
    /// produce sound fixpoints; non-monotone ones cannot be used inside
    /// recursive Locy clauses.
    pub monotone_join: bool,
    /// Bounded domain — `is_at_top()` may return `true`. Enables fixpoint
    /// shortcuts (no further ingest can change the state).
    pub has_top: bool,
}

impl Semilattice {
    /// Properties of a non-monotone aggregate (`SUM`, `AVG`).
    ///
    /// Such aggregates may not appear inside recursive Locy clauses.
    pub const NON_MONOTONE: Self = Self {
        idempotent: false,
        commutative: true,
        associative: true,
        monotone_join: false,
        has_top: false,
    };

    /// Properties of `MIN` / `MAX` over a bounded domain — fully monotone.
    pub const BOUNDED_MIN_MAX: Self = Self {
        idempotent: true,
        commutative: true,
        associative: true,
        monotone_join: true,
        has_top: true,
    };

    /// Properties of `COUNT` — monotone but unbounded.
    pub const COUNT: Self = Self {
        idempotent: false,
        commutative: true,
        associative: true,
        monotone_join: true,
        has_top: false,
    };
}

/// A Locy predicate plugin — boolean (or fuzzy) column over inputs.
pub trait LocyPredicate: Send + Sync {
    /// Static signature.
    fn signature(&self) -> &PredSignature;

    /// Evaluate the predicate over a batch of inputs to a boolean column.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the predicate cannot be evaluated on this input.
    fn evaluate(&self, args: &[ColumnarValue], rows: usize) -> Result<BooleanArray, FnError>;

    /// Optional fuzzy evaluation — `Some(scores)` for predicates that
    /// participate in PROB chains, `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if fuzzy evaluation is unsupported or fails.
    fn evaluate_fuzzy(
        &self,
        _args: &[ColumnarValue],
        _rows: usize,
    ) -> Option<Result<Float64Array, FnError>> {
        None
    }
}

/// A Locy *generator* predicate — a table-valued rule-body condition that binds
/// new variables (1:N), like a Datomic/Souffle functor.
///
/// Where a [`LocyPredicate`] filters rows (1:1 → bool), a generator emits zero or
/// more output tuples per input row, binding a statically-known, fixed number of
/// new variables (`GenSignature::outputs`). The host replicates each input row
/// once per emitted tuple and appends the generated columns.
pub trait LocyGenerator: Send + Sync {
    /// Static signature — input arg types and the fixed output-variable types.
    fn signature(&self) -> &GenSignature;

    /// Generate output tuples for a batch of `rows` input rows.
    ///
    /// Returns a [`GeneratorOutput`] whose `columns` (one per output variable,
    /// in `signature().outputs` order) are parallel to `row_map`, which records
    /// the input row each output tuple was produced from.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if generation fails on this input.
    fn generate(&self, args: &[ColumnarValue], rows: usize) -> Result<GeneratorOutput, FnError>;
}

/// Static signature of a Locy generator predicate.
#[derive(Clone, Debug)]
pub struct GenSignature {
    /// Input argument types.
    pub args: Vec<ArgType>,
    /// Output-variable types. The length is the fixed output arity `K` — it must
    /// be known at plan time so the fixpoint schema can be widened statically.
    pub outputs: Vec<ArgType>,
    /// Volatility.
    pub volatility: Volatility,
}

/// Table-valued output of a [`LocyGenerator::generate`] call.
///
/// `columns[c]` holds the values of output variable `c` (for every emitted
/// tuple), and `row_map[i]` is the index of the input row that produced emitted
/// tuple `i`. All `columns` and `row_map` share the same length (the total number
/// of emitted tuples), which may be more or fewer than the input row count.
#[derive(Clone, Debug)]
pub struct GeneratorOutput {
    /// For each emitted tuple, the input row index it was generated from.
    pub row_map: Vec<u32>,
    /// One array per output variable (in `GenSignature::outputs` order); each has
    /// length `row_map.len()`.
    pub columns: Vec<arrow_array::ArrayRef>,
}

/// Static signature of a Locy predicate.
#[derive(Clone, Debug)]
pub struct PredSignature {
    /// Argument types.
    pub args: Vec<ArgType>,
    /// Volatility.
    pub volatility: Volatility,
    /// Whether `evaluate_fuzzy` returns `Some(...)`.
    pub supports_fuzzy: bool,
    /// Hint for batch sizing — neural predicates often prefer larger batches.
    pub batch_hint: BatchHint,
}

/// Preferred batch size for predicate evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum BatchHint {
    /// Small batches; row-at-a-time is acceptable.
    Small,
    /// Medium batches; the default.
    #[default]
    Medium,
    /// Large batches; the host should accumulate many rows before invoking
    /// (neural predicates benefit dramatically).
    Large,
}

/// Provenance / derivation tracker — placeholder.
///
/// `LocyAggState::provenance` returns an opaque reference that the
/// fixpoint engine uses for shared-proof detection. The exact contents
/// are wired up by the fixpoint engine.
#[derive(Clone, Debug, Default)]
pub struct DerivationTracker {
    _placeholder: (),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semilattice_constants() {
        const {
            assert!(Semilattice::BOUNDED_MIN_MAX.monotone_join);
            assert!(Semilattice::BOUNDED_MIN_MAX.has_top);
            assert!(!Semilattice::NON_MONOTONE.monotone_join);
            assert!(Semilattice::COUNT.monotone_join);
            assert!(!Semilattice::COUNT.has_top);
        }
    }
}
