//! Locy vocabulary shared between the compiler front-end and the plugin surface.
//!
//! `uni-locy` (the compiler) and `uni-plugin` (the aggregate trait) do not
//! depend on one another — deliberately, so the front-end can be built and
//! tested without the plugin stack. Anything both must agree on therefore lives
//! here, in the crate they share.

/// Which way an aggregate's value moves as the fixpoint grows.
///
/// Answers the question monotonicity alone cannot: `uni_plugin`'s
/// `Semilattice::monotone_join` says *whether* an aggregate is monotone, never
/// *which direction*, and `MIN` and `MAX` are indistinguishable within it —
/// both report the same `BOUNDED_MIN_MAX` constant.
///
/// Used by the Locy compiler to decide whether a `REQUIRE` threshold may
/// participate in recursion (issue #265). A **lower** bound over a
/// non-decreasing fold, or an **upper** bound over a non-increasing one, can
/// only flip false to true as the fixpoint grows; the constrained operator
/// stays monotone and its least fixpoint exists. The reverse pairings can flip
/// a fact back out, which the fixpoint's whole-row change test reads as
/// progress rather than oscillation, so they are rejected at compile time
/// rather than left to spin to the iteration limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FoldDirection {
    /// The value never decreases as more facts are derived — `MSUM` over
    /// non-negative inputs, `MMAX`, `MCOUNT`, `MNOR`. A **lower** bound
    /// (`>=`, `>`) over such a fold is monotone.
    NonDecreasing,
    /// The value never increases — `MMIN`, `MPROD`. An **upper** bound
    /// (`<=`, `<`) over such a fold is monotone.
    NonIncreasing,
    /// Not declared. `REQUIRE` is rejected rather than guessed at, which is
    /// what an aggregate that does not override the default reports.
    Unknown,
}

impl FoldDirection {
    /// Whether `self` paired with this comparison can only flip false to true.
    ///
    /// `lower_bound` is `true` for `>=` / `>` and `false` for `<=` / `<`.
    /// [`Self::Unknown`] is never admissible.
    pub fn admits_bound(self, lower_bound: bool) -> bool {
        match self {
            Self::NonDecreasing => lower_bound,
            Self::NonIncreasing => !lower_bound,
            Self::Unknown => false,
        }
    }
}
