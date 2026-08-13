//! Cypher query generator and renderer for the metamorphic oracles.
//!
//! [`render`](render::render) turns a generated `uni_cypher::ast::Query` into
//! canonical Cypher text; the proptest strategies here build the ASTs over the
//! fixed seed schema (`metamorphic::seed`). The TLP and NoREC oracles consume a
//! [`Case`] and ask it for the query variants they compare.
//!
//! # Why AST-first
//!
//! The metamorphic transforms are structured AST edits — partitioning a query
//! by predicate `p` means wrapping its `WHERE` in `And`/`Not`/`IsNull` nodes,
//! and the NoREC barrier means inserting a `WITH` clause — not string splicing.
//! Generating ASTs (then rendering) makes those transforms total and precise.
//!
//! # Round-trip discipline (kept in lock-step with `render`)
//!
//! The generator constructs labels/types in the parser's canonical [`LabelExpr`]
//! form (single node label → `Conjunction`, relationship type → `Disjunction`)
//! and emits only non-negative numeric literals (a leading `-` parses as
//! `UnaryOp::Neg`, not a negative literal). Both are required for the round-trip
//! property test [`render_roundtrips_generated_queries`] to hold.

pub mod render;

use proptest::collection::vec as prop_vec;
use proptest::prelude::*;
use proptest::sample::select;

use uni_cypher::ast::{
    BinaryOp, Clause, CypherLiteral, Direction, Expr, LabelExpr, MatchClause, NodePattern,
    PathPattern, Pattern, PatternElement, Query, RelationshipPattern, ReturnClause, ReturnItem,
    SortItem, Statement, WithClause,
};

// ── Seed schema model ────────────────────────────────────────────────────────

/// Scalar kind of a seed property, controlling which literals/operators apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PropTy {
    /// `Int64` columns (`age`, `founded`).
    Int,
    /// `Float64` columns (`score`).
    Float,
    /// `String` columns (`name`, `city`).
    Str,
}

/// A property on a seed label.
#[derive(Clone, Copy, Debug)]
pub struct Prop {
    /// Property name as it appears in Cypher.
    pub name: &'static str,
    /// Scalar kind.
    pub ty: PropTy,
    /// Whether the column is nullable — only nullable props can make a
    /// comparison evaluate to NULL (the TLP `IS NULL` partition).
    pub nullable: bool,
}

const PERSON_PROPS: &[Prop] = &[
    Prop {
        name: "name",
        ty: PropTy::Str,
        nullable: false,
    },
    Prop {
        name: "age",
        ty: PropTy::Int,
        nullable: true,
    },
    Prop {
        name: "score",
        ty: PropTy::Float,
        nullable: true,
    },
    Prop {
        name: "city",
        ty: PropTy::Str,
        nullable: true,
    },
];

const COMPANY_PROPS: &[Prop] = &[
    Prop {
        name: "name",
        ty: PropTy::Str,
        nullable: false,
    },
    Prop {
        name: "founded",
        ty: PropTy::Int,
        nullable: true,
    },
];

/// A bound variable in a generated pattern, with its label and property set.
#[derive(Clone, Copy, Debug)]
pub struct Bound {
    /// Variable name used in the query (`a`, `b`).
    pub var: &'static str,
    /// Node label.
    pub label: &'static str,
    /// Properties of that label.
    pub props: &'static [Prop],
}

const PERSON_A: Bound = Bound {
    var: "a",
    label: "Person",
    props: PERSON_PROPS,
};
const COMPANY_B: Bound = Bound {
    var: "b",
    label: "Company",
    props: COMPANY_PROPS,
};

/// The MATCH pattern of a generated query.
#[derive(Clone, Debug)]
pub enum Shape {
    /// `(a:Person)`.
    Node(Bound),
    /// `(a:Person)-[:WORKS_AT]->(b:Company)`.
    Edge(Bound, Bound),
}

impl Shape {
    fn bounds(&self) -> Vec<Bound> {
        match self {
            Shape::Node(a) => vec![*a],
            Shape::Edge(a, b) => vec![*a, *b],
        }
    }

    fn var_names(&self) -> Vec<&'static str> {
        self.bounds().iter().map(|b| b.var).collect()
    }

    /// Nullable `(var, prop)` targets — the only ones a TLP predicate compares,
    /// so `IS NULL` is reachable.
    fn nullable_targets(&self) -> Vec<(&'static str, Prop)> {
        let mut out = Vec::new();
        for b in self.bounds() {
            for p in b.props {
                if p.nullable {
                    out.push((b.var, *p));
                }
            }
        }
        out
    }

    /// Numeric `(var, prop)` targets — candidates for `sum(...)`.
    fn numeric_targets(&self) -> Vec<(&'static str, Prop)> {
        let mut out = Vec::new();
        for b in self.bounds() {
            for p in b.props {
                if matches!(p.ty, PropTy::Int | PropTy::Float) {
                    out.push((b.var, *p));
                }
            }
        }
        out
    }

    /// Numeric targets restricted to **integer** properties.
    ///
    /// `sum` over an `f64` is not associative, so two execution paths that
    /// accumulate in different orders can legitimately return
    /// `1.0000000000000002` and `1.0`. The bag comparator is exact by design
    /// (TLP and NoREC depend on that), so a differential oracle must not compare
    /// float aggregates at all. Integer `sum`/`count` are reassociation-invariant
    /// and safe.
    fn integer_targets(&self) -> Vec<(&'static str, Prop)> {
        self.bounds()
            .iter()
            .flat_map(|b| {
                b.props
                    .iter()
                    .filter(|p| matches!(p.ty, PropTy::Int))
                    .map(move |p| (b.var, *p))
            })
            .collect()
    }

    /// Expressions a projection may return: each bound variable plus each of its
    /// properties.
    fn projection_choices(&self) -> Vec<Expr> {
        let mut out = Vec::new();
        for b in self.bounds() {
            out.push(Expr::Variable(b.var.to_string()));
            for p in b.props {
                out.push(prop_expr(b.var, p.name));
            }
        }
        out
    }

    fn to_pattern(&self) -> Pattern {
        let elements = match self {
            Shape::Node(a) => vec![node_element(a)],
            Shape::Edge(a, b) => vec![node_element(a), rel_element(), node_element(b)],
        };
        Pattern {
            paths: vec![PathPattern {
                variable: None,
                elements,
                shortest_path_mode: None,
            }],
        }
    }
}

// ── AST builders ─────────────────────────────────────────────────────────────

fn prop_expr(var: &str, name: &str) -> Expr {
    Expr::Property(Box::new(Expr::Variable(var.to_string())), name.to_string())
}

fn and_expr(a: Expr, b: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOp::And,
        right: Box::new(b),
    }
}

fn or_expr(a: Expr, b: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(a),
        op: BinaryOp::Or,
        right: Box::new(b),
    }
}

fn not_expr(e: Expr) -> Expr {
    Expr::UnaryOp {
        op: uni_cypher::ast::UnaryOp::Not,
        expr: Box::new(e),
    }
}

fn is_null_expr(e: Expr) -> Expr {
    Expr::IsNull(Box::new(e))
}

/// Conjoins an optional base predicate with an extra one.
fn conj(base: Option<Expr>, extra: Expr) -> Expr {
    match base {
        Some(b) => and_expr(b, extra),
        None => extra,
    }
}

fn node_element(b: &Bound) -> PatternElement {
    PatternElement::Node(NodePattern {
        variable: Some(b.var.to_string()),
        // Single node label is `Conjunction` in the parser (walker.rs).
        labels: LabelExpr::Conjunction(vec![b.label.to_string()]),
        properties: None,
        where_clause: None,
    })
}

fn rel_element() -> PatternElement {
    PatternElement::Relationship(RelationshipPattern {
        variable: None,
        // Relationship types are always `Disjunction` in the parser (walker.rs).
        types: LabelExpr::Disjunction(vec!["WORKS_AT".to_string()]),
        direction: Direction::Outgoing,
        range: None,
        properties: None,
        where_clause: None,
    })
}

fn ret_item(expr: Expr, alias: Option<&str>) -> ReturnItem {
    ReturnItem::Expr {
        expr,
        alias: alias.map(str::to_string),
        source_text: None,
    }
}

fn match_clause(shape: &Shape, where_clause: Option<Expr>) -> Clause {
    Clause::Match(MatchClause {
        optional: false,
        pattern: shape.to_pattern(),
        where_clause,
        for_update: false,
    })
}

fn return_clause(items: Vec<ReturnItem>) -> Clause {
    Clause::Return(ReturnClause {
        distinct: false,
        items,
        order_by: None,
        skip: None,
        limit: None,
    })
}

/// Builds `WITH <vars>, (pred) AS keep WHERE keep` — the NoREC pushdown barrier.
///
/// Carrying `pred` through a projected column the optimizer cannot push back
/// into the scan forces the predicate to be evaluated post-scan. The plain
/// `(pred) AS keep` form preserves three-valued logic exactly: a NULL `keep` is
/// dropped by `WHERE keep`, identical to `WHERE pred`.
fn with_barrier_clause(vars: &[&str], pred: Expr) -> Clause {
    let mut items: Vec<ReturnItem> = vars
        .iter()
        .map(|v| ret_item(Expr::Variable((*v).to_string()), None))
        .collect();
    items.push(ret_item(pred, Some("keep")));
    Clause::With(WithClause {
        distinct: false,
        items,
        order_by: None,
        skip: None,
        limit: None,
        where_clause: Some(Expr::Variable("keep".to_string())),
    })
}

fn build_query(clauses: Vec<Clause>) -> Query {
    Query::Single(Statement { clauses })
}

// ── Generated case ───────────────────────────────────────────────────────────

/// Which TLP partition a predicate selects.
#[derive(Clone, Copy, Debug)]
pub enum Partition {
    /// `WHERE p` (p is TRUE).
    True,
    /// `WHERE NOT p` (p is FALSE).
    False,
    /// `WHERE p IS NULL` (p is the three-valued-logic unknown).
    Null,
}

impl Partition {
    /// All three partitions, in the order their bags reunite to the whole.
    pub const ALL: [Partition; 3] = [Partition::True, Partition::False, Partition::Null];

    fn apply(self, p: Expr) -> Expr {
        match self {
            Partition::True => p,
            Partition::False => not_expr(p),
            Partition::Null => is_null_expr(p),
        }
    }
}

/// A generated query plus the predicate the oracles partition or barrier on.
///
/// The query is `MATCH <shape> [WHERE base] RETURN <projection>`; `predicate` is
/// a separate boolean expression over the same bound variables.
#[derive(Clone, Debug)]
pub struct Case {
    shape: Shape,
    base_where: Option<Expr>,
    projection: Vec<ReturnItem>,
    predicate: Expr,
}

impl Case {
    /// Does this case aggregate over a property the schema declares `Float`?
    ///
    /// Exists so a differential oracle can *enforce* its admissibility contract
    /// rather than merely document it. `Case`'s fields are private, so without
    /// this nothing outside the module can tell a safe `sum(a.age)` from an
    /// unsafe `sum(a.score)` — and an unsafe one reaching a comparison surfaces
    /// as a mysterious bag diff rather than as the generator bug it is.
    ///
    /// See [`Shape::integer_targets`] for why float aggregates are unsafe.
    pub fn has_float_aggregate(&self) -> bool {
        let float_props: Vec<&str> = self
            .shape
            .bounds()
            .iter()
            .flat_map(|b| {
                b.props
                    .iter()
                    .filter(|p| matches!(p.ty, PropTy::Float))
                    .map(|p| p.name)
            })
            .collect();
        self.projection.iter().any(|item| match item {
            ReturnItem::Expr { expr, .. } => expr_aggregates_float(expr, &float_props),
            _ => false,
        })
    }

    /// Does this case carry a base `WHERE`?
    ///
    /// Exposed so a caller can assert a *selectivity floor*: two thirds of
    /// ordinary draws have no base filter and scan the fixture end to end, which
    /// is affordable at 1k rows and not at 50k.
    pub fn has_base_filter(&self) -> bool {
        self.base_where.is_some()
    }

    /// Is this case an aggregate at all (`count(*)` or `sum(...)`)?
    pub fn is_aggregate(&self) -> bool {
        self.projection.iter().any(|item| match item {
            ReturnItem::Expr { expr, .. } => matches!(expr, Expr::FunctionCall { .. }),
            _ => false,
        })
    }

    /// The base query `MATCH <shape> [WHERE base] RETURN <projection>`.
    pub fn base_query(&self) -> Query {
        build_query(vec![
            match_clause(&self.shape, self.base_where.clone()),
            return_clause(self.projection.clone()),
        ])
    }

    /// The base query further filtered to one predicate partition.
    pub fn partition_query(&self, part: Partition) -> Query {
        let where_clause = Some(conj(
            self.base_where.clone(),
            part.apply(self.predicate.clone()),
        ));
        build_query(vec![
            match_clause(&self.shape, where_clause),
            return_clause(self.projection.clone()),
        ])
    }

    /// NoREC optimized form: predicate in `WHERE`, reachable by pushdown.
    pub fn norec_optimized(&self) -> Query {
        let where_clause = Some(conj(self.base_where.clone(), self.predicate.clone()));
        build_query(vec![
            match_clause(&self.shape, where_clause),
            return_clause(self.projection.clone()),
        ])
    }

    /// NoREC unoptimized form: predicate behind a `WITH` barrier.
    pub fn norec_unoptimized(&self) -> Query {
        build_query(vec![
            match_clause(&self.shape, self.base_where.clone()),
            with_barrier_clause(&self.shape.var_names(), self.predicate.clone()),
            return_clause(self.projection.clone()),
        ])
    }

    /// The base query with `ORDER BY <firstVar>.name` appended.
    ///
    /// Both seed labels carry a non-null `name`, and the first bound variable is
    /// always present, so the sort key is always a bound scalar — never an
    /// un-orderable node value. Used by the ORDER-BY-permutation structural law
    /// (`bag(ordered) == bag(base)`).
    pub fn ordered_query(&self) -> Query {
        let first_var = self.shape.bounds()[0].var;
        let ret = ReturnClause {
            distinct: false,
            items: self.projection.clone(),
            order_by: Some(vec![SortItem {
                expr: prop_expr(first_var, "name"),
                ascending: true,
            }]),
            skip: None,
            limit: None,
        };
        build_query(vec![
            match_clause(&self.shape, self.base_where.clone()),
            Clause::Return(ret),
        ])
    }

    /// The base query with `LIMIT n` appended.
    ///
    /// Used by the LIMIT-sub-bag structural law (`bag(limited) ⊆ bag(base)`).
    pub fn limited_query(&self, n: u32) -> Query {
        let ret = ReturnClause {
            distinct: false,
            items: self.projection.clone(),
            order_by: None,
            skip: None,
            limit: Some(Expr::Literal(CypherLiteral::Integer(i64::from(n)))),
        };
        build_query(vec![
            match_clause(&self.shape, self.base_where.clone()),
            Clause::Return(ret),
        ])
    }

    /// `MATCH <shape> [WHERE base] RETURN count(*) AS c` over the same rows as
    /// the base query.
    ///
    /// Used by the count-equals-enumeration structural law
    /// (`count(Q) == |rows(Q)|`).
    pub fn count_query(&self) -> Query {
        let count_star = Expr::FunctionCall {
            name: "count".to_string(),
            args: vec![Expr::Wildcard],
            distinct: false,
            window_spec: None,
        };
        build_query(vec![
            match_clause(&self.shape, self.base_where.clone()),
            return_clause(vec![ret_item(count_star, Some("c"))]),
        ])
    }
}

// ── proptest strategies ──────────────────────────────────────────────────────

fn arb_numeric_op() -> impl Strategy<Value = BinaryOp> {
    select(vec![
        BinaryOp::Lt,
        BinaryOp::LtEq,
        BinaryOp::Eq,
        BinaryOp::NotEq,
        BinaryOp::GtEq,
        BinaryOp::Gt,
    ])
}

fn arb_str_op() -> impl Strategy<Value = BinaryOp> {
    select(vec![BinaryOp::Eq, BinaryOp::NotEq])
}

/// Literals for a property kind. All numeric literals are non-negative so they
/// round-trip (a leading `-` would parse as `UnaryOp::Neg`). Values straddle the
/// seed's boundaries (e.g. `age = 30`) so off-by-one partitioning is caught.
fn arb_literal(ty: PropTy) -> BoxedStrategy<CypherLiteral> {
    match ty {
        PropTy::Int => select(vec![0i64, 18, 22, 25, 30, 40, 50, 65, 1999, 2010])
            .prop_map(CypherLiteral::Integer)
            .boxed(),
        PropTy::Float => select(vec![0.0f64, 0.2, 0.5, 0.7, 0.9, 1.0])
            .prop_map(CypherLiteral::Float)
            .boxed(),
        PropTy::Str => select(vec!["NYC", "SF", "LA", "ZZ", "p1"])
            .prop_map(|s| CypherLiteral::String(s.to_string()))
            .boxed(),
    }
}

/// A comparison `var.prop <op> literal` over a nullable target.
fn arb_comparison(targets: Vec<(&'static str, Prop)>) -> impl Strategy<Value = Expr> {
    select(targets).prop_flat_map(|(var, prop)| {
        let op = match prop.ty {
            PropTy::Str => arb_str_op().boxed(),
            _ => arb_numeric_op().boxed(),
        };
        (op, arb_literal(prop.ty)).prop_map(move |(op, lit)| Expr::BinaryOp {
            left: Box::new(prop_expr(var, prop.name)),
            op,
            right: Box::new(Expr::Literal(lit)),
        })
    })
}

/// A predicate of depth ≤ 2 whose leaves all compare nullable props, so the
/// whole predicate can evaluate to NULL (the `IS NULL` partition has teeth).
fn arb_pred(targets: Vec<(&'static str, Prop)>) -> impl Strategy<Value = Expr> {
    prop_oneof![
        3 => arb_comparison(targets.clone()),
        1 => (arb_comparison(targets.clone()), arb_comparison(targets.clone()))
            .prop_map(|(a, b)| and_expr(a, b)),
        1 => (arb_comparison(targets.clone()), arb_comparison(targets.clone()))
            .prop_map(|(a, b)| or_expr(a, b)),
        1 => arb_comparison(targets).prop_map(not_expr),
    ]
}

/// A non-empty plain projection (`RETURN` of vars and/or properties) — no
/// DISTINCT, LIMIT, or aggregation, so the TLP multiset law holds.
///
/// Takes the owned `choices` (each bound variable and its properties) so the
/// returned strategy borrows nothing. Each item is aliased positionally
/// (`AS c0`, `AS c1`, …) so a repeated choice (e.g. `a.age` twice) cannot
/// produce a duplicate `RETURN` column name, which the parser rejects.
fn arb_projection(choices: Vec<Expr>) -> impl Strategy<Value = Vec<ReturnItem>> {
    prop_vec(select(choices), 1..=3).prop_map(|exprs| {
        exprs
            .into_iter()
            .enumerate()
            .map(|(i, e)| ReturnItem::Expr {
                expr: e,
                alias: Some(format!("c{i}")),
                source_text: None,
            })
            .collect()
    })
}

/// An aggregate projection: `count(*) AS c` or `sum(var.prop) AS s`.
///
/// Takes the owned numeric targets so the returned strategy borrows nothing.
fn arb_agg_projection(numeric: Vec<(&'static str, Prop)>) -> BoxedStrategy<Vec<ReturnItem>> {
    let count_star = Expr::FunctionCall {
        name: "count".to_string(),
        args: vec![Expr::Wildcard],
        distinct: false,
        window_spec: None,
    };
    let count = Just(vec![ret_item(count_star, Some("c"))]).boxed();
    if numeric.is_empty() {
        return count;
    }
    let sum = select(numeric)
        .prop_map(|(var, prop)| {
            let sum = Expr::FunctionCall {
                name: "sum".to_string(),
                args: vec![prop_expr(var, prop.name)],
                distinct: false,
                window_spec: None,
            };
            vec![ret_item(sum, Some("s"))]
        })
        .boxed();
    prop_oneof![count, sum].boxed()
}

fn arb_shape() -> impl Strategy<Value = Shape> {
    prop_oneof![
        Just(Shape::Node(PERSON_A)),
        Just(Shape::Edge(PERSON_A, COMPANY_B))
    ]
}

fn arb_base_where(targets: Vec<(&'static str, Prop)>) -> impl Strategy<Value = Option<Expr>> {
    prop_oneof![
        2 => Just(None::<Expr>),
        1 => arb_pred(targets).prop_map(Some),
    ]
}

/// A case with a plain projection — for the row-level TLP and NoREC oracles.
pub fn arb_case() -> impl Strategy<Value = Case> {
    arb_shape().prop_flat_map(|shape| {
        let targets = shape.nullable_targets();
        (
            Just(shape.clone()),
            arb_base_where(targets.clone()),
            arb_projection(shape.projection_choices()),
            arb_pred(targets),
        )
            .prop_map(|(shape, base_where, projection, predicate)| Case {
                shape,
                base_where,
                projection,
                predicate,
            })
    })
}

/// Does `expr` apply an aggregate function to one of `float_props`?
fn expr_aggregates_float(expr: &Expr, float_props: &[&str]) -> bool {
    let Expr::FunctionCall { args, .. } = expr else {
        return false;
    };
    args.iter().any(|a| match a {
        Expr::Property(_, name) => float_props.contains(&name.as_str()),
        _ => false,
    })
}

/// A case with an aggregate projection — for the aggregate-TLP oracle.
pub fn arb_agg_case() -> impl Strategy<Value = Case> {
    arb_shape().prop_flat_map(|shape| {
        let targets = shape.nullable_targets();
        (
            Just(shape.clone()),
            arb_base_where(targets.clone()),
            arb_agg_projection(shape.numeric_targets()),
            arb_pred(targets),
        )
            .prop_map(|(shape, base_where, projection, predicate)| Case {
                shape,
                base_where,
                projection,
                predicate,
            })
    })
}

/// [`arb_case`] with a base `WHERE` on every draw.
///
/// `arb_base_where` is weighted `2 => None, 1 => Some`, so two thirds of ordinary
/// cases carry no filter and scan the whole fixture. Phase 0A measured the
/// consequence exactly: median rows returned equalled the fixture's vertex count
/// at every tier. That is fine on a 1k-row fixture and ruinous on a 50k-row one,
/// so larger fixtures draw from this instead.
pub fn arb_case_selective() -> impl Strategy<Value = Case> {
    arb_shape().prop_flat_map(|shape| {
        let targets = shape.nullable_targets();
        (
            Just(shape.clone()),
            arb_pred(targets.clone()).prop_map(Some),
            arb_projection(shape.projection_choices()),
            arb_pred(targets),
        )
            .prop_map(|(shape, base_where, projection, predicate)| Case {
                shape,
                base_where,
                projection,
                predicate,
            })
    })
}

/// [`arb_agg_case_int`] with a base `WHERE` on every draw. See
/// [`arb_case_selective`].
pub fn arb_agg_case_int_selective() -> impl Strategy<Value = Case> {
    arb_shape().prop_flat_map(|shape| {
        let targets = shape.nullable_targets();
        (
            Just(shape.clone()),
            arb_pred(targets.clone()).prop_map(Some),
            arb_agg_projection(shape.integer_targets()),
            arb_pred(targets),
        )
            .prop_map(|(shape, base_where, projection, predicate)| Case {
                shape,
                base_where,
                projection,
                predicate,
            })
    })
}

/// A case with an aggregate projection over **integer** properties only.
///
/// Identical to [`arb_agg_case`] except that `sum` targets are drawn from
/// [`Shape::integer_targets`] rather than every numeric property. `count(*)` is
/// exact regardless and stays in the mix.
///
/// This is the generator a differential oracle must use: `arb_agg_case` draws
/// `sum` targets from a set that includes `score: Float`, so roughly one draw in
/// four aggregates a float and is unsafe to compare across two execution paths.
pub fn arb_agg_case_int() -> impl Strategy<Value = Case> {
    arb_shape().prop_flat_map(|shape| {
        let targets = shape.nullable_targets();
        (
            Just(shape.clone()),
            arb_base_where(targets.clone()),
            arb_agg_projection(shape.integer_targets()),
            arb_pred(targets),
        )
            .prop_map(|(shape, base_where, projection, predicate)| Case {
                shape,
                base_where,
                projection,
                predicate,
            })
    })
}

#[cfg(test)]
mod roundtrip {
    use super::render::{normalize, render};
    use super::{Partition, arb_case};
    use proptest::prelude::*;
    use uni_cypher::parse;

    proptest! {
        // Cheap (no database): renders every variant a case can produce and
        // asserts the parser reconstructs the identical AST. Guards the
        // generator and renderer against drift in lock-step.
        #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

        #[test]
        fn render_roundtrips_generated_queries(case in arb_case()) {
            let variants = [
                case.base_query(),
                case.partition_query(Partition::True),
                case.partition_query(Partition::False),
                case.partition_query(Partition::Null),
                case.norec_optimized(),
                case.norec_unoptimized(),
                case.ordered_query(),
                case.limited_query(5),
                case.count_query(),
            ];
            for q in variants {
                let rendered = render(&q);
                let reparsed = parse(&rendered).map_err(|e| {
                    TestCaseError::fail(format!("parse failed for `{rendered}`: {e:?}"))
                })?;
                let mut want = q.clone();
                let mut got = reparsed;
                normalize(&mut want);
                normalize(&mut got);
                prop_assert_eq!(want, got, "round-trip mismatch: `{}`", rendered);
            }
        }
    }
}

#[cfg(test)]
mod spike {
    use super::render::{normalize, render};
    use uni_cypher::ast::{
        BinaryOp, Clause, CypherLiteral, Expr, LabelExpr, MatchClause, NodePattern, PathPattern,
        Pattern, PatternElement, Query, ReturnClause, ReturnItem, Statement,
    };
    use uni_cypher::parse;

    /// Builds `a.<prop>` as `Property(Variable("a"), prop)`.
    fn prop(var: &str, name: &str) -> Expr {
        Expr::Property(Box::new(Expr::Variable(var.to_string())), name.to_string())
    }

    /// Asserts the render is a fixed point of the parser: parsing `src`,
    /// rendering it, and reparsing yields the same AST (modulo `normalize`).
    ///
    /// Starting from the parser's own AST guarantees we match its canonical
    /// forms (single-type `LabelExpr`, relationship `Direction`, ...), so this
    /// is the cheapest way to de-risk a whole query shape.
    fn assert_render_fixpoint(src: &str) {
        let parsed = parse(src).unwrap_or_else(|e| panic!("parse failed for `{src}`: {e:?}"));
        let rendered = render(&parsed);
        let reparsed = parse(&rendered)
            .unwrap_or_else(|e| panic!("reparse failed for `{rendered}` (from `{src}`): {e:?}"));
        let mut a = parsed;
        let mut b = reparsed;
        normalize(&mut a);
        normalize(&mut b);
        assert_eq!(a, b, "render fixed-point mismatch: `{src}` -> `{rendered}`");
    }

    #[test]
    fn fixpoint_edge_and_with_barrier_and_agg() {
        // Relationship pattern (NoREC/TLP edge shape): single-type LabelExpr +
        // Outgoing direction must round-trip in the parser's canonical form.
        assert_render_fixpoint("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a, b");
        // WITH-barrier (the NoREC unoptimized form).
        assert_render_fixpoint("MATCH (a:Person) WITH a, (a.age > 30) AS keep WHERE keep RETURN a");
        // Aggregates (the aggregate-TLP / NoREC count form).
        assert_render_fixpoint("MATCH (a:Person) WHERE a.age > 30 RETURN count(*) AS c");
        assert_render_fixpoint("MATCH (a:Person) RETURN sum(a.score) AS s");
        // Compound predicate + IS NULL partition (the TLP unknown branch).
        assert_render_fixpoint(
            "MATCH (a:Person) WHERE (a.age > 30) AND ((a.score < 0.5) IS NULL) RETURN a.name",
        );
    }

    /// Asserts `normalize(parse(render(ast))) == normalize(ast)`, panicking with
    /// the rendered text on any mismatch.
    fn assert_roundtrip(ast: &Query) {
        let rendered = render(ast);
        let mut reparsed =
            parse(&rendered).unwrap_or_else(|e| panic!("parse failed for `{rendered}`: {e:?}"));
        let mut expected = ast.clone();
        normalize(&mut reparsed);
        normalize(&mut expected);
        assert_eq!(
            reparsed, expected,
            "round-trip mismatch; rendered=`{rendered}`"
        );
    }

    #[test]
    fn roundtrip_minimal() {
        // MATCH (a:Person) WHERE a.age > 30 RETURN a.age
        let ast = Query::Single(Statement {
            clauses: vec![
                Clause::Match(MatchClause {
                    optional: false,
                    pattern: Pattern {
                        paths: vec![PathPattern {
                            variable: None,
                            elements: vec![PatternElement::Node(NodePattern {
                                variable: Some("a".to_string()),
                                labels: LabelExpr::Conjunction(vec!["Person".to_string()]),
                                properties: None,
                                where_clause: None,
                            })],
                            shortest_path_mode: None,
                        }],
                    },
                    where_clause: Some(Expr::BinaryOp {
                        left: Box::new(prop("a", "age")),
                        op: BinaryOp::Gt,
                        right: Box::new(Expr::Literal(CypherLiteral::Integer(30))),
                    }),
                    for_update: false,
                }),
                Clause::Return(ReturnClause {
                    distinct: false,
                    items: vec![ReturnItem::Expr {
                        expr: prop("a", "age"),
                        alias: None,
                        source_text: None,
                    }],
                    order_by: None,
                    skip: None,
                    limit: None,
                }),
            ],
        });

        assert_roundtrip(&ast);
    }
}
