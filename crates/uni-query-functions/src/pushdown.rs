// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Predicate pushdown and index-aware query routing.
//!
//! Routes WHERE predicates to the most selective execution path:
//! UID index lookup → BTree prefix scan → JSON FTS → Lance columnar filter → residual.
//! Includes SQL injection prevention for LIKE patterns (CWE-89) and UID validation (CWE-345).

use std::collections::{HashMap, HashSet};
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, Quantifier, UnaryOp};
use uni_store::backend::types::{CmpOp, FilterExpr, Scalar, StringMatchKind};

use uni_common::Value;
use uni_common::core::id::UniId;
use uni_common::core::schema::{
    IndexDefinition, IndexKey, IndexStatus, KeyKind, PropertyMeta, ScalarIndexType, Schema,
};
use uni_store::storage::index_keys::KeyValue;

use crate::key_bounds::{BticPredicate, BticSide, KeyBound};

/// Categorized pushdown strategy for predicates with index awareness.
///
/// This struct represents the optimal execution path for predicates,
/// routing them to the most selective index when available.
#[derive(Debug, Clone, Default)]
pub struct PushdownStrategy {
    /// UID lookup predicate: WHERE n._uid = 'base32string'
    /// Contains the UniId parsed from the predicate value.
    pub uid_lookup: Option<UniId>,

    /// BTree index prefix scans for STARTS WITH predicates.
    /// When a property has a scalar BTree index, STARTS WITH 'prefix' can be
    /// converted to a range scan: column >= 'prefix' AND column < 'prefix_next'.
    /// Vec of: (column_name, lower_bound, upper_bound)
    pub btree_prefix_scans: Vec<(String, String, String)>,

    /// `CONTAINS` conjuncts on a column with an Online JSON full-text index,
    /// as `(column, term, path)`: the term is a string literal or a
    /// parameter, the path the dotted key path inside the document
    /// (`a.doc.meta.author` has path `meta.author`; `None` for the column
    /// itself). The index's derived text column turns each into candidate
    /// vids for the scan; the conjunct itself stays in `residual`.
    pub json_fts_predicates: Vec<(String, Expr, Option<String>)>,

    /// `ANY(x IN n.prop WHERE x IN <list>)` conjuncts on a property with an
    /// Online inverted index, as `(prop, list)`, where the list is a list
    /// literal or a parameter. The index turns the list into candidate vids
    /// for the scan; the conjunct itself stays in the residual (or Lance)
    /// predicates, so the candidates only narrow what is read.
    pub inverted_prefilters: Vec<(String, Expr)>,

    /// Predicates pushable to Lance scan filter.
    pub lance_predicates: Vec<Expr>,

    /// Property columns that have an Online scalar index AND a pushable
    /// equality / IN predicate in this scan, paired with the index type that
    /// serves them. Recorded for two reasons:
    ///   1. Telemetry — EXPLAIN reports these as `IndexUsage { used: true }`,
    ///      naming the real index type.
    ///   2. Routing — the planner pushes the matching `lance_predicates` into
    ///      `GraphScanExec`'s scan-time filter (rather than a `FilterExec` on
    ///      top), so Lance can serve the lookup from the index.
    ///
    /// **Both `Hash` and `BTree` qualify.** This was `Hash`-only, so a BTree
    /// index could never be consulted: nothing was collected, no indexed
    /// pushdown was built, the predicate became an in-process Arrow filter, and
    /// Lance was handed nothing index-servable. Every LDBC SF1 index is a BTree,
    /// which is why all fourteen queries reported `index_scans=0` (#247).
    ///
    /// Equality and `IN` only, for both types. Ranges, prefixes and null tests
    /// are recorded separately in [`Self::indexed_range_columns`].
    ///
    /// See issues #57, #247.
    pub indexed_equality_columns: Vec<(String, ScalarIndexType)>,

    /// Property columns with an Online scalar index that can serve a range,
    /// prefix or null-test conjunct in this scan, paired with the index type.
    ///
    /// `<`, `<=`, `>`, `>=` and `STARTS WITH` need an ordered index, so only a
    /// `BTree` qualifies for them; `IS NULL` / `IS NOT NULL` are served by a
    /// `BTree` or a `Hash` index. The planner turns each such conjunct into a
    /// type-exact [`FilterExpr`] through [`indexed_range_filter`] and keeps the
    /// original predicate above the scan, so a recorded column is a candidate,
    /// not a promise: an operand that cannot be rendered exactly (a NaN, an
    /// out-of-range integer, a type mismatch) is left to the residual filter.
    ///
    /// Perf audit 2026-10-17, row F1: before this, only `=` and `IN` reached
    /// Lance, and every range, prefix and null test read the whole label.
    pub indexed_range_columns: Vec<(String, ScalarIndexType)>,

    /// Conjuncts on a property served by a keyed scalar index (see
    /// [`IndexKey`]). Recorded, not routed: each conjunct is still in
    /// `residual` or `lance_predicates`, and the planner adds a bound on the
    /// key to the storage scan's filter only, after folding the operand (see
    /// [`KeyedPredicate::lance_bound`]).
    pub keyed_predicates: Vec<KeyedPredicate>,

    /// Residual predicates (not pushable to storage).
    pub residual: Vec<Expr>,
}

/// A conjunct on a property indexed through a key.
#[derive(Debug, Clone)]
pub struct KeyedPredicate {
    /// The logical property (`ts`).
    pub property: String,
    /// The keyed index serving it.
    pub index_name: String,
    /// The index's key.
    pub key: IndexKey,
    /// The predicate's shape, with its constant side unevaluated.
    pub form: KeyedForm,
    /// The conjunct as written (property first).
    pub conjunct: Expr,
}

/// The shapes of a [`KeyedPredicate`].
#[derive(Debug, Clone)]
pub enum KeyedForm {
    /// `p op c` for `=`, `<`, `<=`, `>`, `>=`.
    Compare {
        /// The comparison.
        op: CmpOp,
        /// The constant side.
        operand: Expr,
    },
    /// `p IN list`, the list a list of constants or a parameter.
    In {
        /// The list side.
        list: Expr,
    },
    /// A BTIC predicate call with the property as one argument and a
    /// constant as the other (`btic_overlaps(p, c)`, `btic_before(c, p)`,
    /// `btic_contains_point(p, t)`).
    Btic {
        /// The predicate.
        predicate: BticPredicate,
        /// Whether the property is the first argument.
        property_first: bool,
        /// The constant argument.
        operand: Expr,
    },
    /// `btic_lo(p) op c` or `btic_hi(p) op c`, oriented with the accessor on
    /// the left, for `=`, `<`, `<=`, `>`, `>=`.
    BticAccessor {
        /// The bound the accessor reads.
        side: BticSide,
        /// The comparison.
        op: CmpOp,
        /// The constant side.
        operand: Expr,
    },
    /// `point.withinBBox(p, lowerLeft, upperRight)` with constant corners.
    PointWithinBBox {
        /// The lower-left corner.
        lower_left: Expr,
        /// The upper-right corner.
        upper_right: Expr,
    },
    /// `distance(p, c) < r` / `<= r`, either argument order, or the same
    /// comparison written `r > distance(…)` / `r >= distance(…)`; `c` and `r`
    /// constant.
    PointDistance {
        /// The center `c`.
        center: Expr,
        /// The radius `r`.
        radius: Expr,
        /// `<=` rather than `<`.
        inclusive: bool,
    },
}

impl KeyedPredicate {
    /// The bound on the key columns that admits every row the conjunct
    /// admits, for the storage scan's filter; `None` when the operand does
    /// not fold to a value of the property's own type (a cross-type or NULL
    /// comparison, or one that depends on the row), or when the bound would
    /// admit every key, in which case the conjunct is answered without the
    /// key.
    ///
    /// DATETIME: `=` and the ranges compare by instant, which the key is, so
    /// they are exact; `IN` compares structurally (offset and zone), so the
    /// key's instant equality is a superset. BTIC: every interval predicate
    /// compares the raw bounds, which the key is (see
    /// [`crate::key_bounds::btic_bound`]); `=` and `IN` also compare
    /// granularity and certainty, so equal bounds are a superset. TIME: `=`
    /// and the ranges compare UTC nanoseconds, which the key is, so they are
    /// exact; `IN` compares structurally, a superset. A MAP key path: the key
    /// is the value at the path, exact for `=` and the ranges with a
    /// constant of the values' type. POINT: `withinBBox` is exact on the
    /// coordinates; `distance(p, c) < r` is a box around `c` (see
    /// [`crate::key_bounds::point_distance_bound`]); point `=` and ordering
    /// have no bound. `<>` is never served.
    ///
    /// Every bound also admits a row with a NULL key column. Such a row is
    /// written when a flush lands between a key backfill and the key
    /// columns' registration (the batch builder fills only columns the
    /// catalog names), or when a value cannot be keyed. The original
    /// predicate, kept above the scan, rejects the rows that do not match, so
    /// a missing key costs a read, never a match. The NULL tests are OR-ed
    /// around the whole bound (`bound OR k1 IS NULL OR k2 IS NULL`): a
    /// superset for any bound shape, and each disjunct is a BTree lookup.
    #[must_use]
    pub fn lance_bound(
        &self,
        params: &HashMap<String, Value>,
        statement_time: chrono::DateTime<chrono::Utc>,
    ) -> Option<FilterExpr> {
        let bound = self.key_bound(params, statement_time)?;
        if bound == KeyBound::True {
            return None;
        }
        let filter = bound.to_filter(&self.key.columns)?;
        Some(FilterExpr::Or(
            std::iter::once(filter)
                .chain(self.key.columns.iter().cloned().map(FilterExpr::IsNull))
                .collect(),
        ))
    }

    /// The bound on the key columns, by position, before NULL keys are
    /// admitted (see [`Self::lance_bound`]).
    #[must_use]
    pub fn key_bound(
        &self,
        params: &HashMap<String, Value>,
        statement_time: chrono::DateTime<chrono::Utc>,
    ) -> Option<KeyBound> {
        use uni_store::storage::index_keys::constant_key;
        let kind = &self.key.kind;
        // `p = c` on the key: every key column equal. Only a single-column
        // key orders as its property does.
        let equal = |value: &Value| -> Option<KeyBound> {
            let keys = constant_key(kind, value)?;
            Some(KeyBound::and(
                keys.into_iter()
                    .enumerate()
                    .map(|(column, k)| KeyBound::cmp(column, CmpOp::Eq, k)),
            ))
        };
        match &self.form {
            KeyedForm::Compare { op, operand } => {
                let value = fold_constant_operand(operand, params, statement_time)?;
                if *op == CmpOp::Eq {
                    return equal(&value);
                }
                match (kind, constant_key(kind, &value)?.as_slice()) {
                    (KeyKind::DateTimeInstant, [k]) => Some(KeyBound::cmp(0, *op, k.clone())),
                    // A TIME orders by its UTC nanoseconds and a key path by
                    // its value, which the keys are: exact.
                    (KeyKind::TimeUtc | KeyKind::MapPath { .. }, [k]) => {
                        Some(KeyBound::cmp(0, *op, k.clone()))
                    }
                    _ => None,
                }
            }
            KeyedForm::In { list } => {
                let items = match fold_constant_operand(list, params, statement_time)? {
                    Value::List(items) => items,
                    _ => return None,
                };
                // A NULL or other-type element equals no stored value, so it
                // contributes no row; a list of only those is not rewritten.
                let keys: Vec<Vec<KeyValue>> =
                    items.iter().filter_map(|v| constant_key(kind, v)).collect();
                if keys.is_empty() {
                    return None;
                }
                if keys.iter().all(|k| k.len() == 1) {
                    return Some(KeyBound::In {
                        column: 0,
                        values: keys.into_iter().flatten().collect(),
                    });
                }
                Some(KeyBound::or(keys.into_iter().map(|k| {
                    KeyBound::and(
                        k.into_iter()
                            .enumerate()
                            .map(|(column, k)| KeyBound::cmp(column, CmpOp::Eq, k)),
                    )
                })))
            }
            KeyedForm::Btic {
                predicate,
                property_first,
                operand,
            } => {
                if *kind != KeyKind::BticBounds {
                    return None;
                }
                let value = fold_constant_operand(operand, params, statement_time)?;
                if *predicate == BticPredicate::ContainsPoint {
                    return Some(crate::key_bounds::btic_point_bound(
                        crate::key_bounds::btic_point_ms(&value)?,
                    ));
                }
                let (c_lo, c_hi) = crate::key_bounds::btic_constant(&value)?;
                Some(crate::key_bounds::btic_bound(
                    *predicate,
                    *property_first,
                    c_lo,
                    c_hi,
                ))
            }
            KeyedForm::BticAccessor { side, op, operand } => {
                if *kind != KeyKind::BticBounds {
                    return None;
                }
                let value = fold_constant_operand(operand, params, statement_time)?;
                crate::key_bounds::btic_accessor_bound(*side, *op, &value)
            }
            // POINT: the spatial predicates, as boxes on the coordinates (see
            // `key_bounds::point_bbox_bound` / `point_distance_bound`).
            KeyedForm::PointWithinBBox {
                lower_left,
                upper_right,
            } => {
                let KeyKind::PointCoords { geographic, .. } = *kind else {
                    return None;
                };
                let corner = |e: &Expr| {
                    let value = fold_constant_operand(e, params, statement_time)?;
                    crate::key_bounds::point_corner(&value, geographic)
                };
                Some(crate::key_bounds::point_bbox_bound(
                    geographic,
                    corner(lower_left)?,
                    corner(upper_right)?,
                ))
            }
            KeyedForm::PointDistance {
                center,
                radius,
                inclusive,
            } => {
                let KeyKind::PointCoords { geographic, .. } = *kind else {
                    return None;
                };
                let center = fold_constant_operand(center, params, statement_time)?;
                let center =
                    crate::key_bounds::point_center(&center, geographic, kind.point_crs()?)?;
                let radius = match fold_constant_operand(radius, params, statement_time)? {
                    Value::Float(r) => r,
                    Value::Int(r) => r as f64,
                    _ => return None,
                };
                crate::key_bounds::point_distance_bound(geographic, center, radius, *inclusive)
            }
        }
    }
}

/// Analyzer that considers available indexes when categorizing predicates.
///
/// Unlike `PredicateAnalyzer` which only categorizes into pushable/residual,
/// this analyzer routes predicates to the most optimal execution path:
/// 1. UID index lookup (most selective, O(1) lookup)
/// 2. BTree prefix scan (STARTS WITH on scalar-indexed properties)
/// 3. JSON FTS lookup (BM25 full-text search)
/// 4. Lance scan filter (columnar scan with filter)
/// 5. Residual (post-scan evaluation)
// M-PUBLIC-DEBUG: Schema implements Debug, so the derived impl is sound.
#[derive(Debug)]
pub struct IndexAwareAnalyzer<'a> {
    schema: &'a Schema,
}

impl<'a> IndexAwareAnalyzer<'a> {
    /// Create an analyzer bound to the given schema for index-aware predicate routing.
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    /// Analyze predicates and determine optimal pushdown strategy.
    ///
    /// Predicates are categorized in order of selectivity:
    /// 1. `_uid = 'xxx'` -> UID index lookup
    /// 2. BTree prefix scans for STARTS WITH predicates
    /// 3. Pushable to Lance -> Lance filter
    /// 4. Everything else -> Residual
    pub fn analyze(&self, predicate: &Expr, variable: &str, label_id: u16) -> PushdownStrategy {
        let mut strategy = PushdownStrategy::default();
        let conjuncts = Self::split_conjuncts(predicate);
        let lance_analyzer = PredicateAnalyzer::new();

        for conj in conjuncts {
            // Range / prefix / null-test candidates are recorded independently
            // of where the conjunct is routed below: a BTree `STARTS WITH`
            // goes to `btree_prefix_scans`, a parameterized one to `residual`,
            // and both can still be served by the index once resolved.
            if let Some((col, kind)) = self.indexed_range_column(&conj, variable, label_id)
                && !strategy
                    .indexed_range_columns
                    .iter()
                    .any(|(c, _)| c == &col)
            {
                strategy.indexed_range_columns.push((col, kind));
            }

            // A keyed index's bound. Recorded, not routed: the conjunct is
            // still evaluated in full below.
            if let Some(keyed) = self.keyed_predicate(&conj, variable, label_id) {
                strategy.keyed_predicates.push(keyed);
            }

            // An inverted-index candidate set. Recorded, not routed: the
            // conjunct is still evaluated in full below.
            if let Some(found) = self.extract_inverted_any(&conj, variable, label_id) {
                strategy.inverted_prefilters.push(found);
            }

            // 1. Check for _uid = 'xxx' pattern (most selective)
            if let Some(uid) = self.extract_uid_predicate(&conj, variable) {
                strategy.uid_lookup = Some(uid);
                continue;
            }

            // 2. Check for BTree-indexed STARTS WITH predicates
            if let Some((column, lower, upper)) =
                self.extract_btree_prefix_scan(&conj, variable, label_id)
            {
                strategy.btree_prefix_scans.push((column, lower, upper));
                continue;
            }

            // 3. Check for JSON FTS predicates (CONTAINS on FTS-indexed columns)
            if let Some((column, term, path)) =
                self.extract_json_fts_predicate(&conj, variable, label_id)
            {
                strategy.json_fts_predicates.push((column, term, path));
                // A JSON column is not a Lance-filterable string, so the
                // predicate is evaluated above the scan.
                strategy.residual.push(conj);
                continue;
            }

            // 4. Check if pushable to Lance
            if lance_analyzer.is_pushable(&conj, variable) {
                // 4a. Indexed point lookup: equality / IN against an Online
                // scalar-indexed property, Hash or BTree. Record the column and
                // its index type so the planner can push this predicate into the
                // scan filter — where Lance can serve it from the index — and so
                // EXPLAIN reports `used: true` against the type that actually
                // serves it.
                if let Some((col, kind)) = self.indexed_equality_column(&conj, variable, label_id)
                    && !strategy
                        .indexed_equality_columns
                        .iter()
                        .any(|(c, _)| c == &col)
                {
                    strategy.indexed_equality_columns.push((col, kind));
                }
                strategy.lance_predicates.push(conj);
            } else {
                strategy.residual.push(conj);
            }
        }

        strategy
    }

    /// If `expr` is an equality or IN predicate of the form
    /// `variable.prop = ...` / `variable.prop IN ...` where `(label, prop)`
    /// has an Online `ScalarIndexType::Hash` index, return the column name.
    fn indexed_equality_column(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, ScalarIndexType)> {
        let prop = match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                ..
            } => match left.as_ref() {
                Expr::Property(var_expr, prop) => match var_expr.as_ref() {
                    Expr::Variable(v) if v == variable => prop.clone(),
                    _ => return None,
                },
                _ => return None,
            },
            Expr::In { expr: left, .. } => match left.as_ref() {
                Expr::Property(var_expr, prop) => match var_expr.as_ref() {
                    Expr::Variable(v) if v == variable => prop.clone(),
                    _ => return None,
                },
                _ => return None,
            },
            _ => return None,
        };

        let label_name = self.schema.label_name_by_id(label_id)?;
        for idx in &self.schema.indexes {
            if let IndexDefinition::Scalar(cfg) = idx
                && cfg.label == *label_name
                && cfg.properties.contains(&prop)
                && cfg.key.is_none()
                && matches!(
                    cfg.index_type,
                    ScalarIndexType::Hash | ScalarIndexType::BTree
                )
                && cfg.metadata.status == IndexStatus::Online
            {
                return Some((prop, cfg.index_type.clone()));
            }
        }
        None
    }

    /// The indexed column a range, prefix or null-test conjunct targets.
    ///
    /// Recognizes `variable.prop OP operand` for `<`, `<=`, `>`, `>=` and
    /// `STARTS WITH` (operand a literal or a parameter) against an Online
    /// `BTree` index, and `variable.prop IS [NOT] NULL` against an Online
    /// `BTree` or `Hash` index. The operand is not inspected here; whether it
    /// can be rendered exactly is decided after parameter substitution, by
    /// [`indexed_range_filter`].
    fn indexed_range_column(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, ScalarIndexType)> {
        let (prop_side, ordered) = match expr {
            Expr::BinaryOp {
                left,
                op:
                    BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::StartsWith,
                right,
            } if matches!(right.as_ref(), Expr::Literal(_) | Expr::Parameter(_)) => {
                (left.as_ref(), true)
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => (inner.as_ref(), false),
            _ => return None,
        };
        let Expr::Property(var_expr, prop) = prop_side else {
            return None;
        };
        if !matches!(var_expr.as_ref(), Expr::Variable(v) if v == variable) {
            return None;
        }
        let label_name = self.schema.label_name_by_id(label_id)?;
        self.schema.indexes.iter().find_map(|idx| match idx {
            IndexDefinition::Scalar(cfg)
                if cfg.label == *label_name
                    && cfg.properties.contains(prop)
                    && cfg.key.is_none()
                    && cfg.metadata.status == IndexStatus::Online
                    && match cfg.index_type {
                        ScalarIndexType::BTree => true,
                        ScalarIndexType::Hash => !ordered,
                        _ => false,
                    } =>
            {
                Some((prop.clone(), cfg.index_type.clone()))
            }
            _ => None,
        })
    }

    /// The keyed-index shape of `expr`, when `(label, prop)` has an Online
    /// keyed scalar index and `expr` is, with a constant operand `c`:
    /// `variable.prop op c` (`=`, `<`, `<=`, `>`, `>=`) or
    /// `variable.prop IN list` (`variable.m.k` for a key path into a MAP
    /// property `m`, whose index's property is `m.k`); for a BTIC key also a
    /// BTIC predicate call
    /// with the property as either argument and `c` as the other (the point
    /// of `btic_contains_point` is always the second), or
    /// `btic_lo(variable.prop) op c` / `btic_hi(…) op c` in either order.
    fn keyed_predicate(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<KeyedPredicate> {
        let is_property = |e: &Expr| {
            matches!(e, Expr::Property(base, _)
                if matches!(base.as_ref(), Expr::Variable(v) if v == variable))
        };
        // `btic_lo(variable.prop)` / `btic_hi(…)`.
        let accessor = |e: &Expr| -> Option<(BticSide, Expr)> {
            match e {
                Expr::FunctionCall {
                    name,
                    args,
                    distinct: false,
                    window_spec: None,
                } if args.len() == 1 && is_property(&args[0]) => {
                    Some((BticSide::from_name(name)?, args[0].clone()))
                }
                _ => None,
            }
        };
        let cmp_op = |op: &BinaryOp| -> Option<CmpOp> {
            Some(match op {
                BinaryOp::Eq => CmpOp::Eq,
                BinaryOp::Lt => CmpOp::Lt,
                BinaryOp::LtEq => CmpOp::LtEq,
                BinaryOp::Gt => CmpOp::Gt,
                BinaryOp::GtEq => CmpOp::GtEq,
                _ => return None,
            })
        };
        // `btic_lo(p) op c`, or `c op btic_lo(p)` mirrored.
        let oriented_accessor = match expr {
            Expr::BinaryOp { left, op, right } => {
                if let (true, Some((side, prop))) = (is_row_independent(right), accessor(left)) {
                    Some((side, prop, *op, right.as_ref().clone()))
                } else if let (true, Some((side, prop))) =
                    (is_row_independent(left), accessor(right))
                {
                    Some((side, prop, mirrored_comparison(*op)?, left.as_ref().clone()))
                } else {
                    None
                }
            }
            _ => None,
        };
        let spatial = spatial_keyed_form(expr, &is_property);
        let (prop_side, form) = match expr {
            _ if spatial.is_some() => spatial?,
            Expr::BinaryOp { .. } if oriented_accessor.is_some() => {
                let (side, prop, op, operand) = oriented_accessor?;
                let op = cmp_op(&op)?;
                (prop, KeyedForm::BticAccessor { side, op, operand })
            }
            Expr::BinaryOp { left, op, right } if is_row_independent(right) => (
                left.as_ref().clone(),
                KeyedForm::Compare {
                    op: cmp_op(op)?,
                    operand: right.as_ref().clone(),
                },
            ),
            // `c op n.m.k`: `orient_property_comparisons` orients only a
            // property of the variable itself, not a key path.
            Expr::BinaryOp { left, op, right } if is_row_independent(left) => (
                right.as_ref().clone(),
                KeyedForm::Compare {
                    op: cmp_op(&mirrored_comparison(*op)?)?,
                    operand: left.as_ref().clone(),
                },
            ),
            Expr::In { expr: left, list }
                if matches!(list.as_ref(), Expr::Parameter(_))
                    || (matches!(list.as_ref(), Expr::List(_)) && is_row_independent(list)) =>
            {
                (
                    left.as_ref().clone(),
                    KeyedForm::In {
                        list: list.as_ref().clone(),
                    },
                )
            }
            Expr::FunctionCall {
                name,
                args,
                distinct: false,
                window_spec: None,
            } if args.len() == 2 => {
                let predicate = BticPredicate::from_name(name)?;
                let (prop, operand, property_first) =
                    if is_property(&args[0]) && is_row_independent(&args[1]) {
                        (&args[0], &args[1], true)
                    } else if predicate != BticPredicate::ContainsPoint
                        && is_property(&args[1])
                        && is_row_independent(&args[0])
                    {
                        (&args[1], &args[0], false)
                    } else {
                        return None;
                    };
                (
                    prop.clone(),
                    KeyedForm::Btic {
                        predicate,
                        property_first,
                        operand: operand.clone(),
                    },
                )
            }
            _ => return None,
        };
        let Expr::Property(base, prop) = &prop_side else {
            return None;
        };
        // `variable.prop`, or `variable.m.k`: a key path into a MAP property,
        // whose index's property is the dotted path.
        let prop = match base.as_ref() {
            Expr::Variable(v) if v == variable => prop.clone(),
            Expr::Property(inner, head) if matches!(inner.as_ref(), Expr::Variable(v) if v == variable) =>
            {
                format!("{head}.{prop}")
            }
            _ => return None,
        };
        let label_name = self.schema.label_name_by_id(label_id)?;
        let cfg = self.schema.keyed_scalar_index(label_name, &prop)?;
        let key = cfg.key.clone()?;
        // The BTIC forms bound a BTIC key only.
        if matches!(
            form,
            KeyedForm::Btic { .. } | KeyedForm::BticAccessor { .. }
        ) && key.kind != KeyKind::BticBounds
        {
            return None;
        }
        // The spatial forms bound a POINT key only.
        if matches!(
            form,
            KeyedForm::PointWithinBBox { .. } | KeyedForm::PointDistance { .. }
        ) && key.kind.point_crs().is_none()
        {
            return None;
        }
        Some(KeyedPredicate {
            property: prop,
            index_name: cfg.name.clone(),
            key,
            form,
            conjunct: expr.clone(),
        })
    }

    /// Extract UniId from `_uid = 'xxx'` predicate.
    ///
    /// # Security
    ///
    /// **CWE-345 (Insufficient Verification)**: The UID value is validated using
    /// `UniId::from_multibase()` which enforces Base32Lower encoding and 32-byte
    /// length. Invalid UIDs are rejected and the predicate becomes residual.
    fn extract_uid_predicate(&self, expr: &Expr, variable: &str) -> Option<UniId> {
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } = expr
            && let Expr::Property(var_expr, prop) = left.as_ref()
            && let Expr::Variable(v) = var_expr.as_ref()
            && v == variable
            && prop == "_uid"
            && let Expr::Literal(CypherLiteral::String(s)) = right.as_ref()
        {
            // Security: UniId::from_multibase validates Base32Lower and 32-byte length
            return UniId::from_multibase(s).ok();
        }
        None
    }

    /// Extract BTree prefix scan for STARTS WITH predicates on scalar-indexed properties.
    ///
    /// Returns `Some((column, lower_bound, upper_bound))` if:
    /// - The predicate is `variable.property STARTS WITH 'prefix'`
    /// - The property has a scalar BTree index
    /// - The prefix is non-empty (empty prefix matches all, not worth optimizing)
    ///
    /// Converts `column STARTS WITH 'John'` to:
    /// `column >= 'John' AND column < 'Joho'`
    fn extract_btree_prefix_scan(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, String, String)> {
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::StartsWith,
            right,
        } = expr
            && let Expr::Property(var_expr, prop) = left.as_ref()
            && let Expr::Variable(v) = var_expr.as_ref()
            && v == variable
            && let Expr::Literal(CypherLiteral::String(prefix)) = right.as_ref()
        {
            // Skip empty prefix (matches all, no optimization benefit)
            if prefix.is_empty() {
                return None;
            }

            // Check if property has a scalar BTree index
            let label_name = self.schema.label_name_by_id(label_id)?;

            for idx in &self.schema.indexes {
                if let IndexDefinition::Scalar(cfg) = idx
                    && cfg.label == *label_name
                    && cfg.properties.contains(prop)
                    && cfg.index_type == ScalarIndexType::BTree
                    && cfg.metadata.status == IndexStatus::Online
                {
                    // Calculate the upper bound by incrementing the last character
                    // For "John" -> "Joho"
                    // This works for ASCII and most UTF-8 strings
                    if let Some(upper) = increment_last_char(prefix) {
                        return Some((prop.clone(), prefix.clone(), upper));
                    }
                }
            }
        }
        None
    }

    /// `ANY(x IN variable.prop WHERE x IN <list literal | $param>)` where
    /// `(label, prop)` has an Online inverted index: `(prop, list)`.
    fn extract_inverted_any(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, Expr)> {
        let Expr::Quantifier {
            quantifier: Quantifier::Any,
            variable: element,
            list,
            predicate,
        } = expr
        else {
            return None;
        };
        let Expr::Property(owner, prop) = list.as_ref() else {
            return None;
        };
        if !matches!(owner.as_ref(), Expr::Variable(v) if v == variable) {
            return None;
        }
        let Expr::In {
            expr: probe,
            list: terms,
        } = predicate.as_ref()
        else {
            return None;
        };
        if !matches!(probe.as_ref(), Expr::Variable(v) if v == element) {
            return None;
        }
        let constant = match terms.as_ref() {
            Expr::Parameter(_) => true,
            Expr::List(items) => items.iter().all(|i| matches!(i, Expr::Literal(_))),
            _ => false,
        };
        if !constant {
            return None;
        }
        let label_name = self.schema.label_name_by_id(label_id)?;
        self.schema.indexes.iter().find_map(|idx| match idx {
            IndexDefinition::Inverted(cfg)
                if cfg.label == *label_name
                    && cfg.property == *prop
                    && cfg.metadata.status == IndexStatus::Online =>
            {
                Some((prop.clone(), terms.as_ref().clone()))
            }
            _ => None,
        })
    }

    /// `variable.column[.path…] CONTAINS <string literal | $param>` where
    /// `(label, column)` has an Online JSON full-text index that covers the
    /// path: `(column, term, path)`.
    fn extract_json_fts_predicate(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, Expr, Option<String>)> {
        let Expr::BinaryOp {
            left,
            op: BinaryOp::Contains,
            right,
        } = expr
        else {
            return None;
        };
        if !matches!(
            right.as_ref(),
            Expr::Literal(CypherLiteral::String(_)) | Expr::Parameter(_)
        ) {
            return None;
        }
        // Walk `a.doc.x.y` down to `a.doc`, collecting `x.y`.
        let mut keys: Vec<&str> = Vec::new();
        let mut at = left.as_ref();
        let column = loop {
            let Expr::Property(inner, key) = at else {
                return None;
            };
            match inner.as_ref() {
                Expr::Variable(v) if v == variable => break key,
                Expr::Variable(_) => return None,
                deeper => {
                    keys.push(key);
                    at = deeper;
                }
            }
        };
        keys.reverse();
        let path = (!keys.is_empty()).then(|| keys.join("."));

        let label_name = self.schema.label_name_by_id(label_id)?;
        self.schema.indexes.iter().find_map(|idx| match idx {
            IndexDefinition::JsonFullText(cfg)
                if cfg.label == *label_name
                    && cfg.column == *column
                    && cfg.metadata.status == IndexStatus::Online
                    && cfg.covers_path(path.as_deref()) =>
            {
                Some((column.clone(), right.as_ref().clone(), path.clone()))
            }
            _ => None,
        })
    }

    /// Split AND-connected predicates into a list.
    fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let mut result = Self::split_conjuncts(left);
                result.extend(Self::split_conjuncts(right));
                result
            }
            _ => vec![expr.clone()],
        }
    }
}

/// Split result of predicate analysis: pushable vs residual.
#[derive(Debug)]
pub struct PredicateAnalysis {
    /// Predicates that can be pushed to storage
    pub pushable: Vec<Expr>,
    /// Predicates that must be evaluated post-scan
    pub residual: Vec<Expr>,
    /// Properties needed for residual evaluation
    pub required_properties: Vec<String>,
}

/// Classifies predicates as pushable to Lance or residual (post-scan).
#[derive(Debug, Default)]
pub struct PredicateAnalyzer;

impl PredicateAnalyzer {
    /// Create a new analyzer for classifying predicates.
    pub fn new() -> Self {
        Self
    }

    /// Split a predicate into pushable (Lance) and residual (post-scan) parts.
    pub fn analyze(&self, predicate: &Expr, scan_variable: &str) -> PredicateAnalysis {
        let mut pushable = Vec::new();
        let mut residual = Vec::new();

        self.split_conjuncts(predicate, scan_variable, &mut pushable, &mut residual);

        let required_properties = self.extract_properties(&residual, scan_variable);

        PredicateAnalysis {
            pushable,
            residual,
            required_properties,
        }
    }

    /// Split AND-connected predicates
    fn split_conjuncts(
        &self,
        expr: &Expr,
        variable: &str,
        pushable: &mut Vec<Expr>,
        residual: &mut Vec<Expr>,
    ) {
        // Try OR-to-IN conversion first
        if let Some(in_expr) = try_or_to_in(expr, variable)
            && self.is_pushable(&in_expr, variable)
        {
            pushable.push(in_expr);
            return;
        }

        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.split_conjuncts(left, variable, pushable, residual);
                self.split_conjuncts(right, variable, pushable, residual);
            }
            _ => {
                if self.is_pushable(expr, variable) {
                    pushable.push(expr.clone());
                } else {
                    residual.push(expr.clone());
                }
            }
        }
    }

    /// Returns `true` if a predicate can be pushed down to Lance storage.
    pub fn is_pushable(&self, expr: &Expr, variable: &str) -> bool {
        match expr {
            Expr::In {
                expr: left,
                list: right,
            } => {
                // Check left side is a property of the scan variable
                let left_is_property = matches!(
                    left.as_ref(),
                    Expr::Property(box_expr, _) if matches!(box_expr.as_ref(), Expr::Variable(v) if v == variable)
                );
                // Check right side is a constant list or a parameter. A list
                // holding another variable's property (`n.i IN [m.f]`) cannot
                // be evaluated by `n`'s scan.
                let right_valid =
                    matches!(right.as_ref(), Expr::Parameter(_)) || is_constant_list(right);
                left_is_property && right_valid
            }
            Expr::BinaryOp { left, op, right } => {
                // Check operator is supported
                let op_supported = matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                        | BinaryOp::Contains
                        | BinaryOp::StartsWith
                        | BinaryOp::EndsWith
                );

                if !op_supported {
                    return false;
                }

                // Check left side is a property of the scan variable
                // Structure: Property(Identifier(var), prop_name)
                let left_is_property = matches!(
                    left.as_ref(),
                    Expr::Property(box_expr, _) if matches!(box_expr.as_ref(), Expr::Variable(v) if v == variable)
                );

                // Check right side is a literal or parameter or list of literals.
                // A string operator takes a string literal or a parameter. A
                // parameter reaches the scan's filter so the index can serve it
                // once resolved (`STARTS WITH $p` as a prefix range, perf audit
                // F1); `LanceFilterGenerator` never renders a parameter, so an
                // unresolved one is simply not handed to storage.
                let right_valid = if matches!(
                    op,
                    BinaryOp::Contains | BinaryOp::StartsWith | BinaryOp::EndsWith
                ) {
                    matches!(
                        right.as_ref(),
                        Expr::Literal(CypherLiteral::String(_)) | Expr::Parameter(_)
                    )
                } else {
                    matches!(right.as_ref(), Expr::Literal(_) | Expr::Parameter(_))
                        || is_constant_list(right)
                };

                left_is_property && right_valid
            }
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => self.is_pushable(expr, variable),

            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                // Check if inner is a property of the scan variable
                matches!(
                    inner.as_ref(),
                    Expr::Property(var_expr, _)
                        if matches!(var_expr.as_ref(), Expr::Variable(v) if v == variable)
                )
            }

            _ => false,
        }
    }

    /// Extract property names required by residual predicates
    fn extract_properties(&self, exprs: &[Expr], variable: &str) -> Vec<String> {
        let mut props = HashSet::new();
        for expr in exprs {
            collect_properties(expr, variable, &mut props);
        }
        props.into_iter().collect()
    }
}

/// Detect a chain of single-label `LabelCheck`s combined with `OR` over
/// the same variable, collecting the labels into a flat list.
///
/// Example: `n:Person OR n:Organization` → `Some(["Person", "Organization"])`.
///
/// Returns `None` if the predicate isn't a pure OR-tree of single-label
/// label checks on `variable` (mixed predicates, multi-label conjunctions,
/// or different variables abort the rewrite). The `LabelCheck` AST node
/// uses `labels: Vec<String>` with conjunction semantics (`n:A:B`); we
/// only accept single-element lists since a conjunctive leaf can't be
/// expressed as a label-scoped scan without an additional residual filter.
pub fn try_label_or_to_union(expr: &Expr, variable: &str) -> Option<Vec<String>> {
    let mut labels: Vec<String> = Vec::new();
    if collect_or_branches(expr, variable, &mut labels, &label_leaf) && labels.len() >= 2 {
        Some(labels)
    } else {
        None
    }
}

/// Walk an `OR`-tree, requiring every leaf to satisfy `leaf` (push into
/// `out` and return true). Shared by `try_label_or_to_union` and
/// `try_type_or_to_union`; the predicates differ only in what shape they
/// recognize at the leaf.
fn collect_or_branches<F>(expr: &Expr, variable: &str, out: &mut Vec<String>, leaf: &F) -> bool
where
    F: Fn(&Expr, &str, &mut Vec<String>) -> bool,
{
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            collect_or_branches(left, variable, out, leaf)
                && collect_or_branches(right, variable, out, leaf)
        }
        _ => leaf(expr, variable, out),
    }
}

fn label_leaf(expr: &Expr, variable: &str, out: &mut Vec<String>) -> bool {
    let Expr::LabelCheck {
        expr: target,
        labels,
    } = expr
    else {
        return false;
    };
    if labels.len() != 1 {
        // Conjunction-of-multiple is not pushable as a single label scan
        // branch — fall back to residual filter.
        return false;
    }
    if let Expr::Variable(v) = target.as_ref()
        && v == variable
    {
        out.push(labels[0].clone());
        return true;
    }
    false
}

/// Detect a chain of `type(r) = 'A'` equality checks combined with `OR`
/// over the same relationship variable, collecting the type names.
///
/// Example: `type(r) = 'KNOWS' OR type(r) = 'FOLLOWS'` →
/// `Some(["KNOWS", "FOLLOWS"])`.
pub fn try_type_or_to_union(expr: &Expr, variable: &str) -> Option<Vec<String>> {
    let mut types: Vec<String> = Vec::new();
    if collect_or_branches(expr, variable, &mut types, &type_eq_leaf) && types.len() >= 2 {
        Some(types)
    } else {
        None
    }
}

fn type_eq_leaf(expr: &Expr, variable: &str, out: &mut Vec<String>) -> bool {
    let Expr::BinaryOp {
        left,
        op: BinaryOp::Eq,
        right,
    } = expr
    else {
        return false;
    };
    is_type_eq_string(left, right, variable, out) || is_type_eq_string(right, left, variable, out)
}

fn is_type_eq_string(
    fn_side: &Expr,
    str_side: &Expr,
    variable: &str,
    out: &mut Vec<String>,
) -> bool {
    if let Expr::FunctionCall { name, args, .. } = fn_side
        && name.eq_ignore_ascii_case("type")
        && args.len() == 1
        && let Expr::Variable(v) = &args[0]
        && v == variable
        && let Expr::Literal(CypherLiteral::String(s)) = str_side
    {
        out.push(s.clone());
        return true;
    }
    false
}

/// Attempt to convert OR disjunctions to IN predicates
fn try_or_to_in(expr: &Expr, variable: &str) -> Option<Expr> {
    match expr {
        Expr::BinaryOp {
            op: BinaryOp::Or, ..
        } => {
            // Collect all equality comparisons on the same property
            let mut property: Option<String> = None;
            let mut values: Vec<Expr> = Vec::new();

            if collect_or_equals(expr, variable, &mut property, &mut values)
                && let Some(prop) = property
                && values.len() >= 2
            {
                return Some(Expr::In {
                    expr: Box::new(Expr::Property(
                        Box::new(Expr::Variable(variable.to_string())),
                        prop,
                    )),
                    list: Box::new(Expr::List(values)),
                });
            }
            None
        }
        _ => None,
    }
}

fn collect_or_equals(
    expr: &Expr,
    variable: &str,
    property: &mut Option<String>,
    values: &mut Vec<Expr>,
) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            collect_or_equals(left, variable, property, values)
                && collect_or_equals(right, variable, property, values)
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => {
            if let Expr::Property(var_expr, prop) = left.as_ref()
                && let Expr::Variable(v) = var_expr.as_ref()
                && v == variable
            {
                match property {
                    None => {
                        *property = Some(prop.clone());
                        values.push(right.as_ref().clone());
                        return true;
                    }
                    Some(p) if p == prop => {
                        values.push(right.as_ref().clone());
                        return true;
                    }
                    _ => return false, // Different properties
                }
            }
            false
        }
        _ => false,
    }
}

fn collect_properties(expr: &Expr, variable: &str, props: &mut HashSet<String>) {
    match expr {
        Expr::Property(box_expr, prop) => {
            if let Expr::Variable(v) = box_expr.as_ref()
                && v == variable
            {
                props.insert(prop.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_properties(left, variable, props);
            collect_properties(right, variable, props);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_properties(expr, variable, props);
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
            collect_properties(expr, variable, props);
        }
        Expr::List(items) => {
            for item in items {
                collect_properties(item, variable, props);
            }
        }
        Expr::Map(items) => {
            for (_, item) in items {
                collect_properties(item, variable, props);
            }
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_properties(arg, variable, props);
            }
        }
        Expr::ArrayIndex {
            array: arr,
            index: idx,
        } => {
            collect_properties(arr, variable, props);
            collect_properties(idx, variable, props);
        }
        _ => {}
    }
}

/// The least string greater than every string that starts with `s`.
///
/// Used as the exclusive upper bound of a prefix range: a string starts with
/// `s` exactly when it lies in `[s, prefix_successor(s))` under byte order,
/// which for UTF-8 is code-point order. The successor drops trailing
/// U+10FFFF (the largest scalar value has no successor) and replaces the new
/// last character with the next scalar value, stepping over the surrogate gap
/// (U+D7FF is followed by U+E000, not U+D800).
///
/// `None` when no upper bound exists: `s` is empty or consists only of
/// U+10FFFF, so every string at or above it shares the prefix (or `s` matches
/// everything).
///
/// Examples: `"John"` → `"Joho"`, `"a\u{10FFFF}"` → `"b"`, `"u\u{D7FF}"` →
/// `"u\u{E000}"`.
fn increment_last_char(s: &str) -> Option<String> {
    let trimmed = s.trim_end_matches(char::MAX);
    let mut chars = trimmed.chars();
    let last = chars.next_back()?;
    let next = match last {
        '\u{D7FF}' => '\u{E000}',
        c => char::from_u32(u32::from(c) + 1)?,
    };
    let mut out = String::with_capacity(trimmed.len() + 4);
    out.push_str(chars.as_str());
    out.push(next);
    Some(out)
}

/// The conjuncts of `expr`: its `AND` tree flattened, left to right.
///
/// A predicate that is not an `AND` is its own single conjunct.
pub fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    flatten_ands(expr)
}

/// Flatten nested AND expressions into a vector
fn flatten_ands(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut result = flatten_ands(left);
            result.extend(flatten_ands(right));
            result
        }
        _ => vec![expr],
    }
}

/// Converts pushable predicates to Lance SQL filter strings.
#[derive(Debug)]
pub struct LanceFilterGenerator;

impl LanceFilterGenerator {
    /// Convert pushable predicates into a structured [`FilterExpr`].
    ///
    /// Conjuncts with no structured representation — parameters, unsupported
    /// operators, a non-property left-hand side — are **silently dropped**. The
    /// result therefore matches a *superset* of the intended rows, and is only
    /// sound where the caller re-applies the original predicates above the scan
    /// (`apply_scan_filter` / `verify_and_filter_candidates` both do).
    ///
    /// Returns [`FilterExpr::Literal`]`(true)` when nothing is pushable.
    ///
    /// The predicate may contain a [`FilterExpr::StringMatch`] whose pattern
    /// holds a SQL wildcard, which [`FilterExpr::to_sql`] declines. Callers
    /// feeding a SQL backend must project through
    /// [`FilterExpr::sql_pushable`] first; [`Self::generate`] does exactly that.
    pub fn generate_expr(
        predicates: &[Expr],
        variable: &str,
        schema_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> FilterExpr {
        // Flattening nested ANDs keeps `all()` a single flat conjunction, which
        // is what the range-aware planners downstream expect to see.
        //
        // There used to be a "range fusion" pass here that recognized a
        // `col >= L` / `col <= U` pair and emitted one combined clause. It is
        // gone: structured output makes it a no-op — the generic path below
        // produces the same two `Compare` nodes — and its bespoke string form
        // (`"col" >= L`) was a live defect, because Lance parses a
        // double-quoted name as a string literal, so the whole clause collapsed
        // to a data-independent constant that matched every row.
        FilterExpr::all(
            predicates
                .iter()
                .flat_map(flatten_ands)
                .filter_map(|e| Self::expr_to_filter(e, variable, schema_props)),
        )
    }

    /// Converts pushable predicates to Lance SQL filter string.
    ///
    /// When `schema_props` is provided, properties not in the schema (overflow properties)
    /// are skipped since they don't exist as physical columns in Lance.
    ///
    /// `None` means "nothing to push" — the caller should not build a filtered
    /// scan path at all. It does not distinguish "no input" from "nothing was
    /// pushable"; both leave the residual filter to do all the work.
    pub fn generate(
        predicates: &[Expr],
        variable: &str,
        schema_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> Option<String> {
        if predicates.is_empty() {
            return None;
        }
        let expr = Self::generate_expr(predicates, variable, schema_props).sql_pushable();
        if expr.is_trivially_true() {
            return None;
        }
        expr.to_sql().ok()
    }

    fn expr_to_filter(
        expr: &Expr,
        variable: &str,
        schema_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> Option<FilterExpr> {
        match expr {
            Expr::In {
                expr: left,
                list: right,
            } => {
                let column = Self::extract_column(left, variable, schema_props)?;
                let temporal = Self::may_be_temporal(&column, schema_props);
                Some(FilterExpr::one_of(
                    column,
                    Self::list_to_scalars(right, temporal)?,
                ))
            }
            Expr::BinaryOp { left, op, right } => {
                let column = Self::extract_column(left, variable, schema_props)?;
                let kind = match op {
                    BinaryOp::Contains => Some(StringMatchKind::Contains),
                    BinaryOp::StartsWith => Some(StringMatchKind::StartsWith),
                    BinaryOp::EndsWith => Some(StringMatchKind::EndsWith),
                    _ => None,
                };
                match kind {
                    // The pattern travels as data. Whether a `%`/`_` inside it
                    // can be honored is the backend's call: `to_sql` refuses
                    // (no ESCAPE clause — CWE-89), a native evaluator matches
                    // the substring exactly.
                    Some(kind) => Some(FilterExpr::StringMatch {
                        column,
                        kind,
                        pattern: Self::get_string_value(right)?,
                    }),
                    None => {
                        let temporal = Self::may_be_temporal(&column, schema_props);
                        Some(FilterExpr::compare(
                            column,
                            Self::op_to_cmp(op)?,
                            Self::value_to_scalar(right, temporal)?,
                        ))
                    }
                }
            }
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => Some(FilterExpr::negate(Self::expr_to_filter(
                expr,
                variable,
                schema_props,
            )?)),
            Expr::IsNull(inner) => Some(FilterExpr::IsNull(Self::extract_column(
                inner,
                variable,
                schema_props,
            )?)),
            Expr::IsNotNull(inner) => Some(FilterExpr::IsNotNull(Self::extract_column(
                inner,
                variable,
                schema_props,
            )?)),
            _ => None,
        }
    }

    fn extract_column(
        expr: &Expr,
        variable: &str,
        schema_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> Option<String> {
        match expr {
            Expr::Property(box_expr, prop) => {
                if let Expr::Variable(var) = box_expr.as_ref()
                    && var == variable
                {
                    // Reserved system columns are always physical Lance columns
                    if uni_common::core::schema::is_reserved_property_name(prop) {
                        return Some(prop.clone());
                    }
                    // If schema_props is provided, only allow properties that are
                    // physical columns in Lance. Overflow properties (not in schema)
                    // don't exist as Lance columns.
                    // If schema_props is Some but empty (schemaless label), ALL
                    // non-system properties are overflow.
                    // If schema_props is None, no filtering is applied (caller
                    // doesn't have schema info).
                    if let Some(props) = schema_props
                        && !props.contains_key(prop.as_str())
                    {
                        return None;
                    }
                    return Some(prop.clone());
                }
                None
            }
            _ => None,
        }
    }

    fn op_to_cmp(op: &BinaryOp) -> Option<CmpOp> {
        match op {
            BinaryOp::Eq => Some(CmpOp::Eq),
            BinaryOp::NotEq => Some(CmpOp::NotEq),
            BinaryOp::Lt => Some(CmpOp::Lt),
            BinaryOp::LtEq => Some(CmpOp::LtEq),
            BinaryOp::Gt => Some(CmpOp::Gt),
            BinaryOp::GtEq => Some(CmpOp::GtEq),
            _ => None,
        }
    }

    /// Whether `column` may hold a temporal type, so a string literal compared
    /// with it is a timestamp spelling rather than a string.
    ///
    /// Unknown (no schema, or a column the schema does not declare) counts as
    /// possibly temporal, which keeps the behavior callers without a schema
    /// always had.
    fn may_be_temporal(column: &str, schema_props: Option<&HashMap<String, PropertyMeta>>) -> bool {
        use uni_common::core::schema::DataType as T;
        schema_props
            .and_then(|props| props.get(column))
            .is_none_or(|meta| {
                matches!(meta.r#type, T::Timestamp | T::Date | T::Time | T::DateTime)
            })
    }

    /// The literal `expr` as a [`Scalar`]; `temporal` enables datetime
    /// normalization of a string literal.
    fn value_to_scalar(expr: &Expr, temporal: bool) -> Option<Scalar> {
        match expr {
            Expr::Literal(CypherLiteral::String(s)) if temporal => {
                // Normalize datetime strings to include seconds for Arrow timestamp parsing.
                // Our Cypher datetime formatting omits `:00` seconds (e.g. `2021-06-01T00:00Z`)
                // but Arrow/Lance requires full `HH:MM:SS` for timestamp parsing.
                Some(Scalar::Str(
                    super::df_expr::normalize_datetime_str(s).unwrap_or_else(|| s.clone()),
                ))
            }
            // On a column known not to be temporal the literal is a string and
            // must reach the storage filter unchanged: normalizing it turned
            // `n.s = '2021-06-01T00:00Z'` on an indexed STRING column into
            // `s = '2021-06-01T00:00:00Z'`, and the stored row was lost
            // (measured: `[]` instead of the row).
            Expr::Literal(CypherLiteral::String(s)) => Some(Scalar::Str(s.clone())),
            Expr::Literal(CypherLiteral::Integer(i)) => Some(Scalar::Int(*i)),
            Expr::Literal(CypherLiteral::Float(f)) => Some(Scalar::Float(*f)),
            Expr::Literal(CypherLiteral::Bool(b)) => Some(Scalar::Bool(*b)),
            Expr::Literal(CypherLiteral::Null) => Some(Scalar::Null),
            // Security: CWE-89 - Parameters are NOT pushed to storage layer.
            // Parameterized predicates stay in the application layer where the
            // query executor can safely substitute values with proper type handling.
            // This prevents potential SQL injection if Lance doesn't support the $name syntax.
            Expr::Parameter(_) => None,
            _ => None,
        }
    }

    /// The right-hand side of an `IN`, which must be a literal list.
    ///
    /// One unrenderable element rejects the whole list — a partial `IN` would
    /// silently narrow the predicate.
    fn list_to_scalars(expr: &Expr, temporal: bool) -> Option<Vec<Scalar>> {
        match expr {
            Expr::List(items) => items
                .iter()
                .map(|item| Self::value_to_scalar(item, temporal))
                .collect(),
            _ => None,
        }
    }

    /// Extracts raw string value from expression for pattern-match use.
    fn get_string_value(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal(CypherLiteral::String(s)) => Some(s.clone()),
            _ => None,
        }
    }
}

/// Rewrites `constant OP var.prop` comparisons to `var.prop OP' constant`.
///
/// Every pushdown and index matcher downstream of the WHERE planner —
/// [`PredicateAnalyzer::is_pushable`], [`IndexAwareAnalyzer`]'s indexed-equality,
/// UID, prefix and FTS routes, and [`LanceFilterGenerator`] — recognizes a
/// property comparison only with the property on the left. A commuted
/// `$p = n.uid` or `'u42' = n.uid` therefore became a residual filter over a
/// full scan while `n.uid = $p` read one row from the index (perf audit
/// 2026-10-17, A9). Orienting the predicate once, before any matcher runs, fixes
/// every matcher at the same time.
///
/// The operator is mirrored, not negated: `=` and `<>` are symmetric, `<`
/// becomes `>`, `<=` becomes `>=` and so on. `a < b` and `b > a` are the same
/// Cypher comparison, including the NULL result for a NULL or incomparable
/// operand, so the rewrite is semantics-preserving wherever it fires.
///
/// Only the boolean skeleton (`AND`, `OR`, `XOR`, `NOT`) is descended; a
/// comparison nested inside any other expression is left untouched. A side
/// counts as constant only when it is built from literals and parameters
/// (see `is_row_independent`); the other side must be a property of a
/// variable. Non-commutative operators — `IN`, `STARTS WITH`, `ENDS WITH`,
/// `CONTAINS`, `=~` — are never flipped.
pub fn orient_property_comparisons(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp {
            left,
            op: op @ (BinaryOp::And | BinaryOp::Or | BinaryOp::Xor),
            right,
        } => Expr::BinaryOp {
            left: Box::new(orient_property_comparisons(*left)),
            op,
            right: Box::new(orient_property_comparisons(*right)),
        },
        Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: inner,
        } => Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(orient_property_comparisons(*inner)),
        },
        Expr::BinaryOp { left, op, right } => match mirrored_comparison(op) {
            Some(mirrored)
                if is_row_independent(&left)
                    && matches!(right.as_ref(), Expr::Property(base, _)
                        if matches!(base.as_ref(), Expr::Variable(_))) =>
            {
                Expr::BinaryOp {
                    left: right,
                    op: mirrored,
                    right: left,
                }
            }
            _ => Expr::BinaryOp { left, op, right },
        },
        other => other,
    }
}

/// The POINT shape of `expr`, with the property argument (as
/// `is_property` recognizes it) and constant other operands:
/// `point.withinBBox(p, ll, ur)`, or `distance(p, c) < r` / `<= r` with
/// `distance`'s arguments in either order and the comparison in either
/// orientation (`r > distance(p, c)`). `>` / `>=` on a distance (the
/// complement of a disk) and `=` are not shapes.
fn spatial_keyed_form(
    expr: &Expr,
    is_property: &dyn Fn(&Expr) -> bool,
) -> Option<(Expr, KeyedForm)> {
    let call = |e: &Expr, fname: &str, arity: usize| -> Option<Vec<Expr>> {
        match e {
            Expr::FunctionCall {
                name,
                args,
                distinct: false,
                window_spec: None,
            } if args.len() == arity && name.eq_ignore_ascii_case(fname) => Some(args.clone()),
            _ => None,
        }
    };
    if let Some(args) = call(expr, "point.withinBBox", 3) {
        return (is_property(&args[0])
            && is_row_independent(&args[1])
            && is_row_independent(&args[2]))
        .then(|| {
            (
                args[0].clone(),
                KeyedForm::PointWithinBBox {
                    lower_left: args[1].clone(),
                    upper_right: args[2].clone(),
                },
            )
        });
    }
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    // Oriented as `distance(…) op r`.
    let (distance, op, radius) = if is_row_independent(right) {
        (left.as_ref(), *op, right.as_ref())
    } else if is_row_independent(left) {
        (right.as_ref(), mirrored_comparison(*op)?, left.as_ref())
    } else {
        return None;
    };
    let inclusive = match op {
        BinaryOp::Lt => false,
        BinaryOp::LtEq => true,
        _ => return None,
    };
    let args = call(distance, "distance", 2)?;
    let (prop, center) = if is_property(&args[0]) && is_row_independent(&args[1]) {
        (&args[0], &args[1])
    } else if is_property(&args[1]) && is_row_independent(&args[0]) {
        (&args[1], &args[0])
    } else {
        return None;
    };
    Some((
        prop.clone(),
        KeyedForm::PointDistance {
            center: center.clone(),
            radius: radius.clone(),
            inclusive,
        },
    ))
}

/// The operator `op'` with `a op b` ≡ `b op' a`, for comparisons only.
fn mirrored_comparison(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Eq => Some(BinaryOp::Eq),
        BinaryOp::NotEq => Some(BinaryOp::NotEq),
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::LtEq => Some(BinaryOp::GtEq),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::GtEq => Some(BinaryOp::LtEq),
        _ => None,
    }
}

/// Whether `expr` is built only from literals, parameters and temporal
/// constructors called on such arguments (see [`foldable_temporal_function`]),
/// and `point(m)` with `m` a map literal or parameter.
///
/// Deliberately structural and conservative: any variable, property access,
/// other function call or subquery makes it row-dependent, so an expression
/// this function cannot classify is never treated as a constant.
fn is_row_independent(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::List(items) => items.iter().all(is_row_independent),
        Expr::Map(entries) => entries.iter().all(|(_, v)| is_row_independent(v)),
        Expr::UnaryOp { expr, .. } => is_row_independent(expr),
        Expr::FunctionCall {
            name,
            args,
            distinct: false,
            window_spec: None,
        } if is_point_constructor(name, args) => is_row_independent(&args[0]),
        Expr::FunctionCall {
            name,
            args,
            distinct: false,
            window_spec: None,
        } => foldable_temporal_function(name).is_some() && args.iter().all(is_row_independent),
        _ => false,
    }
}

/// Whether `name(args)` is `point(m)` with `m` a map literal or a
/// parameter: the constructor reads no clock and no row, so it folds when
/// `m` is constant.
fn is_point_constructor(name: &str, args: &[Expr]) -> bool {
    name.eq_ignore_ascii_case("point") && matches!(args, [Expr::Map(_) | Expr::Parameter(_)])
}

/// The canonical (upper-case) name of a temporal constructor whose value is
/// fixed for a statement given constant arguments: `datetime`, `date`,
/// `time`, `localtime`, `localdatetime` and their `.statement()` /
/// `.transaction()` forms, and `btic`. Never a `.realtime()` form, which
/// reads the wall clock at every call.
fn foldable_temporal_function(name: &str) -> Option<String> {
    let upper = name.to_uppercase();
    let base = upper
        .strip_suffix(".STATEMENT")
        .or_else(|| upper.strip_suffix(".TRANSACTION"))
        .unwrap_or(&upper);
    matches!(
        base,
        "DATETIME" | "DATE" | "TIME" | "LOCALTIME" | "LOCALDATETIME"
    )
    .then_some(upper.clone())
    // `btic(literal)` reads no clock; it has no `.statement()` form.
    .or_else(|| (upper == "BTIC").then_some(upper))
}

/// The value of `expr` when it is constant for the statement: a literal, a
/// resolved parameter, a list or map of those, or a temporal constructor
/// called on constant arguments (see [`foldable_temporal_function`]), or
/// `point(m)` on a constant map (see `is_point_constructor`).
///
/// A no-argument constructor reads the statement clock, `statement_time`,
/// which must be the one the statement's other uses of the clock see (the
/// physical planner's, which its filters are translated with), so that a
/// bound computed from the folded value agrees with the predicate kept
/// above the scan. `None` for anything row-dependent, a `.realtime()` form,
/// a missing parameter, and a call that fails to evaluate.
#[must_use]
pub fn fold_constant_operand(
    expr: &Expr,
    params: &HashMap<String, Value>,
    statement_time: chrono::DateTime<chrono::Utc>,
) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(lit.to_value()),
        Expr::Parameter(name) => params.get(name).cloned(),
        Expr::List(items) => items
            .iter()
            .map(|i| fold_constant_operand(i, params, statement_time))
            .collect::<Option<Vec<_>>>()
            .map(Value::List),
        Expr::Map(entries) => entries
            .iter()
            .map(|(k, v)| Some((k.clone(), fold_constant_operand(v, params, statement_time)?)))
            .collect::<Option<HashMap<_, _>>>()
            .map(Value::Map),
        // `point({…})`: the constructor's own value, or `None` where it
        // raises (the residual raises the same).
        Expr::FunctionCall {
            name,
            args,
            distinct: false,
            window_spec: None,
        } if is_point_constructor(name, args) => {
            let map = fold_constant_operand(&args[0], params, statement_time)?;
            crate::spatial::eval_spatial_function("POINT", &[map]).ok()
        }
        Expr::FunctionCall {
            name,
            args,
            distinct: false,
            window_spec: None,
        } => {
            let name = foldable_temporal_function(name)?;
            let args = args
                .iter()
                .map(|a| fold_constant_operand(a, params, statement_time))
                .collect::<Option<Vec<_>>>()?;
            crate::datetime::eval_datetime_function_with_clock(&name, &args, statement_time).ok()
        }
        _ => None,
    }
}

/// If `expr` is a property predicate (`Property(var, p) <op> _` or
/// `Property(var, p) IN _`) on `variable`, return the property name.
/// Used by the planner to match analyzer-detected hash-index columns back
/// to the originating predicate.
pub fn predicate_target_column(expr: &Expr, variable: &str) -> Option<String> {
    let prop_side = match expr {
        Expr::BinaryOp { left, .. } => left.as_ref(),
        Expr::In { expr: left, .. } => left.as_ref(),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => inner.as_ref(),
        _ => return None,
    };
    if let Expr::Property(var_expr, prop) = prop_side
        && let Expr::Variable(v) = var_expr.as_ref()
        && v == variable
    {
        return Some(prop.clone());
    }
    None
}

/// Translate one range, prefix or null-test conjunct into an index-servable filter.
///
/// `resolved` is a property-first conjunct with parameters already substituted
/// (`n.p < 10`, `n.p STARTS WITH 'ab'`, `n.p IS NULL`); `column_type` is the
/// declared type of `p`. The result is handed to the storage scan, where a
/// scalar index can serve it, while the caller keeps `resolved` itself above
/// the scan. The contract is therefore one-sided: the returned filter must
/// accept **every** row `resolved` accepts under Cypher semantics, and may
/// accept more. `None` means "no exact rendering", and the conjunct stays a
/// residual filter only.
///
/// What is translated, and why each form is exact or a superset:
///
/// Only `INT64`, `FLOAT64` and `STRING` columns take a range: a bound on an
/// `INT32` or `FLOAT32` column would be narrowed to the column type by the
/// storage layer, and that rounding is not one-sided.
///
/// * **Integer column, integer bound** — the same comparison.
/// * **Integer column, float bound** — rounded to the integer comparison that
///   selects the same integers: `x < 9.5` ⇔ `x < 10` (`ceil`), `x <= 9.5` ⇔
///   `x <= 9` (`floor`), `x > 9.5` ⇔ `x > 9`, `x >= 9.5` ⇔ `x >= 10`. Exact
///   in real arithmetic, and a superset of a comparison done after widening
///   the integer to `f64`, because rounding to the nearest `f64` is monotone.
///   A zero bound becomes an inclusive bound at 0 (see
///   `integer_bound_for_float`). A NaN, infinite or out-of-`i64` bound is not
///   translated.
/// * **Float column** — a finite, non-zero float bound, or an integer bound of
///   magnitude at most 2^53 (exactly representable). See `float_bound` for
///   why zero, NaN and infinite bounds are not translated.
/// * **String column, string bound** — the same comparison; both sides order
///   strings by bytes. No datetime normalization: on a STRING column the
///   literal is a string, not a timestamp.
/// * **`STARTS WITH p`** on a string column — `col >= p AND col <
///   increment_last_char(p)`. The empty prefix is not translated (it selects
///   every string, so the index has nothing to narrow).
/// * **`IS NULL` / `IS NOT NULL`** on a string, integer, float or boolean
///   column — the same test. Restricted to those types because a
///   `CypherValue` column can hold an encoded NULL in a non-null cell.
///
/// Every other operand type pairing (a string bound on an integer column, a
/// NULL bound, a boolean bound, any other column type) is not translated: in
/// Cypher such a comparison is NULL, and the storage layer would either reject
/// the literal or coerce it.
pub fn indexed_range_filter(
    resolved: &Expr,
    variable: &str,
    column_type: Option<&uni_common::core::schema::DataType>,
) -> Option<FilterExpr> {
    use uni_common::core::schema::DataType as T;

    let column_of = |e: &Expr| -> Option<String> {
        let Expr::Property(base, prop) = e else {
            return None;
        };
        // System columns (`_vid`, `_version`, ...) have their own routes.
        (matches!(base.as_ref(), Expr::Variable(v) if v == variable)
            && !uni_common::core::schema::is_reserved_property_name(prop))
        .then(|| prop.clone())
    };
    match resolved {
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            let column = column_of(inner)?;
            if !matches!(
                column_type?,
                T::String | T::Int32 | T::Int64 | T::Float32 | T::Float64 | T::Bool
            ) {
                return None;
            }
            Some(match resolved {
                Expr::IsNull(_) => FilterExpr::IsNull(column),
                _ => FilterExpr::IsNotNull(column),
            })
        }
        Expr::BinaryOp {
            left,
            op: BinaryOp::StartsWith,
            right,
        } => {
            let column = column_of(left)?;
            let (Some(T::String), Expr::Literal(CypherLiteral::String(prefix))) =
                (column_type, right.as_ref())
            else {
                return None;
            };
            if prefix.is_empty() {
                return None;
            }
            let lower =
                FilterExpr::compare(column.clone(), CmpOp::GtEq, Scalar::Str(prefix.clone()));
            Some(match increment_last_char(prefix) {
                Some(upper) => FilterExpr::all([
                    lower,
                    FilterExpr::compare(column, CmpOp::Lt, Scalar::Str(upper)),
                ]),
                None => lower,
            })
        }
        Expr::BinaryOp { left, op, right } => {
            let column = column_of(left)?;
            let op = match op {
                BinaryOp::Lt => CmpOp::Lt,
                BinaryOp::LtEq => CmpOp::LtEq,
                BinaryOp::Gt => CmpOp::Gt,
                BinaryOp::GtEq => CmpOp::GtEq,
                _ => return None,
            };
            let Expr::Literal(lit) = right.as_ref() else {
                return None;
            };
            match (column_type?, lit) {
                (T::Int64, CypherLiteral::Integer(i)) => {
                    Some(FilterExpr::compare(column, op, Scalar::Int(*i)))
                }
                (T::Int64, CypherLiteral::Float(f)) => {
                    let (op, bound) = integer_bound_for_float(op, *f)?;
                    Some(FilterExpr::compare(column, op, Scalar::Int(bound)))
                }
                (T::Float64, CypherLiteral::Float(f)) => float_bound(column, op, *f),
                (T::Float64, CypherLiteral::Integer(i)) if i.unsigned_abs() <= 1_u64 << 53 => {
                    // Exact: |i| <= 2^53 is representable as an f64.
                    let f = *i as f64;
                    float_bound(column, op, f)
                }
                (T::String, CypherLiteral::String(s)) => {
                    Some(FilterExpr::compare(column, op, Scalar::Str(s.clone())))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// The integer comparison selecting exactly the integers `x` with `x op f`.
///
/// `None` for a NaN or infinite `f`, or when the rounded bound does not fit
/// in an `i64`.
fn integer_bound_for_float(op: CmpOp, f: f64) -> Option<(CmpOp, i64)> {
    if !f.is_finite() {
        return None;
    }
    if f == 0.0 {
        // Either zero. The residual compares numerically (`0 > -0.0` is
        // false; it was true under the total order the residual once used).
        // An inclusive bound at 0 is a superset of the residual's answer
        // under either reading.
        let inclusive = match op {
            CmpOp::Lt | CmpOp::LtEq => CmpOp::LtEq,
            CmpOp::Gt | CmpOp::GtEq => CmpOp::GtEq,
            CmpOp::Eq | CmpOp::NotEq => return None,
        };
        return Some((inclusive, 0));
    }
    let rounded = match op {
        CmpOp::Lt | CmpOp::GtEq => f.ceil(),
        CmpOp::LtEq | CmpOp::Gt => f.floor(),
        CmpOp::Eq | CmpOp::NotEq => return None,
    };
    // 2^63 is exactly representable; every f64 in [-2^63, 2^63) is an i64.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if !(-TWO_POW_63..TWO_POW_63).contains(&rounded) {
        return None;
    }
    // Exact: `rounded` is integral and in `i64` range, checked above.
    let bound = rounded as i64;
    Some((op, bound))
}

/// A float-column comparison against a finite, non-zero `f`.
///
/// A bound of either zero is not rendered. The residual compares floats
/// numerically (`-0.0 = 0.0`), but storage orders them by IEEE total order, so
/// `f >= 0` there would lose a stored `-0.0` row the residual accepts; and the
/// SQL dialect has no spelling for `-0.0` (it renders as `-0`, the negation of
/// an integer, which is `+0.0`). Non-zero bounds are exact: total order and
/// numeric order differ only on zeros and NaNs, and the residual rejects every
/// NaN. NaN and infinite bounds have no SQL literal at all (`NaN` and `inf`
/// parse as column names).
fn float_bound(column: String, op: CmpOp, f: f64) -> Option<FilterExpr> {
    if !f.is_finite() || f == 0.0 {
        return None;
    }
    Some(FilterExpr::compare(column, op, Scalar::Float(f)))
}

/// Convert a runtime `Value` to a Cypher AST `Expr` literal.
///
/// Returns `None` for variants we cannot represent inline in a pushed-down
/// Lance filter (e.g. nodes/edges/paths). Maps and lists nest.
fn value_to_expr(v: &Value) -> Option<Expr> {
    Some(match v {
        Value::Null => Expr::Literal(CypherLiteral::Null),
        Value::Bool(b) => Expr::Literal(CypherLiteral::Bool(*b)),
        Value::Int(i) => Expr::Literal(CypherLiteral::Integer(*i)),
        Value::Float(f) => Expr::Literal(CypherLiteral::Float(*f)),
        Value::String(s) => Expr::Literal(CypherLiteral::String(s.clone())),
        Value::List(items) => {
            let items: Option<Vec<Expr>> = items.iter().map(value_to_expr).collect();
            Expr::List(items?)
        }
        // Bytes / Map / Node / Edge / Path / Vector / Temporal can't be
        // represented as a Lance literal here. Bail out and let the caller
        // fall back to FilterExec.
        _ => return None,
    })
}

/// Recursively replace `Expr::Parameter(name)` with a literal `Expr` resolved
/// from `params`. Returns `None` if any parameter is missing or its `Value`
/// cannot be represented as a Cypher literal (so the predicate cannot be
/// safely pushed to storage and the caller should fall back).
///
/// `LanceFilterGenerator::value_to_lance` deliberately rejects
/// `Expr::Parameter` to prevent SQL injection (CWE-89). Substituting at plan
/// time with the resolved value sidesteps that — values come from already-
/// authenticated query params and are emitted via the same string-escaping
/// path as inline literals.
pub fn substitute_params(expr: &Expr, params: &HashMap<String, Value>) -> Option<Expr> {
    Some(match expr {
        Expr::Parameter(name) => value_to_expr(params.get(name)?)?,
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_params(left, params)?),
            op: *op,
            right: Box::new(substitute_params(right, params)?),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(substitute_params(inner, params)?),
        },
        Expr::In { expr: left, list } => Expr::In {
            expr: Box::new(substitute_params(left, params)?),
            list: Box::new(substitute_params(list, params)?),
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(substitute_params(inner, params)?)),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(substitute_params(inner, params)?)),
        Expr::List(items) => {
            let items: Option<Vec<Expr>> =
                items.iter().map(|i| substitute_params(i, params)).collect();
            Expr::List(items?)
        }
        // Leaves with no parameter references — passthrough.
        _ => expr.clone(),
    })
}

/// Whether `expr` is a list whose items are all literals, parameters or
/// such lists: a value the scan can evaluate without any other binding.
fn is_constant_list(expr: &Expr) -> bool {
    match expr {
        Expr::List(items) => items.iter().all(|item| {
            matches!(item, Expr::Literal(_) | Expr::Parameter(_)) || is_constant_list(item)
        }),
        _ => false,
    }
}

#[cfg(test)]
mod security_tests {
    use super::*;

    /// Tests for CWE-89 (SQL Injection) prevention in LIKE patterns.
    mod wildcard_protection {
        use super::*;

        /// The wildcard check moved from generation time into the renderer:
        /// `StringMatch` carries the pattern as data, and `to_sql` is what
        /// declines it (no ESCAPE clause in the dialect). These assert the
        /// refusal directly, where the old tests asserted the private helper.
        fn like_sql(pattern: &str) -> Result<String, uni_store::backend::types::ToSqlError> {
            FilterExpr::StringMatch {
                column: "name".to_string(),
                kind: StringMatchKind::Contains,
                pattern: pattern.to_string(),
            }
            .to_sql()
        }

        #[test]
        fn test_percent_wildcard_refused_by_renderer() {
            for p in ["admin%", "%admin", "ad%min"] {
                assert!(like_sql(p).is_err(), "pattern {p:?} must not render");
            }
        }

        #[test]
        fn test_underscore_wildcard_refused_by_renderer() {
            for p in ["a_min", "_admin", "admin_"] {
                assert!(like_sql(p).is_err(), "pattern {p:?} must not render");
            }
        }

        #[test]
        fn test_safe_strings_render() {
            assert_eq!(like_sql("admin").unwrap(), "`name` LIKE '%admin%'");
            assert_eq!(
                like_sql("John Smith").unwrap(),
                "`name` LIKE '%John Smith%'"
            );
            assert_eq!(
                like_sql("test@example.com").unwrap(),
                "`name` LIKE '%test@example.com%'"
            );
        }

        #[test]
        fn test_wildcard_in_contains_not_pushed_down() {
            // Input with % should NOT be pushed to storage
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "name".to_string(),
                )),
                op: BinaryOp::Contains,
                right: Box::new(Expr::Literal(CypherLiteral::String("admin%".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n", None);
            assert!(
                filter.is_none(),
                "CONTAINS with wildcard should not be pushed to storage"
            );
        }

        #[test]
        fn test_underscore_in_startswith_not_pushed_down() {
            // Input with _ should NOT be pushed to storage
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "name".to_string(),
                )),
                op: BinaryOp::StartsWith,
                right: Box::new(Expr::Literal(CypherLiteral::String("user_".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n", None);
            assert!(
                filter.is_none(),
                "STARTSWITH with underscore should not be pushed to storage"
            );
        }

        #[test]
        fn test_safe_contains_is_pushed_down() {
            // Input without wildcards SHOULD be pushed to storage
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "name".to_string(),
                )),
                op: BinaryOp::Contains,
                right: Box::new(Expr::Literal(CypherLiteral::String("admin".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n", None);
            assert!(filter.is_some(), "Safe CONTAINS should be pushed down");
            assert!(
                filter.as_ref().unwrap().contains("LIKE '%admin%'"),
                "Generated filter: {:?}",
                filter
            );
        }

        #[test]
        fn test_single_quotes_escaped_in_safe_string() {
            // Single quotes should be doubled in safe strings
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "name".to_string(),
                )),
                op: BinaryOp::Contains,
                right: Box::new(Expr::Literal(CypherLiteral::String("O'Brien".to_string()))),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n", None).unwrap();
            assert!(
                filter.contains("O''Brien"),
                "Single quotes should be doubled: {}",
                filter
            );
        }
    }

    /// Tests for parameter handling (not pushed to storage).
    mod parameter_safety {
        use super::*;

        #[test]
        fn test_parameters_not_pushed_down() {
            let expr = Expr::BinaryOp {
                left: Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".to_string())),
                    "name".to_string(),
                )),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Parameter("userInput".to_string())),
            };

            let filter = LanceFilterGenerator::generate(&[expr], "n", None);
            assert!(
                filter.is_none(),
                "Parameterized predicates should not be pushed to storage"
            );
        }
    }
}

#[cfg(test)]
mod orientation_tests {
    use super::*;

    fn prop(var: &str, p: &str) -> Expr {
        Expr::Property(Box::new(Expr::Variable(var.to_string())), p.to_string())
    }

    fn cmp(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn param(name: &str) -> Expr {
        Expr::Parameter(name.to_string())
    }

    /// Each comparison flips to its mirror, never its negation.
    #[test]
    fn every_comparison_is_mirrored() {
        for (op, mirrored) in [
            (BinaryOp::Eq, BinaryOp::Eq),
            (BinaryOp::NotEq, BinaryOp::NotEq),
            (BinaryOp::Lt, BinaryOp::Gt),
            (BinaryOp::LtEq, BinaryOp::GtEq),
            (BinaryOp::Gt, BinaryOp::Lt),
            (BinaryOp::GtEq, BinaryOp::LtEq),
        ] {
            let got = orient_property_comparisons(cmp(param("p"), op, prop("n", "uid")));
            assert_eq!(got, cmp(prop("n", "uid"), mirrored, param("p")), "{op:?}");
        }
    }

    /// Non-commutative operators keep their orientation.
    #[test]
    fn non_commutative_operators_are_left_alone() {
        for op in [
            BinaryOp::StartsWith,
            BinaryOp::EndsWith,
            BinaryOp::Contains,
            BinaryOp::Regex,
            BinaryOp::Sub,
        ] {
            let e = cmp(
                Expr::Literal(CypherLiteral::String("u".into())),
                op,
                prop("n", "uid"),
            );
            assert_eq!(orient_property_comparisons(e.clone()), e, "{op:?}");
        }
        let in_expr = Expr::In {
            expr: Box::new(Expr::Literal(CypherLiteral::String("u".into()))),
            list: Box::new(prop("n", "tags")),
        };
        assert_eq!(orient_property_comparisons(in_expr.clone()), in_expr);
    }

    /// A row-dependent left side is never treated as a constant.
    #[test]
    fn row_dependent_left_side_is_not_flipped() {
        for left in [
            prop("m", "uid"),
            Expr::Variable("x".into()),
            Expr::FunctionCall {
                name: "toUpper".into(),
                args: vec![param("p")],
                distinct: false,
                window_spec: None,
            },
            Expr::List(vec![param("p"), Expr::Variable("x".into())]),
        ] {
            let e = cmp(left, BinaryOp::Eq, prop("n", "uid"));
            assert_eq!(orient_property_comparisons(e.clone()), e);
        }
    }

    /// The boolean skeleton is descended so every conjunct and disjunct is
    /// oriented; an already-canonical comparison is unchanged.
    #[test]
    fn boolean_skeleton_is_descended() {
        let lit = |s: &str| Expr::Literal(CypherLiteral::String(s.into()));
        let e = cmp(
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(cmp(lit("a"), BinaryOp::Lt, prop("n", "x"))),
            },
            BinaryOp::And,
            cmp(
                cmp(lit("b"), BinaryOp::Eq, prop("n", "y")),
                BinaryOp::Or,
                cmp(prop("n", "z"), BinaryOp::Eq, lit("c")),
            ),
        );
        let want = cmp(
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(cmp(prop("n", "x"), BinaryOp::Gt, lit("a"))),
            },
            BinaryOp::And,
            cmp(
                cmp(prop("n", "y"), BinaryOp::Eq, lit("b")),
                BinaryOp::Or,
                cmp(prop("n", "z"), BinaryOp::Eq, lit("c")),
            ),
        );
        assert_eq!(orient_property_comparisons(e), want);
    }

    /// The oriented form is what the analyzers accept; the commuted one is not.
    #[test]
    fn oriented_form_is_pushable() {
        let analyzer = PredicateAnalyzer::new();
        let commuted = cmp(param("p"), BinaryOp::Eq, prop("n", "uid"));
        assert!(!analyzer.is_pushable(&commuted, "n"));
        assert!(analyzer.is_pushable(&orient_property_comparisons(commuted), "n"));
    }
}

#[cfg(test)]
mod range_tests {
    //! Unit tests for the F1 translation: prefix successors and the
    //! type-exact range / null renderings handed to the index.

    use super::*;
    use uni_common::core::schema::DataType as T;

    fn prop(name: &str) -> Expr {
        Expr::Property(Box::new(Expr::Variable("n".into())), name.into())
    }

    fn bin(op: BinaryOp, lit: CypherLiteral) -> Expr {
        Expr::BinaryOp {
            left: Box::new(prop("p")),
            op,
            right: Box::new(Expr::Literal(lit)),
        }
    }

    fn render(e: &Expr, ty: T) -> Option<String> {
        indexed_range_filter(e, "n", Some(&ty)).map(|f| f.to_sql().expect("renderable"))
    }

    /// `[p, succ(p))` holds exactly the strings starting with `p`, compared
    /// by bytes, over strings straddling U+D7FF/U+E000 and U+10FFFF.
    #[test]
    fn prefix_successor_bounds_exactly_the_prefixed_strings() {
        let alphabet = [
            'a',
            'b',
            '\u{7F}',
            '\u{80}',
            '\u{D7FF}',
            '\u{E000}',
            '\u{FFFF}',
            char::MAX,
        ];
        let mut strings = vec![String::new()];
        for len in 1..=3 {
            let mut frontier = vec![String::new()];
            for _ in 0..len {
                frontier = frontier
                    .iter()
                    .flat_map(|s| {
                        alphabet.iter().map(move |c| {
                            let mut t = s.clone();
                            t.push(*c);
                            t
                        })
                    })
                    .collect();
            }
            strings.extend(frontier);
        }
        for p in strings.iter().filter(|p| !p.is_empty()) {
            let upper = increment_last_char(p);
            for s in &strings {
                let in_range = s.as_bytes() >= p.as_bytes()
                    && upper.as_ref().is_none_or(|u| s.as_bytes() < u.as_bytes());
                assert_eq!(
                    in_range,
                    s.starts_with(p.as_str()),
                    "prefix {p:?}, upper {upper:?}, string {s:?}"
                );
            }
        }
    }

    #[test]
    fn prefix_successor_steps_over_the_surrogate_gap_and_the_maximum() {
        assert_eq!(increment_last_char("John").as_deref(), Some("Joho"));
        assert_eq!(
            increment_last_char("u\u{D7FF}").as_deref(),
            Some("u\u{E000}")
        );
        assert_eq!(increment_last_char("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(
            increment_last_char("a\u{10FFFF}\u{10FFFF}").as_deref(),
            Some("b")
        );
        assert_eq!(increment_last_char("\u{10FFFF}"), None);
        assert_eq!(increment_last_char(""), None);
    }

    /// Float bounds on an INT column round to the integer comparison that
    /// selects the same integers; zero widens to an inclusive bound.
    #[test]
    fn float_bound_on_int_column_rounds_outward() {
        use BinaryOp::{Gt, GtEq, Lt, LtEq};
        let f = CypherLiteral::Float;
        assert_eq!(
            render(&bin(Lt, f(9.5)), T::Int64).as_deref(),
            Some("`p` < 10")
        );
        assert_eq!(
            render(&bin(LtEq, f(9.5)), T::Int64).as_deref(),
            Some("`p` <= 9")
        );
        assert_eq!(
            render(&bin(Gt, f(9.5)), T::Int64).as_deref(),
            Some("`p` > 9")
        );
        assert_eq!(
            render(&bin(GtEq, f(9.5)), T::Int64).as_deref(),
            Some("`p` >= 10")
        );
        assert_eq!(
            render(&bin(Lt, f(-2.5)), T::Int64).as_deref(),
            Some("`p` < -2")
        );
        assert_eq!(
            render(&bin(Gt, f(-0.0)), T::Int64).as_deref(),
            Some("`p` >= 0")
        );
        assert_eq!(
            render(&bin(Lt, f(0.0)), T::Int64).as_deref(),
            Some("`p` <= 0")
        );
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1e19, -1e19] {
            assert_eq!(render(&bin(Lt, f(bad)), T::Int64), None, "{bad}");
        }
        // Every integer in a window agrees with the real-number comparison.
        for bound in [-2.5, -2.0, -0.5, 0.5, 9.5, 10.0, 10.25] {
            for op in [Lt, LtEq, Gt, GtEq] {
                let cmp = match op {
                    Lt => CmpOp::Lt,
                    LtEq => CmpOp::LtEq,
                    Gt => CmpOp::Gt,
                    _ => CmpOp::GtEq,
                };
                let (iop, ib) = integer_bound_for_float(cmp, bound).expect("finite");
                for x in -20_i64..20 {
                    let xf = x as f64;
                    let real = match cmp {
                        CmpOp::Lt => xf < bound,
                        CmpOp::LtEq => xf <= bound,
                        CmpOp::Gt => xf > bound,
                        _ => xf >= bound,
                    };
                    let int = match iop {
                        CmpOp::Lt => x < ib,
                        CmpOp::LtEq => x <= ib,
                        CmpOp::Gt => x > ib,
                        _ => x >= ib,
                    };
                    assert_eq!(int, real, "x={x} {cmp:?} {bound}");
                }
            }
        }
    }

    /// Operands with no exact rendering stay residual.
    #[test]
    fn untranslatable_operands_are_left_residual() {
        use BinaryOp::Lt;
        let cases = [
            (bin(Lt, CypherLiteral::String("a".into())), T::Int64),
            (bin(Lt, CypherLiteral::Integer(5)), T::String),
            (bin(Lt, CypherLiteral::Null), T::Int64),
            (bin(Lt, CypherLiteral::Bool(true)), T::Int64),
            (bin(Lt, CypherLiteral::Float(0.0)), T::Float64),
            (bin(Lt, CypherLiteral::Float(-0.0)), T::Float64),
            (bin(Lt, CypherLiteral::Float(f64::NAN)), T::Float64),
            (bin(Lt, CypherLiteral::Integer((1 << 53) + 1)), T::Float64),
            (bin(Lt, CypherLiteral::Integer(5)), T::Int32),
            (bin(Lt, CypherLiteral::Float(1.5)), T::Float32),
            (bin(Lt, CypherLiteral::Integer(5)), T::DateTime),
            (
                bin(BinaryOp::StartsWith, CypherLiteral::String(String::new())),
                T::String,
            ),
            (
                bin(BinaryOp::StartsWith, CypherLiteral::Integer(4)),
                T::String,
            ),
            (
                bin(BinaryOp::StartsWith, CypherLiteral::String("a".into())),
                T::Int64,
            ),
            (Expr::IsNull(Box::new(prop("p"))), T::CypherValue),
        ];
        for (e, ty) in cases {
            assert_eq!(render(&e, ty.clone()), None, "{e:?} on {ty:?}");
        }
        // An awkward property name is translated and quoted (`to_sql`
        // backtick-quotes every identifier). The one name no spelling reaches,
        // a backtick, is refused at render time, so the planner's
        // `sql_pushable` leaves it to the residual filter.
        let odd = Expr::IsNull(Box::new(prop("a b")));
        assert_eq!(render(&odd, T::Int64).as_deref(), Some("`a b` IS NULL"));
        let kw = Expr::IsNull(Box::new(prop("order")));
        assert_eq!(render(&kw, T::Int64).as_deref(), Some("`order` IS NULL"));
        let tick = indexed_range_filter(&Expr::IsNull(Box::new(prop("a`b"))), "n", Some(&T::Int64))
            .expect("translated");
        assert!(tick.to_sql().is_err());
        assert_eq!(tick.sql_pushable(), FilterExpr::Literal(true));
    }

    #[test]
    fn prefix_and_null_tests_render_as_ranges_and_null_checks() {
        let sw = |p: &str| bin(BinaryOp::StartsWith, CypherLiteral::String(p.into()));
        assert_eq!(
            render(&sw("u42"), T::String).as_deref(),
            Some("(`p` >= 'u42') AND (`p` < 'u43')")
        );
        assert_eq!(
            render(&sw("it's"), T::String).as_deref(),
            Some("(`p` >= 'it''s') AND (`p` < 'it''t')")
        );
        assert_eq!(
            render(&sw("\u{10FFFF}"), T::String).as_deref(),
            Some("`p` >= '\u{10FFFF}'")
        );
        // A datetime-shaped string on a STRING column is not normalized.
        assert_eq!(
            render(
                &bin(
                    BinaryOp::Lt,
                    CypherLiteral::String("2021-06-01T00:00Z".into())
                ),
                T::String
            )
            .as_deref(),
            Some("`p` < '2021-06-01T00:00Z'")
        );
        assert_eq!(
            render(&Expr::IsNull(Box::new(prop("p"))), T::Float64).as_deref(),
            Some("`p` IS NULL")
        );
        assert_eq!(
            render(&Expr::IsNotNull(Box::new(prop("p"))), T::String).as_deref(),
            Some("`p` IS NOT NULL")
        );
    }

    /// The analyzer records range, prefix and null-test columns against the
    /// right index types: ordered forms need a BTree, null tests take a Hash.
    #[test]
    fn analyzer_records_range_columns_by_index_type() {
        use uni_common::core::schema::{IndexDefinition, ScalarIndexConfig};
        let mut schema = Schema::default();
        schema.labels.insert(
            "L".into(),
            uni_common::core::schema::LabelMeta {
                id: 1,
                created_at: chrono::Utc::now(),
                state: uni_common::core::schema::SchemaElementState::Active,
                description: None,
            },
        );
        for (prop_name, kind) in [("b", ScalarIndexType::BTree), ("h", ScalarIndexType::Hash)] {
            schema
                .indexes
                .push(IndexDefinition::Scalar(ScalarIndexConfig {
                    name: format!("idx_{prop_name}"),
                    label: "L".into(),
                    properties: vec![prop_name.into()],
                    index_type: kind,
                    where_clause: None,
                    metadata: Default::default(),
                    key: None,
                }));
        }
        let analyzer = IndexAwareAnalyzer::new(&schema);
        let on = |e: Expr| analyzer.analyze(&e, "n", 1).indexed_range_columns;
        let with = |name: &str, op: BinaryOp| Expr::BinaryOp {
            left: Box::new(prop(name)),
            op,
            right: Box::new(Expr::Parameter("x".into())),
        };
        assert_eq!(
            on(with("b", BinaryOp::Lt)),
            vec![("b".into(), ScalarIndexType::BTree)]
        );
        assert_eq!(on(with("b", BinaryOp::StartsWith)).len(), 1);
        assert!(
            on(with("h", BinaryOp::Lt)).is_empty(),
            "a Hash index is not ordered"
        );
        assert_eq!(
            on(Expr::IsNull(Box::new(prop("h")))),
            vec![("h".into(), ScalarIndexType::Hash)]
        );
        assert!(on(with("b", BinaryOp::NotEq)).is_empty());
        assert!(on(with("z", BinaryOp::Lt)).is_empty(), "unindexed");
    }
}

#[cfg(test)]
mod keyed_tests {
    use super::*;
    use uni_common::TemporalValue;
    use uni_common::core::schema::KeyKind;

    fn call(name: &str, args: Vec<Expr>) -> Expr {
        Expr::FunctionCall {
            name: name.to_string(),
            args,
            distinct: false,
            window_spec: None,
        }
    }

    fn string(s: &str) -> Expr {
        Expr::Literal(CypherLiteral::String(s.to_string()))
    }

    fn noon() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(1_767_268_800, 0).unwrap()
    }

    fn nanos(v: &Value) -> Option<i64> {
        match v {
            Value::Temporal(TemporalValue::DateTime {
                nanos_since_epoch, ..
            }) => Some(*nanos_since_epoch),
            _ => None,
        }
    }

    #[test]
    fn folds_constructors_on_constant_arguments() {
        let params = HashMap::new();
        let lit = call("datetime", vec![string("2026-01-01T13:00:00+01:00")]);
        assert_eq!(
            fold_constant_operand(&lit, &params, noon())
                .as_ref()
                .and_then(nanos),
            Some(1_767_268_800_000_000_000)
        );
        // The statement clock, for every no-argument form but `.realtime()`.
        for name in ["datetime", "DATETIME.STATEMENT", "datetime.transaction"] {
            let folded = fold_constant_operand(&call(name, vec![]), &params, noon());
            assert_eq!(
                folded.as_ref().and_then(nanos),
                Some(1_767_268_800_000_000_000),
                "{name}"
            );
        }
        assert!(
            fold_constant_operand(&call("datetime.realtime", vec![]), &params, noon()).is_none()
        );
        // Row-dependent and unknown calls do not fold.
        let prop = Expr::Property(Box::new(Expr::Variable("n".into())), "s".into());
        assert!(fold_constant_operand(&call("datetime", vec![prop]), &params, noon()).is_none());
        assert!(
            fold_constant_operand(&call("toUpper", vec![string("a")]), &params, noon()).is_none()
        );
        // A failing evaluation does not fold.
        assert!(
            fold_constant_operand(&call("datetime", vec![string("nope")]), &params, noon())
                .is_none()
        );
    }

    #[test]
    fn constructors_are_row_independent_and_oriented() {
        let lit = call("datetime", vec![string("2026-01-01T12:00:00Z")]);
        assert!(is_row_independent(&lit));
        assert!(!is_row_independent(&call("datetime.realtime", vec![])));
        let prop = Expr::Property(Box::new(Expr::Variable("n".into())), "ts".into());
        let oriented = orient_property_comparisons(Expr::BinaryOp {
            left: Box::new(lit),
            op: BinaryOp::Lt,
            right: Box::new(prop.clone()),
        });
        assert!(
            matches!(oriented, Expr::BinaryOp { op: BinaryOp::Gt, ref left, .. } if **left == prop)
        );
    }

    fn keyed(form: KeyedForm) -> KeyedPredicate {
        KeyedPredicate {
            property: "ts".into(),
            index_name: "idx_ts".into(),
            key: IndexKey::new(KeyKind::DateTimeInstant, "ts"),
            form,
            conjunct: Expr::Literal(CypherLiteral::Null),
        }
    }

    #[test]
    fn bounds_render_on_the_key_column_and_refuse_other_types() {
        let params = HashMap::from([
            (
                "s".to_string(),
                Value::String("2026-01-01T12:00:00Z".into()),
            ),
            (
                "l".to_string(),
                Value::Temporal(TemporalValue::LocalDateTime {
                    nanos_since_epoch: 1,
                }),
            ),
        ]);
        let cmp = |operand: Expr| {
            keyed(KeyedForm::Compare {
                op: CmpOp::GtEq,
                operand,
            })
        };
        let bound = cmp(call("datetime", vec![string("2026-01-01T12:00:00Z")]))
            .lance_bound(&params, noon())
            .expect("a DATETIME operand bounds the key");
        // Every bound also admits a NULL key (a row flushed before its key).
        assert_eq!(
            bound,
            FilterExpr::Or(vec![
                FilterExpr::compare(
                    "__key_dt_ts",
                    CmpOp::GtEq,
                    Scalar::Int(1_767_268_800_000_000_000)
                ),
                FilterExpr::IsNull("__key_dt_ts".into()),
            ])
        );
        for operand in [
            string("2026-01-01T12:00:00Z"),
            Expr::Literal(CypherLiteral::Integer(5)),
            Expr::Literal(CypherLiteral::Null),
            Expr::Parameter("s".into()),
            Expr::Parameter("l".into()),
            Expr::Parameter("missing".into()),
            call("date", vec![string("2026-01-01")]),
        ] {
            assert!(
                cmp(operand.clone()).lance_bound(&params, noon()).is_none(),
                "{operand:?}"
            );
        }
        let list = |items: Vec<Expr>| {
            keyed(KeyedForm::In {
                list: Expr::List(items),
            })
        };
        let bound = list(vec![
            call("datetime", vec![string("2026-01-01T12:00:00Z")]),
            Expr::Literal(CypherLiteral::Null),
            string("x"),
        ])
        .lance_bound(&params, noon());
        assert_eq!(
            bound,
            Some(FilterExpr::Or(vec![
                FilterExpr::one_of("__key_dt_ts", [Scalar::Int(1_767_268_800_000_000_000)]),
                FilterExpr::IsNull("__key_dt_ts".into()),
            ]))
        );
        assert!(
            list(vec![string("x")])
                .lance_bound(&params, noon())
                .is_none()
        );
    }

    /// A schema with label `L` (id 1) and an Online keyed index on `p` of
    /// `kind`, its key columns registered.
    fn keyed_schema(kind: KeyKind, ty: uni_common::core::schema::DataType) -> Schema {
        use uni_common::core::schema::{
            IndexMetadata, LabelMeta, PropertyMeta, ScalarIndexConfig, SchemaElementState,
        };
        let mut schema = Schema::default();
        schema.labels.insert(
            "L".into(),
            LabelMeta {
                id: 1,
                created_at: chrono::Utc::now(),
                state: SchemaElementState::Active,
                description: None,
            },
        );
        let key = IndexKey::new(kind, "p");
        let meta = |ty| PropertyMeta {
            r#type: ty,
            nullable: true,
            added_in: 1,
            state: SchemaElementState::Active,
            generation_expression: None,
            description: None,
            default: None,
        };
        let props = schema.properties.entry("L".into()).or_default();
        props.insert("p".into(), meta(ty));
        for (c, t) in key.columns.iter().zip(key.kind.column_types()) {
            props.insert(c.clone(), meta(t));
        }
        schema
            .indexes
            .push(IndexDefinition::Scalar(ScalarIndexConfig {
                name: "idx_p".into(),
                label: "L".into(),
                properties: vec!["p".into()],
                index_type: ScalarIndexType::BTree,
                where_clause: None,
                metadata: IndexMetadata {
                    status: IndexStatus::Online,
                    ..Default::default()
                },
                key: Some(key),
            }));
        schema
    }

    fn property() -> Expr {
        Expr::Property(Box::new(Expr::Variable("n".into())), "p".into())
    }

    /// Every BTIC predicate is recognized with the property in either
    /// argument position (the point of `contains_point` only second), and
    /// the accessor comparisons in either order; the bound renders on both
    /// key columns and admits NULL keys.
    #[test]
    fn btic_predicates_are_recognized_in_both_positions() {
        use uni_common::core::schema::DataType;
        let schema = keyed_schema(KeyKind::BticBounds, DataType::Btic);
        let analyzer = IndexAwareAnalyzer::new(&schema);
        let keyed = |e: Expr| analyzer.analyze(&e, "n", 1).keyed_predicates;
        let c = || call("btic", vec![string("1985")]);
        let params = HashMap::new();
        for name in [
            "btic_overlaps",
            "BTIC_CONTAINS",
            "btic_during",
            "btic_before",
            "btic_after",
            "btic_meets",
            "btic_starts",
            "btic_finishes",
            "btic_equals",
            "btic_disjoint",
            "btic_adjacent",
        ] {
            for (args, first) in [
                (vec![property(), c()], true),
                (vec![c(), property()], false),
            ] {
                let found = keyed(call(name, args));
                assert_eq!(found.len(), 1, "{name} first={first}");
                assert!(
                    matches!(found[0].form, KeyedForm::Btic { property_first, .. }
                        if property_first == first)
                );
                let bound = found[0].lance_bound(&params, noon()).expect("bounds");
                let sql = bound.to_sql().unwrap();
                assert!(
                    sql.contains("`__key_lo_p` IS NULL") && sql.contains("`__key_hi_p` IS NULL"),
                    "{sql}"
                );
            }
        }
        // `contains_point`'s point is never the property.
        assert_eq!(
            keyed(call(
                "btic_contains_point",
                vec![property(), Expr::Literal(CypherLiteral::Integer(5))]
            ))
            .len(),
            1
        );
        assert!(
            keyed(call(
                "btic_contains_point",
                vec![Expr::Literal(CypherLiteral::Integer(5)), property()]
            ))
            .is_empty()
        );
        // Row-dependent and two-property calls are not keyed.
        assert!(keyed(call("btic_overlaps", vec![property(), property()])).is_empty());
        assert!(keyed(call("btic_span", vec![property(), c()])).is_empty());
        // Accessors, both orders; `c op btic_lo(p)` is mirrored.
        let at = || call("datetime", vec![string("1985-01-01T00:00:00Z")]);
        let lo = || call("btic_lo", vec![property()]);
        let found = keyed(Expr::BinaryOp {
            left: Box::new(at()),
            op: BinaryOp::Lt,
            right: Box::new(lo()),
        });
        assert!(matches!(
            found[0].form,
            KeyedForm::BticAccessor {
                side: BticSide::Lo,
                op: CmpOp::Gt,
                ..
            }
        ));
        assert_eq!(
            found[0].key_bound(&params, noon()),
            Some(KeyBound::int(0, CmpOp::Gt, 473_385_600_000))
        );
        // `=` bounds both columns; `<` on a BTIC is not served.
        let eq = keyed(Expr::BinaryOp {
            left: Box::new(property()),
            op: BinaryOp::Eq,
            right: Box::new(c()),
        });
        assert_eq!(
            eq[0].key_bound(&params, noon()),
            Some(KeyBound::and([
                KeyBound::int(0, CmpOp::Eq, 473_385_600_000),
                KeyBound::int(1, CmpOp::Eq, 504_921_600_000),
            ]))
        );
        let lt = keyed(Expr::BinaryOp {
            left: Box::new(property()),
            op: BinaryOp::Lt,
            right: Box::new(c()),
        });
        assert!(lt[0].lance_bound(&params, noon()).is_none());
        // `btic_contains(btic('/'), p)` bounds no key: not pushed.
        let all = keyed(call(
            "btic_contains",
            vec![call("btic", vec![string("/")]), property()],
        ));
        assert!(all[0].lance_bound(&params, noon()).is_none());
    }

    /// The BTIC forms never bound a key of another kind.
    #[test]
    fn btic_forms_do_not_bound_other_keys() {
        use uni_common::core::schema::DataType;
        let schema = keyed_schema(KeyKind::DateTimeInstant, DataType::DateTime);
        let analyzer = IndexAwareAnalyzer::new(&schema);
        let e = call(
            "btic_overlaps",
            vec![property(), call("btic", vec![string("1985")])],
        );
        assert!(analyzer.analyze(&e, "n", 1).keyed_predicates.is_empty());
    }

    fn compare(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        cmp(left, op, right)
    }

    fn float(f: f64) -> Expr {
        Expr::Literal(CypherLiteral::Float(f))
    }

    /// `point({x: …, y: …})`.
    fn xy(x: f64, y: f64) -> Expr {
        call(
            "point",
            vec![Expr::Map(vec![
                ("x".into(), float(x)),
                ("y".into(), float(y)),
            ])],
        )
    }

    fn cmp(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// A TIME bound is the constant's UTC nanoseconds with no modulo, for
    /// `=` and every range; `<>`, `localtime` and other types are refused.
    #[test]
    fn time_bounds_are_utc_nanoseconds() {
        use uni_common::core::schema::DataType;
        let schema = keyed_schema(KeyKind::TimeUtc, DataType::Time);
        let analyzer = IndexAwareAnalyzer::new(&schema);
        let params = HashMap::from([(
            "t".to_string(),
            Value::Temporal(TemporalValue::Time {
                nanos_since_midnight: 0,
                offset_seconds: 14 * 3600,
            }),
        )]);
        let bound = |e: Expr| {
            analyzer
                .analyze(&e, "n", 1)
                .keyed_predicates
                .first()
                .and_then(|k| k.lance_bound(&params, noon()))
        };
        let key = |op: CmpOp, v: i64| {
            Some(FilterExpr::Or(vec![
                FilterExpr::compare("__key_time_p", op, Scalar::Int(v)),
                FilterExpr::IsNull("__key_time_p".into()),
            ]))
        };
        // 23:00-05:00 is 28 h UTC, past midnight: no modulo.
        let late = call("time", vec![string("23:00-05:00")]);
        for (op, cmp) in [
            (BinaryOp::Eq, CmpOp::Eq),
            (BinaryOp::Lt, CmpOp::Lt),
            (BinaryOp::LtEq, CmpOp::LtEq),
            (BinaryOp::Gt, CmpOp::Gt),
            (BinaryOp::GtEq, CmpOp::GtEq),
        ] {
            assert_eq!(
                bound(compare(property(), op, late.clone())),
                key(cmp, 28 * 3_600_000_000_000),
                "{op:?}"
            );
        }
        // `+14:00` midnight is 14 h before midnight UTC.
        assert_eq!(
            bound(compare(
                property(),
                BinaryOp::Gt,
                Expr::Parameter("t".into())
            )),
            key(CmpOp::Gt, -14 * 3_600_000_000_000)
        );
        for e in [
            compare(property(), BinaryOp::NotEq, late.clone()),
            compare(
                property(),
                BinaryOp::Lt,
                call("localtime", vec![string("12:00")]),
            ),
            compare(property(), BinaryOp::Eq, string("12:00")),
            compare(
                property(),
                BinaryOp::Eq,
                call("datetime", vec![string("2026-01-01T12:00:00Z")]),
            ),
        ] {
            assert_eq!(bound(e.clone()), None, "{e:?}");
        }
    }

    /// `n.m.k op c` is a keyed predicate on the property `m.k`, bounded by
    /// `c` when it is of the map's value type.
    #[test]
    fn map_path_bounds_need_a_constant_of_the_value_type() {
        use uni_common::core::schema::DataType;
        let kind = |value: DataType| KeyKind::MapPath {
            path: vec!["k".into()],
            value,
        };
        // `n.p.k`.
        let path = || {
            Expr::Property(
                Box::new(Expr::Property(
                    Box::new(Expr::Variable("n".into())),
                    "p".into(),
                )),
                "k".into(),
            )
        };
        for (value, operand, want) in [
            (
                DataType::Int32,
                Expr::Literal(CypherLiteral::Integer(5)),
                Some(Scalar::Int(5)),
            ),
            (
                DataType::Int64,
                Expr::Literal(CypherLiteral::Float(5.5)),
                None,
            ),
            (DataType::Int64, string("5"), None),
            (
                DataType::Float32,
                Expr::Literal(CypherLiteral::Integer(2)),
                Some(Scalar::Float(2.0)),
            ),
            (
                DataType::Float64,
                Expr::Literal(CypherLiteral::Float(-0.0)),
                Some(Scalar::Float(0.0)),
            ),
            (
                DataType::Float64,
                Expr::Literal(CypherLiteral::Float(f64::NAN)),
                None,
            ),
            (DataType::String, string("x"), Some(Scalar::Str("x".into()))),
            (
                DataType::String,
                Expr::Literal(CypherLiteral::Integer(1)),
                None,
            ),
        ] {
            let ty = DataType::Map(Box::new(DataType::String), Box::new(value.clone()));
            let mut schema = keyed_schema(kind(value.clone()), ty);
            // The index's property is the dotted path.
            if let Some(IndexDefinition::Scalar(cfg)) = schema.indexes.first_mut() {
                cfg.properties = vec!["p.k".into()];
            }
            let column = IndexKey::new(kind(value.clone()), "p").columns[0].clone();
            let analyzer = IndexAwareAnalyzer::new(&schema);
            let keyed = analyzer
                .analyze(&compare(path(), BinaryOp::LtEq, operand.clone()), "n", 1)
                .keyed_predicates;
            assert_eq!(keyed.len(), 1, "{value:?} {operand:?}");
            assert_eq!(keyed[0].property, "p.k");
            assert_eq!(
                keyed[0].lance_bound(&HashMap::new(), noon()),
                want.map(|s| FilterExpr::Or(vec![
                    FilterExpr::compare(column.clone(), CmpOp::LtEq, s),
                    FilterExpr::IsNull(column.clone()),
                ])),
                "{value:?} {operand:?}"
            );
            // The map itself is not a key path.
            let whole = Expr::Property(Box::new(Expr::Variable("n".into())), "p".into());
            assert!(
                analyzer
                    .analyze(&compare(whole, BinaryOp::Eq, operand), "n", 1)
                    .keyed_predicates
                    .is_empty()
            );
        }
    }

    /// `withinBBox` and `distance` are recognized with constant `point()`
    /// literals or parameters, `distance` in both argument orders and both
    /// comparison orientations; `>`, `=`, other-CRS and row-dependent
    /// operands are not bounded.
    #[test]
    fn point_predicates_are_recognized() {
        use uni_common::core::schema::{DataType, PointType};
        let schema = keyed_schema(
            KeyKind::PointCoords {
                geographic: false,
                three_d: false,
            },
            DataType::Point(PointType::Cartesian2D),
        );
        let analyzer = IndexAwareAnalyzer::new(&schema);
        let bound = |e: Expr, params: &HashMap<String, Value>| {
            let found = analyzer.analyze(&e, "n", 1).keyed_predicates;
            assert!(found.len() <= 1);
            found.first().and_then(|k| k.key_bound(params, noon()))
        };
        let none = HashMap::new();
        let bbox = call(
            "point.withinBBox",
            vec![property(), xy(0.0, -0.0), xy(2.0, 3.0)],
        );
        let want = KeyBound::and([
            KeyBound::cmp(0, CmpOp::GtEq, KeyValue::Float(0.0)),
            KeyBound::cmp(0, CmpOp::LtEq, KeyValue::Float(2.0)),
            KeyBound::cmp(1, CmpOp::GtEq, KeyValue::Float(0.0)),
            KeyBound::cmp(1, CmpOp::LtEq, KeyValue::Float(3.0)),
        ]);
        assert_eq!(bound(bbox, &none), Some(want.clone()));
        // Corners as parameters: a map, as `point()` returns it.
        let mut params = HashMap::new();
        for (name, x, y) in [("ll", 0.0, 0.0), ("ur", 2.0, 3.0)] {
            params.insert(
                name.to_string(),
                crate::spatial::eval_spatial_function(
                    "POINT",
                    &[Value::Map(HashMap::from([
                        ("x".to_string(), Value::Float(x)),
                        ("y".to_string(), Value::Float(y)),
                    ]))],
                )
                .unwrap(),
            );
        }
        let by_param = call(
            "point.withinBBox",
            vec![
                property(),
                Expr::Parameter("ll".into()),
                Expr::Parameter("ur".into()),
            ],
        );
        assert_eq!(bound(by_param, &params), Some(want));

        let d = |a: Expr, b: Expr| call("distance", vec![a, b]);
        for e in [
            cmp(d(property(), xy(1.0, 1.0)), BinaryOp::Lt, float(2.0)),
            cmp(d(xy(1.0, 1.0), property()), BinaryOp::LtEq, float(2.0)),
            cmp(float(2.0), BinaryOp::Gt, d(property(), xy(1.0, 1.0))),
            cmp(
                Expr::Literal(CypherLiteral::Integer(2)),
                BinaryOp::GtEq,
                d(xy(1.0, 1.0), property()),
            ),
        ] {
            let b = bound(e.clone(), &none).unwrap_or_else(|| panic!("{e:?}"));
            let admits =
                |x: f64, y: f64| b.admits(&[Some(KeyValue::Float(x)), Some(KeyValue::Float(y))]);
            assert!(
                admits(-1.0, 3.0) && admits(3.0, -1.0) && !admits(3.1, 1.0),
                "{b:?}"
            );
        }
        let center_param = cmp(
            d(property(), Expr::Parameter("c".into())),
            BinaryOp::Lt,
            Expr::Parameter("r".into()),
        );
        let mut params = HashMap::new();
        params.insert("c".to_string(), params_point(1.0, 1.0));
        params.insert("r".to_string(), Value::Int(2));
        assert!(bound(center_param.clone(), &params).is_some());
        params.insert("r".to_string(), Value::String("2".into()));
        assert_eq!(bound(center_param, &params), None);

        // Not bounded: the complement of a disk, `=`, a point of another
        // CRS, a row-dependent center, and point equality.
        let geo = call(
            "point",
            vec![Expr::Map(vec![
                ("latitude".into(), float(1.0)),
                ("longitude".into(), float(1.0)),
            ])],
        );
        for e in [
            cmp(d(property(), xy(1.0, 1.0)), BinaryOp::Gt, float(2.0)),
            cmp(d(property(), xy(1.0, 1.0)), BinaryOp::Eq, float(2.0)),
            cmp(d(property(), geo.clone()), BinaryOp::Lt, float(2.0)),
            call("point.withinBBox", vec![property(), geo.clone(), geo]),
            cmp(d(property(), property()), BinaryOp::Lt, float(2.0)),
            cmp(property(), BinaryOp::Eq, xy(1.0, 1.0)),
            cmp(property(), BinaryOp::Lt, xy(1.0, 1.0)),
        ] {
            assert_eq!(bound(e.clone(), &none), None, "{e:?}");
        }
        // The spatial forms never bound a key of another kind.
        let dt = keyed_schema(KeyKind::DateTimeInstant, DataType::DateTime);
        let e = cmp(d(property(), xy(1.0, 1.0)), BinaryOp::Lt, float(2.0));
        assert!(
            IndexAwareAnalyzer::new(&dt)
                .analyze(&e, "n", 1)
                .keyed_predicates
                .is_empty()
        );
    }

    fn params_point(x: f64, y: f64) -> Value {
        crate::spatial::eval_spatial_function(
            "POINT",
            &[Value::Map(HashMap::from([
                ("x".to_string(), Value::Float(x)),
                ("y".to_string(), Value::Float(y)),
            ]))],
        )
        .unwrap()
    }

    #[test]
    fn point_constructor_folds_on_constant_maps_only() {
        let none = HashMap::new();
        let folded = fold_constant_operand(&xy(1.0, 2.0), &none, noon()).unwrap();
        assert_eq!(folded, params_point(1.0, 2.0));
        assert!(is_row_independent(&xy(1.0, 2.0)));
        // A row-dependent map, a non-map argument, a raising call.
        let row = call(
            "point",
            vec![Expr::Map(vec![
                ("x".into(), property()),
                ("y".into(), float(1.0)),
            ])],
        );
        assert!(!is_row_independent(&row));
        assert_eq!(fold_constant_operand(&row, &none, noon()), None);
        let bad = call("point", vec![Expr::Map(vec![("q".into(), float(1.0))])]);
        assert_eq!(fold_constant_operand(&bad, &none, noon()), None);
        assert!(!is_row_independent(&call("point", vec![float(1.0)])));
        // Oriented like a temporal constant: `point(…) = n.p` → `n.p = point(…)`.
        let e = orient_property_comparisons(cmp(xy(1.0, 2.0), BinaryOp::Eq, property()));
        assert!(
            matches!(e, Expr::BinaryOp { ref left, .. } if matches!(left.as_ref(), Expr::Property(..)))
        );
    }
}
