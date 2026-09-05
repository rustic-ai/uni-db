// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Predicate pushdown and index-aware query routing.
//!
//! Routes WHERE predicates to the most selective execution path:
//! UID index lookup → BTree prefix scan → JSON FTS → Lance columnar filter → residual.
//! Includes SQL injection prevention for LIKE patterns (CWE-89) and UID validation (CWE-345).

use std::collections::{HashMap, HashSet};
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, UnaryOp};
use uni_store::backend::types::{CmpOp, FilterExpr, Scalar, StringMatchKind};

use uni_common::Value;
use uni_common::core::id::UniId;
use uni_common::core::schema::{
    IndexDefinition, IndexStatus, PropertyMeta, ScalarIndexType, Schema,
};

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

    /// JSON FTS predicates for full-text search on JSON columns.
    /// Vec of: (column_name, search_term, optional_path_filter)
    pub json_fts_predicates: Vec<(String, String, Option<String>)>,

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
    /// Equality and `IN` only, for both types. A BTree can also serve ranges,
    /// but pushing a range inside the scan is a different question from pushing
    /// a point lookup — see the boundary note in
    /// `build_indexed_property_pushdown` — and is left alone here.
    ///
    /// See issues #57, #247.
    pub indexed_equality_columns: Vec<(String, ScalarIndexType)>,

    /// Residual predicates (not pushable to storage).
    pub residual: Vec<Expr>,
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

    /// Extract JSON FTS predicate from CONTAINS on an FTS-indexed column.
    ///
    /// Returns `Some((column, search_term, optional_path))` if:
    /// - The predicate is `variable.column CONTAINS 'term'`
    /// - The column has a `JsonFullText` index
    fn extract_json_fts_predicate(
        &self,
        expr: &Expr,
        variable: &str,
        label_id: u16,
    ) -> Option<(String, String, Option<String>)> {
        if let Expr::BinaryOp {
            left,
            op: BinaryOp::Contains,
            right,
        } = expr
            && let Expr::Property(var_expr, prop) = left.as_ref()
            && let Expr::Variable(v) = var_expr.as_ref()
            && v == variable
            && let Expr::Literal(CypherLiteral::String(term)) = right.as_ref()
        {
            let label_name = self.schema.label_name_by_id(label_id)?;

            // Check if property has a JsonFullText index
            for idx in &self.schema.indexes {
                if let IndexDefinition::JsonFullText(cfg) = idx
                    && cfg.label == *label_name
                    && cfg.column == *prop
                    && cfg.metadata.status == IndexStatus::Online
                {
                    return Some((prop.clone(), term.clone(), None));
                }
            }
        }
        None
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
                // Check right side is list or parameter
                let right_valid = matches!(right.as_ref(), Expr::List(_) | Expr::Parameter(_));
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

                // Check right side is a literal or parameter or list of literals
                // For string operators, strict requirement on String Literal
                let right_valid = if matches!(
                    op,
                    BinaryOp::Contains | BinaryOp::StartsWith | BinaryOp::EndsWith
                ) {
                    matches!(right.as_ref(), Expr::Literal(CypherLiteral::String(_)))
                } else {
                    matches!(
                        right.as_ref(),
                        Expr::Literal(_) | Expr::Parameter(_) | Expr::List(_)
                    )
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

/// Increment the last character of a string to create an exclusive upper bound.
///
/// For ASCII strings, this increments the last character.
/// For example: "John" -> "Joho"
///
/// Returns `None` if the last character is at its maximum value (cannot be incremented).
fn increment_last_char(s: &str) -> Option<String> {
    if s.is_empty() {
        return None;
    }

    let mut chars: Vec<char> = s.chars().collect();
    let last_idx = chars.len() - 1;
    let last_char = chars[last_idx];

    // Increment the last character
    // For most ASCII/UTF-8 characters, this works correctly
    if let Some(next_char) = char::from_u32(last_char as u32 + 1) {
        chars[last_idx] = next_char;
        Some(chars.into_iter().collect())
    } else {
        // Last character is at maximum, cannot increment
        None
    }
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
                Some(FilterExpr::one_of(column, Self::list_to_scalars(right)?))
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
                    None => Some(FilterExpr::compare(
                        column,
                        Self::op_to_cmp(op)?,
                        Self::value_to_scalar(right)?,
                    )),
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
                    // System columns (starting with _) are always physical Lance columns
                    if prop.starts_with('_') {
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

    fn value_to_scalar(expr: &Expr) -> Option<Scalar> {
        match expr {
            Expr::Literal(CypherLiteral::String(s)) => {
                // Normalize datetime strings to include seconds for Arrow timestamp parsing.
                // Our Cypher datetime formatting omits `:00` seconds (e.g. `2021-06-01T00:00Z`)
                // but Arrow/Lance requires full `HH:MM:SS` for timestamp parsing.
                Some(Scalar::Str(
                    super::df_expr::normalize_datetime_str(s).unwrap_or_else(|| s.clone()),
                ))
            }
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
    fn list_to_scalars(expr: &Expr) -> Option<Vec<Scalar>> {
        match expr {
            Expr::List(items) => items.iter().map(Self::value_to_scalar).collect(),
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
            assert_eq!(like_sql("admin").unwrap(), "name LIKE '%admin%'");
            assert_eq!(like_sql("John Smith").unwrap(), "name LIKE '%John Smith%'");
            assert_eq!(
                like_sql("test@example.com").unwrap(),
                "name LIKE '%test@example.com%'"
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
