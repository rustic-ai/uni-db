// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::{HashMap, HashSet};
use uni_cypher::ast::{BinaryOp, CypherLiteral, Expr, UnaryOp};

use uni_common::core::id::UniId;
use uni_common::core::schema::{IndexDefinition, PropertyMeta, Schema};

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
pub struct IndexAwareAnalyzer<'a> {
    schema: &'a Schema,
}

impl<'a> IndexAwareAnalyzer<'a> {
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
            let analyzer = PredicateAnalyzer::new();
            if analyzer.is_pushable(&conj, variable) {
                strategy.lance_predicates.push(conj);
            } else {
                strategy.residual.push(conj);
            }
        }

        strategy
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
                if let uni_common::core::schema::IndexDefinition::Scalar(cfg) = idx
                    && cfg.label == *label_name
                    && cfg.properties.contains(prop)
                    && cfg.index_type == uni_common::core::schema::ScalarIndexType::BTree
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

pub struct PredicateAnalysis {
    /// Predicates that can be pushed to storage
    pub pushable: Vec<Expr>,
    /// Predicates that must be evaluated post-scan
    pub residual: Vec<Expr>,
    /// Properties needed for residual evaluation
    pub required_properties: Vec<String>,
}

#[derive(Default)]
pub struct PredicateAnalyzer;

impl PredicateAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Analyze a predicate and determine pushdown strategy
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

    /// Check if a predicate can be pushed to Lance
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

pub struct LanceFilterGenerator;

impl LanceFilterGenerator {
    /// Checks if a string contains SQL LIKE wildcard characters.
    ///
    /// # Security
    ///
    /// **CWE-89 (SQL Injection)**: Predicates containing wildcards are NOT pushed
    /// to storage because Lance DataFusion doesn't support the ESCAPE clause.
    /// Instead, they're evaluated at the application layer where we have full
    /// control over string matching semantics.
    fn contains_sql_wildcards(s: &str) -> bool {
        s.contains('%') || s.contains('_')
    }

    /// Escapes special characters in LIKE patterns.
    ///
    /// **Note**: This function is kept for documentation and potential future use,
    /// but currently we do not push down LIKE patterns containing wildcards
    /// because Lance DataFusion doesn't support the ESCAPE clause.
    #[expect(
        dead_code,
        reason = "Reserved for future use when Lance supports ESCAPE"
    )]
    fn escape_like_pattern(s: &str) -> String {
        s.replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_")
            .replace('\'', "''")
    }

    /// Converts pushable predicates to Lance SQL filter string.
    ///
    /// When `schema_props` is provided, properties not in the schema (overflow properties)
    /// are skipped since they don't exist as physical columns in Lance.
    pub fn generate(
        predicates: &[Expr],
        variable: &str,
        schema_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> Option<String> {
        if predicates.is_empty() {
            return None;
        }

        // Flatten nested ANDs first
        let flattened: Vec<&Expr> = predicates.iter().flat_map(|p| flatten_ands(p)).collect();

        // Optimize Ranges: Group predicates by column and combine into >= AND <= if possible
        let mut by_column: std::collections::HashMap<String, Vec<&Expr>> =
            std::collections::HashMap::new();
        let mut optimized_filters: Vec<String> = Vec::new();
        let mut used_expressions: std::collections::HashSet<*const Expr> =
            std::collections::HashSet::new();

        for expr in flattened.iter() {
            if let Some(col) = Self::extract_column_from_range(expr, variable, schema_props) {
                by_column.entry(col).or_default().push(expr);
            }
        }

        for (col, exprs) in &by_column {
            if exprs.len() < 2 {
                continue;
            }

            // Try to find pairs of >/>= and </<=
            // Very naive: find ONE pair and emit range expression.
            // Complex ranges (e.g. >10 AND >20) are not merged but valid.
            // We look for: (col > L OR col >= L) AND (col < R OR col <= R)

            let mut lower: Option<(bool, &Expr, &Expr)> = None; // (inclusive, val_expr, original_expr)
            let mut upper: Option<(bool, &Expr, &Expr)> = None;

            for expr in exprs {
                if let Expr::BinaryOp { op, right, .. } = expr {
                    match op {
                        BinaryOp::Gt => {
                            // If we have multiple lower bounds, pick the last one (arbitrary for now, intersection handles logic)
                            lower = Some((false, right, expr));
                        }
                        BinaryOp::GtEq => {
                            lower = Some((true, right, expr));
                        }
                        BinaryOp::Lt => {
                            upper = Some((false, right, expr));
                        }
                        BinaryOp::LtEq => {
                            upper = Some((true, right, expr));
                        }
                        _ => {}
                    }
                }
            }

            if let (Some((true, l_val, l_expr)), Some((true, u_val, u_expr))) = (lower, upper) {
                // Both inclusive -> use >= AND <= (Lance doesn't support BETWEEN)
                if let (Some(l_str), Some(u_str)) =
                    (Self::value_to_lance(l_val), Self::value_to_lance(u_val))
                {
                    optimized_filters.push(format!(
                        "\"{}\" >= {} AND \"{}\" <= {}",
                        col, l_str, col, u_str
                    ));
                    used_expressions.insert(l_expr as *const Expr);
                    used_expressions.insert(u_expr as *const Expr);
                }
            }
        }

        let mut filters = optimized_filters;

        for expr in flattened {
            if used_expressions.contains(&(expr as *const Expr)) {
                continue;
            }
            if let Some(s) = Self::expr_to_lance(expr, variable, schema_props) {
                filters.push(s);
            }
        }

        if filters.is_empty() {
            None
        } else {
            Some(filters.join(" AND "))
        }
    }

    fn extract_column_from_range(
        expr: &Expr,
        variable: &str,
        schema_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> Option<String> {
        match expr {
            Expr::BinaryOp { left, op, .. } => {
                if matches!(
                    op,
                    BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq
                ) {
                    return Self::extract_column(left, variable, schema_props);
                }
                None
            }
            _ => None,
        }
    }

    fn expr_to_lance(
        expr: &Expr,
        variable: &str,
        schema_props: Option<&HashMap<String, PropertyMeta>>,
    ) -> Option<String> {
        match expr {
            Expr::In {
                expr: left,
                list: right,
            } => {
                let column = Self::extract_column(left, variable, schema_props)?;
                let value = Self::value_to_lance(right)?;
                Some(format!("{} IN {}", column, value))
            }
            Expr::BinaryOp { left, op, right } => {
                let column = Self::extract_column(left, variable, schema_props)?;

                // Special handling for string operators
                // Security: CWE-89 - Prevent SQL wildcard injection
                //
                // Lance DataFusion doesn't support the ESCAPE clause, so we cannot
                // safely push down LIKE predicates containing SQL wildcards (% or _).
                // If the input contains these characters, we return None to keep
                // the predicate as a residual for application-level evaluation.
                match op {
                    BinaryOp::Contains | BinaryOp::StartsWith | BinaryOp::EndsWith => {
                        let raw_value = Self::get_string_value(right)?;

                        // If the value contains SQL wildcards, don't push down
                        // to prevent wildcard injection attacks
                        if Self::contains_sql_wildcards(&raw_value) {
                            return None;
                        }

                        // Escape single quotes for the SQL string
                        let escaped = raw_value.replace('\'', "''");

                        match op {
                            BinaryOp::Contains => Some(format!("{} LIKE '%{}%'", column, escaped)),
                            BinaryOp::StartsWith => Some(format!("{} LIKE '{}%'", column, escaped)),
                            BinaryOp::EndsWith => Some(format!("{} LIKE '%{}'", column, escaped)),
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        let op_str = Self::op_to_lance(op)?;
                        let value = Self::value_to_lance(right)?;
                        // Use unquoted column name for DataFusion compatibility
                        // DataFusion treats unquoted identifiers case-insensitively
                        Some(format!("{} {} {}", column, op_str, value))
                    }
                }
            }
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => {
                let inner = Self::expr_to_lance(expr, variable, schema_props)?;
                Some(format!("NOT ({})", inner))
            }
            Expr::IsNull(inner) => {
                let column = Self::extract_column(inner, variable, schema_props)?;
                Some(format!("{} IS NULL", column))
            }
            Expr::IsNotNull(inner) => {
                let column = Self::extract_column(inner, variable, schema_props)?;
                Some(format!("{} IS NOT NULL", column))
            }
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

    fn op_to_lance(op: &BinaryOp) -> Option<&'static str> {
        match op {
            BinaryOp::Eq => Some("="),
            BinaryOp::NotEq => Some("!="),
            BinaryOp::Lt => Some("<"),
            BinaryOp::LtEq => Some("<="),
            BinaryOp::Gt => Some(">"),
            BinaryOp::GtEq => Some(">="),
            _ => None,
        }
    }

    fn value_to_lance(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal(CypherLiteral::String(s)) => {
                // Normalize datetime strings to include seconds for Arrow timestamp parsing.
                // Our Cypher datetime formatting omits `:00` seconds (e.g. `2021-06-01T00:00Z`)
                // but Arrow/Lance requires full `HH:MM:SS` for timestamp parsing.
                let s = super::df_expr::normalize_datetime_str(s).unwrap_or_else(|| s.clone());
                Some(format!("'{}'", s.replace("'", "''")))
            }
            Expr::Literal(CypherLiteral::Integer(i)) => Some(i.to_string()),
            Expr::Literal(CypherLiteral::Float(f)) => Some(f.to_string()),
            Expr::Literal(CypherLiteral::Bool(b)) => Some(b.to_string()),
            Expr::Literal(CypherLiteral::Null) => Some("NULL".to_string()),
            Expr::List(items) => {
                let values: Option<Vec<String>> = items.iter().map(Self::value_to_lance).collect();
                values.map(|v| format!("({})", v.join(", ")))
            }
            // Security: CWE-89 - Parameters are NOT pushed to storage layer.
            // Parameterized predicates stay in the application layer where the
            // query executor can safely substitute values with proper type handling.
            // This prevents potential SQL injection if Lance doesn't support the $name syntax.
            Expr::Parameter(_) => None,
            _ => None,
        }
    }

    /// Extracts raw string value from expression for LIKE pattern use.
    ///
    /// Returns the raw string without escaping - escaping is handled by
    /// `escape_like_pattern` for LIKE clauses.
    fn get_string_value(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal(CypherLiteral::String(s)) => Some(s.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod security_tests {
    use super::*;

    /// Tests for CWE-89 (SQL Injection) prevention in LIKE patterns.
    mod wildcard_protection {
        use super::*;

        #[test]
        fn test_contains_sql_wildcards_detects_percent() {
            assert!(LanceFilterGenerator::contains_sql_wildcards("admin%"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("%admin"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("ad%min"));
        }

        #[test]
        fn test_contains_sql_wildcards_detects_underscore() {
            assert!(LanceFilterGenerator::contains_sql_wildcards("a_min"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("_admin"));
            assert!(LanceFilterGenerator::contains_sql_wildcards("admin_"));
        }

        #[test]
        fn test_contains_sql_wildcards_safe_strings() {
            assert!(!LanceFilterGenerator::contains_sql_wildcards("admin"));
            assert!(!LanceFilterGenerator::contains_sql_wildcards("John Smith"));
            assert!(!LanceFilterGenerator::contains_sql_wildcards(
                "test@example.com"
            ));
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
