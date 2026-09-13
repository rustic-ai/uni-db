use pest::Parser;
use pest::iterators::Pair;

use super::locy_parser::Rule as LocyRule;
use super::walker;
use super::{CypherParser, ParseError, Rule as CypherRule};
use crate::ast;
use crate::locy_ast::*;

/// Build a LocyProgram from the top-level parse result.
pub fn build_program(pair: Pair<LocyRule>) -> Result<LocyProgram, ParseError> {
    debug_assert_eq!(pair.as_rule(), LocyRule::locy_query);

    let mut module = None;
    let mut uses = Vec::new();
    let mut statements = Vec::new();

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::module_declaration => {
                module = Some(build_module_declaration(child)?);
            }
            LocyRule::use_declaration => {
                uses.push(build_use_declaration(child)?);
            }
            LocyRule::locy_union_query => {
                statements = build_locy_union_query(child)?;
            }
            LocyRule::EOI => {}
            other => {
                return Err(ParseError::new(format!(
                    "Unexpected rule in locy_query: {other:?}"
                )));
            }
        }
    }

    Ok(LocyProgram {
        module,
        uses,
        statements,
    })
}

fn build_locy_union_query(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    let text = pair.as_str();
    let mut inner = pair.into_inner();

    let first = inner.next().unwrap();
    let has_union = inner.peek().is_some();

    if has_union {
        // UNION query — re-parse the entire text as Cypher
        let cypher_query = reparse_as_cypher_query(text)?;
        return Ok(vec![LocyStatement::Cypher(cypher_query)]);
    }

    // Single query — process through locy_single_query
    build_locy_single_query(first)
}

fn build_locy_single_query(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::explain_query | LocyRule::schema_command => {
            // Cypher passthrough — re-parse the text
            let cypher_query = reparse_as_cypher_query(inner.as_str())?;
            Ok(vec![LocyStatement::Cypher(cypher_query)])
        }
        LocyRule::locy_statement_block => build_locy_statement_block(inner),
        other => Err(ParseError::new(format!(
            "Unexpected rule in locy_single_query: {other:?}"
        ))),
    }
}

fn build_locy_statement_block(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    let mut statements = Vec::new();
    let mut cypher_clause_texts: Vec<String> = Vec::new();

    for clause_pair in pair.into_inner() {
        debug_assert_eq!(clause_pair.as_rule(), LocyRule::locy_clause);
        let inner = clause_pair.into_inner().next().unwrap();

        // Statement-producing clauses common to both top-level blocks and
        // ASSUME bodies are dispatched here; anything it does not recognize is
        // returned for the block-specific handling below.
        let Some(inner) =
            dispatch_common_locy_clause(inner, &mut statements, &mut cypher_clause_texts)?
        else {
            continue;
        };

        match inner.as_rule() {
            LocyRule::model_definition => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::Model(build_model_definition(inner)?));
            }
            LocyRule::calibrate_command => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::Calibrate(build_calibrate_command(inner)?));
            }
            LocyRule::validate_command => {
                flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
                statements.push(LocyStatement::Validate(build_validate_command(inner)?));
            }
            other => {
                return Err(ParseError::new(format!(
                    "Unexpected rule in locy_clause: {other:?}"
                )));
            }
        }
    }

    // Flush any remaining Cypher clauses
    flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;

    Ok(statements)
}

/// Dispatch a single `locy_clause` inner pair for the kinds shared by both
/// [`build_locy_statement_block`] and [`build_assume_body`]: rule definitions,
/// GOAL / DERIVE / ABDUCE / EXPLAIN queries, nested ASSUME blocks, and standard
/// Cypher `clause` text accumulation.
///
/// Returns `Ok(None)` if the pair was handled, or `Ok(Some(inner))` if its rule
/// is not one of the shared kinds — letting each caller apply its own routing to
/// the remainder (the top-level block recognizes MODEL/CALIBRATE/VALIDATE and
/// errors on anything else, while the ASSUME body treats the remainder as Cypher).
fn dispatch_common_locy_clause<'a>(
    inner: Pair<'a, LocyRule>,
    statements: &mut Vec<LocyStatement>,
    cypher_clause_texts: &mut Vec<String>,
) -> Result<Option<Pair<'a, LocyRule>>, ParseError> {
    match inner.as_rule() {
        LocyRule::rule_definition => {
            // Flush accumulated Cypher clauses first
            flush_cypher_clauses(cypher_clause_texts, statements)?;
            statements.push(LocyStatement::Rule(build_rule_definition(inner)?));
        }
        LocyRule::goal_query => {
            flush_cypher_clauses(cypher_clause_texts, statements)?;
            statements.push(LocyStatement::GoalQuery(build_goal_query(inner)?));
        }
        LocyRule::derive_command => {
            flush_cypher_clauses(cypher_clause_texts, statements)?;
            statements.push(LocyStatement::DeriveCommand(build_derive_command(inner)?));
        }
        LocyRule::assume_block => {
            flush_cypher_clauses(cypher_clause_texts, statements)?;
            statements.push(LocyStatement::AssumeBlock(build_assume_block(inner)?));
        }
        LocyRule::abduce_query => {
            flush_cypher_clauses(cypher_clause_texts, statements)?;
            statements.push(LocyStatement::AbduceQuery(build_abduce_query(inner)?));
        }
        LocyRule::explain_rule_query => {
            flush_cypher_clauses(cypher_clause_texts, statements)?;
            statements.push(LocyStatement::ExplainRule(build_explain_rule_query(inner)?));
        }
        LocyRule::clause => {
            // Standard Cypher clause — accumulate its text
            cypher_clause_texts.push(inner.as_str().to_string());
        }
        _ => return Ok(Some(inner)),
    }
    Ok(None)
}

/// Flush accumulated Cypher clause texts into a single Cypher statement.
fn flush_cypher_clauses(
    clause_texts: &mut Vec<String>,
    statements: &mut Vec<LocyStatement>,
) -> Result<(), ParseError> {
    if clause_texts.is_empty() {
        return Ok(());
    }

    // Join clause texts and re-parse as a complete Cypher query
    let combined = clause_texts.join(" ");
    clause_texts.clear();

    let cypher_query = reparse_as_cypher_query(&combined)?;
    statements.push(LocyStatement::Cypher(cypher_query));
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
// CYPHER RE-PARSE BRIDGE
// ═══════════════════════════════════════════════════════════════════════════

/// Re-parse a text span under `rule`, then build the first resulting pair with
/// `build`. On a pest failure the error is wrapped as `Cypher{label} re-parse
/// error: {e}` (with `label` carrying its own leading space, e.g. `" expression"`).
fn reparse<T>(
    text: &str,
    rule: CypherRule,
    label: &str,
    build: fn(Pair<CypherRule>) -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    let pairs = CypherParser::parse(rule, text)
        .map_err(|e| ParseError::new(format!("Cypher{label} re-parse error: {e}")))?;
    build(pairs.into_iter().next().unwrap())
}

/// Re-parse a text span as a complete Cypher query.
fn reparse_as_cypher_query(text: &str) -> Result<ast::Query, ParseError> {
    let pairs = CypherParser::parse(CypherRule::query, text)
        .map_err(|e| ParseError::new(format!("Cypher re-parse error: {e}")))?;
    walker::build_query(pairs)
}

/// Re-parse a text span as a Cypher expression.
fn reparse_as_cypher_expression(text: &str) -> Result<ast::Expr, ParseError> {
    reparse(
        text,
        CypherRule::expression,
        " expression",
        walker::build_expression,
    )
}

/// Re-parse a text span as a Cypher pattern.
fn reparse_as_cypher_pattern(text: &str) -> Result<ast::Pattern, ParseError> {
    reparse(text, CypherRule::pattern, " pattern", walker::build_pattern)
}

/// Re-parse a text span as a Cypher clause.
fn reparse_as_cypher_clause(text: &str) -> Result<ast::Clause, ParseError> {
    reparse(text, CypherRule::clause, " clause", walker::build_clause)
}

/// Re-parse a text span as Cypher return_items.
fn reparse_as_cypher_return_items(text: &str) -> Result<Vec<ast::ReturnItem>, ParseError> {
    reparse(
        text,
        CypherRule::return_items,
        " return_items",
        walker::build_return_items,
    )
}

/// Re-parse a text span as Cypher sort_items.
fn reparse_as_cypher_sort_items(text: &str) -> Result<Vec<ast::SortItem>, ParseError> {
    reparse(
        text,
        CypherRule::sort_items,
        " sort_items",
        walker::build_sort_items,
    )
}

/// Re-parse a text span as Cypher properties (map literal or parameter).
fn reparse_as_cypher_properties(text: &str) -> Result<ast::Expr, ParseError> {
    reparse(
        text,
        CypherRule::properties,
        " properties",
        walker::build_properties,
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════════════════════

use walker::normalize_identifier as normalize_locy_identifier;

fn build_qualified_name(pair: Pair<LocyRule>) -> Result<QualifiedName, ParseError> {
    let parts = pair
        .into_inner()
        .map(|p| normalize_locy_identifier(p.as_str()))
        .collect();
    Ok(QualifiedName { parts })
}

// ═══════════════════════════════════════════════════════════════════════════
// MODULE / USE
// ═══════════════════════════════════════════════════════════════════════════

fn build_module_declaration(pair: Pair<LocyRule>) -> Result<ModuleDecl, ParseError> {
    let name_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == LocyRule::locy_qualified_name)
        .unwrap();
    Ok(ModuleDecl {
        name: build_qualified_name(name_pair)?,
    })
}

fn build_use_declaration(pair: Pair<LocyRule>) -> Result<UseDecl, ParseError> {
    let mut name = None;
    let mut imports = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::locy_qualified_name => {
                name = Some(build_qualified_name(child)?);
            }
            LocyRule::use_import_list => {
                let selected: Vec<String> = child
                    .into_inner()
                    .filter(|p| p.as_rule() == LocyRule::locy_identifier)
                    .map(|p| normalize_locy_identifier(p.as_str()))
                    .collect();
                imports = Some(selected);
            }
            _ => {}
        }
    }

    Ok(UseDecl {
        name: name.unwrap(),
        imports,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// RULE DEFINITION
// ═══════════════════════════════════════════════════════════════════════════

fn build_rule_definition(pair: Pair<LocyRule>) -> Result<RuleDefinition, ParseError> {
    let mut name = None;
    let mut priority = None;
    let mut match_pattern = None;
    let mut where_conditions = Vec::new();
    let mut along = Vec::new();
    let mut fold = Vec::new();
    let mut require = Vec::new();
    let mut having = Vec::new();
    let mut best_by = None;
    let mut output = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn_pair = child.into_inner().next().unwrap();
                name = Some(build_qualified_name(qn_pair)?);
            }
            LocyRule::priority_clause => {
                priority = Some(build_priority_clause(child)?);
            }
            LocyRule::rule_match_clause => {
                // The rule_match_clause contains MATCH keyword + pattern
                // Extract the pattern text span and re-parse via Cypher
                let pattern_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::pattern)
                    .unwrap();
                match_pattern = Some(reparse_as_cypher_pattern(pattern_pair.as_str())?);
            }
            LocyRule::rule_where_clause => {
                where_conditions = build_rule_where_clause(child)?;
            }
            LocyRule::along_clause => {
                along = build_along_clause(child)?;
            }
            LocyRule::fold_clause => {
                fold = build_fold_clause(child)?;
            }
            LocyRule::fold_require_clause => {
                require = build_fold_require_clause(child)?;
            }
            LocyRule::fold_having_clause => {
                having = build_fold_having_clause(child)?;
            }
            LocyRule::best_by_clause => {
                let items = build_best_by_clause(child)?;
                if !items.is_empty() {
                    best_by = Some(BestByClause { items });
                }
            }
            LocyRule::rule_terminal_clause => {
                output = Some(build_rule_terminal_clause(child)?);
            }
            // Skip keywords: CREATE, RULE, AS
            LocyRule::CREATE | LocyRule::RULE | LocyRule::AS => {}
            _ => {}
        }
    }

    Ok(RuleDefinition {
        name: name.unwrap(),
        priority,
        match_pattern: match_pattern.unwrap(),
        where_conditions,
        along,
        fold,
        require,
        having,
        best_by,
        output: output.unwrap(),
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// MODEL DEFINITION (Phase B neural-predicate preview)
// ═══════════════════════════════════════════════════════════════════════════

fn build_model_definition(pair: Pair<LocyRule>) -> Result<ModelDefinition, ParseError> {
    let mut annotations = ModelAnnotations::default();
    let mut name = None;
    let mut inputs: Vec<InputBinding> = Vec::new();
    let mut features: Vec<ast::Expr> = Vec::new();
    let mut path_context: Option<PathContextFeature> = None;
    let mut output: Option<OutputBinding> = None;
    let mut xervo_alias: Option<String> = None;
    let mut embedder_alias: Option<String> = None;
    let mut calibration: Option<CalibrationMethod> = None;
    let mut version: Option<String> = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::model_annotations => {
                for ann in child.into_inner() {
                    if ann.as_rule() == LocyRule::model_annotation {
                        // The annotation is `@` followed by INDEPENDENT or
                        // an identifier. We only act on `@independent` in
                        // Slice 1+2; other annotations parse and are ignored.
                        let raw = ann.as_str();
                        if raw
                            .trim_start_matches('@')
                            .trim()
                            .eq_ignore_ascii_case("independent")
                        {
                            annotations.independent = true;
                        }
                    }
                }
            }
            LocyRule::rule_name => {
                let qn_pair = child.into_inner().next().unwrap();
                name = Some(build_qualified_name(qn_pair)?);
            }
            LocyRule::model_input_clause => {
                inputs = build_model_input_clause(child)?;
            }
            LocyRule::model_features_clause => {
                let (exprs, ctx) = build_model_features_clause(child)?;
                features = exprs;
                path_context = ctx;
            }
            LocyRule::model_output_clause => {
                output = Some(build_model_output_clause(child)?);
            }
            LocyRule::model_using_clause => {
                let (xa, ea) = build_model_using_clause(child)?;
                xervo_alias = Some(xa);
                embedder_alias = ea;
            }
            LocyRule::model_calibration_clause => {
                calibration = Some(build_model_calibration_clause(child)?);
            }
            LocyRule::model_version_clause => {
                version = Some(build_model_version_clause(child)?);
            }
            _ => {}
        }
    }

    Ok(ModelDefinition {
        name: name.ok_or_else(|| ParseError::new("CREATE MODEL missing name".to_string()))?,
        inputs,
        features,
        path_context,
        output: output
            .ok_or_else(|| ParseError::new("CREATE MODEL missing OUTPUT clause".to_string()))?,
        xervo_alias: xervo_alias.ok_or_else(|| {
            ParseError::new("CREATE MODEL missing USING xervo(...) clause".to_string())
        })?,
        embedder_alias,
        calibration,
        version,
        annotations,
    })
}

fn build_model_input_clause(pair: Pair<LocyRule>) -> Result<Vec<InputBinding>, ParseError> {
    let mut bindings = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::model_input_binding {
            let idents: Vec<String> = child
                .into_inner()
                .filter(|p| p.as_rule() == LocyRule::locy_identifier)
                .map(|p| normalize_locy_identifier(p.as_str()))
                .collect();
            // Grammar: `( var (: label)? )` — first identifier is variable,
            // second (if present) is label.
            let (variable, label) = match idents.len() {
                0 => return Err(ParseError::new("empty INPUT binding".to_string())),
                1 => (idents[0].clone(), None),
                _ => (idents[0].clone(), Some(idents[1].clone())),
            };
            bindings.push(InputBinding { variable, label });
        }
    }
    Ok(bindings)
}

fn build_model_features_clause(
    pair: Pair<LocyRule>,
) -> Result<(Vec<ast::Expr>, Option<PathContextFeature>), ParseError> {
    let mut features = Vec::new();
    let mut path_context = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::expression => {
                features.push(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::model_features_path_context => {
                path_context = Some(build_model_features_path_context(child)?);
            }
            _ => {}
        }
    }
    Ok((features, path_context))
}

fn build_model_features_path_context(
    pair: Pair<LocyRule>,
) -> Result<PathContextFeature, ParseError> {
    let mut idents: Vec<String> = Vec::new();
    let mut source_rule: Option<String> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                idents.push(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::rule_name => {
                let qn = child.into_inner().next().ok_or_else(|| {
                    ParseError::new("FEATURES FROM rule_name missing identifier".to_string())
                })?;
                source_rule = Some(build_qualified_name(qn)?.to_string());
            }
            _ => {}
        }
    }
    if idents.len() != 2 {
        return Err(ParseError::new(
            "FEATURES (subject, column) FROM rule_name requires exactly 2 identifiers".to_string(),
        ));
    }
    Ok(PathContextFeature {
        subject_var: idents[0].clone(),
        column: idents[1].clone(),
        source_rule: source_rule.ok_or_else(|| {
            ParseError::new("FEATURES (subject, column) FROM rule_name missing rule".to_string())
        })?,
    })
}

fn build_model_output_clause(pair: Pair<LocyRule>) -> Result<OutputBinding, ParseError> {
    let mut output_type: Option<OutputType> = None;
    let mut name: Option<String> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::model_output_type => {
                output_type = Some(parse_output_type(child.as_str()));
            }
            LocyRule::locy_identifier => {
                name = Some(normalize_locy_identifier(child.as_str()));
            }
            _ => {}
        }
    }
    Ok(OutputBinding {
        output_type: output_type
            .ok_or_else(|| ParseError::new("OUTPUT missing type".to_string()))?,
        name: name.ok_or_else(|| ParseError::new("OUTPUT missing name".to_string()))?,
    })
}

fn parse_output_type(raw: &str) -> OutputType {
    match raw.trim().to_ascii_lowercase().as_str() {
        "prob" => OutputType::Prob,
        "score" => OutputType::Score,
        "label" => OutputType::Label,
        "vector" => OutputType::Vector,
        _ => OutputType::Prob, // grammar guarantees one of the four
    }
}

/// Returns `(xervo_alias, embedder_alias_opt)`. The grammar accepts an
/// optional `embedder='alias'` named argument; when present we lift it
/// into the second tuple slot, otherwise `None` lets the runtime use
/// its `"default"` fallback.
fn build_model_using_clause(pair: Pair<LocyRule>) -> Result<(String, Option<String>), ParseError> {
    let mut xervo_alias: Option<String> = None;
    let mut embedder_alias: Option<String> = None;
    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::string if xervo_alias.is_none() => {
                xervo_alias = Some(unquote_string_literal(child.as_str()));
            }
            LocyRule::model_using_embedder => {
                for inner in child.into_inner() {
                    if inner.as_rule() == LocyRule::string {
                        embedder_alias = Some(unquote_string_literal(inner.as_str()));
                    }
                }
            }
            _ => {}
        }
    }
    let xervo_alias = xervo_alias
        .ok_or_else(|| ParseError::new("USING xervo() missing alias literal".to_string()))?;
    Ok((xervo_alias, embedder_alias))
}

fn build_model_calibration_clause(pair: Pair<LocyRule>) -> Result<CalibrationMethod, ParseError> {
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::model_calibration_method {
            return parse_calibration_method(child);
        }
    }
    Err(ParseError::new("CALIBRATION missing method".to_string()))
}

/// Phase C C1a: shared between CREATE MODEL's CALIBRATION clause
/// and CALIBRATE's METHOD clause. Recognizes the `conformal(alpha)`
/// shape (and bare `conformal` with default alpha = 0.1) in
/// addition to the existing four methods + `none`.
fn parse_calibration_method(pair: Pair<LocyRule>) -> Result<CalibrationMethod, ParseError> {
    // Look for a `conformal_with_alpha` child first; if present, parse the
    // alpha from its inner float.
    for inner in pair.clone().into_inner() {
        if inner.as_rule() == LocyRule::conformal_with_alpha {
            let alpha_pair = inner
                .into_inner()
                .find(|p| p.as_rule() == LocyRule::float)
                .ok_or_else(|| {
                    ParseError::new("conformal calibration expects a float alpha".to_string())
                })?;
            let alpha: f64 = alpha_pair
                .as_str()
                .parse()
                .map_err(|e| ParseError::new(format!("invalid conformal alpha: {e}")))?;
            if !(0.0..1.0).contains(&alpha) {
                return Err(ParseError::new(format!(
                    "conformal alpha must be in (0, 1); got {alpha}"
                )));
            }
            return Ok(CalibrationMethod::Conformal { alpha });
        }
    }
    let raw = pair.as_str().trim().to_ascii_lowercase();
    Ok(match raw.as_str() {
        "platt_scaling" => CalibrationMethod::PlattScaling,
        "isotonic_regression" => CalibrationMethod::IsotonicRegression,
        "temperature_scaling" => CalibrationMethod::TemperatureScaling,
        "beta_calibration" => CalibrationMethod::BetaCalibration,
        "conformal" => CalibrationMethod::Conformal { alpha: 0.1 },
        "dirichlet" => CalibrationMethod::Dirichlet,
        "none" => CalibrationMethod::None,
        other => {
            return Err(ParseError::new(format!(
                "Unknown calibration method '{other}'"
            )));
        }
    })
}

fn build_model_version_clause(pair: Pair<LocyRule>) -> Result<String, ParseError> {
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::string {
            return Ok(unquote_string_literal(child.as_str()));
        }
    }
    Err(ParseError::new("VERSION missing literal".to_string()))
}

/// Strip the surrounding quotes from a `string` rule's match text.
/// Cypher `string` matches `'...'` or `"..."`. Escape handling is the
/// minimal interpretation needed for model aliases / version strings —
/// the same `\'` / `\"` / `\\` set the Cypher walker uses.
fn unquote_string_literal(raw: &str) -> String {
    let s = raw.trim();
    if s.len() < 2 {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let first = bytes[0];
    let last = bytes[s.len() - 1];
    if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
        let inner = &s[1..s.len() - 1];
        let mut out = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(next) = chars.next() {
                    match next {
                        '\'' | '"' | '\\' => out.push(next),
                        'n' => out.push('\n'),
                        't' => out.push('\t'),
                        other => {
                            out.push('\\');
                            out.push(other);
                        }
                    }
                }
            } else {
                out.push(c);
            }
        }
        out
    } else {
        s.to_string()
    }
}

fn build_priority_clause(pair: Pair<LocyRule>) -> Result<i64, ParseError> {
    let int_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == LocyRule::integer)
        .unwrap();
    int_pair
        .as_str()
        .parse::<i64>()
        .map_err(|e| ParseError::new(format!("Invalid priority value: {e}")))
}

// ═══════════════════════════════════════════════════════════════════════════
// RULE WHERE CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_rule_where_clause(pair: Pair<LocyRule>) -> Result<Vec<RuleCondition>, ParseError> {
    let mut conditions = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::rule_condition {
            conditions.push(build_rule_condition(child)?);
        }
    }
    Ok(conditions)
}

fn build_rule_condition(pair: Pair<LocyRule>) -> Result<RuleCondition, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::is_rule_reference => Ok(RuleCondition::IsReference(build_is_reference(
            inner, false,
        )?)),
        LocyRule::is_not_rule_reference => {
            Ok(RuleCondition::IsReference(build_is_reference(inner, true)?))
        }
        LocyRule::expression => {
            let expr = reparse_as_cypher_expression(inner.as_str())?;
            Ok(RuleCondition::Expression(expr))
        }
        LocyRule::generator_reference => {
            let mut name = String::new();
            let mut args = Vec::new();
            let mut outputs = Vec::new();
            for child in inner.into_inner() {
                match child.as_rule() {
                    LocyRule::generator_name => name = child.as_str().to_string(),
                    LocyRule::generator_arg_list => {
                        for arg in child.into_inner() {
                            args.push(reparse_as_cypher_expression(arg.as_str())?);
                        }
                    }
                    LocyRule::generator_outputs => {
                        for out in child.into_inner() {
                            outputs.push(out.as_str().to_string());
                        }
                    }
                    _ => {}
                }
            }
            Ok(RuleCondition::Generator(GeneratorRef {
                name,
                args,
                outputs,
            }))
        }
        other => Err(ParseError::new(format!(
            "Unexpected rule in rule_condition: {other:?}"
        ))),
    }
}

/// Build an [`IsReference`] from an `is_rule_reference` (`negated = false`) or
/// `is_not_rule_reference` (`negated = true`) pair. Both grammar rules share the
/// same child layout; only the negation flag and the presence of a `NOT` token
/// differ.
fn build_is_reference(pair: Pair<LocyRule>, negated: bool) -> Result<IsReference, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();

    // Identify which form we have by looking at the children
    // Form 1: (x, y, ...) IS rule_name  — has parentheses, identifiers before IS, then rule_name
    // Form 2: x IS rule_name TO y       — identifier, IS, rule_name, TO, identifier
    // Form 3: x IS rule_name            — identifier, IS, rule_name

    let mut identifiers = Vec::new();
    let mut rule_name = None;
    let mut target = None;
    let mut saw_to = false;

    for child in &children {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                if rule_name.is_some() && saw_to {
                    // This is the target identifier after TO
                    target = Some(normalize_locy_identifier(child.as_str()));
                } else if rule_name.is_none() {
                    // Subject identifier(s)
                    identifiers.push(normalize_locy_identifier(child.as_str()));
                }
            }
            LocyRule::rule_name => {
                let qn = child.clone().into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::TO => {
                saw_to = true;
            }
            LocyRule::IS | LocyRule::NOT => {}
            _ => {}
        }
    }

    Ok(IsReference {
        subjects: identifiers,
        rule_name: rule_name.unwrap(),
        target,
        negated,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// ALONG CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_along_clause(pair: Pair<LocyRule>) -> Result<Vec<AlongBinding>, ParseError> {
    let mut bindings = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::along_declaration {
            bindings.push(build_along_declaration(child)?);
        }
    }
    Ok(bindings)
}

fn build_along_declaration(pair: Pair<LocyRule>) -> Result<AlongBinding, ParseError> {
    let mut name = None;
    let mut expr = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                name = Some(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::along_expression => {
                expr = Some(build_along_expression(child)?);
            }
            LocyRule::eq => {}
            _ => {}
        }
    }

    Ok(AlongBinding {
        name: name.unwrap(),
        expr: expr.unwrap(),
    })
}

fn build_along_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    // along_expression = { locy_or_expression }
    let inner = pair.into_inner().next().unwrap();
    build_locy_or_expression(inner)
}

/// Left-fold the `operand_rule` children of `pair` into a left-associative chain
/// of `LocyExpr::BinaryOp { op, .. }` nodes, building each operand with `build_operand`.
///
/// A single operand is returned directly (no wrapping `BinaryOp`), matching the
/// per-precedence-level grammar shape.
fn fold_binary(
    pair: Pair<LocyRule>,
    operand_rule: LocyRule,
    op: LocyBinaryOp,
    build_operand: fn(Pair<LocyRule>) -> Result<LocyExpr, ParseError>,
) -> Result<LocyExpr, ParseError> {
    let mut children: Vec<_> = pair
        .into_inner()
        .filter(|p| p.as_rule() == operand_rule)
        .collect();

    let mut result = build_operand(children.remove(0))?;
    for child in children {
        let right = build_operand(child)?;
        result = LocyExpr::BinaryOp {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn build_locy_or_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    fold_binary(
        pair,
        LocyRule::locy_xor_expression,
        LocyBinaryOp::Or,
        build_locy_xor_expression,
    )
}

fn build_locy_xor_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    fold_binary(
        pair,
        LocyRule::locy_and_expression,
        LocyBinaryOp::Xor,
        build_locy_and_expression,
    )
}

fn build_locy_and_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    fold_binary(
        pair,
        LocyRule::locy_not_expression,
        LocyBinaryOp::And,
        build_locy_not_expression,
    )
}

fn build_locy_not_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let not_count = children
        .iter()
        .filter(|p| p.as_rule() == LocyRule::NOT)
        .count();
    let comparison = children
        .into_iter()
        .find(|p| p.as_rule() == LocyRule::locy_comparison_expression)
        .unwrap();
    let mut result = build_locy_comparison_expression(comparison)?;
    for _ in 0..not_count {
        result = LocyExpr::UnaryOp(crate::ast::UnaryOp::Not, Box::new(result));
    }
    Ok(result)
}

/// Find a `prev.<field>` reference anywhere beneath `pair`, returning the field.
///
/// Used to refuse a `prev` reference before it reaches the Cypher re-parse,
/// which would silently strip it. Recursive because a comparison operand may
/// nest arbitrarily (`prev.h * 2 > f(x)`), and a reference at any depth is lost
/// just the same.
fn find_prev_reference(pair: &Pair<LocyRule>) -> Option<String> {
    if pair.as_rule() == LocyRule::prev_reference {
        // `prev_reference = { PREV ~ "." ~ identifier }` -- the field is the
        // last child; fall back to the whole span if the shape ever changes.
        return Some(
            pair.clone()
                .into_inner()
                .next_back()
                .map_or_else(|| pair.as_str().to_string(), |f| f.as_str().to_string()),
        );
    }
    pair.clone()
        .into_inner()
        .find_map(|child| find_prev_reference(&child))
}

fn build_locy_comparison_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    // locy_comparison_expression = { locy_additive_expression ~ comparison_tail* }
    // If there are comparison_tail elements, the entire thing is better handled as
    // a Cypher expression re-parse since comparisons are complex.
    let children: Vec<_> = pair.into_inner().collect();
    let has_comparison = children
        .iter()
        .any(|p| p.as_rule() == LocyRule::comparison_tail);

    if has_comparison {
        // The re-parse below hands the text to `CypherParser`, which does not
        // reserve `PREV` -- `locy.pest` does, `cypher.pest` does not. So a
        // `prev.<field>` inside a comparison came back as an ordinary
        // `Expr::Property` on a variable named `prev`, and the `PrevRef` marker
        // was gone. `collect_prev_refs` and `validate_prev_refs` both see only a
        // `LocyExpr::Cypher` and return empty/`Ok`, so nothing downstream
        // noticed: `prev.h + 1` was correctly refused in a base case while
        // `prev.h > 5` compiled, and in a recursive rule the comparison
        // compiled with the previous-hop binding silently dropped.
        //
        // Refuse it rather than re-parse it. Walking comparisons structurally
        // instead would mean re-implementing what the re-parse handles for free
        // -- `IN`, `STARTS WITH`, `IS NULL`, list literals -- none of which
        // `LocyBinaryOp` models; regressing those to rescue a form that has
        // never worked is the worse trade. A user with a variable genuinely
        // named `prev` still has the backtick escape that `locy.pest`
        // documents.
        if let Some(field) = children.iter().find_map(find_prev_reference) {
            return Err(ParseError::new(format!(
                "`prev.{field}` cannot be used inside a comparison: the \
                 previous-hop binding is not carried through comparison \
                 expressions. Compute the value in a separate ALONG binding \
                 and compare that, or backtick-escape a variable literally \
                 named `prev`."
            )));
        }

        // Re-parse the whole comparison as a Cypher expression using span offsets
        let first_start = children.first().unwrap().as_span().start();
        let last_end = children.last().unwrap().as_span().end();
        let full_input = children.first().unwrap().as_span().get_input();
        let text = &full_input[first_start..last_end];
        let expr = reparse_as_cypher_expression(text)?;
        return Ok(LocyExpr::Cypher(expr));
    }

    build_locy_additive_expression(children.into_iter().next().unwrap())
}

/// Left-fold an interleaved `term (op term)*` precedence level into a
/// left-associative `LocyExpr::BinaryOp` chain.
///
/// Operands are built with `build_operand`; operator tokens are mapped with
/// `map_op`, which returns `None` for an unexpected rule (producing an error
/// naming `level` for diagnostics). A single operand is returned unwrapped.
fn fold_interleaved_binary(
    pair: Pair<LocyRule>,
    level: &str,
    map_op: impl Fn(LocyRule) -> Option<LocyBinaryOp>,
    build_operand: fn(Pair<LocyRule>) -> Result<LocyExpr, ParseError>,
) -> Result<LocyExpr, ParseError> {
    let mut iter = pair.into_inner();
    let mut result = build_operand(iter.next().unwrap())?;
    while let Some(op_pair) = iter.next() {
        let op = map_op(op_pair.as_rule()).ok_or_else(|| {
            ParseError::new(format!(
                "Unexpected token in {level} expression: {:?}",
                op_pair.as_rule()
            ))
        })?;
        let right = build_operand(iter.next().unwrap())?;
        result = LocyExpr::BinaryOp {
            left: Box::new(result),
            op,
            right: Box::new(right),
        };
    }
    Ok(result)
}

fn build_locy_additive_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    fold_interleaved_binary(
        pair,
        "additive",
        |rule| match rule {
            LocyRule::plus => Some(LocyBinaryOp::Add),
            LocyRule::minus => Some(LocyBinaryOp::Sub),
            _ => None,
        },
        build_locy_multiplicative_expression,
    )
}

fn build_locy_multiplicative_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    fold_interleaved_binary(
        pair,
        "multiplicative",
        |rule| match rule {
            LocyRule::star => Some(LocyBinaryOp::Mul),
            LocyRule::slash => Some(LocyBinaryOp::Div),
            LocyRule::percent => Some(LocyBinaryOp::Mod),
            _ => None,
        },
        build_locy_power_expression,
    )
}

fn build_locy_power_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    if children.len() == 1 {
        return build_locy_unary_expression(children.into_iter().next().unwrap());
    }

    let mut iter = children.into_iter();
    let mut result = build_locy_unary_expression(iter.next().unwrap())?;
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == LocyRule::caret {
            let right = build_locy_unary_expression(iter.next().unwrap())?;
            result = LocyExpr::BinaryOp {
                left: Box::new(result),
                op: LocyBinaryOp::Pow,
                right: Box::new(right),
            };
        }
    }
    Ok(result)
}

fn build_locy_unary_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let has_neg = children.iter().any(|p| p.as_rule() == LocyRule::minus);
    let postfix = children
        .into_iter()
        .find(|p| p.as_rule() == LocyRule::locy_postfix_expression)
        .unwrap();

    let mut result = build_locy_postfix_expression(postfix)?;
    if has_neg {
        result = LocyExpr::UnaryOp(crate::ast::UnaryOp::Neg, Box::new(result));
    }
    Ok(result)
}

fn build_locy_postfix_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let has_postfix = children
        .iter()
        .any(|p| p.as_rule() == LocyRule::postfix_suffix);

    if has_postfix {
        // If there are postfix suffixes, the whole thing is a standard expression.
        // Re-parse as Cypher.
        let first_start = children.first().unwrap().as_span().start();
        let last_end = children.last().unwrap().as_span().end();
        let full_input = children.first().unwrap().as_span().get_input();
        let text = &full_input[first_start..last_end];
        let expr = reparse_as_cypher_expression(text)?;
        return Ok(LocyExpr::Cypher(expr));
    }

    let primary = children.into_iter().next().unwrap();
    build_locy_primary_expression(primary)
}

fn build_locy_primary_expression(pair: Pair<LocyRule>) -> Result<LocyExpr, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::prev_reference => {
            // prev.fieldName
            let field = inner
                .into_inner()
                .find(|p| p.as_rule() == LocyRule::identifier_or_keyword)
                .unwrap();
            Ok(LocyExpr::PrevRef(field.as_str().to_string()))
        }
        LocyRule::primary_expression => {
            // Standard Cypher primary — re-parse
            let expr = reparse_as_cypher_expression(inner.as_str())?;
            Ok(LocyExpr::Cypher(expr))
        }
        other => Err(ParseError::new(format!(
            "Unexpected rule in locy_primary_expression: {other:?}"
        ))),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// FOLD CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_fold_clause(pair: Pair<LocyRule>) -> Result<Vec<FoldBinding>, ParseError> {
    let mut bindings = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::fold_declaration {
            bindings.push(build_fold_declaration(child)?);
        }
    }
    Ok(bindings)
}

fn build_fold_declaration(pair: Pair<LocyRule>) -> Result<FoldBinding, ParseError> {
    let mut name = None;
    let mut expr = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::locy_identifier => {
                name = Some(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::fold_expression => {
                // fold_expression = { expression }
                let expr_pair = child.into_inner().next().unwrap();
                expr = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::eq => {}
            _ => {}
        }
    }

    Ok(FoldBinding {
        name: name.unwrap(),
        aggregate: expr.unwrap(),
    })
}

/// Parse a post-FOLD `REQUIRE` clause into definitional-threshold expressions.
///
/// Structurally identical to [`build_fold_having_clause`] — the two differ only
/// in when the runtime applies them (issue #265) — but kept separate so the two
/// clause kinds stay independently greppable.
fn build_fold_require_clause(pair: Pair<LocyRule>) -> Result<Vec<ast::Expr>, ParseError> {
    let mut conditions = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::expression {
            conditions.push(reparse_as_cypher_expression(child.as_str())?);
        }
    }
    Ok(conditions)
}

/// Parse post-FOLD WHERE (HAVING) clause into filter expressions.
fn build_fold_having_clause(pair: Pair<LocyRule>) -> Result<Vec<ast::Expr>, ParseError> {
    let mut conditions = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::expression {
            conditions.push(reparse_as_cypher_expression(child.as_str())?);
        }
    }
    Ok(conditions)
}

// ═══════════════════════════════════════════════════════════════════════════
// BEST BY CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_best_by_clause(pair: Pair<LocyRule>) -> Result<Vec<BestByItem>, ParseError> {
    let mut items = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::best_by_item {
            items.push(build_best_by_item(child)?);
        }
    }
    Ok(items)
}

fn build_best_by_item(pair: Pair<LocyRule>) -> Result<BestByItem, ParseError> {
    let mut expr = None;
    let mut ascending = true; // default

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::expression => {
                expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::ASC => {
                ascending = true;
            }
            LocyRule::DESC => {
                ascending = false;
            }
            _ => {}
        }
    }

    Ok(BestByItem {
        expr: expr.unwrap(),
        ascending,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// RULE OUTPUT (YIELD / DERIVE)
// ═══════════════════════════════════════════════════════════════════════════

fn build_rule_terminal_clause(pair: Pair<LocyRule>) -> Result<RuleOutput, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        LocyRule::locy_yield_clause => {
            let items = build_locy_yield_clause(inner)?;
            Ok(RuleOutput::Yield(YieldClause { items }))
        }
        LocyRule::derive_clause => {
            let derive = build_derive_clause(inner)?;
            Ok(RuleOutput::Derive(derive))
        }
        other => Err(ParseError::new(format!(
            "Unexpected rule in rule_terminal_clause: {other:?}"
        ))),
    }
}

fn build_locy_yield_clause(pair: Pair<LocyRule>) -> Result<Vec<LocyYieldItem>, ParseError> {
    let mut items = Vec::new();
    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::locy_yield_item {
            items.push(build_locy_yield_item(child)?);
        }
    }
    Ok(items)
}

fn build_locy_yield_item(pair: Pair<LocyRule>) -> Result<LocyYieldItem, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();
    let first = &children[0];

    if first.as_rule() == LocyRule::key_projection {
        let inner: Vec<_> = first.clone().into_inner().collect();
        let expr_pair = inner
            .iter()
            .find(|p| p.as_rule() == LocyRule::expression)
            .unwrap();
        let expr = reparse_as_cypher_expression(expr_pair.as_str())?;
        let alias = inner
            .iter()
            .find(|p| p.as_rule() == LocyRule::alias_identifier)
            .map(|p| normalize_locy_identifier(p.as_str()));
        return Ok(LocyYieldItem {
            is_key: true,
            is_prob: false,
            expr,
            alias,
        });
    }

    if first.as_rule() == LocyRule::prob_projection {
        return build_prob_projection(first.clone());
    }

    // expression ~ (AS ~ alias_identifier)?
    let expr = reparse_as_cypher_expression(first.as_str())?;
    let alias = children
        .iter()
        .find(|p| p.as_rule() == LocyRule::alias_identifier)
        .map(|p| normalize_locy_identifier(p.as_str()));

    Ok(LocyYieldItem {
        is_key: false,
        is_prob: false,
        expr,
        alias,
    })
}

/// Parse a `prob_projection` node into a `LocyYieldItem` with `is_prob = true`.
///
/// Grammar forms:
///   `expression AS alias_identifier PROB` → explicit alias
///   `expression AS PROB`                  → alias derived from expression
///   `expression PROB`                     → alias derived from expression
fn build_prob_projection(pair: Pair<LocyRule>) -> Result<LocyYieldItem, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();

    // First child is always the expression
    let expr_pair = &children[0];
    let expr = reparse_as_cypher_expression(expr_pair.as_str())?;

    // Look for an explicit alias_identifier (present only in form 1)
    let alias = children
        .iter()
        .find(|p| p.as_rule() == LocyRule::alias_identifier)
        .map(|p| normalize_locy_identifier(p.as_str()));

    // For forms without explicit alias, derive from expression
    let alias = alias.or_else(|| match &expr {
        ast::Expr::Variable(name) => Some(name.clone()),
        ast::Expr::Property(_, key) => Some(key.clone()),
        _ => None,
    });

    Ok(LocyYieldItem {
        is_key: false,
        is_prob: true,
        expr,
        alias,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// DERIVE CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

fn build_derive_clause(pair: Pair<LocyRule>) -> Result<DeriveClause, ParseError> {
    let children: Vec<_> = pair.into_inner().collect();

    // Check for MERGE form: DERIVE MERGE ident, ident
    let has_merge = children.iter().any(|p| p.as_rule() == LocyRule::MERGE);
    if has_merge {
        let idents: Vec<_> = children
            .iter()
            .filter(|p| p.as_rule() == LocyRule::locy_identifier)
            .map(|p| normalize_locy_identifier(p.as_str()))
            .collect();
        return Ok(DeriveClause::Merge(idents[0].clone(), idents[1].clone()));
    }

    // Pattern form: DERIVE pattern, pattern, ...
    let patterns: Vec<_> = children
        .into_iter()
        .filter(|p| p.as_rule() == LocyRule::derive_pattern)
        .map(build_derive_pattern)
        .collect::<Result<_, _>>()?;

    Ok(DeriveClause::Patterns(patterns))
}

fn build_derive_pattern(pair: Pair<LocyRule>) -> Result<DerivePattern, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    let direction = match inner.as_rule() {
        LocyRule::derive_forward_pattern => crate::ast::Direction::Outgoing,
        LocyRule::derive_backward_pattern => crate::ast::Direction::Incoming,
        other => {
            return Err(ParseError::new(format!(
                "Unexpected rule in derive_pattern: {other:?}"
            )));
        }
    };

    let mut nodes = Vec::new();
    let mut edge = None;

    for child in inner.into_inner() {
        match child.as_rule() {
            LocyRule::derive_node_spec => {
                nodes.push(build_derive_node_spec(child)?);
            }
            LocyRule::derive_edge_spec => {
                edge = Some(build_derive_edge_spec(child)?);
            }
            _ => {}
        }
    }

    Ok(DerivePattern {
        direction,
        source: nodes.remove(0),
        edge: edge.unwrap(),
        target: nodes.remove(0),
    })
}

fn build_derive_node_spec(pair: Pair<LocyRule>) -> Result<DeriveNodeSpec, ParseError> {
    let mut is_new = false;
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::NEW => {
                is_new = true;
            }
            LocyRule::locy_identifier => {
                variable = Some(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::node_labels => {
                // `node_labels` wraps either `node_label_disjunction` or
                // `node_label_conjunction` (parser change for issue #56).
                // Drill one level to reach the identifier list. DERIVE
                // (NEW x:A:B) is conjunction; (NEW x:A|B) would be
                // disjunction (currently meaningless on the create side
                // — DERIVE creates a single node — but accept both for
                // parser robustness).
                for variant in child.into_inner() {
                    for label_child in variant.into_inner() {
                        if matches!(
                            label_child.as_rule(),
                            LocyRule::identifier_or_keyword | LocyRule::identifier
                        ) {
                            // Strip backticks so a quoted DERIVE label (`My Label`)
                            // matches the normalized MATCH-side label (My Label);
                            // the raw `as_str()` would keep the backticks.
                            labels.push(normalize_locy_identifier(label_child.as_str()));
                        }
                    }
                }
            }
            LocyRule::properties => {
                properties = Some(reparse_as_cypher_properties(child.as_str())?);
            }
            _ => {}
        }
    }

    Ok(DeriveNodeSpec {
        is_new,
        variable: variable.unwrap(),
        labels,
        properties,
    })
}

fn build_derive_edge_spec(pair: Pair<LocyRule>) -> Result<DeriveEdgeSpec, ParseError> {
    let mut edge_type = None;
    let mut properties = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::identifier_or_keyword => {
                // Strip backticks so a quoted DERIVE edge type (`HAS ITEM`)
                // matches the normalized MATCH-side edge type (HAS ITEM).
                edge_type = Some(normalize_locy_identifier(child.as_str()));
            }
            LocyRule::properties => {
                properties = Some(reparse_as_cypher_properties(child.as_str())?);
            }
            _ => {}
        }
    }

    Ok(DeriveEdgeSpec {
        edge_type: edge_type.unwrap(),
        properties,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// GOAL QUERY
// ═══════════════════════════════════════════════════════════════════════════

/// Common header shared by GOAL/ABDUCE/EXPLAIN RULE queries: a rule name, an
/// optional bare-`expression` WHERE, an optional return clause, and a negation
/// flag (only ABDUCE's grammar produces a `NOT` token; the others always parse
/// `negated = false`).
struct RuleQueryHeader {
    rule_name: QualifiedName,
    negated: bool,
    where_expr: Option<ast::Expr>,
    return_clause: Option<ast::ReturnClause>,
}

/// Extract the [`RuleQueryHeader`] from a GOAL/ABDUCE/EXPLAIN query pair.
/// `return_rule` selects the command-specific return-clause rule
/// (`goal_return_clause`, `abduce_return_clause`, `explain_rule_return_clause`).
fn build_rule_query_header(
    pair: Pair<LocyRule>,
    return_rule: LocyRule,
) -> Result<RuleQueryHeader, ParseError> {
    let mut rule_name = None;
    let mut negated = false;
    let mut where_expr = None;
    let mut return_clause = None;

    for child in pair.into_inner() {
        let rule = child.as_rule();
        if rule == return_rule {
            return_clause = Some(build_locy_return_clause(child)?);
            continue;
        }
        match rule {
            LocyRule::NOT => {
                negated = true;
            }
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::expression => {
                where_expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            _ => {}
        }
    }

    Ok(RuleQueryHeader {
        rule_name: rule_name.unwrap(),
        negated,
        where_expr,
        return_clause,
    })
}

fn build_goal_query(pair: Pair<LocyRule>) -> Result<GoalQuery, ParseError> {
    let header = build_rule_query_header(pair, LocyRule::goal_return_clause)?;
    Ok(GoalQuery {
        rule_name: header.rule_name,
        where_expr: header.where_expr,
        return_clause: header.return_clause,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// DERIVE COMMAND (top-level)
// ═══════════════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════════════
// CALIBRATE COMMAND (Phase C C2)
// ═══════════════════════════════════════════════════════════════════════════

fn build_calibrate_command(pair: Pair<LocyRule>) -> Result<CalibrateCommand, ParseError> {
    let mut model_name = None;
    let mut pattern: Option<ast::Pattern> = None;
    let mut where_expr: Option<ast::Expr> = None;
    let mut target_expr: Option<ast::Expr> = None;
    let mut method: Option<CalibrationMethod> = None;
    let mut holdout: Option<f64> = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                model_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::pattern => {
                pattern = Some(reparse_as_cypher_pattern(child.as_str())?);
            }
            LocyRule::where_clause => {
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                where_expr = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::TARGET => {}
            LocyRule::expression => {
                // The grammar puts `TARGET ~ expression` after the
                // optional WHERE; by the time the standalone
                // `expression` arrives here we've already consumed
                // the where_clause's inner expression. So this is the
                // TARGET expression.
                target_expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::model_calibration_method => {
                method = Some(parse_calibration_method(child)?);
            }
            LocyRule::holdout_clause => {
                for n in child.into_inner() {
                    match n.as_rule() {
                        LocyRule::float | LocyRule::integer => {
                            let parsed: f64 = n.as_str().parse().map_err(|_| {
                                ParseError::new(format!("Invalid HOLDOUT value: '{}'", n.as_str()))
                            })?;
                            holdout = Some(parsed);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(CalibrateCommand {
        model_name: model_name
            .ok_or_else(|| ParseError::new("CALIBRATE missing model name".to_string()))?,
        pattern: pattern
            .ok_or_else(|| ParseError::new("CALIBRATE missing MATCH pattern".to_string()))?,
        where_expr,
        target_expr: target_expr
            .ok_or_else(|| ParseError::new("CALIBRATE missing TARGET expression".to_string()))?,
        method: method.ok_or_else(|| ParseError::new("CALIBRATE missing METHOD".to_string()))?,
        holdout,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// VALIDATE COMMAND (Phase C C3)
// ═══════════════════════════════════════════════════════════════════════════

fn build_validate_command(pair: Pair<LocyRule>) -> Result<ValidateCommand, ParseError> {
    let mut rule_name = None;
    let mut pattern: Option<ast::Pattern> = None;
    let mut where_expr: Option<ast::Expr> = None;
    let mut target_expr: Option<ast::Expr> = None;
    let mut metrics: Vec<ValidationMetric> = Vec::new();

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::pattern => {
                pattern = Some(reparse_as_cypher_pattern(child.as_str())?);
            }
            LocyRule::where_clause => {
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                where_expr = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::expression => {
                target_expr = Some(reparse_as_cypher_expression(child.as_str())?);
            }
            LocyRule::validate_metric => {
                let raw = child.as_str().trim().to_ascii_lowercase();
                metrics.push(match raw.as_str() {
                    "brier_score" => ValidationMetric::BrierScore,
                    "log_loss" => ValidationMetric::LogLoss,
                    "debiased_ece" => ValidationMetric::DebiasedEce,
                    "ece" => ValidationMetric::Ece,
                    "accuracy" => ValidationMetric::Accuracy,
                    "auc" => ValidationMetric::Auc,
                    other => {
                        return Err(ParseError::new(format!(
                            "Unknown VALIDATE metric '{other}'"
                        )));
                    }
                });
            }
            _ => {}
        }
    }

    if metrics.is_empty() {
        return Err(ParseError::new(
            "VALIDATE requires at least one metric".to_string(),
        ));
    }

    Ok(ValidateCommand {
        rule_name: rule_name
            .ok_or_else(|| ParseError::new("VALIDATE missing rule name".to_string()))?,
        pattern: pattern
            .ok_or_else(|| ParseError::new("VALIDATE missing MATCH pattern".to_string()))?,
        where_expr,
        target_expr: target_expr
            .ok_or_else(|| ParseError::new("VALIDATE missing TARGET expression".to_string()))?,
        metrics,
    })
}

fn build_derive_command(pair: Pair<LocyRule>) -> Result<DeriveCommand, ParseError> {
    let mut rule_name = None;
    let mut where_expr = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::rule_name => {
                let qn = child.into_inner().next().unwrap();
                rule_name = Some(build_qualified_name(qn)?);
            }
            LocyRule::where_clause => {
                // where_clause = { WHERE ~ expression }
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                where_expr = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::DERIVE => {}
            _ => {}
        }
    }

    Ok(DeriveCommand {
        rule_name: rule_name.unwrap(),
        where_expr,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// ASSUME BLOCK
// ═══════════════════════════════════════════════════════════════════════════

fn build_assume_block(pair: Pair<LocyRule>) -> Result<AssumeBlock, ParseError> {
    let mut mutations = Vec::new();
    let mut body = Vec::new();

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::assume_mutation => {
                let inner = child.into_inner().next().unwrap();
                let clause = reparse_as_cypher_clause(inner.as_str())?;
                mutations.push(clause);
            }
            LocyRule::assume_body => {
                body = build_assume_body(child)?;
            }
            LocyRule::ASSUME | LocyRule::THEN => {}
            _ => {}
        }
    }

    Ok(AssumeBlock { mutations, body })
}

fn build_assume_body(pair: Pair<LocyRule>) -> Result<Vec<LocyStatement>, ParseError> {
    // assume_body = { "{" ~ locy_clause+ ~ "}" | locy_clause }
    // Shares the statement-producing dispatch with build_locy_statement_block.
    // Unlike that block, any clause the shared dispatch does not recognize
    // (including MODEL/CALIBRATE/VALIDATE) is treated as Cypher rather than
    // routed to a dedicated statement — preserving the prior assume-body
    // behavior.
    let mut statements = Vec::new();
    let mut cypher_clause_texts: Vec<String> = Vec::new();

    for child in pair.into_inner() {
        if child.as_rule() == LocyRule::locy_clause {
            let inner = child.into_inner().next().unwrap();
            if let Some(inner) =
                dispatch_common_locy_clause(inner, &mut statements, &mut cypher_clause_texts)?
            {
                // Treat as Cypher
                cypher_clause_texts.push(inner.as_str().to_string());
            }
        }
    }

    flush_cypher_clauses(&mut cypher_clause_texts, &mut statements)?;
    Ok(statements)
}

// ═══════════════════════════════════════════════════════════════════════════
// ABDUCE QUERY
// ═══════════════════════════════════════════════════════════════════════════

fn build_abduce_query(pair: Pair<LocyRule>) -> Result<AbduceQuery, ParseError> {
    let header = build_rule_query_header(pair, LocyRule::abduce_return_clause)?;
    Ok(AbduceQuery {
        negated: header.negated,
        rule_name: header.rule_name,
        where_expr: header.where_expr,
        return_clause: header.return_clause,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// EXPLAIN RULE QUERY
// ═══════════════════════════════════════════════════════════════════════════

fn build_explain_rule_query(pair: Pair<LocyRule>) -> Result<ExplainRule, ParseError> {
    let header = build_rule_query_header(pair, LocyRule::explain_rule_return_clause)?;
    Ok(ExplainRule {
        rule_name: header.rule_name,
        where_expr: header.where_expr,
        return_clause: header.return_clause,
    })
}

// ═══════════════════════════════════════════════════════════════════════════
// SHARED RETURN CLAUSE
// ═══════════════════════════════════════════════════════════════════════════

/// Build a ReturnClause from goal_return_clause, abduce_return_clause,
/// or explain_rule_return_clause (they all share the same structure).
fn build_locy_return_clause(pair: Pair<LocyRule>) -> Result<ast::ReturnClause, ParseError> {
    let mut distinct = false;
    let mut items = Vec::new();
    let mut order_by = None;
    let mut skip = None;
    let mut limit = None;

    for child in pair.into_inner() {
        match child.as_rule() {
            LocyRule::RETURN => {}
            LocyRule::DISTINCT => {
                distinct = true;
            }
            LocyRule::return_items => {
                items = reparse_as_cypher_return_items(child.as_str())?;
            }
            LocyRule::order_clause => {
                // order_clause = { ORDER ~ BY ~ sort_items }
                let sort_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::sort_items)
                    .unwrap();
                order_by = Some(reparse_as_cypher_sort_items(sort_pair.as_str())?);
            }
            LocyRule::skip_clause => {
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                skip = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            LocyRule::limit_clause => {
                let expr_pair = child
                    .into_inner()
                    .find(|p| p.as_rule() == LocyRule::expression)
                    .unwrap();
                limit = Some(reparse_as_cypher_expression(expr_pair.as_str())?);
            }
            _ => {}
        }
    }

    Ok(ast::ReturnClause {
        distinct,
        items,
        order_by,
        skip,
        limit,
    })
}
