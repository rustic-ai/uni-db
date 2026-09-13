use crate::steps::assertions::{assert_err, assert_err_mentions, assert_ok, parse_gherkin_value};
use crate::LocyWorld;
use cucumber::then;
use std::collections::HashMap;
use uni_common::Value;
use uni_locy::result::CommandResult;

/// Extract a value from a Row (HashMap<String, Value>), supporting dotted
/// property access such as `a.name` → row["a"] → Node.properties["name"].
fn extract_field_value<'a>(row: &'a HashMap<String, Value>, field_path: &str) -> Option<&'a Value> {
    if let Some((col, prop)) = field_path.split_once('.') {
        match row.get(col)? {
            Value::Node(node) => node.properties.get(prop),
            Value::Edge(edge) => edge.properties.get(prop),
            Value::Map(map) => map.get(prop),
            _ => None,
        }
    } else {
        row.get(field_path)
    }
}

/// Flexible value comparison (int/float cross-compare, etc.)
///
/// Tolerance is 1e-6 rather than 1e-9 to accommodate f32-precision values that
/// come from the similar_to() computation path (cosine is computed in f32 and
/// widened to f64, introducing ~2e-8 rounding error for unit vectors).
fn values_match(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Float(a), Value::Float(b)) => (a - b).abs() < 1e-6,
        (Value::Int(a), Value::Float(b)) => (*a as f64 - b).abs() < 1e-6,
        (Value::Float(a), Value::Int(b)) => (a - *b as f64).abs() < 1e-6,
        _ => actual == expected,
    }
}

/// Borrow command result `$idx`, matching it against `$pat` (a `CommandResult`
/// variant) and binding its payload as `$bind`, or panic naming `$kind`.
///
/// Replaces the repeated `match world.expect_command_result(idx) { Variant(x)
/// => …, other => panic!(…) }` shape shared by the command-result assertions.
macro_rules! expect_command_variant {
    ($world:expr, $idx:expr, $kind:literal, $pat:pat => $bind:expr) => {{
        let idx = $idx;
        match $world.expect_command_result(idx) {
            $pat => $bind,
            other => panic!(
                "Expected command result {} to be {}, got {:?}",
                idx, $kind, other
            ),
        }
    }};
}

/// Assert that a warning with the given `code` and `rule_name` is present.
fn assert_warning_present(
    world: &LocyWorld,
    code: uni_locy::RuntimeWarningCode,
    rule_name: &str,
    code_label: &str,
) {
    let result = world.expect_locy_ok();
    let found = result
        .warnings()
        .iter()
        .any(|w| w.code == code && w.rule_name == rule_name);
    assert!(
        found,
        "Expected {} warning for rule '{}', but warnings were: {:?}",
        code_label,
        rule_name,
        result
            .warnings()
            .iter()
            .map(|w| format!("{:?} ({})", w.code, w.rule_name))
            .collect::<Vec<_>>()
    );
}

/// Assert that no warning with the given `code` is present.
fn assert_warning_absent(world: &LocyWorld, code: uni_locy::RuntimeWarningCode, code_label: &str) {
    let result = world.expect_locy_ok();
    let found = result.warnings().iter().any(|w| w.code == code);
    assert!(
        !found,
        "Expected no {} warning, but found: {:?}",
        code_label,
        result
            .warnings()
            .iter()
            .filter(|w| w.code == code)
            .map(|w| format!("rule={}", w.rule_name))
            .collect::<Vec<_>>()
    );
}

/// Assert that a compile warning with the given `code` is present.
fn assert_compile_warning_present(
    world: &LocyWorld,
    code: uni_locy::types::WarningCode,
    code_label: &str,
) {
    let result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");
    let found = result.compile_warnings().iter().any(|w| w.code == code);
    assert!(
        found,
        "expected {} compile warning, got: {:?}",
        code_label,
        result
            .compile_warnings()
            .iter()
            .map(|w| format!("{:?}", w.code))
            .collect::<Vec<_>>()
    );
}

/// Assert that no compile warning with the given `code` is present.
fn assert_compile_warning_absent(
    world: &LocyWorld,
    code: uni_locy::types::WarningCode,
    code_label: &str,
) {
    let result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");
    let found = result.compile_warnings().iter().any(|w| w.code == code);
    assert!(
        !found,
        "expected no {} warning, got: {:?}",
        code_label,
        result
            .compile_warnings()
            .iter()
            .filter(|w| w.code == code)
            .map(|w| w.message.clone())
            .collect::<Vec<_>>()
    );
}

/// Borrow the neural call for `model_name` on the EXPLAIN child at
/// `child_idx` of command result `idx`, panicking with a clear message if the
/// command is not an Explain, the child is missing, or no matching call exists.
fn find_explain_neural_call<'a>(
    world: &'a LocyWorld,
    idx: usize,
    child_idx: usize,
    model_name: &str,
) -> &'a uni_locy::result::NeuralProvenance {
    let node = match world.expect_command_result(idx) {
        CommandResult::Explain(node) => node,
        other => panic!(
            "Expected command result {} to be an Explain, got {:?}",
            idx, other
        ),
    };
    let child = node
        .children
        .get(child_idx)
        .unwrap_or_else(|| panic!("No child at index {}", child_idx));
    child
        .neural_calls
        .iter()
        .find(|c| c.model_name == model_name)
        .unwrap_or_else(|| {
            panic!(
                "Child {} has no NeuralProvenance for model '{}' (got: {:?})",
                child_idx,
                model_name,
                child
                    .neural_calls
                    .iter()
                    .map(|c| c.model_name.clone())
                    .collect::<Vec<_>>()
            )
        })
}

/// Find the `CalibrationResult` for `model_name`, panicking if absent.
fn find_calibration<'a>(
    world: &'a LocyWorld,
    model_name: &str,
) -> &'a uni_locy::result::CalibrationResult {
    let result = world.expect_locy_ok();
    result
        .command_results()
        .iter()
        .find_map(|cr| match cr {
            uni_locy::CommandResult::Calibrate(c) if c.model_name == model_name => Some(c),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no CalibrationResult for model '{}'", model_name))
}

/// Find the `ValidationResult` for `rule_name`, panicking if absent.
fn find_validation<'a>(
    world: &'a LocyWorld,
    rule_name: &str,
) -> &'a uni_locy::result::ValidationResult {
    let result = world.expect_locy_ok();
    result
        .command_results()
        .iter()
        .find_map(|cr| match cr {
            uni_locy::CommandResult::Validate(v) if v.rule_name == rule_name => Some(v),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no ValidationResult for rule '{}'", rule_name))
}

#[then("evaluation should succeed")]
async fn evaluation_should_succeed(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");
    assert_ok(locy_result, "evaluation");
}

#[then(regex = r#"^evaluation should succeed with timed_out (true|false)$"#)]
async fn evaluation_should_succeed_with_timed_out(world: &mut LocyWorld, expected: String) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    let result = locy_result
        .as_ref()
        .unwrap_or_else(|e| panic!("Expected successful evaluation, but got error: {e}"));

    let expected_flag = expected == "true";
    assert_eq!(
        result.timed_out(),
        expected_flag,
        "expected timed_out={expected_flag}, got timed_out={}",
        result.timed_out(),
    );
}

#[then(
    regex = r#"^evaluation should be incomplete with reason ['"](timeout|iteration_limit)['"]$"#
)]
async fn evaluation_should_be_incomplete_with_reason(world: &mut LocyWorld, expected: String) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    let reason = match locy_result {
        // allow_partial path: the partial result carries the diagnostics.
        Ok(result) => result.incomplete.as_ref().map(|d| d.reason),
        // Default path: an over-budget evaluation is a hard error.
        Err(uni_common::UniError::LocyIncomplete { detail }) => Some(detail.reason),
        Err(e) => panic!("expected LocyIncomplete (or a partial result), got error: {e}"),
    };
    let reason = reason.expect("evaluation completed normally; expected an incomplete result");
    assert_eq!(
        reason.as_str(),
        expected,
        "incomplete reason mismatch: got {}, expected {expected}",
        reason.as_str(),
    );
}

#[then("evaluation should fail")]
async fn evaluation_should_fail(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");
    assert_err(locy_result, "evaluation");
}

#[then(regex = r#"^the evaluation error should mention ['"](.+)['"]$"#)]
async fn evaluation_error_should_mention(world: &mut LocyWorld, expected_text: String) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");
    assert_err_mentions(locy_result, "evaluation", &expected_text);
}

#[then(regex = r#"^the derived relation ['"](.+)['"] should have (\d+) facts$"#)]
async fn derived_relation_should_have_n_facts(
    world: &mut LocyWorld,
    relation_name: String,
    expected_count: usize,
) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    match locy_result {
        Ok(result) => {
            let facts = result.derived.get(&relation_name);
            let actual = facts.map(|f| f.len()).unwrap_or(0);
            assert_eq!(
                actual, expected_count,
                "Expected derived relation '{}' to have {} facts, but got {}. Available relations: {:?}",
                relation_name,
                expected_count,
                actual,
                result.derived.keys().collect::<Vec<_>>()
            );
        }
        Err(err) => {
            panic!(
                "Expected successful evaluation with derived relation '{}', but got error: {}",
                relation_name, err
            );
        }
    }
}

#[then(regex = r#"^the derived relation ['"](.+)['"] should contain at least (\d+) facts$"#)]
async fn derived_relation_should_contain_at_least_n_facts(
    world: &mut LocyWorld,
    relation_name: String,
    min_count: usize,
) {
    let locy_result = world
        .locy_result()
        .expect("No evaluation result found - did you forget to evaluate a program?");

    match locy_result {
        Ok(result) => {
            let facts = result.derived.get(&relation_name);
            let actual = facts.map(|f| f.len()).unwrap_or(0);
            assert!(
                actual >= min_count,
                "Expected derived relation '{}' to have at least {} facts, but got {}",
                relation_name,
                min_count,
                actual
            );
        }
        Err(err) => {
            panic!(
                "Expected successful evaluation with derived relation '{}', but got error: {}",
                relation_name, err
            );
        }
    }
}

// ── Value-Level Assertions ────────────────────────────────────────────────

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should contain a fact where ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null) and ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null) and ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
#[allow(clippy::too_many_arguments)]
async fn derived_relation_should_contain_fact_where_and_and(
    world: &mut LocyWorld,
    relation: String,
    f1: String,
    v1: String,
    f2: String,
    v2: String,
    f3: String,
    v3: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");
    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let expected1 = parse_gherkin_value(&v1);
    let expected2 = parse_gherkin_value(&v2);
    let expected3 = parse_gherkin_value(&v3);

    let found = facts.iter().any(|row| {
        let m1 = extract_field_value(row, f1.trim())
            .map(|v| values_match(v, &expected1))
            .unwrap_or(false);
        let m2 = extract_field_value(row, f2.trim())
            .map(|v| values_match(v, &expected2))
            .unwrap_or(false);
        let m3 = extract_field_value(row, f3.trim())
            .map(|v| values_match(v, &expected3))
            .unwrap_or(false);
        m1 && m2 && m3
    });

    assert!(
        found,
        "Expected derived relation '{}' to contain a fact where {} = {} and {} = {} and {} = {}, but no match found in {} facts",
        relation, f1, v1, f2, v2, f3, v3, facts.len()
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should contain a fact where ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null) and ([^ =]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
async fn derived_relation_should_contain_fact_where_and(
    world: &mut LocyWorld,
    relation: String,
    f1: String,
    v1: String,
    f2: String,
    v2: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let expected1 = parse_gherkin_value(&v1);
    let expected2 = parse_gherkin_value(&v2);

    let found = facts.iter().any(|row| {
        let m1 = extract_field_value(row, f1.trim())
            .map(|v| values_match(v, &expected1))
            .unwrap_or(false);
        let m2 = extract_field_value(row, f2.trim())
            .map(|v| values_match(v, &expected2))
            .unwrap_or(false);
        m1 && m2
    });

    assert!(
        found,
        "Expected derived relation '{}' to contain a fact where {} = {} and {} = {}, but no match found in {} facts",
        relation, f1, v1, f2, v2, facts.len()
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should contain a fact where ([^ ]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
async fn derived_relation_should_contain_fact_where(
    world: &mut LocyWorld,
    relation: String,
    field: String,
    value_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let expected = parse_gherkin_value(&value_str);

    let found = facts.iter().any(|row| {
        extract_field_value(row, field.trim())
            .map(|v| values_match(v, &expected))
            .unwrap_or(false)
    });

    assert!(
        found,
        "Expected derived relation '{}' to contain a fact where {} = {}, but no match found in {} facts",
        relation, field, value_str, facts.len()
    );
}

/// Phase B A3: probabilistic-output approximate-equality match.
/// Used for real-classifier scenarios where the exact float depends
/// on sigmoid/matmul precision — assert proximity within a tolerance
/// rather than bit-equality.
#[then(
    regex = r#"^the derived relation ['"](.+)['"] should contain a fact where ([^ ]+) is approximately (-?\d+(?:\.\d+)?) within (-?\d+(?:\.\d+)?)$"#
)]
async fn derived_relation_should_contain_fact_approximately(
    world: &mut LocyWorld,
    relation: String,
    field: String,
    expected: f64,
    tolerance: f64,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");
    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));
    let found = facts.iter().any(|row| {
        extract_field_value(row, field.trim())
            .and_then(|v| match v {
                uni_common::Value::Float(f) => Some(*f),
                uni_common::Value::Int(i) => Some(*i as f64),
                _ => None,
            })
            .map(|v: f64| (v - expected).abs() <= tolerance)
            .unwrap_or(false)
    });
    assert!(
        found,
        "Expected derived relation '{}' to contain a fact where {} ≈ {} (±{}), but no match found in {} facts: {:?}",
        relation,
        field,
        expected,
        tolerance,
        facts.len(),
        facts.iter().take(5).collect::<Vec<_>>()
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should not contain a fact where (.+) = (.+) and (.+) = (.+)$"#
)]
async fn derived_relation_should_not_contain_fact_where_and(
    world: &mut LocyWorld,
    relation: String,
    f1: String,
    v1: String,
    f2: String,
    v2: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .map(|f| f.as_slice())
        .unwrap_or(&[]);

    let expected1 = parse_gherkin_value(&v1);
    let expected2 = parse_gherkin_value(&v2);

    let found = facts.iter().any(|row| {
        let m1 = extract_field_value(row, f1.trim())
            .map(|v| values_match(v, &expected1))
            .unwrap_or(false);
        let m2 = extract_field_value(row, f2.trim())
            .map(|v| values_match(v, &expected2))
            .unwrap_or(false);
        m1 && m2
    });

    assert!(
        !found,
        "Expected derived relation '{}' NOT to contain a fact where {} = {} and {} = {}, but match was found",
        relation, f1, v1, f2, v2
    );
}

#[then(
    regex = r#"^the derived relation ['"](.+)['"] should not contain a fact where ([^ ]+) = ('[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null)$"#
)]
async fn derived_relation_should_not_contain_fact_where(
    world: &mut LocyWorld,
    relation: String,
    field: String,
    value_str: String,
) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    let facts = result
        .derived
        .get(&relation)
        .map(|f| f.as_slice())
        .unwrap_or(&[]);

    let expected = parse_gherkin_value(&value_str);

    let found = facts.iter().any(|row| {
        extract_field_value(row, field.trim())
            .map(|v| values_match(v, &expected))
            .unwrap_or(false)
    });

    assert!(
        !found,
        "Expected derived relation '{}' NOT to contain a fact where {} = {}, but match was found",
        relation, field, value_str
    );
}

// ── Stats Assertions ──────────────────────────────────────────────────────

#[then(regex = r#"^the evaluation stats should show (\d+) total iterations$"#)]
async fn stats_total_iterations(world: &mut LocyWorld, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    assert_eq!(
        result.stats.total_iterations, expected,
        "Expected {} total iterations, got {}",
        expected, result.stats.total_iterations
    );
}

#[then(regex = r#"^the evaluation stats should show (\d+) queries executed$"#)]
async fn stats_queries_executed(world: &mut LocyWorld, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    assert_eq!(
        result.stats.queries_executed, expected,
        "Expected {} queries executed, got {}",
        expected, result.stats.queries_executed
    );
}

#[then(regex = r#"^the evaluation stats should show (\d+) mutations executed$"#)]
async fn stats_mutations_executed(world: &mut LocyWorld, expected: usize) {
    let locy_result = world.locy_result().expect("No evaluation result found");

    let result = locy_result.as_ref().expect("Evaluation failed");
    assert_eq!(
        result.stats.mutations_executed, expected,
        "Expected {} mutations executed, got {}",
        expected, result.stats.mutations_executed
    );
}

// ── Command Result Assertions ─────────────────────────────────────────────

#[then(regex = r#"^the command result (\d+) should be a Query with (\d+) rows$"#)]
async fn command_result_query_rows(world: &mut LocyWorld, idx: usize, expected: usize) {
    let rows = expect_command_variant!(world, idx, "a Query", CommandResult::Query(rows) => rows);
    assert_eq!(
        rows.len(),
        expected,
        "Expected Query command result {} to have {} rows, got {}",
        idx,
        expected,
        rows.len()
    );
}

#[then(regex = r#"^the command result (\d+) should be a Query containing row where (.+) = (.+)$"#)]
async fn command_result_query_containing_row(
    world: &mut LocyWorld,
    idx: usize,
    field: String,
    value_str: String,
) {
    let rows = expect_command_variant!(world, idx, "a Query", CommandResult::Query(rows) => rows);
    let expected = parse_gherkin_value(&value_str);
    let found = rows.iter().any(|row| {
        extract_field_value(row, field.trim())
            .map(|v| values_match(v, &expected))
            .unwrap_or(false)
    });
    assert!(
        found,
        "Expected Query result {} to contain row where {} = {}, but not found in {} rows",
        idx,
        field,
        value_str,
        rows.len()
    );
}

#[then(regex = r#"^the command result (\d+) should be an Assume with (\d+) rows$"#)]
async fn command_result_assume_rows(world: &mut LocyWorld, idx: usize, expected: usize) {
    let rows =
        expect_command_variant!(world, idx, "an Assume", CommandResult::Assume(rows) => rows);
    assert_eq!(
        rows.len(),
        expected,
        "Expected Assume command result {} to have {} rows, got {}",
        idx,
        expected,
        rows.len()
    );
}

#[then(
    regex = r#"^the command result (\d+) should be an Assume containing row where (.+) = (.+)$"#
)]
async fn command_result_assume_containing_row(
    world: &mut LocyWorld,
    idx: usize,
    field: String,
    value_str: String,
) {
    let rows =
        expect_command_variant!(world, idx, "an Assume", CommandResult::Assume(rows) => rows);
    let expected = parse_gherkin_value(&value_str);
    let found = rows.iter().any(|row| {
        extract_field_value(row, field.trim())
            .map(|v| values_match(v, &expected))
            .unwrap_or(false)
    });
    assert!(
        found,
        "Expected Assume result {} to contain row where {} = {}, but not found in {} rows",
        idx,
        field,
        value_str,
        rows.len()
    );
}

#[then(regex = r#"^the command result (\d+) should be an Explain with rule ['"](.+)['"]$"#)]
async fn command_result_explain_rule(world: &mut LocyWorld, idx: usize, rule_name: String) {
    let node =
        expect_command_variant!(world, idx, "an Explain", CommandResult::Explain(node) => node);
    assert_eq!(
        node.rule, rule_name,
        "Expected Explain root rule '{}', got '{}'",
        rule_name, node.rule
    );
}

#[then(regex = r#"^the command result (\d+) should be an Explain with (\d+) children$"#)]
async fn command_result_explain_children(world: &mut LocyWorld, idx: usize, expected: usize) {
    let node =
        expect_command_variant!(world, idx, "an Explain", CommandResult::Explain(node) => node);
    assert_eq!(
        node.children.len(),
        expected,
        "Expected Explain with {} children, got {}",
        expected,
        node.children.len()
    );
}

#[then(
    regex = r#"^the command result (\d+) should be an Abduce with at least (\d+) modifications$"#
)]
async fn command_result_abduce_modifications(world: &mut LocyWorld, idx: usize, min: usize) {
    let abduce_result =
        expect_command_variant!(world, idx, "an Abduce", CommandResult::Abduce(r) => r);
    assert!(
        abduce_result.modifications.len() >= min,
        "Expected Abduce with at least {} modifications, got {}",
        min,
        abduce_result.modifications.len()
    );
}

#[then(regex = r#"^the command result (\d+) should be a Derive affecting (\d+) elements$"#)]
async fn command_result_derive_affecting(world: &mut LocyWorld, idx: usize, expected: usize) {
    let affected = expect_command_variant!(world, idx, "a Derive", CommandResult::Derive { affected } => affected);
    assert_eq!(
        *affected, expected,
        "Expected Derive affecting {} elements, got {}",
        expected, affected
    );
}

// ── Cypher Command Result Assertions ─────────────────────────────────────

#[then(regex = r#"^the command result (\d+) should be a Cypher with at least (\d+) rows$"#)]
async fn command_result_cypher_at_least_rows(world: &mut LocyWorld, idx: usize, min: usize) {
    let rows = expect_command_variant!(world, idx, "a Cypher", CommandResult::Cypher(rows) => rows);
    assert!(
        rows.len() >= min,
        "Expected Cypher command result {} to have at least {} rows, got {}",
        idx,
        min,
        rows.len()
    );
}

#[then(regex = r#"^the command result (\d+) should be a Cypher containing row where (.+) = (.+)$"#)]
async fn command_result_cypher_containing_row(
    world: &mut LocyWorld,
    idx: usize,
    field: String,
    value_str: String,
) {
    let rows = expect_command_variant!(world, idx, "a Cypher", CommandResult::Cypher(rows) => rows);
    let expected = parse_gherkin_value(&value_str);
    let found = rows.iter().any(|row| {
        extract_field_value(row, field.trim())
            .map(|v| values_match(v, &expected))
            .unwrap_or(false)
    });
    assert!(
        found,
        "Cypher command result {} has no row where {} = {} (out of {} rows)",
        idx,
        field,
        value_str,
        rows.len()
    );
}

// ── "at least" Variants for Query and Derive ─────────────────────────────

#[then(regex = r#"^the command result (\d+) should be a Query with at least (\d+) rows$"#)]
async fn command_result_query_at_least_rows(world: &mut LocyWorld, idx: usize, min: usize) {
    let rows = expect_command_variant!(world, idx, "a Query", CommandResult::Query(rows) => rows);
    assert!(
        rows.len() >= min,
        "Expected Query command result {} to have at least {} rows, got {}",
        idx,
        min,
        rows.len()
    );
}

#[then(regex = r#"^the command result (\d+) should be a Derive with at least (\d+) affected$"#)]
async fn command_result_derive_at_least_affected(world: &mut LocyWorld, idx: usize, min: usize) {
    let affected = expect_command_variant!(world, idx, "a Derive", CommandResult::Derive { affected } => affected);
    assert!(
        *affected >= min,
        "Expected Derive command result {} to have at least {} affected, got {}",
        idx,
        min,
        affected
    );
}

// ── Graph State Assertions ────────────────────────────────────────────────

#[then(regex = r#"^the graph should contain (\d+) nodes with label ['"](.+)['"]$"#)]
async fn graph_should_contain_n_nodes_with_label(
    world: &mut LocyWorld,
    expected: usize,
    label: String,
) {
    let query = format!("MATCH (n:{}) RETURN count(n) AS cnt", label);
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert_eq!(
        cnt as usize, expected,
        "Expected {} nodes with label '{}', got {}",
        expected, label, cnt
    );
}

#[then(
    regex = r#"^the graph should contain an edge from ['"](.+)['"] to ['"](.+)['"] with type ['"](.+)['"]$"#
)]
async fn graph_should_contain_edge(
    world: &mut LocyWorld,
    from: String,
    to: String,
    edge_type: String,
) {
    let query = format!(
        "MATCH (a {{name: '{}'}})-[r:{}]->(b {{name: '{}'}}) RETURN count(r) AS cnt",
        from, edge_type, to
    );
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert!(
        cnt > 0,
        "Expected edge from '{}' to '{}' with type '{}', but none found",
        from,
        to,
        edge_type
    );
}

#[then(
    regex = r#"^the graph should not contain an edge from ['"](.+)['"] to ['"](.+)['"] with type ['"](.+)['"]$"#
)]
async fn graph_should_not_contain_edge(
    world: &mut LocyWorld,
    from: String,
    to: String,
    edge_type: String,
) {
    let query = format!(
        "MATCH (a {{name: '{}'}})-[r:{}]->(b {{name: '{}'}}) RETURN count(r) AS cnt",
        from, edge_type, to
    );
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert_eq!(
        cnt, 0,
        "Expected no edge from '{}' to '{}' with type '{}', but found {}",
        from, to, edge_type, cnt
    );
}

#[then(regex = r#"^the graph should NOT contain an edge with type ['"](.+)['"]$"#)]
async fn graph_should_not_contain_edge_type(world: &mut LocyWorld, edge_type: String) {
    let query = format!("MATCH ()-[r:{}]->() RETURN count(r) AS cnt", edge_type);
    let result = world
        .db()
        .session()
        .query(&query)
        .await
        .expect("graph query failed");
    let cnt: i64 = result.rows()[0].get("cnt").expect("missing cnt column");
    assert_eq!(
        cnt, 0,
        "Expected no edges with type '{}', but found {}",
        edge_type, cnt
    );
}

// ── Warning Assertions ──────────────────────────────────────────────────

#[then(
    regex = r#"^the result should contain a SharedProbabilisticDependency warning for rule ['"](.+)['"]$"#
)]
async fn result_should_contain_shared_dep_warning(world: &mut LocyWorld, rule_name: String) {
    assert_warning_present(
        world,
        uni_locy::RuntimeWarningCode::SharedProbabilisticDependency,
        &rule_name,
        "SharedProbabilisticDependency",
    );
}

#[then("the result should not contain a SharedProbabilisticDependency warning")]
async fn result_should_not_contain_shared_dep_warning(world: &mut LocyWorld) {
    assert_warning_absent(
        world,
        uni_locy::RuntimeWarningCode::SharedProbabilisticDependency,
        "SharedProbabilisticDependency",
    );
}

#[then(
    regex = r#"^the result should contain a FuzzyNotProbabilistic warning for rule ['"](.+)['"]$"#
)]
async fn result_should_contain_fuzzy_warning(world: &mut LocyWorld, rule_name: String) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result.warnings().iter().any(|w| {
        w.code == uni_locy::RuntimeWarningCode::FuzzyNotProbabilistic && w.rule_name == rule_name
    });
    assert!(
        found,
        "Expected FuzzyNotProbabilistic warning for rule '{}', but warnings were: {:?}",
        rule_name,
        locy_result
            .warnings()
            .iter()
            .map(|w| format!("{:?} ({})", w.code, w.rule_name))
            .collect::<Vec<_>>()
    );
}

#[then(
    regex = r#"^the calibration result for ['"](.+)['"] should have calibrated_ece less than half the raw_ece$"#
)]
async fn calibration_result_ece_reduction(world: &mut LocyWorld, model_name: String) {
    let result = find_calibration(world, &model_name);
    assert!(
        result.calibrated_ece < result.raw_ece * 0.5,
        "expected calibrated_ece (={}) < raw_ece (={}) * 0.5",
        result.calibrated_ece,
        result.raw_ece
    );
}

/// Phase C C1a: assert the conformal quantile (half-width of every
/// confidence band the calibrator will emit) on a CALIBRATE result.
#[then(
    regex = r#"^the calibration result for ['"](.+)['"] should have confidence_band_quantile approximately (-?\d+(?:\.\d+)?) within (-?\d+(?:\.\d+)?)$"#
)]
async fn calibration_confidence_band_quantile(
    world: &mut LocyWorld,
    model_name: String,
    expected: f64,
    tolerance: f64,
) {
    let c = find_calibration(world, &model_name);
    let q = c.confidence_band_quantile.unwrap_or_else(|| {
        panic!(
            "calibration result for '{}' has no confidence_band_quantile (method = {:?})",
            model_name, c.method
        )
    });
    assert!(
        (q - expected).abs() <= tolerance,
        "expected confidence_band_quantile ≈ {} (±{}), got {}",
        expected,
        tolerance,
        q
    );
}

#[then(regex = r#"^the calibration result for ['"](.+)['"] should report method ['"](.+)['"]$"#)]
async fn calibration_result_method(world: &mut LocyWorld, model_name: String, method: String) {
    let c = find_calibration(world, &model_name);
    let actual = format!("{:?}", c.method);
    assert!(
        actual.eq_ignore_ascii_case(&method),
        "expected method {method}, got {actual}"
    );
}

#[then(
    regex = r#"^the validation result for rule ['"](.+)['"] should report ['"](.+)['"] (greater|less) than ([0-9]*\.?[0-9]+)$"#
)]
async fn validation_metric_threshold(
    world: &mut LocyWorld,
    rule_name: String,
    metric_name: String,
    comparator: String,
    threshold: f64,
) {
    let metric = match metric_name.to_ascii_lowercase().as_str() {
        "brier_score" => uni_cypher::locy_ast::ValidationMetric::BrierScore,
        "log_loss" => uni_cypher::locy_ast::ValidationMetric::LogLoss,
        "ece" => uni_cypher::locy_ast::ValidationMetric::Ece,
        "debiased_ece" => uni_cypher::locy_ast::ValidationMetric::DebiasedEce,
        "accuracy" => uni_cypher::locy_ast::ValidationMetric::Accuracy,
        "auc" => uni_cypher::locy_ast::ValidationMetric::Auc,
        other => panic!("unknown VALIDATE metric '{other}'"),
    };
    let v = find_validation(world, &rule_name);
    let value = v.metric(metric).unwrap_or_else(|| {
        panic!(
            "rule '{}' validation did not report metric '{}'",
            rule_name, metric_name
        )
    });
    let pass = match comparator.as_str() {
        "greater" => value > threshold,
        "less" => value < threshold,
        other => panic!("unknown comparator '{other}'"),
    };
    assert!(
        pass,
        "expected {metric_name} {comparator} {threshold}, got {value}"
    );
}

#[then(regex = r#"^the result should contain an EceBinningBias warning$"#)]
async fn result_contains_ece_binning_bias(world: &mut LocyWorld) {
    assert_compile_warning_present(
        world,
        uni_locy::types::WarningCode::EceBinningBias,
        "EceBinningBias",
    );
}

#[then(regex = r#"^the result should contain a FoldInRecursivePath warning$"#)]
async fn result_contains_fold_in_recursive_path(world: &mut LocyWorld) {
    assert_compile_warning_present(
        world,
        uni_locy::types::WarningCode::FoldInRecursivePath,
        "FoldInRecursivePath",
    );
}

#[then(regex = r#"^the result should contain a HavingInRecursivePath warning$"#)]
async fn result_contains_having_in_recursive_path(world: &mut LocyWorld) {
    assert_compile_warning_present(
        world,
        uni_locy::types::WarningCode::HavingInRecursivePath,
        "HavingInRecursivePath",
    );
}

#[then(regex = r#"^the result should not contain a HavingInRecursivePath warning$"#)]
async fn result_lacks_having_in_recursive_path(world: &mut LocyWorld) {
    assert_compile_warning_absent(
        world,
        uni_locy::types::WarningCode::HavingInRecursivePath,
        "HavingInRecursivePath",
    );
}

#[then(regex = r#"^the result should contain a PositiveComplementCorrelation warning$"#)]
async fn result_contains_positive_complement_correlation(world: &mut LocyWorld) {
    assert_compile_warning_present(
        world,
        uni_locy::types::WarningCode::PositiveComplementCorrelation,
        "PositiveComplementCorrelation",
    );
}

#[then(regex = r#"^the result should contain a SharedRetrievalContext warning$"#)]
async fn result_contains_shared_retrieval_context(world: &mut LocyWorld) {
    assert_compile_warning_present(
        world,
        uni_locy::types::WarningCode::SharedRetrievalContext,
        "SharedRetrievalContext",
    );
}

#[then(regex = r#"^the result should not contain a SharedRetrievalContext warning$"#)]
async fn result_does_not_contain_shared_retrieval_context(world: &mut LocyWorld) {
    assert_compile_warning_absent(
        world,
        uni_locy::types::WarningCode::SharedRetrievalContext,
        "SharedRetrievalContext",
    );
}

#[then(regex = r#"^the result should contain a CrossPredicateCorrelation warning$"#)]
async fn result_contains_cross_predicate_correlation(world: &mut LocyWorld) {
    assert_compile_warning_present(
        world,
        uni_locy::types::WarningCode::CrossPredicateCorrelation,
        "CrossPredicateCorrelation",
    );
}

#[then(regex = r#"^the result should not contain a CrossPredicateCorrelation warning$"#)]
async fn result_does_not_contain_cross_predicate_correlation(world: &mut LocyWorld) {
    assert_compile_warning_absent(
        world,
        uni_locy::types::WarningCode::CrossPredicateCorrelation,
        "CrossPredicateCorrelation",
    );
}

#[then(regex = r#"^the result should not contain a PositiveComplementCorrelation warning$"#)]
async fn result_does_not_contain_positive_complement_correlation(world: &mut LocyWorld) {
    assert_compile_warning_absent(
        world,
        uni_locy::types::WarningCode::PositiveComplementCorrelation,
        "PositiveComplementCorrelation",
    );
}

#[then(regex = r#"^classifier ['"](.+)['"] should have been called at most (\d+) times$"#)]
async fn classifier_call_count_at_most(world: &mut LocyWorld, name: String, max_calls: usize) {
    let counter = world
        .classifier_call_counts
        .get(&name)
        .unwrap_or_else(|| panic!("no counting classifier registered as '{name}'"));
    let observed = counter.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        observed <= max_calls,
        "expected classifier '{name}' ≤ {max_calls} classify() calls, got {observed}"
    );
}

#[then(regex = r#"^the result should contain an UncalibratedNeuralPredicate warning$"#)]
async fn result_contains_uncalibrated_warning(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");
    let found = locy_result.compile_warnings().iter().any(|w| {
        matches!(
            w.code,
            uni_locy::types::WarningCode::UncalibratedNeuralPredicate
        )
    });
    assert!(
        found,
        "expected UncalibratedNeuralPredicate compile warning, got: {:?}",
        locy_result
            .compile_warnings()
            .iter()
            .map(|w| format!("{:?}", w.code))
            .collect::<Vec<_>>()
    );
}

/// Phase C F2: assert a `SharedNeuralInputArgument` (F2a) or
/// `SharedNeuralFeatureValue` (F2b) compile warning fired for the
/// named rule.
#[then(
    regex = r#"^the result should contain a (SharedNeuralInputArgument|SharedNeuralFeatureValue) warning for rule ['"](.+)['"]$"#
)]
async fn result_contains_shared_neural_warning(
    world: &mut LocyWorld,
    code: String,
    rule_name: String,
) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");
    let expected = match code.as_str() {
        "SharedNeuralInputArgument" => uni_locy::types::WarningCode::SharedNeuralInputArgument,
        "SharedNeuralFeatureValue" => uni_locy::types::WarningCode::SharedNeuralFeatureValue,
        _ => unreachable!(),
    };
    let found = locy_result
        .compile_warnings()
        .iter()
        .any(|w| w.code == expected && w.rule_name == rule_name);
    assert!(
        found,
        "expected {} warning for rule '{}', got: {:?}",
        code,
        rule_name,
        locy_result
            .compile_warnings()
            .iter()
            .map(|w| (format!("{:?}", w.code), w.rule_name.clone()))
            .collect::<Vec<_>>()
    );
}

#[then(
    regex = r#"^the result should not contain a (SharedNeuralInputArgument|SharedNeuralFeatureValue) warning$"#
)]
async fn result_does_not_contain_shared_neural_warning(world: &mut LocyWorld, code: String) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");
    let expected = match code.as_str() {
        "SharedNeuralInputArgument" => uni_locy::types::WarningCode::SharedNeuralInputArgument,
        "SharedNeuralFeatureValue" => uni_locy::types::WarningCode::SharedNeuralFeatureValue,
        _ => unreachable!(),
    };
    let found = locy_result
        .compile_warnings()
        .iter()
        .any(|w| w.code == expected);
    assert!(
        !found,
        "expected NO {} warning, but found: {:?}",
        code,
        locy_result
            .compile_warnings()
            .iter()
            .filter(|w| w.code == expected)
            .map(|w| w.rule_name.clone())
            .collect::<Vec<_>>()
    );
}

#[then("the result should not contain a FuzzyNotProbabilistic warning")]
async fn result_should_not_contain_fuzzy_warning(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result
        .warnings()
        .iter()
        .any(|w| w.code == uni_locy::RuntimeWarningCode::FuzzyNotProbabilistic);
    assert!(
        !found,
        "Expected no FuzzyNotProbabilistic warning, but found: {:?}",
        locy_result
            .warnings()
            .iter()
            .filter(|w| w.code == uni_locy::RuntimeWarningCode::FuzzyNotProbabilistic)
            .map(|w| format!("rule={}", w.rule_name))
            .collect::<Vec<_>>()
    );
}

#[then(regex = r#"^the result should contain a BddLimitExceeded warning for rule ['"](.+)['"]$"#)]
async fn result_should_contain_bdd_limit_warning(world: &mut LocyWorld, rule_name: String) {
    assert_warning_present(
        world,
        uni_locy::RuntimeWarningCode::BddLimitExceeded,
        &rule_name,
        "BddLimitExceeded",
    );
}

#[then("the result should not contain a TopKPruningCrossedDependency warning")]
async fn result_should_not_contain_topk_pruning_warning(world: &mut LocyWorld) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");

    let found = locy_result
        .warnings()
        .iter()
        .any(|w| w.code == uni_locy::RuntimeWarningCode::TopKPruningCrossedDependency);
    assert!(
        !found,
        "Expected no TopKPruningCrossedDependency warning, but found: {:?}",
        locy_result
            .warnings()
            .iter()
            .filter(|w| w.code == uni_locy::RuntimeWarningCode::TopKPruningCrossedDependency)
            .map(|w| format!("rule={}", w.rule_name))
            .collect::<Vec<_>>()
    );
}

#[then("the result should not contain a BddLimitExceeded warning")]
async fn result_should_not_contain_bdd_limit_warning(world: &mut LocyWorld) {
    assert_warning_absent(
        world,
        uni_locy::RuntimeWarningCode::BddLimitExceeded,
        "BddLimitExceeded",
    );
}

// ── CrossGroupCorrelationNotExact Assertions ────────────────────────────

#[then(
    regex = r#"^the result should contain a CrossGroupCorrelationNotExact warning for rule ['"](.+)['"]$"#
)]
async fn result_should_contain_cross_group_warning(world: &mut LocyWorld, rule_name: String) {
    assert_warning_present(
        world,
        uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact,
        &rule_name,
        "CrossGroupCorrelationNotExact",
    );
}

#[then("the result should not contain a CrossGroupCorrelationNotExact warning")]
async fn result_should_not_contain_cross_group_warning(world: &mut LocyWorld) {
    assert_warning_absent(
        world,
        uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact,
        "CrossGroupCorrelationNotExact",
    );
}

// ── BddLimitExceeded Metadata Assertions ────────────────────────────────

#[then(
    regex = r#"^the BddLimitExceeded warning for rule ['"](.+)['"] should have variable_count >= (\d+)$"#
)]
async fn bdd_warning_should_have_variable_count(
    world: &mut LocyWorld,
    rule_name: String,
    min_count: usize,
) {
    let locy_result = world.expect_locy_ok();

    let warning = locy_result.warnings().iter().find(|w| {
        w.code == uni_locy::RuntimeWarningCode::BddLimitExceeded && w.rule_name == rule_name
    });
    let warning = warning.unwrap_or_else(|| {
        panic!(
            "Expected BddLimitExceeded warning for rule '{}', but none found",
            rule_name
        )
    });
    let vc = warning.variable_count.unwrap_or_else(|| {
        panic!(
            "BddLimitExceeded warning for rule '{}' has no variable_count",
            rule_name
        )
    });
    assert!(
        vc >= min_count,
        "Expected variable_count >= {} for rule '{}', but got {}",
        min_count,
        rule_name,
        vc
    );
}

#[then(
    regex = r#"^the CrossGroupCorrelationNotExact warning for rule ['"](.+)['"] should have variable_count >= (\d+)$"#
)]
async fn crossgroup_warning_should_have_variable_count(
    world: &mut LocyWorld,
    rule_name: String,
    min_count: usize,
) {
    let locy_result = world
        .locy_result()
        .expect("no evaluation result")
        .as_ref()
        .expect("evaluation failed");
    let warning = locy_result.warnings().iter().find(|w| {
        w.code == uni_locy::RuntimeWarningCode::CrossGroupCorrelationNotExact
            && w.rule_name == rule_name
    });
    let warning = warning.unwrap_or_else(|| {
        panic!(
            "Expected CrossGroupCorrelationNotExact warning for rule '{}', but none found",
            rule_name
        )
    });
    let vc = warning.variable_count.unwrap_or_else(|| {
        panic!(
            "CrossGroupCorrelationNotExact warning for rule '{}' has no variable_count",
            rule_name
        )
    });
    assert!(
        vc >= min_count,
        "Expected variable_count >= {} for rule '{}', but got {}",
        min_count,
        rule_name,
        vc
    );
}

#[then(regex = r#"^the BddLimitExceeded warning for rule ['"](.+)['"] should have a key_group$"#)]
async fn bdd_warning_should_have_key_group(world: &mut LocyWorld, rule_name: String) {
    let locy_result = world.expect_locy_ok();

    let warning = locy_result.warnings().iter().find(|w| {
        w.code == uni_locy::RuntimeWarningCode::BddLimitExceeded && w.rule_name == rule_name
    });
    let warning = warning.unwrap_or_else(|| {
        panic!(
            "Expected BddLimitExceeded warning for rule '{}', but none found",
            rule_name
        )
    });
    assert!(
        warning.key_group.is_some(),
        "BddLimitExceeded warning for rule '{}' has no key_group field",
        rule_name
    );
}

// ── Approximate Fact Assertions ──────────────────────────────────────────

#[then(regex = r#"^the derived relation ['"](.+)['"] should not contain any approximate facts$"#)]
async fn derived_relation_should_not_contain_approximate_facts(
    world: &mut LocyWorld,
    relation: String,
) {
    let locy_result = world.expect_locy_ok();

    let facts = locy_result
        .derived
        .get(&relation)
        .unwrap_or_else(|| panic!("No derived relation '{}'", relation));

    let approximate_count = facts
        .iter()
        .filter(|row| {
            row.get("_approximate")
                .map(|v| matches!(v, Value::Bool(true)))
                .unwrap_or(false)
        })
        .count();

    assert!(
        approximate_count == 0,
        "Expected no approximate facts in '{}', but found {} approximate fact(s)",
        relation,
        approximate_count
    );
}

#[then(
    regex = r#"^the command result (\d+) should be an Explain where child (\d+) has proof_probability approximately (.+)$"#
)]
async fn command_result_explain_child_proof_probability(
    world: &mut LocyWorld,
    idx: usize,
    child_idx: usize,
    expected_str: String,
) {
    match world.expect_command_result(idx) {
        CommandResult::Explain(node) => {
            let child = node
                .children
                .get(child_idx)
                .unwrap_or_else(|| panic!("No child at index {}", child_idx));
            let expected: f64 = expected_str.parse().expect("Invalid float");
            let actual = child.proof_probability.unwrap_or_else(|| {
                panic!(
                    "Child {} has no proof_probability (expected ~{})",
                    child_idx, expected
                )
            });
            assert!(
                (actual - expected).abs() < 1e-6,
                "Child {} proof_probability: expected ~{}, got {}",
                child_idx,
                expected,
                actual
            );
        }
        other => panic!(
            "Expected command result {} to be an Explain, got {:?}",
            idx, other
        ),
    }
}

/// Phase C B1–B3: assert that the EXPLAIN derivation tree's
/// child at `child_idx` has a `NeuralProvenance` entry for the
/// named model with raw_probability matching `expected` within
/// 1e-6.
#[then(
    regex = r#"^the command result (\d+) should be an Explain where child (\d+) has a neural call for ['"](.+)['"] with raw_probability approximately (-?\d+(?:\.\d+)?)$"#
)]
async fn command_result_explain_child_neural_call(
    world: &mut LocyWorld,
    idx: usize,
    child_idx: usize,
    model_name: String,
    expected: f64,
) {
    let call = find_explain_neural_call(world, idx, child_idx, &model_name);
    assert!(
        (call.raw_probability - expected).abs() < 1e-6,
        "model '{}' raw_probability: expected {}, got {}",
        model_name,
        expected,
        call.raw_probability
    );
}

/// Phase C B1-B3 follow-up: assert the EXPLAIN child has a
/// confidence band on the named model's neural call.
#[then(
    regex = r#"^the command result (\d+) should be an Explain where child (\d+) has a neural call for ['"](.+)['"] with confidence_band$"#
)]
async fn command_result_explain_child_neural_call_has_band(
    world: &mut LocyWorld,
    idx: usize,
    child_idx: usize,
    model_name: String,
) {
    let call = find_explain_neural_call(world, idx, child_idx, &model_name);
    assert!(
        call.confidence_band.is_some(),
        "model '{}' expected confidence_band, got None",
        model_name
    );
}

/// Phase C B1-B3 follow-up: assert calibrated_probability is set.
#[then(
    regex = r#"^the command result (\d+) should be an Explain where child (\d+) has a neural call for ['"](.+)['"] with calibrated_probability set$"#
)]
async fn command_result_explain_child_neural_call_has_calibrated(
    world: &mut LocyWorld,
    idx: usize,
    child_idx: usize,
    model_name: String,
) {
    let call = find_explain_neural_call(world, idx, child_idx, &model_name);
    assert!(
        call.calibrated_probability.is_some(),
        "model '{}' expected calibrated_probability=Some, got None",
        model_name
    );
}

#[then(
    regex = r#"^the command result (\d+) should be an Explain where all children have proof_probability$"#
)]
async fn command_result_explain_all_children_have_proof_probability(
    world: &mut LocyWorld,
    idx: usize,
) {
    match world.expect_command_result(idx) {
        CommandResult::Explain(node) => {
            for (i, child) in node.children.iter().enumerate() {
                assert!(
                    child.proof_probability.is_some(),
                    "Child {} is missing proof_probability",
                    i
                );
            }
        }
        other => panic!(
            "Expected command result {} to be an Explain, got {:?}",
            idx, other
        ),
    }
}
