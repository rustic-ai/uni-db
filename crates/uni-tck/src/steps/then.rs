use crate::matcher::{match_error, match_result, match_result_unordered, ErrorPhase, TckErrorType};
use crate::parser::parse_table;
use crate::UniWorld;
use cucumber::then;

#[then("the result should be empty")]
async fn result_should_be_empty(world: &mut UniWorld) {
    let result = world.result().expect("No result found");
    assert_eq!(
        result.len(),
        0,
        "Expected empty result, but got {} rows",
        result.len()
    );
}

#[then(regex = r"^the result should be, in any order:$")]
async fn result_should_be_in_any_order(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let result = world.result().expect("No result found");
    let table = step.table().expect("Step is missing a data table");
    let expected_rows = parse_table(table).expect("Failed to parse expected table");

    if let Err(msg) = match_result_unordered(result, &expected_rows) {
        panic!("Result mismatch (any order): {}", msg);
    }
}

#[then(regex = r"^the result should be, in order:$")]
async fn result_should_be_in_order(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let result = world.result().expect("No result found");
    let table = step.table().expect("Step is missing a data table");
    let expected_rows = parse_table(table).expect("Failed to parse expected table");

    if let Err(msg) = match_result(result, &expected_rows) {
        panic!("Result mismatch (in order): {}", msg);
    }
}

#[then(regex = r"^a (\w+) should be raised at (compile time|runtime): (\w+)$")]
async fn error_should_be_raised(
    world: &mut UniWorld,
    error_type: String,
    phase: String,
    detail_code: String,
) {
    let error = world.error().expect("No error found");

    let expected_type: TckErrorType = error_type.parse().unwrap();
    let expected_phase = match phase.as_str() {
        "compile time" => ErrorPhase::CompileTime,
        "runtime" => ErrorPhase::Runtime,
        other => panic!("Unknown error phase: {}", other),
    };

    if let Err(msg) = match_error(error, expected_type, expected_phase, Some(&detail_code)) {
        panic!("Error mismatch: {}", msg);
    }
}
