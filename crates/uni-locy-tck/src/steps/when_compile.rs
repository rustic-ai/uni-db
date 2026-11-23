use crate::LocyWorld;
use cucumber::when;
use uni_common::UniError;

#[when("compiling the following Locy program:")]
async fn when_compiling_locy_program(world: &mut LocyWorld, step: &cucumber::gherkin::Step) {
    let program = step
        .docstring()
        .expect("Expected a docstring with the Locy program to compile");

    let result = uni_cypher::parse_locy(program);
    match result {
        Err(e) => {
            world.set_compile_result(Err(UniError::Parse {
                message: format!("LocyParseError: {e}"),
                position: None,
                line: None,
                column: None,
                context: None,
            }));
        }
        Ok(ast) => {
            // Compile through the registry-backed oracle, matching every host
            // path. A bare `uni_locy::compile` uses the six-name default, so
            // the TCK could not exercise the registry at all — which is how
            // this feature's prose came to describe behaviour the suite never
            // checked.
            let plugins = uni_query::query::df_graph::locy_fold::default_locy_plugin_registry();
            let oracle = |name: &str| {
                uni_query::query::df_graph::locy_fold::locy_monotonicity_verdict(&plugins, name)
            };
            let direction = |name: &str| {
                uni_query::query::df_graph::locy_fold::locy_direction_verdict(&plugins, name)
            };
            let compile_result = uni_locy::compile_with_oracles_and_config(
                &ast,
                &std::collections::HashMap::new(),
                &[],
                &uni_locy::LocyConfig {
                    neural_predicates_preview: false,
                    ..Default::default()
                },
                &oracle,
                &direction,
            )
            .map_err(|e| UniError::Query {
                message: format!("LocyCompileError: {e}"),
                query: None,
            });
            // A program the compiler accepts must plan: a planner "No field
            // named" is a scope error the compiler should have reported. Most
            // compile-only scenarios set up no data, so any other evaluation
            // outcome is allowed.
            if compile_result.is_ok() {
                if let Err(e) = world.db().session().locy(program).await {
                    let message = e.to_string();
                    assert!(
                        !message.contains("No field named"),
                        "the program compiled but failed scope in the planner: {message}\n\
                         Program:\n{program}"
                    );
                }
            }
            world.set_compile_result(compile_result);
        }
    }
}
