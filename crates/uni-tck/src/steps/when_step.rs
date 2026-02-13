use crate::UniWorld;
use cucumber::when;

#[when("executing query:")]
async fn executing_query(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(query) = step.docstring() else {
        return;
    };

    if let Err(e) = world.capture_state_before().await {
        panic!("Failed to capture state before: {}", e);
    }

    // Build query with parameters if any are set
    let mut query_builder = world.db().query_with(query);
    for (key, value) in world.params() {
        query_builder = query_builder.param(key, value.clone());
    }

    match query_builder.fetch_all().await {
        Ok(result) => {
            world.set_result(result);
            if let Err(e) = world.capture_state_after().await {
                panic!("Failed to capture state after: {}", e);
            }
        }
        Err(e) => world.set_error(e),
    }
}

#[when("executing control query:")]
async fn executing_control_query(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(query) = step.docstring() else {
        return;
    };

    if let Err(e) = world.capture_state_before().await {
        panic!("Failed to capture state before: {}", e);
    }

    // Build query with parameters if any are set
    let mut query_builder = world.db().query_with(query);
    for (key, value) in world.params() {
        query_builder = query_builder.param(key, value.clone());
    }

    match query_builder.fetch_all().await {
        Ok(result) => {
            world.set_result(result);
            if let Err(e) = world.capture_state_after().await {
                panic!("Failed to capture state after: {}", e);
            }
        }
        Err(e) => world.set_error(e),
    }
}

#[when(regex = r"^executing query with parameters (.+):$")]
async fn executing_query_with_params(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(query) = step.docstring() else {
        return;
    };

    if let Err(e) = world.capture_state_before().await {
        panic!("Failed to capture state before: {}", e);
    }

    // Build query with parameters
    let mut query_builder = world.db().query_with(query);
    for (key, value) in world.params() {
        query_builder = query_builder.param(key, value.clone());
    }

    match query_builder.fetch_all().await {
        Ok(result) => {
            world.set_result(result);
            if let Err(e) = world.capture_state_after().await {
                panic!("Failed to capture state after: {}", e);
            }
        }
        Err(e) => world.set_error(e),
    }
}
