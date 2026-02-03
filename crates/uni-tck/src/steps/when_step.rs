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

    match world.db().query(query).await {
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

    // TODO: pass params via .param() calls on the query builder
    match world.db().query_with(query).fetch_all().await {
        Ok(result) => {
            world.set_result(result);
            if let Err(e) = world.capture_state_after().await {
                panic!("Failed to capture state after: {}", e);
            }
        }
        Err(e) => world.set_error(e),
    }
}
