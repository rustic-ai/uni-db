use cucumber::then;
use crate::UniWorld;

#[then("no side effects")]
async fn no_side_effects(world: &mut UniWorld) {
    let effects = world.side_effects();

    assert_eq!(
        effects.nodes_before, effects.nodes_after,
        "Node count changed: {} -> {}",
        effects.nodes_before, effects.nodes_after
    );

    assert_eq!(
        effects.edges_before, effects.edges_after,
        "Edge count changed: {} -> {}",
        effects.edges_before, effects.edges_after
    );

    assert_eq!(
        effects.labels_before, effects.labels_after,
        "Labels changed: {:?} -> {:?}",
        effects.labels_before, effects.labels_after
    );
}

#[then(regex = r"^the side effects should be:$")]
async fn side_effects_should_be(_world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    if let Some(_table) = step.table() {
        // TODO: Parse table and verify side effects
        // Table format example:
        // | +nodes | -edges | +labels |
        // |    2   |    1   |    1    |
        todo!("Verify side effects from table");
    }
}
