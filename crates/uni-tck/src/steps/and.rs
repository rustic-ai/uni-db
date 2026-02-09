use crate::UniWorld;
use cucumber::then;

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
async fn side_effects_should_be(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    let Some(table) = step.table() else {
        return;
    };

    let effects = world.side_effects();

    // Table format is: | header | value | (each row is a header-value pair)
    for row in &table.rows {
        if row.len() < 2 {
            continue;
        }
        let header = row[0].trim();
        let expected: i64 = row[1].trim().parse().unwrap_or(0);

        match header {
            "+nodes" => {
                let actual = effects.nodes_after as i64 - effects.nodes_before as i64;
                assert_eq!(
                    actual, expected,
                    "Expected +nodes={}, but got {} (before={}, after={})",
                    expected, actual, effects.nodes_before, effects.nodes_after
                );
            }
            "-nodes" => {
                let actual = effects.nodes_before as i64 - effects.nodes_after as i64;
                assert_eq!(
                    actual, expected,
                    "Expected -nodes={}, but got {} (before={}, after={})",
                    expected, actual, effects.nodes_before, effects.nodes_after
                );
            }
            "+relationships" | "+edges" => {
                let actual = effects.edges_after as i64 - effects.edges_before as i64;
                assert_eq!(
                    actual, expected,
                    "Expected +relationships={}, but got {} (before={}, after={})",
                    expected, actual, effects.edges_before, effects.edges_after
                );
            }
            "-relationships" | "-edges" => {
                let actual = effects.edges_before as i64 - effects.edges_after as i64;
                assert_eq!(
                    actual, expected,
                    "Expected -relationships={}, but got {} (before={}, after={})",
                    expected, actual, effects.edges_before, effects.edges_after
                );
            }
            "+labels" => {
                let new_labels: Vec<_> = effects
                    .labels_after
                    .difference(&effects.labels_before)
                    .collect();
                let actual = new_labels.len() as i64;
                assert_eq!(
                    actual, expected,
                    "Expected +labels={}, but got {} (new labels: {:?})",
                    expected, actual, new_labels
                );
            }
            "-labels" => {
                let removed_labels: Vec<_> = effects
                    .labels_before
                    .difference(&effects.labels_after)
                    .collect();
                let actual = removed_labels.len() as i64;
                assert_eq!(
                    actual, expected,
                    "Expected -labels={}, but got {} (removed labels: {:?})",
                    expected, actual, removed_labels
                );
            }
            "+properties" => {
                let actual = effects.properties_after as i64 - effects.properties_before as i64;
                assert_eq!(
                    actual, expected,
                    "Expected +properties={}, but got {} (before={}, after={})",
                    expected, actual, effects.properties_before, effects.properties_after
                );
            }
            "-properties" => {
                let actual = effects.properties_before as i64 - effects.properties_after as i64;
                assert_eq!(
                    actual, expected,
                    "Expected -properties={}, but got {} (before={}, after={})",
                    expected, actual, effects.properties_before, effects.properties_after
                );
            }
            _ => {
                // Unknown column - ignore silently (may be comments or other data)
            }
        }
    }
}
