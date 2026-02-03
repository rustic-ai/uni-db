use crate::fixtures::load_graph;
use crate::UniWorld;
use cucumber::given;

#[given("an empty graph")]
async fn an_empty_graph(world: &mut UniWorld) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
}

#[given("any graph")]
async fn any_graph(world: &mut UniWorld) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
}

#[given(regex = r"^the (.+) graph$")]
async fn named_graph(world: &mut UniWorld, graph_name: String) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");
    load_graph(world.db(), &graph_name)
        .await
        .unwrap_or_else(|e| panic!("Failed to load graph '{}': {}", graph_name, e));
}

#[given("having executed:")]
async fn having_executed(world: &mut UniWorld, step: &cucumber::gherkin::Step) {
    world
        .init_db()
        .await
        .expect("Failed to initialize database");

    if let Some(query) = step.docstring() {
        world.db().execute(query).await.unwrap_or_else(|e| {
            panic!("Setup query failed: {}", e);
        });
    }
}
