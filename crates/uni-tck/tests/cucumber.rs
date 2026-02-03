use cucumber::World;
use uni_tck::UniWorld;

#[tokio::main]
async fn main() {
    // Configure tracing for debugging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .init();

    UniWorld::cucumber()
        .fail_on_skipped()
        .run("features/")
        .await;
}
