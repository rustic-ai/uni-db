use cucumber::{World, WriterExt};
use std::fs;
use uni_tck::UniWorld;

#[tokio::main]
async fn main() {
    // Configure tracing for debugging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .init();

    // Create output directory for reports (use absolute path from crate root)
    let output_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/cucumber");

    fs::create_dir_all(&output_dir).expect("Failed to create cucumber output directory");

    let json_path = output_dir.join("results.json");
    eprintln!("📝 Writing JSON results to: {}", json_path.display());

    // Run tests with JSON output
    UniWorld::cucumber()
        .fail_on_skipped()
        .with_writer(
            cucumber::writer::Json::for_tee(
                fs::File::create(&json_path).expect("Failed to create JSON output file"),
            )
            .normalized(),
        )
        .run("tck/features/")
        .await;
}
