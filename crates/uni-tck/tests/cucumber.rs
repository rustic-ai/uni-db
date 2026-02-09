use cucumber::{World, WriterExt};
use std::fs;
use uni_tck::UniWorld;

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
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

    // Use environment variable for feature filtering
    // Usage: TCK_FEATURE=boolean cargo test --package uni-tck --test cucumber
    let feature_path = if let Ok(arg) = std::env::var("TCK_FEATURE") {
        if arg.starts_with("tck/") {
            arg
        } else {
            // Check common shortcuts
            match arg.as_str() {
                "boolean" => "tck/features/expressions/boolean".to_string(),
                "comparison" => "tck/features/expressions/comparison".to_string(),
                "match" => "tck/features/clauses/match".to_string(),
                "where" => "tck/features/clauses/match-where".to_string(), // TCK structure varies
                _ => format!("tck/features/{}", arg),                      // Try generic mapping
            }
        }
    } else {
        "tck/features/".to_string()
    };

    eprintln!("🚀 Running features from: {}", feature_path);

    // Run tests with JSON output and scenario timeout
    rt.block_on(async {
        UniWorld::cucumber()
            .fail_on_skipped()
            .max_concurrent_scenarios(Some(16)) // Parallel execution
            .with_writer(
                cucumber::writer::Json::for_tee(
                    fs::File::create(&json_path).expect("Failed to create JSON output file"),
                )
                .normalized(),
            )
            .before(move |_feature, _rule, scenario, _world| {
                let scenario_name = scenario.name.clone();
                Box::pin(async move {
                    eprintln!("▶ Starting: {}", scenario_name);
                })
            })
            .after(move |_feature, _rule, scenario, _ev, _world| {
                let scenario_name = scenario.name.clone();
                Box::pin(async move {
                    eprintln!("✓ Finished: {}", scenario_name);
                })
            })
            .run(feature_path)
            .await;
    });

    // Force exit to avoid runtime hanging.
    // The cucumber framework holds onto world instances until after all tests complete,
    // preventing Uni Drop from being called in time. Since background tasks are disabled
    // for TCK tests (see UniWorld::init_db), this is safe.
    std::process::exit(0);
}
