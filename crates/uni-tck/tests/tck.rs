//! libtest-mimic harness that exposes every TCK scenario as an individual test
//! for `cargo nextest`. Each scenario is discovered from `.feature` files,
//! Scenario Outlines are expanded, and each is run through the cucumber
//! framework's `filter_run` with a name+line filter.
//!
//! Each scenario writes a result JSON to `target/cucumber/nextest/` so that
//! results can be aggregated into a report after the nextest run.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use cucumber::{writer::Stats, World};
use gherkin::GherkinEnv;
use libtest_mimic::{Arguments, Failed, Trial};
use regex::Regex;
use uni_tck::UniWorld;

fn main() {
    let args = Arguments::from_args();

    let feature_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tck/features");
    let scenarios = discover_scenarios(&feature_dir);

    // Build base test names and detect duplicates
    let base_names: Vec<String> = scenarios
        .iter()
        .map(|(fp, sn, _)| make_test_name(&feature_dir, fp, sn))
        .collect();

    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for name in &base_names {
        *name_counts.entry(name.clone()).or_default() += 1;
    }

    // For duplicate names, append @L<line> to disambiguate
    let mut name_index: HashMap<String, usize> = HashMap::new();
    let tests: Vec<Trial> = scenarios
        .into_iter()
        .zip(base_names)
        .map(|((feature_path, scenario_name, scenario_line), base_name)| {
            let test_name = if name_counts[&base_name] > 1 {
                let idx = name_index.entry(base_name.clone()).or_default();
                *idx += 1;
                format!("{base_name} @L{scenario_line}")
            } else {
                base_name
            };
            let fp = feature_path.clone();
            let sn = scenario_name.clone();
            Trial::test(test_name, move || {
                run_single_scenario(fp, sn, scenario_line)
            })
        })
        .collect();

    libtest_mimic::run(&args, tests).exit();
}

/// Walk the feature directory and parse all `.feature` files, expanding
/// Scenario Outlines into individual scenarios.
fn discover_scenarios(feature_dir: &Path) -> Vec<(PathBuf, String, usize)> {
    let mut results = Vec::new();
    let mut feature_files: Vec<PathBuf> = Vec::new();

    collect_feature_files(feature_dir, &mut feature_files);
    feature_files.sort();

    for path in feature_files {
        match gherkin::Feature::parse_path(&path, GherkinEnv::default()) {
            Ok(feature) => {
                // Collect scenarios from top-level
                collect_expanded_scenarios(&feature.scenarios, &path, &mut results);
                // Collect scenarios from rules
                for rule in &feature.rules {
                    collect_expanded_scenarios(&rule.scenarios, &path, &mut results);
                }
            }
            Err(e) => {
                eprintln!("Warning: failed to parse {}: {e}", path.display());
            }
        }
    }

    results
}

/// Recursively collect `.feature` files from a directory.
fn collect_feature_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_feature_files(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("feature") {
            out.push(path);
        }
    }
}

/// Expand Scenario Outlines in a list of scenarios and collect the results.
///
/// Replicates cucumber's expansion logic from `feature.rs`:
/// - For scenarios without examples: use as-is
/// - For scenarios with examples: expand each row, adjusting
///   `position.line = examples_position.line + row_id + 2`
fn collect_expanded_scenarios(
    scenarios: &[gherkin::Scenario],
    feature_path: &Path,
    out: &mut Vec<(PathBuf, String, usize)>,
) {
    let template_re = Regex::new(r"<([^>\s]+)>").expect("valid regex");

    for scenario in scenarios {
        if scenario.examples.is_empty() {
            // Plain scenario, no expansion needed
            out.push((
                feature_path.to_path_buf(),
                scenario.name.clone(),
                scenario.position.line,
            ));
        } else {
            // Scenario Outline: expand each examples table row
            for example in &scenario.examples {
                let table = match &example.table {
                    Some(t) => t,
                    None => continue,
                };
                let (header, rows) = match table.rows.split_first() {
                    Some(pair) => pair,
                    None => continue,
                };

                for (id, row) in rows.iter().enumerate() {
                    // Replicate cucumber's line calculation:
                    // expanded.position = example.position;
                    // expanded.position.line += id + 2;
                    let expanded_line = example.position.line + id + 2;

                    // Expand template placeholders in the scenario name
                    let expanded_name =
                        template_re
                            .replace_all(&scenario.name, |cap: &regex::Captures<'_>| {
                                let placeholder = cap.get(1).unwrap().as_str();
                                header
                                    .iter()
                                    .zip(row.iter())
                                    .find_map(|(h, v)| {
                                        if h == placeholder {
                                            Some(v.as_str())
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or("")
                            });

                    out.push((
                        feature_path.to_path_buf(),
                        expanded_name.into_owned(),
                        expanded_line,
                    ));
                }
            }
        }
    }
}

/// Build a human-readable test name from the feature path and scenario.
///
/// Format: `clauses::match::Match1::[1] Match non-existent nodes returns empty`
fn make_test_name(feature_dir: &Path, feature_path: &Path, scenario_name: &str) -> String {
    let relative = feature_path
        .strip_prefix(feature_dir)
        .unwrap_or(feature_path);

    // Strip .feature extension and convert path separators to ::
    let stem = relative.with_extension("");
    let path_part = stem
        .components()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("::");

    // Sanitize scenario name: replace characters that could confuse test filtering
    let sanitized_name = scenario_name
        .replace("::", "__")
        .replace('\n', " ")
        .replace('\r', "");

    format!("{path_part}::{sanitized_name}")
}

/// Run a single scenario through the cucumber framework.
fn run_single_scenario(
    feature_path: PathBuf,
    scenario_name: String,
    scenario_line: usize,
) -> Result<(), Failed> {
    // Initialize tracing (ignore errors if already initialized)
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();

    let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create runtime: {e}"))?;

    let fp = feature_path.clone();
    let sn = scenario_name.clone();
    let failed = rt.block_on(async move {
        let writer = UniWorld::cucumber()
            .with_default_cli()
            .fail_on_skipped()
            .max_concurrent_scenarios(Some(1))
            .filter_run(fp, move |_feat, _rule, sc| {
                sc.name == sn && sc.position.line == scenario_line
            })
            .await;

        writer.execution_has_failed()
    });

    let status = if failed { "failed" } else { "passed" };
    write_result_json(&feature_path, &scenario_name, scenario_line, status);

    if failed {
        Err(format!("Scenario failed: {scenario_name}").into())
    } else {
        Ok(())
    }
}

/// Write a per-scenario result JSON to `target/cucumber/nextest/`.
///
/// Each file is named `{line}_{hash}.json` where hash is derived from
/// the feature path and scenario line to ensure uniqueness across
/// concurrent processes.
fn write_result_json(
    feature_path: &Path,
    scenario_name: &str,
    scenario_line: usize,
    status: &str,
) {
    let results_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("target/cucumber/nextest");

    // Best-effort: don't fail the test if we can't write the result
    let _ = std::fs::create_dir_all(&results_dir);

    // Build a unique filename from feature path + line
    let feature_stem = feature_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let filename = format!("{}_{}.json", feature_stem, scenario_line);

    let result = serde_json::json!({
        "feature_path": feature_path.to_string_lossy(),
        "scenario_name": scenario_name,
        "line": scenario_line,
        "status": status,
    });

    if let Ok(mut f) = std::fs::File::create(results_dir.join(&filename)) {
        let _ = f.write_all(result.to_string().as_bytes());
    }
}
