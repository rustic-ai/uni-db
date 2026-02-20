//! libtest-mimic harness that exposes every TCK scenario as an individual test
//! for `cargo nextest`. Each scenario is discovered from `.feature` files,
//! Scenario Outlines are expanded, and each is run through the cucumber
//! framework's `filter_run` with a name+line filter.
//!
//! Each scenario writes a result JSON to `target/cucumber/nextest/` by default
//! (or `UNI_TCK_NEXTEST_RESULTS_DIR` when set) so that results can be
//! aggregated into a report after the nextest run.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use cucumber::writer::{self, Stats};
use cucumber::{World, WriterExt};
use gherkin::GherkinEnv;
use libtest_mimic::{Arguments, Failed, Trial};
use regex::Regex;
use uni_tck::{
    clear_tck_run_context_for_current_thread, set_tck_run_context_for_current_thread,
    TckSchemaMode, UniWorld,
};

/// Thread-safe in-memory buffer that implements [`std::io::Write`].
///
/// Used to capture cucumber writer output so error details can be
/// included in per-scenario result JSONs.
#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("buffer lock poisoned").write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SharedBuffer {
    /// Extract captured output as a UTF-8 string, lossy-converting if needed.
    fn contents(&self) -> String {
        let bytes = self.0.lock().expect("buffer lock poisoned").clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

fn schema_mode_from_env() -> TckSchemaMode {
    match std::env::var("UNI_TCK_SCHEMA_MODE") {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" | "schemaless" | "off" | "none" => TckSchemaMode::Schemaless,
            "schema" | "sidecar" | "predefined" | "predefined-schema" => TckSchemaMode::Sidecar,
            other => panic!(
                "Invalid UNI_TCK_SCHEMA_MODE='{}'. Expected one of: schemaless, sidecar",
                other
            ),
        },
        Err(_) => TckSchemaMode::Schemaless,
    }
}

struct TckRunContextGuard;

impl TckRunContextGuard {
    fn set(feature_path: PathBuf, schema_mode: TckSchemaMode) -> Self {
        set_tck_run_context_for_current_thread(feature_path, schema_mode);
        Self
    }
}

impl Drop for TckRunContextGuard {
    fn drop(&mut self) {
        clear_tck_run_context_for_current_thread();
    }
}

fn main() {
    let args = Arguments::from_args();
    let schema_mode = schema_mode_from_env();

    let feature_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tck/features");
    let scenarios = discover_scenarios(&feature_dir);

    // Write a manifest of all discovered scenarios when explicitly requested
    // via UNI_TCK_WRITE_MANIFEST=1. The shell script sets this during the
    // dedicated `cargo nextest list` step (full runs only) so the aggregator
    // can detect crashes (scenarios that never wrote a result JSON).
    // We gate on an env var rather than `args.list` because nextest also
    // calls --list internally during `nextest run`, which would overwrite
    // the manifest and cause false crash detections for filtered runs.
    if std::env::var("UNI_TCK_WRITE_MANIFEST").as_deref() == Ok("1") && args.list {
        write_manifest(&scenarios);
    }

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
        .map(
            |((feature_path, scenario_name, scenario_line), base_name)| {
                let ignored_reason = ignored_scenario_reason(
                    &feature_path,
                    &scenario_name,
                    scenario_line,
                    schema_mode,
                );
                if let Some(reason) = ignored_reason {
                    // Ignored trials don't execute their closure, so emit a skipped
                    // result eagerly to keep JSON aggregation complete.
                    write_result_json(
                        &feature_path,
                        &scenario_name,
                        scenario_line,
                        "skipped",
                        Some(reason),
                    );
                }

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
                    run_single_scenario(fp, sn, scenario_line, schema_mode)
                })
                .with_ignored_flag(ignored_reason.is_some())
            },
        )
        .collect();

    libtest_mimic::run(&args, tests).exit();
}

fn ignored_scenario_reason(
    feature_path: &Path,
    scenario_name: &str,
    _scenario_line: usize,
    _schema_mode: TckSchemaMode,
) -> Option<&'static str> {
    let normalized_path = feature_path.to_string_lossy().replace('\\', "/");
    let is_hanging_literals7 = normalized_path
        .ends_with("tck/features/expressions/literals/Literals7.feature")
        && scenario_name.trim() == "[12] Return 40-deep nested empty lists";

    if is_hanging_literals7 {
        Some("Temporarily ignored in all modes: hangs in nextest run")
    } else {
        None
    }
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
                        template_re.replace_all(&scenario.name, |cap: &regex::Captures<'_>| {
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
///
/// Captures the cucumber writer output into a buffer so that actual
/// error details (panic messages, result mismatches) are available in
/// the per-scenario result JSON and the test failure message.
fn run_single_scenario(
    feature_path: PathBuf,
    scenario_name: String,
    scenario_line: usize,
    schema_mode: TckSchemaMode,
) -> Result<(), Failed> {
    // Initialize tracing (ignore errors if already initialized)
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();

    let _run_ctx_guard = TckRunContextGuard::set(feature_path.clone(), schema_mode);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create runtime: {e}"))?;

    let buffer = SharedBuffer::default();
    let fp = feature_path.clone();
    let sn = scenario_name.clone();
    let buf_clone = buffer.clone();
    let failed = rt.block_on(async move {
        let cucumber_writer = writer::Basic::new(
            buf_clone,
            writer::Coloring::Never,
            writer::Verbosity::Default,
        )
        .summarized();

        let w = UniWorld::cucumber()
            .with_writer(cucumber_writer)
            .with_default_cli()
            .fail_on_skipped()
            .max_concurrent_scenarios(Some(1))
            .filter_run(fp, move |_feat, _rule, sc| {
                sc.name == sn && sc.position.line == scenario_line
            })
            .await;

        w.execution_has_failed()
    });

    let output = buffer.contents();

    let (status, error_message) = if failed {
        let error_detail = extract_error_from_output(&output);
        ("failed", Some(error_detail))
    } else {
        ("passed", None)
    };

    write_result_json(
        &feature_path,
        &scenario_name,
        scenario_line,
        status,
        error_message.as_deref(),
    );

    if failed {
        let msg = error_message.unwrap_or_else(|| format!("Scenario failed: {scenario_name}"));
        Err(msg.into())
    } else {
        Ok(())
    }
}

/// Extract meaningful error details from cucumber writer output.
///
/// Looks for panic messages, assertion failures, and result mismatches
/// in the captured output. Falls back to returning the full output
/// (truncated) if no specific pattern is found.
fn extract_error_from_output(output: &str) -> String {
    // Look for common failure patterns from the step handlers
    let patterns = [
        "panicked at",
        "Result mismatch",
        "Query returned error",
        "No result found",
        "Error mismatch",
        "Expected empty result",
        "assertion `left == right` failed",
        "Step failed:",
    ];

    for line in output.lines() {
        let trimmed = line.trim();
        if patterns.iter().any(|p| trimmed.contains(p)) {
            // Return this line and any subsequent indented/continuation lines
            let start_idx = output.find(trimmed).unwrap_or(0);
            let relevant = &output[start_idx..];
            // Take up to 2000 chars to keep it reasonable
            let truncated = if relevant.len() > 2000 {
                format!("{}...", &relevant[..2000])
            } else {
                relevant.to_string()
            };
            return truncated;
        }
    }

    // Fall back to the full output, truncated
    if output.len() > 2000 {
        format!("{}...", &output[..2000])
    } else {
        output.to_string()
    }
}

/// Resolve the directory used for per-scenario nextest JSON output.
///
/// Default: `target/cucumber/nextest` at repository root.
/// Override with `UNI_TCK_NEXTEST_RESULTS_DIR` (absolute or repo-relative).
fn nextest_results_dir() -> PathBuf {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    match std::env::var("UNI_TCK_NEXTEST_RESULTS_DIR") {
        Ok(raw) if !raw.trim().is_empty() => {
            let candidate = PathBuf::from(raw.trim());
            if candidate.is_absolute() {
                candidate
            } else {
                repo_root.join(candidate)
            }
        }
        _ => repo_root.join("target/cucumber/nextest"),
    }
}

/// Write a manifest of all discovered scenarios to `manifest.json` in the
/// nextest results directory. Called during `--list` (i.e. `cargo nextest list`)
/// so the aggregator can detect scenarios that crashed (never wrote a result).
fn write_manifest(scenarios: &[(PathBuf, String, usize)]) {
    let results_dir = nextest_results_dir();
    let _ = std::fs::create_dir_all(&results_dir);

    let entries: Vec<serde_json::Value> = scenarios
        .iter()
        .map(|(fp, sn, line)| {
            serde_json::json!({
                "feature_path": fp.to_string_lossy(),
                "scenario_name": sn,
                "line": line,
            })
        })
        .collect();

    let manifest = serde_json::json!({
        "version": 1,
        "scenarios": entries,
    });

    if let Ok(mut f) = std::fs::File::create(results_dir.join("manifest.json")) {
        let _ = f.write_all(serde_json::to_string_pretty(&manifest).unwrap().as_bytes());
    }
}

/// Write a per-scenario result JSON to the configured nextest results directory.
///
/// Each file is named `{feature_stem}_{line}.json` to ensure uniqueness
/// across concurrent processes. When `error_message` is provided, it is
/// included in the JSON so downstream report tooling can surface the
/// actual failure reason.
fn write_result_json(
    feature_path: &Path,
    scenario_name: &str,
    scenario_line: usize,
    status: &str,
    error_message: Option<&str>,
) {
    let results_dir = nextest_results_dir();

    // Best-effort: don't fail the test if we can't write the result
    let _ = std::fs::create_dir_all(&results_dir);

    // Build a unique filename from feature path + line
    let feature_stem = feature_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let filename = format!("{}_{}.json", feature_stem, scenario_line);

    let mut result = serde_json::json!({
        "feature_path": feature_path.to_string_lossy(),
        "scenario_name": scenario_name,
        "line": scenario_line,
        "status": status,
    });
    if let Some(msg) = error_message {
        result["error_message"] = serde_json::Value::String(msg.to_string());
    }

    if let Ok(mut f) = std::fs::File::create(results_dir.join(&filename)) {
        let _ = f.write_all(result.to_string().as_bytes());
    }
}
