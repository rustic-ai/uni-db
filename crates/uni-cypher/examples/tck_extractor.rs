// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Extract all TCK queries from Cucumber .feature files with placeholders properly substituted.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
struct Query {
    feature_file: String,
    _scenario_name: String,
    query_text: String,
    expected_error: Option<String>,
}

fn main() {
    let tck_root = "../../cypher-tck/tck-M23/tck/features";
    let valid_output = "VALID_TCK_QUERIES.md";
    let invalid_output = "INVALID_TCK_QUERIES.md";

    println!("Extracting TCK queries from: {}", tck_root);

    let (valid_queries, invalid_queries) = extract_all_queries(tck_root);

    println!("Extracted {} valid queries", valid_queries.len());
    println!(
        "Extracted {} invalid queries (expected to fail)",
        invalid_queries.len()
    );

    write_queries_file(valid_output, &valid_queries, false);
    write_queries_file(invalid_output, &invalid_queries, true);

    println!("Wrote valid queries to: {}", valid_output);
    println!("Wrote invalid queries to: {}", invalid_output);
}

fn extract_all_queries(root: &str) -> (Vec<Query>, Vec<Query>) {
    let mut queries = Vec::new();

    visit_features(Path::new(root), &mut queries);

    // Separate valid and invalid queries
    let mut valid = Vec::new();
    let mut invalid = Vec::new();

    for query in queries {
        if query.expected_error.is_some() {
            invalid.push(query);
        } else {
            valid.push(query);
        }
    }

    valid.sort_by(|a, b| a.feature_file.cmp(&b.feature_file));
    invalid.sort_by(|a, b| a.feature_file.cmp(&b.feature_file));

    (valid, invalid)
}

fn visit_features(dir: &Path, queries: &mut Vec<Query>) {
    if !dir.is_dir() {
        return;
    }

    let mut entries: Vec<_> = fs::read_dir(dir).unwrap().filter_map(|e| e.ok()).collect();

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            visit_features(&path, queries);
        } else if path.extension().and_then(|s| s.to_str()) == Some("feature") {
            extract_from_feature(&path, queries);
        }
    }
}

fn extract_from_feature(path: &Path, queries: &mut Vec<Query>) {
    let content = fs::read_to_string(path).unwrap();
    let feature_path = path.to_string_lossy().to_string();

    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim_start();

        // Check for Scenario or Scenario Outline
        if line.starts_with("Scenario:") || line.starts_with("Scenario Outline:") {
            let is_outline = line.starts_with("Scenario Outline:");

            // Check if previous line has @skipGrammarCheck tag
            let has_skip_tag = i > 0 && lines[i - 1].trim().contains("@skipGrammarCheck");

            // Extract scenario name (format: "Scenario: [N] Name")
            let scenario_name = line.split("] ").nth(1).unwrap_or("unnamed").trim();

            // Find the end of this scenario (next Scenario or end of file)
            let mut end = i + 1;
            while end < lines.len() {
                let next_line = lines[end].trim_start();
                if next_line.starts_with("Scenario:") || next_line.starts_with("Scenario Outline:")
                {
                    break;
                }
                end += 1;
            }

            let scenario_lines = &lines[i..end];
            let scenario_text = scenario_lines.join("\n");

            if is_outline {
                extract_scenario_outline(
                    &feature_path,
                    scenario_name,
                    &scenario_text,
                    has_skip_tag,
                    queries,
                );
            } else {
                extract_scenario(
                    &feature_path,
                    scenario_name,
                    &scenario_text,
                    has_skip_tag,
                    queries,
                );
            }

            i = end;
        } else {
            i += 1;
        }
    }
}

fn extract_scenario(
    feature_path: &str,
    scenario_name: &str,
    scenario_text: &str,
    has_skip_tag: bool,
    queries: &mut Vec<Query>,
) {
    if let Some(query) = extract_query_text(scenario_text) {
        let expected_error = if has_skip_tag {
            extract_expected_error(scenario_text)
        } else {
            None
        };

        queries.push(Query {
            feature_file: feature_path.to_string(),
            _scenario_name: scenario_name.to_string(),
            query_text: query,
            expected_error,
        });
    }
}

fn extract_scenario_outline(
    feature_path: &str,
    scenario_name: &str,
    scenario_text: &str,
    has_skip_tag: bool,
    queries: &mut Vec<Query>,
) {
    let template = match extract_query_text(scenario_text) {
        Some(t) => t,
        None => return,
    };

    let expected_error = if has_skip_tag {
        extract_expected_error(scenario_text)
    } else {
        None
    };

    let examples = extract_examples(scenario_text);

    if examples.is_empty() {
        // No examples, just add the template
        queries.push(Query {
            feature_file: feature_path.to_string(),
            _scenario_name: scenario_name.to_string(),
            query_text: template,
            expected_error,
        });
        return;
    }

    // Generate one query per example row
    for (idx, example) in examples.iter().enumerate() {
        let substituted = substitute_placeholders(&template, example);
        queries.push(Query {
            feature_file: feature_path.to_string(),
            _scenario_name: format!("{} (example {})", scenario_name, idx + 1),
            query_text: substituted,
            expected_error: expected_error.clone(),
        });
    }
}

fn extract_expected_error(text: &str) -> Option<String> {
    // Look for "Then a SyntaxError should be raised at compile time: <ErrorType>"
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("Then a SyntaxError should be raised")
            || trimmed.starts_with("Then a") && trimmed.contains("should be raised")
        {
            // Extract error type after the last colon
            if let Some(colon_pos) = trimmed.rfind(':') {
                let error_type = trimmed[colon_pos + 1..].trim();
                if !error_type.is_empty() {
                    return Some(error_type.to_string());
                }
            }
        }
    }
    None
}

fn extract_query_text(text: &str) -> Option<String> {
    // Find the query between """ markers
    let start_marker = "When executing query:\n      \"\"\"";
    let end_marker = "\"\"\"";

    let start = text.find(start_marker)?;
    let query_start = start + start_marker.len();

    let remaining = &text[query_start..];
    let end = remaining.find(end_marker)?;

    let query = &remaining[..end];

    // Clean up the query text - remove leading whitespace from each line
    let cleaned: Vec<&str> = query
        .lines()
        .map(|line| {
            // Remove exactly 6 spaces of indentation (Gherkin convention)
            if let Some(stripped) = line.strip_prefix("      ") {
                stripped
            } else {
                line.trim_start()
            }
        })
        .filter(|line| !line.is_empty())
        .collect();

    Some(cleaned.join("\n"))
}

fn extract_examples(text: &str) -> Vec<HashMap<String, String>> {
    let mut results = Vec::new();

    // Find "Examples:" section
    let examples_start = match text.find("Examples:") {
        Some(pos) => pos,
        None => return results,
    };

    let examples_text = &text[examples_start..];

    // Parse table
    let lines: Vec<&str> = examples_text.lines().collect();

    // Find header row (starts with |)
    let header_idx = match lines.iter().position(|l| l.trim().starts_with('|')) {
        Some(idx) => idx,
        None => return results,
    };

    let header_line = lines[header_idx];

    // Parse header
    let headers: Vec<&str> = header_line
        .split('|')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    // Parse data rows
    for line in &lines[header_idx + 1..] {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            break; // End of table
        }

        let values: Vec<&str> = trimmed
            .split('|')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();

        if values.len() == headers.len() {
            let mut row = HashMap::new();
            for (header, value) in headers.iter().zip(values.iter()) {
                row.insert(header.to_string(), value.to_string());
            }
            results.push(row);
        }
    }

    results
}

fn substitute_placeholders(template: &str, values: &HashMap<String, String>) -> String {
    let mut result = template.to_string();

    // Replace placeholders in order (longest first to avoid partial replacements)
    let mut keys: Vec<_> = values.keys().collect();
    keys.sort_by_key(|k| std::cmp::Reverse(k.len()));

    for key in keys {
        let placeholder = format!("<{}>", key);
        let value = &values[key];
        result = result.replace(&placeholder, value);
    }

    result
}

fn write_queries_file(path: &str, queries: &Vec<Query>, include_expected_errors: bool) {
    let mut file = fs::File::create(path).unwrap();

    for query in queries {
        writeln!(file, "// {}", query.feature_file).unwrap();
        if include_expected_errors && let Some(ref error) = query.expected_error {
            writeln!(file, "// Expected error: {}", error).unwrap();
        }
        writeln!(file, "{}", query.query_text).unwrap();
        writeln!(file).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_placeholders() {
        let template = "CALL test.my.proc(null) YIELD <yield>\nRETURN a, b";
        let mut values = HashMap::new();
        values.insert("yield".to_string(), "a, b".to_string());

        let result = substitute_placeholders(template, &values);
        assert_eq!(result, "CALL test.my.proc(null) YIELD a, b\nRETURN a, b");
    }

    #[test]
    fn test_substitute_multiple() {
        let template = "RETURN <a> AND <b>";
        let mut values = HashMap::new();
        values.insert("a".to_string(), "123".to_string());
        values.insert("b".to_string(), "true".to_string());

        let result = substitute_placeholders(template, &values);
        assert_eq!(result, "RETURN 123 AND true");
    }
}
