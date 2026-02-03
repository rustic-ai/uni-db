// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! TCK (Technology Compatibility Kit) Parser Test Suite
//!
//! This test suite validates that our Pest-based Cypher parser:
//! 1. Successfully parses all valid TCK queries
//! 2. Correctly rejects all invalid TCK queries

use std::collections::HashMap;
use std::fs;
use std::panic;

/// Represents a single TCK query with metadata
#[derive(Debug, Clone)]
struct TckQuery {
    /// Feature file path (from comment line)
    feature: String,
    /// The Cypher query text
    query: String,
    /// Expected error type (for invalid queries)
    expected_error: Option<String>,
    /// Line number in source file for error reporting
    line_number: usize,
    /// Whether this query should be ignored (expected to pass even if invalid)
    ignored: bool,
    /// Reason for ignoring (semantic validation vs parse error)
    ignore_reason: Option<String>,
}

/// Parse VALID_TCK_QUERIES.md and extract all valid queries
fn load_valid_queries() -> Vec<TckQuery> {
    load_queries_from_file("VALID_TCK_QUERIES.md", false)
}

/// Parse INVALID_TCK_QUERIES.md and extract all invalid queries
fn load_invalid_queries() -> Vec<TckQuery> {
    load_queries_from_file("INVALID_TCK_QUERIES.md", true)
}

/// Generic query loader
fn load_queries_from_file(filename: &str, _expect_errors: bool) -> Vec<TckQuery> {
    let test_dir = env!("CARGO_MANIFEST_DIR");
    let queries_path = format!("{}/tests/{}", test_dir, filename);
    let content = fs::read_to_string(&queries_path)
        .unwrap_or_else(|e| panic!("Failed to read {} from {}: {}", filename, queries_path, e));

    let mut queries = Vec::new();
    let mut current_feature = String::new();
    let mut current_expected_error = None;
    let mut current_ignored = false;
    let mut current_ignore_reason = None;
    let mut current_query = String::new();
    let mut query_start_line = 0;

    for (line_num, line) in content.lines().enumerate() {
        if let Some(stripped) = line.strip_prefix("//") {
            let comment = stripped.trim();

            if comment.starts_with("Expected error:") {
                // Extract expected error type
                current_expected_error = Some(
                    comment
                        .strip_prefix("Expected error:")
                        .unwrap()
                        .trim()
                        .to_string(),
                );
            } else if comment.starts_with("IGNORED:") {
                // Mark query as ignored with reason
                current_ignored = true;
                current_ignore_reason =
                    Some(comment.strip_prefix("IGNORED:").unwrap().trim().to_string());
            } else {
                // Feature file comment - save previous query if exists
                if !current_query.is_empty() {
                    queries.push(TckQuery {
                        feature: current_feature.clone(),
                        query: current_query.trim().to_string(),
                        expected_error: current_expected_error.clone(),
                        line_number: query_start_line,
                        ignored: current_ignored,
                        ignore_reason: current_ignore_reason.clone(),
                    });
                    current_query.clear();
                    current_expected_error = None;
                    current_ignored = false;
                    current_ignore_reason = None;
                }
                current_feature = comment.to_string();
                query_start_line = line_num + 2; // Next non-blank line
            }
        } else if line.trim().is_empty() {
            // Blank line - end of query
            if !current_query.is_empty() {
                queries.push(TckQuery {
                    feature: current_feature.clone(),
                    query: current_query.trim().to_string(),
                    expected_error: current_expected_error.clone(),
                    line_number: query_start_line,
                    ignored: current_ignored,
                    ignore_reason: current_ignore_reason.clone(),
                });
                current_query.clear();
                current_expected_error = None;
                current_ignored = false;
                current_ignore_reason = None;
            }
        } else {
            // Query line
            if current_query.is_empty() {
                query_start_line = line_num + 1;
            }
            current_query.push_str(line);
            current_query.push('\n');
        }
    }

    // Don't forget the last query
    if !current_query.is_empty() {
        queries.push(TckQuery {
            feature: current_feature,
            query: current_query.trim().to_string(),
            expected_error: current_expected_error,
            line_number: query_start_line,
            ignored: current_ignored,
            ignore_reason: current_ignore_reason,
        });
    }

    queries
}

// ============================================================================
// Main Tests: Valid and Invalid Queries
// ============================================================================

#[test]
fn test_valid_tck_queries() {
    let queries = load_valid_queries();

    println!("Testing {} valid TCK queries", queries.len());
    assert!(
        queries.len() > 3000,
        "Expected ~3800 valid queries, found {}",
        queries.len()
    );

    let mut failures = Vec::new();

    for (idx, tck_query) in queries.iter().enumerate() {
        let parse_result = panic::catch_unwind(|| uni_cypher::parse(&tck_query.query));

        match parse_result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                failures.push((idx, tck_query, format!("{:?}", e)));
            }
            Err(panic_info) => {
                eprintln!(
                    "\nPANIC at query #{} (line {}):",
                    idx, tck_query.line_number
                );
                eprintln!("Feature: {}", tck_query.feature);
                eprintln!("Query: {}", tck_query.query);
                failures.push((idx, tck_query, format!("PANIC: {:?}", panic_info)));
            }
        }
    }

    // Report all failures
    if !failures.is_empty() {
        eprintln!(
            "\n=== VALID QUERY PARSE FAILURES ({}/{}) ===\n",
            failures.len(),
            queries.len()
        );

        for (idx, tck_query, error) in &failures {
            eprintln!(
                "FAIL #{}: {} (line {})",
                idx, tck_query.feature, tck_query.line_number
            );
            eprintln!("Query: {}", tck_query.query);
            eprintln!("Error: {}\n", error);
        }

        panic!(
            "{} / {} valid TCK queries failed to parse",
            failures.len(),
            queries.len()
        );
    }

    println!(
        "✅ All {} valid TCK queries parsed successfully!",
        queries.len()
    );
}

#[test]
fn test_invalid_tck_queries() {
    let queries = load_invalid_queries();

    let ignored_count = queries.iter().filter(|q| q.ignored).count();
    let testable_count = queries.len() - ignored_count;

    println!(
        "Testing {} invalid TCK queries (should fail)",
        testable_count
    );
    if ignored_count > 0 {
        println!(
            "Ignoring {} queries (semantic errors, not parse errors)",
            ignored_count
        );
    }

    let mut unexpected_passes = Vec::new();
    let mut ignored_queries = Vec::new();

    for (idx, tck_query) in queries.iter().enumerate() {
        // Skip ignored queries
        if tck_query.ignored {
            ignored_queries.push((idx, tck_query));
            continue;
        }

        let parse_result = panic::catch_unwind(|| uni_cypher::parse(&tck_query.query));

        match parse_result {
            Ok(Ok(_)) => {
                // This should have failed but passed!
                unexpected_passes.push((idx, tck_query));
            }
            Ok(Err(e)) => {
                // Good - it failed as expected
                if let Some(ref expected) = tck_query.expected_error {
                    // Note: We're not strictly matching error types yet, just logging
                    println!(
                        "Query {} correctly failed. Expected: {}, Got: {:?}",
                        idx, expected, e
                    );
                }
            }
            Err(_) => {
                // Panic is also a valid form of rejection
            }
        }
    }

    // Report ignored queries
    if !ignored_queries.is_empty() {
        println!(
            "\n=== IGNORED INVALID QUERIES ({}) ===\n",
            ignored_queries.len()
        );
        for (idx, tck_query) in &ignored_queries {
            println!("IGNORED #{}: {}", idx, tck_query.feature);
            println!("Query: {}", tck_query.query);
            if let Some(ref reason) = tck_query.ignore_reason {
                println!("Reason: {}", reason);
            }
            if let Some(ref expected) = tck_query.expected_error {
                println!("Expected error: {}", expected);
            }
            println!();
        }
    }

    // Report queries that should have failed but passed
    if !unexpected_passes.is_empty() {
        eprintln!(
            "\n=== INVALID QUERIES THAT INCORRECTLY PASSED ({}/{}) ===\n",
            unexpected_passes.len(),
            testable_count
        );

        for (idx, tck_query) in &unexpected_passes {
            eprintln!("UNEXPECTED PASS #{}: {}", idx, tck_query.feature);
            eprintln!("Query: {}", tck_query.query);
            if let Some(ref expected) = tck_query.expected_error {
                eprintln!("Expected error: {}", expected);
            }
            eprintln!();
        }

        panic!(
            "{} / {} invalid TCK queries incorrectly passed (should have failed)",
            unexpected_passes.len(),
            testable_count
        );
    }

    println!(
        "✅ All {} testable invalid TCK queries correctly rejected!",
        testable_count
    );
}

// ============================================================================
// Statistics and Reporting
// ============================================================================

#[test]
fn test_tck_statistics() {
    let valid_queries = load_valid_queries();
    let invalid_queries = load_invalid_queries();

    let mut stats = HashMap::new();
    let mut failures_map = HashMap::new();

    println!("\n=== Testing Valid Queries ===");
    for (idx, tck_query) in valid_queries.iter().enumerate() {
        // Extract feature category (e.g., "clauses/call", "expressions", etc.)
        let category = extract_category(&tck_query.feature);
        *stats.entry(category.clone()).or_insert(0) += 1;

        let parse_result = panic::catch_unwind(|| uni_cypher::parse(&tck_query.query));

        match parse_result {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!(
                    "\nParse error at valid query #{} (line {}):",
                    idx, tck_query.line_number
                );
                eprintln!("Feature: {}", tck_query.feature);
                eprintln!("Query: {}", tck_query.query);
                eprintln!("Error: {:?}", e);
                *failures_map.entry(category.clone()).or_insert(0) += 1;
            }
            Err(panic_info) => {
                eprintln!(
                    "\nPANIC at valid query #{} (line {}):",
                    idx, tck_query.line_number
                );
                eprintln!("Feature: {}", tck_query.feature);
                eprintln!("Query: {}", tck_query.query);
                eprintln!("Panic: {:?}", panic_info);
                *failures_map.entry(category).or_insert(0) += 1;
            }
        }
    }

    println!("\n=== TCK Valid Query Statistics ===");
    println!(
        "{:<30} {:>8} {:>8} {:>8}",
        "Category", "Total", "Failed", "Pass %"
    );
    println!("{:-<58}", "");

    let mut categories: Vec<_> = stats.iter().collect();
    categories.sort_by_key(|(cat, _)| *cat);

    let mut total_queries = 0;
    let mut total_failures = 0;

    for (category, &count) in categories {
        let failed = failures_map.get(category).copied().unwrap_or(0);
        let pass_rate = ((count - failed) as f64 / count as f64) * 100.0;

        println!(
            "{:<30} {:>8} {:>8} {:>7.1}%",
            category, count, failed, pass_rate
        );

        total_queries += count;
        total_failures += failed;
    }

    println!("{:-<58}", "");
    let overall_pass_rate =
        ((total_queries - total_failures) as f64 / total_queries as f64) * 100.0;
    println!(
        "{:<30} {:>8} {:>8} {:>7.1}%",
        "TOTAL (VALID)", total_queries, total_failures, overall_pass_rate
    );

    println!("\n=== Invalid Query Summary ===");
    let ignored_count = invalid_queries.iter().filter(|q| q.ignored).count();
    let testable_invalid = invalid_queries.len() - ignored_count;

    println!("Total invalid queries: {}", invalid_queries.len());
    println!("Ignored (semantic errors): {}", ignored_count);
    println!("Testable (parse errors): {}", testable_invalid);

    let mut invalid_passed = 0;
    for tck_query in &invalid_queries {
        if !tck_query.ignored && uni_cypher::parse(&tck_query.query).is_ok() {
            invalid_passed += 1;
        }
    }

    println!(
        "Invalid queries that incorrectly passed: {}",
        invalid_passed
    );
    println!(
        "Invalid queries correctly rejected: {}",
        testable_invalid - invalid_passed
    );

    println!("\n📊 Statistics generated successfully!");
}

fn extract_category(feature_path: &str) -> String {
    if let Some(features_idx) = feature_path.find("/features/") {
        let after_features = &feature_path[features_idx + 10..];
        // Get first two path components (e.g., "clauses/call")
        let parts: Vec<&str> = after_features.split('/').take(2).collect();
        parts.join("/")
    } else {
        "other".to_string()
    }
}
