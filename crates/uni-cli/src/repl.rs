// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use colored::Colorize;
use prettytable::{Cell, Row, Table};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::time::Instant;
use uni_db::{ExplainOutput, ProfileOutput, QueryResult, Uni};

pub async fn run_repl(db: &Uni) -> Result<()> {
    let mut rl = DefaultEditor::new()?;

    // Load history if it exists; absence is not an error.
    let _ = rl.load_history("history.txt");

    println!("{}", "Welcome to UniDB CLI".green().bold());
    println!("Type 'help' for commands or enter a Cypher query.");

    loop {
        let readline = rl.readline("uni> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                rl.add_history_entry(line)?;

                match line {
                    "exit" | "quit" => break,
                    "help" => print_help(),
                    "clear" => {
                        print!("\x1B[2J\x1B[1;1H");
                        continue;
                    }
                    _ => {
                        execute_query(db, line).await;
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {err:?}");
                break;
            }
        }
    }

    rl.save_history("history.txt")?;
    Ok(())
}

fn print_help() {
    println!("\nAvailable commands:");
    println!("  help          Show this help message");
    println!("  exit, quit    Exit the REPL");
    println!("  clear         Clear the screen");
    println!("  <cypher>      Execute a Cypher query (e.g. MATCH (n) RETURN n LIMIT 5)");
    println!();
}

/// Strip a case-insensitive leading keyword (e.g. `EXPLAIN`, `PROFILE`)
/// from `query` and return the remainder, trimmed. Returns `None` if the
/// keyword is not a prefix (after leading whitespace).
fn strip_query_keyword<'a>(query: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = query.trim_start();
    let head = trimmed.get(..keyword.len())?;
    if !head.eq_ignore_ascii_case(keyword) {
        return None;
    }
    // The next byte (if any) must be whitespace, so we don't match
    // identifiers like `EXPLAINING` as `EXPLAIN`.
    match trimmed.as_bytes().get(keyword.len()) {
        Some(b) if !b.is_ascii_whitespace() => None,
        _ => Some(trimmed[keyword.len()..].trim_start()),
    }
}

/// Print a query execution error to stdout in the REPL's red error style.
fn print_query_error(e: impl std::fmt::Display) {
    // Errors go to stderr so the one-shot `uni query` command's stdout stays
    // clean for result piping and failures are visible to `2>` redirects.
    eprintln!("{}", format!("Error: {e}").red());
}

/// Execute a query and print its results, returning whether it succeeded.
///
/// The REPL loop ignores the return value (it keeps looping regardless), while
/// the one-shot `uni query` command uses it to set a non-zero exit code so
/// callers and scripts can detect failure. Errors are printed to stderr.
pub async fn execute_query(db: &Uni, query: &str) -> bool {
    let start = Instant::now();

    if let Some(clean_query) = strip_query_keyword(query, "EXPLAIN") {
        match db.session().query_with(clean_query).explain().await {
            Ok(output) => {
                print_explain(output, start.elapsed());
                return true;
            }
            Err(e) => {
                print_query_error(e);
                return false;
            }
        }
    }

    if let Some(clean_query) = strip_query_keyword(query, "PROFILE") {
        match db.session().query_with(clean_query).profile().await {
            Ok((results, output)) => {
                print_results(results, start.elapsed());
                println!();
                print_profile(output);
                return true;
            }
            Err(e) => {
                print_query_error(e);
                return false;
            }
        }
    }

    // `Session::run` auto-commits writes (CREATE/SET/MERGE/DELETE/DDL) and
    // forwards reads to `query`, so the REPL and one-shot `uni query` both
    // execute mutations instead of hitting the read-only `query` gate.
    match db.session().run(query).await {
        Ok(results) => {
            print_results(results, start.elapsed());
            true
        }
        Err(e) => {
            print_query_error(e);
            false
        }
    }
}

fn print_results(results: QueryResult, duration: std::time::Duration) {
    // Handle empty results (e.g. CREATE, SET, or no matches)
    if results.is_empty() {
        if results.columns().is_empty() {
            println!(
                "{}",
                format!("Query executed successfully in {duration:?}").dimmed()
            );
        } else {
            println!("{}", format!("No results found ({duration:?})").yellow());
        }
        return;
    }

    let mut table = Table::new();

    // Add header
    let header_cells: Vec<Cell> = results
        .columns()
        .iter()
        .map(|c| Cell::new(c).style_spec("bfg"))
        .collect();
    table.add_row(Row::new(header_cells));

    // Add rows
    for row in results.into_rows() {
        let cells: Vec<Cell> = row
            .values()
            .iter()
            .map(|v| {
                let s = match serde_json::Value::from(v.clone()) {
                    serde_json::Value::String(str_val) => str_val,
                    json_val => json_val.to_string(),
                };
                Cell::new(&s)
            })
            .collect();
        table.add_row(Row::new(cells));
    }

    table.printstd();
    // `table.len()` includes the header row added above, so subtract it for the data-row count.
    let data_rows = table.len().saturating_sub(1);
    let row_word = if data_rows == 1 { "row" } else { "rows" };
    println!(
        "{}",
        format!("{data_rows} {row_word} in {duration:?}").dimmed()
    );
}

fn print_explain(output: ExplainOutput, duration: std::time::Duration) {
    println!("{}", "Query Plan:".bold());
    println!("{}", output.plan_text);

    if !output.index_usage.is_empty() {
        println!("\n{}", "Index Usage:".bold());
        let mut table = Table::new();
        table.add_row(Row::new(vec![
            Cell::new("Label/Type").style_spec("bf"),
            Cell::new("Property").style_spec("bf"),
            Cell::new("Index Type").style_spec("bf"),
            Cell::new("Used").style_spec("bf"),
        ]));

        for usage in output.index_usage {
            table.add_row(Row::new(vec![
                Cell::new(&usage.label_or_type),
                Cell::new(&usage.property),
                Cell::new(&usage.index_type),
                Cell::new(&usage.used.to_string()),
            ]));
        }
        table.printstd();
    }

    println!(
        "\n{}",
        format!(
            "Estimated Rows: {:.0}, Cost: {:.2} ({:?})",
            output.cost_estimates.estimated_rows, output.cost_estimates.estimated_cost, duration
        )
        .dimmed()
    );
}

fn print_profile(output: ProfileOutput) {
    println!("{}", "Execution Profile:".bold());
    // Print plan from profile
    println!("{}", output.explain.plan_text);

    println!(
        "\n{}",
        format!(
            "Total Time: {} ms, Peak Memory: {} bytes",
            output.total_time_ms, output.peak_memory_bytes
        )
        .dimmed()
    );
}

#[cfg(test)]
mod tests {
    use super::strip_query_keyword;

    #[test]
    fn strips_uppercase_keyword() {
        assert_eq!(
            strip_query_keyword("EXPLAIN MATCH (n) RETURN n", "EXPLAIN"),
            Some("MATCH (n) RETURN n")
        );
    }

    #[test]
    fn strips_lowercase_and_mixed_case_keyword() {
        assert_eq!(
            strip_query_keyword("profile MATCH (n) RETURN n", "PROFILE"),
            Some("MATCH (n) RETURN n")
        );
        assert_eq!(
            strip_query_keyword("Profile MATCH (n) RETURN n", "PROFILE"),
            Some("MATCH (n) RETURN n")
        );
    }

    #[test]
    fn tolerates_leading_whitespace() {
        assert_eq!(
            strip_query_keyword("   EXPLAIN MATCH (n) RETURN n", "EXPLAIN"),
            Some("MATCH (n) RETURN n")
        );
    }

    #[test]
    fn rejects_keyword_not_followed_by_whitespace() {
        // `EXPLAINING` must not be stripped as `EXPLAIN`. This was the
        // bug class the byte-slice `query[7..]` form could enable.
        assert!(strip_query_keyword("EXPLAINING MATCH (n)", "EXPLAIN").is_none());
        assert!(strip_query_keyword("PROFILER", "PROFILE").is_none());
    }

    #[test]
    fn returns_none_when_keyword_absent() {
        assert!(strip_query_keyword("MATCH (n) RETURN n", "EXPLAIN").is_none());
    }

    #[test]
    fn handles_query_equal_to_bare_keyword() {
        // `EXPLAIN` alone (no body) — function should return Some("")
        // rather than panic or misparse.
        assert_eq!(strip_query_keyword("EXPLAIN", "EXPLAIN"), Some(""));
    }
}
