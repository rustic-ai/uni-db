// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use colored::*;
use prettytable::{Cell, Row, Table};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::time::Instant;
use uni_db::{ExplainOutput, ProfileOutput, QueryResult, Uni};

pub async fn run_repl(db: Uni) -> Result<()> {
    let mut rl = DefaultEditor::new()?;

    // Load history if exists
    if rl.load_history("history.txt").is_err() {
        // No history found
    }

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
                        execute_query(&db, line).await;
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
                println!("Error: {:?}", err);
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

pub async fn execute_query(db: &Uni, query: &str) {
    let start = Instant::now();

    let query_upper = query.trim_start().to_uppercase();
    if query_upper.starts_with("EXPLAIN") {
        match db.explain(query).await {
            Ok(output) => print_explain(output, start.elapsed()),
            Err(e) => println!("{}", format!("Error: {}", e).red()),
        }
        return;
    }

    if query_upper.starts_with("PROFILE") {
        let clean_query = query[7..].trim();
        match db.profile(clean_query).await {
            Ok((results, output)) => {
                print_results(results, start.elapsed());
                println!();
                print_profile(output);
            }
            Err(e) => println!("{}", format!("Error: {}", e).red()),
        }
        return;
    }

    match db.query(query).await {
        Ok(results) => print_results(results, start.elapsed()),
        Err(e) => {
            println!("{}", format!("Error: {}", e).red());
        }
    }
}

fn print_results(results: QueryResult, duration: std::time::Duration) {
    // Handle empty results (e.g. CREATE, SET, or no matches)
    if results.rows.is_empty() {
        if results.columns.is_empty() {
            println!(
                "{}",
                format!("Query executed successfully in {:?}", duration).dimmed()
            );
        } else {
            println!("{}", format!("No results found ({:?})", duration).yellow());
        }
        return;
    }

    let mut table = Table::new();

    // Add header
    let header_cells: Vec<Cell> = results
        .columns
        .iter()
        .map(|c| Cell::new(c).style_spec("bfg"))
        .collect();
    table.add_row(Row::new(header_cells));

    // Add rows
    for row in results.rows {
        let cells: Vec<Cell> = row
            .values
            .iter()
            .map(|v| {
                let json_val = serde_json::Value::from(v.clone());
                let s = if let serde_json::Value::String(str_val) = json_val {
                    str_val
                } else {
                    json_val.to_string()
                };
                Cell::new(&s)
            })
            .collect();
        table.add_row(Row::new(cells));
    }

    table.printstd();
    println!(
        "{}",
        format!("{} rows in {:?}", table.len(), duration).dimmed()
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
