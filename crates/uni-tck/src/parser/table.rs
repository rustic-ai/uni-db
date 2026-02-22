use cucumber::gherkin::Table;
use std::collections::HashMap;
use uni_query::Value;

use super::parse_value;

/// Unescape Gherkin data table cell content.
/// Per the Gherkin specification, table cells use `\\` for literal `\`,
/// `\|` for literal `|`, and `\n` for newline.
fn unescape_gherkin_cell(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.peek() {
                Some('\\') => {
                    result.push('\\');
                    chars.next();
                }
                Some('|') => {
                    result.push('|');
                    chars.next();
                }
                Some('n') => {
                    result.push('\n');
                    chars.next();
                }
                _ => result.push('\\'),
            }
        } else {
            result.push(ch);
        }
    }
    result
}

/// Parse a Gherkin data table into rows of named values.
///
/// The first row is treated as column headers; subsequent rows are parsed
/// as TCK values keyed by those headers.
pub fn parse_table(table: &Table) -> Result<Vec<HashMap<String, Value>>, String> {
    let rows = &table.rows;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let headers: Vec<String> = rows[0].iter().map(|s| s.to_string()).collect();

    rows[1..]
        .iter()
        .map(|row| {
            headers
                .iter()
                .zip(row.iter())
                .map(|(header, cell)| {
                    let unescaped = unescape_gherkin_cell(cell.trim());
                    let value = parse_value(&unescaped)?;
                    Ok((header.clone(), value))
                })
                .collect::<Result<HashMap<String, Value>, String>>()
        })
        .collect()
}
