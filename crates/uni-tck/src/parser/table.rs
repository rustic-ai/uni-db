use cucumber::gherkin::Table;
use std::collections::HashMap;
use uni_query::Value;

use super::parse_value;

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
                    let value = parse_value(cell.trim())?;
                    Ok((header.clone(), value))
                })
                .collect::<Result<HashMap<String, Value>, String>>()
        })
        .collect()
}
