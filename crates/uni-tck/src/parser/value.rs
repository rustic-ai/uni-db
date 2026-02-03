use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, digit1, multispace0, one_of},
    combinator::{map, opt, recognize},
    multi::separated_list0,
    sequence::{delimited, preceded, tuple},
    IResult,
};
use std::collections::HashMap;
use uni_common::core::id::{Eid, Vid};
use uni_query::{Edge, Node, Path, Value};

/// Parse a TCK value string into a `Value`, failing on trailing input.
pub fn parse_value(input: &str) -> Result<Value, String> {
    match value(input.trim()) {
        Ok((remaining, val)) => {
            if remaining.trim().is_empty() {
                Ok(val)
            } else {
                Err(format!("Unexpected trailing input: {}", remaining))
            }
        }
        Err(e) => Err(format!("Parse error: {}", e)),
    }
}

fn value(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;

    alt((
        map(tag("null"), |_| Value::Null),
        map(tag("true"), |_| Value::Bool(true)),
        map(tag("false"), |_| Value::Bool(false)),
        map(node, Value::Node),
        map(edge, Value::Edge),
        map(path, Value::Path),
        map(list, Value::List),
        map(map_parser, Value::Map),
        map(string, Value::String),
        number,
    ))(input)
}

fn number(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;

    let (input, num_str) = recognize(tuple((
        opt(char('-')),
        digit1,
        opt(tuple((char('.'), digit1))),
        opt(tuple((one_of("eE"), opt(one_of("+-")), digit1))),
    )))(input)?;

    if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
        match num_str.parse::<f64>() {
            Ok(f) => Ok((input, Value::Float(f))),
            Err(_) => Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Float,
            ))),
        }
    } else {
        match num_str.parse::<i64>() {
            Ok(i) => Ok((input, Value::Int(i))),
            Err(_) => Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Digit,
            ))),
        }
    }
}

fn string(input: &str) -> IResult<&str, String> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('\'')(input)?;

    let mut result = String::new();
    let mut chars = input.chars();
    let mut pos = 0;

    while let Some(ch) = chars.next() {
        pos += ch.len_utf8();
        match ch {
            '\'' => {
                return Ok((&input[pos..], result));
            }
            '\\' => {
                if let Some(next_ch) = chars.next() {
                    pos += next_ch.len_utf8();
                    match next_ch {
                        'n' => result.push('\n'),
                        't' => result.push('\t'),
                        'r' => result.push('\r'),
                        '\\' => result.push('\\'),
                        '\'' => result.push('\''),
                        _ => {
                            result.push('\\');
                            result.push(next_ch);
                        }
                    }
                }
            }
            _ => result.push(ch),
        }
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Char,
    )))
}

fn list(input: &str) -> IResult<&str, Vec<Value>> {
    delimited(
        preceded(multispace0, char('[')),
        separated_list0(
            preceded(multispace0, char(',')),
            preceded(multispace0, value),
        ),
        preceded(multispace0, char(']')),
    )(input)
}

fn map_parser(input: &str) -> IResult<&str, HashMap<String, Value>> {
    let (input, pairs) = delimited(
        preceded(multispace0, char('{')),
        separated_list0(preceded(multispace0, char(',')), map_entry),
        preceded(multispace0, char('}')),
    )(input)?;

    Ok((input, pairs.into_iter().collect()))
}

fn map_entry(input: &str) -> IResult<&str, (String, Value)> {
    let (input, _) = multispace0(input)?;
    let (input, key) = identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(':')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, val) = value(input)?;

    Ok((input, (key.to_string(), val)))
}

fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

fn node(input: &str) -> IResult<&str, Node> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, label) = opt(preceded(char(':'), identifier))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, properties) = opt(map_parser)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    Ok((
        input,
        Node {
            vid: Vid::from(0),
            label: label.unwrap_or_default().to_string(),
            properties: properties.unwrap_or_default(),
        },
    ))
}

fn edge(input: &str) -> IResult<&str, Edge> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('[')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, edge_type) = opt(preceded(char(':'), identifier))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, properties) = opt(map_parser)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;

    Ok((
        input,
        Edge {
            eid: Eid::from(0),
            edge_type: edge_type.unwrap_or_default().to_string(),
            src: Vid::from(0),
            dst: Vid::from(0),
            properties: properties.unwrap_or_default(),
        },
    ))
}

/// Parse a path `<n0-[r1]->n1>`. Currently only handles empty paths.
fn path(input: &str) -> IResult<&str, Path> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('<')(input)?;
    // TODO: parse node-edge-node sequences
    let (input, _) = multispace0(input)?;
    let (input, _) = char('>')(input)?;

    Ok((
        input,
        Path {
            nodes: vec![],
            edges: vec![],
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_null() {
        assert_eq!(parse_value("null").unwrap(), Value::Null);
    }

    #[test]
    fn test_parse_bool() {
        assert_eq!(parse_value("true").unwrap(), Value::Bool(true));
        assert_eq!(parse_value("false").unwrap(), Value::Bool(false));
    }

    #[test]
    fn test_parse_int() {
        assert_eq!(parse_value("123").unwrap(), Value::Int(123));
        assert_eq!(parse_value("-456").unwrap(), Value::Int(-456));
    }

    #[test]
    fn test_parse_float() {
        assert_eq!(parse_value("3.15").unwrap(), Value::Float(3.15));
        assert_eq!(parse_value("-2.5").unwrap(), Value::Float(-2.5));
    }

    #[test]
    fn test_parse_string() {
        assert_eq!(
            parse_value("'hello'").unwrap(),
            Value::String("hello".to_string())
        );
        assert_eq!(
            parse_value("'world'").unwrap(),
            Value::String("world".to_string())
        );
    }

    #[test]
    fn test_parse_list() {
        if let Value::List(items) = parse_value("[1, 2, 3]").unwrap() {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int(1));
            assert_eq!(items[1], Value::Int(2));
            assert_eq!(items[2], Value::Int(3));
        } else {
            panic!("Expected list");
        }
    }

    #[test]
    fn test_parse_map() {
        if let Value::Map(map) = parse_value("{name: 'Alice', age: 30}").unwrap() {
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(map.get("age"), Some(&Value::Int(30)));
        } else {
            panic!("Expected map");
        }
    }
}
