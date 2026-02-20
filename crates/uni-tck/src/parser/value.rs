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

/// Type alias for edge bracket parsing result: (edge_type, properties)
type EdgeBracketResult<'a> = (Option<&'a str>, Option<HashMap<String, Value>>);

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
        special_float,
        number,
    ))(input)
}

fn special_float(input: &str) -> IResult<&str, Value> {
    let (input, _) = multispace0(input)?;
    alt((
        map(tag("-Infinity"), |_| Value::Float(f64::NEG_INFINITY)),
        map(tag("Infinity"), |_| Value::Float(f64::INFINITY)),
        map(tag("NaN"), |_| Value::Float(f64::NAN)),
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

/// Parse multiple labels like `:A:B:C` and return as a vector
fn labels(input: &str) -> IResult<&str, Vec<String>> {
    let (input, first_label) = preceded(char(':'), identifier)(input)?;
    let mut label_list = vec![first_label.to_string()];

    let mut remaining = input;
    while let Ok((rest, label)) = preceded(multispace0, preceded(char(':'), identifier))(remaining)
    {
        label_list.push(label.to_string());
        remaining = rest;
    }

    Ok((remaining, label_list))
}

fn node(input: &str) -> IResult<&str, Node> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('(')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, label_list) = opt(labels)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, properties) = opt(map_parser)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(')')(input)?;

    let labels = label_list.unwrap_or_default();

    Ok((
        input,
        Node {
            vid: Vid::from(0),
            labels,
            properties: properties.unwrap_or_default(),
        },
    ))
}

/// Parse a bracketed edge `[:TYPE {props}]`.
///
/// Requires at least an edge type or properties to disambiguate from empty
/// list `[]`. In TCK result tables, `[]` is always an empty list.
fn edge(input: &str) -> IResult<&str, Edge> {
    let (input, (edge_type, properties)) = parse_edge_brackets(input)?;

    // Require an explicit edge type to disambiguate from lists.
    // `[{}]` is a list containing an empty map, not an edge with empty properties.
    // `[:TYPE {props}]` or `[:TYPE]` are edges.
    if edge_type.is_none() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Verify,
        )));
    }

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

/// Parse a bracketed edge within a path, allowing empty `[]` for untyped edges.
fn edge_in_path(input: &str) -> IResult<&str, Edge> {
    let (input, (edge_type, properties)) = parse_edge_brackets(input)?;
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

/// Shared bracket parsing for edge functions.
fn parse_edge_brackets(input: &str) -> IResult<&str, EdgeBracketResult<'_>> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('[')(input)?;
    let (input, _) = multispace0(input)?;
    let (input, edge_type) = opt(preceded(char(':'), identifier))(input)?;
    let (input, _) = multispace0(input)?;
    let (input, properties) = opt(map_parser)(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char(']')(input)?;
    Ok((input, (edge_type, properties)))
}

/// Parse a path `<node-edge-node-edge-node>` with proper direction handling
fn path(input: &str) -> IResult<&str, Path> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('<')(input)?;
    let (input, _) = multispace0(input)?;

    // Parse first node
    let (input, first_node) = node(input)?;
    let mut nodes = vec![first_node];
    let mut edges = vec![];

    // Parse edge-node pairs
    let mut remaining = input;
    loop {
        let (input, _) = multispace0(remaining)?;

        // Try to parse edge: <-[edge]- or -[edge]->
        match tuple((
            opt(char('<')),
            char('-'),
            edge_in_path,
            char('-'),
            opt(char('>')),
        ))(input)
        {
            Ok((input, (left_arrow, _, mut edge_val, _, right_arrow))) => {
                // Determine direction: -> is outgoing, <- is incoming
                let outgoing = left_arrow.is_none() && right_arrow.is_some();

                // Parse next node
                let (input, next_node) = node(input)?;

                // Set edge source/destination based on direction
                if outgoing {
                    edge_val.src = nodes.last().unwrap().vid;
                    edge_val.dst = next_node.vid;
                } else {
                    edge_val.src = next_node.vid;
                    edge_val.dst = nodes.last().unwrap().vid;
                }

                edges.push(edge_val);
                nodes.push(next_node);
                remaining = input;
            }
            Err(_) => break,
        }
    }

    let (input, _) = multispace0(remaining)?;
    let (input, _) = char('>')(input)?;

    Ok((input, Path { nodes, edges }))
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
    fn test_parse_special_float_literals() {
        match parse_value("NaN").unwrap() {
            Value::Float(f) => assert!(f.is_nan()),
            other => panic!("Expected NaN float, got {other:?}"),
        }
        assert_eq!(
            parse_value("Infinity").unwrap(),
            Value::Float(f64::INFINITY)
        );
        assert_eq!(
            parse_value("-Infinity").unwrap(),
            Value::Float(f64::NEG_INFINITY)
        );
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

    #[test]
    fn test_parse_multi_label_node() {
        if let Value::Node(node) = parse_value("(:A:B:C)").unwrap() {
            assert_eq!(
                node.labels,
                vec!["A".to_string(), "B".to_string(), "C".to_string()]
            );
            assert!(node.properties.is_empty());
        } else {
            panic!("Expected node");
        }
    }

    #[test]
    fn test_parse_multi_label_with_props() {
        if let Value::Node(node) = parse_value("(:A:B {name: 'test'})").unwrap() {
            assert_eq!(node.labels, vec!["A".to_string(), "B".to_string()]);
            assert_eq!(node.properties.len(), 1);
        } else {
            panic!("Expected node");
        }
    }

    #[test]
    fn test_parse_single_label() {
        if let Value::Node(node) = parse_value("(:Person)").unwrap() {
            assert_eq!(node.labels, vec!["Person".to_string()]);
        } else {
            panic!("Expected node");
        }
    }

    #[test]
    fn test_parse_unlabeled_node() {
        if let Value::Node(node) = parse_value("()").unwrap() {
            assert_eq!(node.labels, Vec::<String>::new());
        } else {
            panic!("Expected node");
        }
    }

    #[test]
    fn test_parse_empty_list() {
        if let Value::List(items) = parse_value("[]").unwrap() {
            assert!(items.is_empty());
        } else {
            panic!("Expected empty list, not edge");
        }
    }

    #[test]
    fn test_parse_list_with_null() {
        if let Value::List(items) = parse_value("[null]").unwrap() {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Value::Null);
        } else {
            panic!("Expected list");
        }
    }

    #[test]
    fn test_parse_list_with_string() {
        if let Value::List(items) = parse_value("['val']").unwrap() {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Value::String("val".to_string()));
        } else {
            panic!("Expected list");
        }
    }

    #[test]
    fn test_parse_standalone_edge() {
        if let Value::Edge(e) = parse_value("[:KNOWS]").unwrap() {
            assert_eq!(e.edge_type, "KNOWS");
        } else {
            panic!("Expected edge");
        }
    }

    #[test]
    fn test_parse_edge_with_properties() {
        if let Value::Edge(e) = parse_value("[:T {name: 'bar'}]").unwrap() {
            assert_eq!(e.edge_type, "T");
            assert_eq!(
                e.properties.get("name"),
                Some(&Value::String("bar".to_string()))
            );
        } else {
            panic!("Expected edge");
        }
    }

    #[test]
    fn test_parse_empty_path_single_node() {
        if let Value::Path(path) = parse_value("<()>").unwrap() {
            assert_eq!(path.nodes.len(), 1);
            assert_eq!(path.edges.len(), 0);
        } else {
            panic!("Expected path");
        }
    }

    #[test]
    fn test_parse_simple_path() {
        if let Value::Path(path) = parse_value("<(:A)-[:KNOWS]->(:B)>").unwrap() {
            assert_eq!(path.nodes.len(), 2);
            assert_eq!(path.edges.len(), 1);
            assert_eq!(path.nodes[0].labels, vec!["A".to_string()]);
            assert_eq!(path.nodes[1].labels, vec!["B".to_string()]);
            assert_eq!(path.edges[0].edge_type, "KNOWS");
        } else {
            panic!("Expected path");
        }
    }

    #[test]
    fn test_parse_path_with_properties() {
        let path_str = "<(:A {name: 'Alice'})-[:KNOWS]->(:B {name: 'Bob'})>";
        if let Value::Path(path) = parse_value(path_str).unwrap() {
            assert_eq!(path.nodes.len(), 2);
            assert_eq!(
                path.nodes[0].properties.get("name"),
                Some(&Value::String("Alice".to_string()))
            );
            assert_eq!(
                path.nodes[1].properties.get("name"),
                Some(&Value::String("Bob".to_string()))
            );
        } else {
            panic!("Expected path");
        }
    }

    #[test]
    fn test_parse_multi_hop_path() {
        if let Value::Path(path) = parse_value("<(:A)-[:T1]->(:B)-[:T2]->(:C)>").unwrap() {
            assert_eq!(path.nodes.len(), 3);
            assert_eq!(path.edges.len(), 2);
            assert_eq!(path.edges[0].edge_type, "T1");
            assert_eq!(path.edges[1].edge_type, "T2");
        } else {
            panic!("Expected path");
        }
    }

    #[test]
    fn test_parse_incoming_edge_path() {
        if let Value::Path(path) = parse_value("<(:B)<-[:KNOWS]-(:A)>").unwrap() {
            assert_eq!(path.nodes.len(), 2);
            assert_eq!(path.edges.len(), 1);
            assert_eq!(path.nodes[0].labels, vec!["B".to_string()]);
            assert_eq!(path.nodes[1].labels, vec!["A".to_string()]);
        } else {
            panic!("Expected path");
        }
    }

    #[test]
    fn test_parse_path_with_multi_label_nodes() {
        if let Value::Path(path) = parse_value("<(:A:B)-[:R]->(:C:D)>").unwrap() {
            assert_eq!(path.nodes.len(), 2);
            assert_eq!(path.nodes[0].labels, vec!["A".to_string(), "B".to_string()]);
            assert_eq!(path.nodes[1].labels, vec!["C".to_string(), "D".to_string()]);
        } else {
            panic!("Expected path");
        }
    }
}
