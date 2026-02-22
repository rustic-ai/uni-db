use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uni_common::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimeTravelSpec {
    /// VERSION AS OF 'snapshot_id'
    Version(String),
    /// TIMESTAMP AS OF '2025-02-01T12:00:00Z'
    Timestamp(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Query {
    Single(Statement),
    Union {
        left: Box<Query>,
        right: Box<Query>,
        all: bool,
    },
    Schema(Box<SchemaCommand>),
    Transaction(TransactionCommand),
    Explain(Box<Query>),
    /// Query with time-travel: wraps any query with a VERSION/TIMESTAMP AS OF clause.
    /// Resolved at the API layer before planning.
    TimeTravel {
        query: Box<Query>,
        spec: TimeTravelSpec,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionCommand {
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SchemaCommand {
    CreateVectorIndex(CreateVectorIndex),
    CreateFullTextIndex(CreateFullTextIndex),
    CreateScalarIndex(CreateScalarIndex),
    CreateJsonFtsIndex(CreateJsonFtsIndex),
    DropIndex(DropIndex),
    CreateConstraint(CreateConstraint),
    DropConstraint(DropConstraint),
    CreateLabel(CreateLabel),
    CreateEdgeType(CreateEdgeType),
    AlterLabel(AlterLabel),
    AlterEdgeType(AlterEdgeType),
    DropLabel(DropLabel),
    DropEdgeType(DropEdgeType),
    ShowConstraints(ShowConstraints),
    ShowIndexes(ShowIndexes),
    ShowDatabase,
    ShowConfig,
    ShowStatistics,
    Vacuum,
    Checkpoint,
    Backup { path: String },
    CopyTo(CopyToCommand),
    CopyFrom(CopyFromCommand),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateVectorIndex {
    pub name: String,
    pub label: String,
    pub property: String,
    pub options: HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateFullTextIndex {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub options: HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateScalarIndex {
    pub name: String,
    pub label: String,
    pub expressions: Vec<Expr>,
    pub where_clause: Option<Expr>,
    pub options: HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateJsonFtsIndex {
    pub name: String,
    pub label: String,
    pub column: String,
    pub options: HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateLabel {
    pub name: String,
    pub properties: Vec<PropertyDefinition>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateEdgeType {
    pub name: String,
    pub src_labels: Vec<String>,
    pub dst_labels: Vec<String>,
    pub properties: Vec<PropertyDefinition>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterLabel {
    pub name: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterEdgeType {
    pub name: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterAction {
    AddProperty(PropertyDefinition),
    DropProperty(String),
    RenameProperty { old_name: String, new_name: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropLabel {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropEdgeType {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShowConstraints {
    pub target: Option<ConstraintTarget>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintTarget {
    Label(String),
    EdgeType(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShowIndexes {
    pub filter: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyToCommand {
    pub label: String,
    pub path: String,
    pub format: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyFromCommand {
    pub label: String,
    pub path: String,
    pub format: String,
    pub options: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyDefinition {
    pub name: String,
    pub data_type: String, // String representation of type
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropIndex {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateConstraint {
    pub name: Option<String>,
    pub constraint_type: ConstraintType,
    pub label: String,
    pub properties: Vec<String>,
    pub expression: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropConstraint {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintType {
    Unique,
    NodeKey,
    Exists,
    Check,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Statement {
    pub clauses: Vec<Clause>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintDef {
    Unique(String),
    NodeKey(Vec<String>),
    Exists(String),
    Check(Expr),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Clause {
    Match(MatchClause),
    Create(CreateClause),
    Merge(MergeClause),
    With(WithClause),
    WithRecursive(WithRecursiveClause),
    Unwind(UnwindClause),
    Return(ReturnClause),
    Delete(DeleteClause),
    Set(SetClause),
    Remove(RemoveClause),
    Call(CallClause),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchClause {
    pub optional: bool,
    pub pattern: Pattern,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateClause {
    pub pattern: Pattern,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_match: Vec<SetItem>,
    pub on_create: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Option<Vec<SortItem>>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithRecursiveClause {
    pub name: String,
    pub query: Box<Query>,
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub order_by: Option<Vec<SortItem>>,
    pub skip: Option<Expr>,
    pub limit: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReturnItem {
    /// RETURN * - return all variables
    All,
    /// RETURN expr [AS alias]
    Expr {
        expr: Expr,
        alias: Option<String>,
        /// Original source text of the expression (for column naming).
        #[serde(skip_serializing_if = "Option::is_none", default)]
        source_text: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnwindClause {
    pub expr: Expr,
    pub variable: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteClause {
    pub detach: bool,
    pub items: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SetItem {
    Property {
        expr: Expr, // Expected to be a property access
        value: Expr,
    },
    Labels {
        variable: String,
        labels: Vec<String>,
    },
    Variable {
        variable: String,
        value: Expr,
    },
    VariablePlus {
        variable: String,
        value: Expr,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoveClause {
    pub items: Vec<RemoveItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RemoveItem {
    Property(Expr),
    Labels {
        variable: String,
        labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallClause {
    pub kind: CallKind,
    pub yield_items: Vec<YieldItem>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CallKind {
    Procedure {
        procedure: String,
        arguments: Vec<Expr>,
    },
    Subquery(Box<Query>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct YieldItem {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pattern {
    pub paths: Vec<PathPattern>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PathPattern {
    pub variable: Option<String>,
    pub elements: Vec<PatternElement>,
    pub shortest_path_mode: Option<ShortestPathMode>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShortestPathMode {
    Shortest,    // shortestPath(...)
    AllShortest, // allShortestPaths(...)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PatternElement {
    Node(NodePattern),
    Relationship(RelationshipPattern),
    Parenthesized {
        pattern: Box<PathPattern>,
        range: Option<Range>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Option<Expr>,   // Map literal
    pub where_clause: Option<Expr>, // Inline WHERE clause
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipPattern {
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub direction: Direction,
    pub range: Option<Range>,
    pub properties: Option<Expr>,   // Map literal
    pub where_clause: Option<Expr>, // Inline WHERE clause
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Range {
    pub min: Option<u32>,
    pub max: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortItem {
    pub expr: Expr,
    pub ascending: bool,
}

/// Window specification for window functions (OVER clause)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortItem>,
}

/// A typed Cypher literal value, replacing `serde_json::Value` in the AST.
///
/// This makes impossible states unrepresentable: no arrays/objects (those are
/// `Expr::List`/`Expr::Map`), and integer vs. float is always known.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CypherLiteral {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    /// Pre-encoded CypherValue bytes (LargeBinary).
    /// Used when a runtime Value (e.g. list or map) must round-trip through the
    /// AST while preserving its CypherValue-encoded storage format.
    Bytes(Vec<u8>),
}

impl CypherLiteral {
    /// Convert to `uni_common::Value` for the executor.
    pub fn to_value(&self) -> Value {
        match self {
            CypherLiteral::Null => Value::Null,
            CypherLiteral::Bool(b) => Value::Bool(*b),
            CypherLiteral::Integer(i) => Value::Int(*i),
            CypherLiteral::Float(f) => Value::Float(*f),
            CypherLiteral::String(s) => Value::String(s.clone()),
            CypherLiteral::Bytes(b) => {
                uni_common::cypher_value_codec::decode(b).unwrap_or(Value::Null)
            }
        }
    }
}

impl std::fmt::Display for CypherLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CypherLiteral::Null => f.write_str("null"),
            CypherLiteral::Bool(b) => write!(f, "{b}"),
            CypherLiteral::Integer(i) => write!(f, "{i}"),
            CypherLiteral::Float(v) => write!(f, "{v}"),
            CypherLiteral::String(s) => write!(f, "\"{s}\""),
            CypherLiteral::Bytes(b) => write!(f, "<bytes:{}>", b.len()),
        }
    }
}

impl From<i64> for CypherLiteral {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<f64> for CypherLiteral {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<bool> for CypherLiteral {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<String> for CypherLiteral {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<&str> for CypherLiteral {
    fn from(v: &str) -> Self {
        Self::String(v.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(CypherLiteral),
    Parameter(String),
    Variable(String),
    Wildcard,
    Property(Box<Expr>, String),
    List(Vec<Expr>),
    Map(Vec<(String, Expr)>),
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
        window_spec: Option<WindowSpec>,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Case {
        expr: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Exists {
        query: Box<Query>,
        /// True when created from a bare pattern predicate `(n)-->()` in expression context.
        from_pattern_predicate: bool,
    },
    CountSubquery(Box<Query>),
    CollectSubquery(Box<Query>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsUnique(Box<Expr>),
    In {
        expr: Box<Expr>,
        list: Box<Expr>,
    },
    // Array/list indexing and slicing
    ArrayIndex {
        array: Box<Expr>,
        index: Box<Expr>,
    },
    ArraySlice {
        array: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    // Quantifier expressions: ALL, ANY, SINGLE, NONE
    Quantifier {
        quantifier: Quantifier,
        variable: String,
        list: Box<Expr>,
        predicate: Box<Expr>,
    },
    // REDUCE expression: REDUCE(acc = init, var IN list | expr)
    Reduce {
        accumulator: String,
        init: Box<Expr>,
        variable: String,
        list: Box<Expr>,
        expr: Box<Expr>,
    },
    // List comprehension: [x IN list WHERE pred | expr]
    ListComprehension {
        variable: String,
        list: Box<Expr>,
        where_clause: Option<Box<Expr>>,
        map_expr: Box<Expr>,
    },
    // Pattern comprehension: [p = (n)-->(m) WHERE pred | expr]
    PatternComprehension {
        path_variable: Option<String>,
        pattern: Pattern,
        where_clause: Option<Box<Expr>>,
        map_expr: Box<Expr>,
    },
    // VALID_AT macro: e VALID_AT timestamp or e VALID_AT(timestamp, 'start', 'end')
    ValidAt {
        entity: Box<Expr>,
        timestamp: Box<Expr>,
        start_prop: Option<String>,
        end_prop: Option<String>,
    },
    // Map projection: node{.name, .age, city: node.address.city}
    MapProjection {
        base: Box<Expr>,
        items: Vec<MapProjectionItem>,
    },
    /// Label check expression: `a:B` or conjunctive `a:A:B`
    LabelCheck {
        expr: Box<Expr>,
        labels: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MapProjectionItem {
    Property(String),                // .name
    AllProperties,                   // .*
    LiteralEntry(String, Box<Expr>), // key: expr
    Variable(String),                // variable
}

impl MapProjectionItem {
    fn to_string_repr(&self) -> String {
        match self {
            MapProjectionItem::Property(prop) => format!(".{prop}"),
            MapProjectionItem::AllProperties => ".*".to_string(),
            MapProjectionItem::LiteralEntry(key, expr) => {
                format!("{key}: {}", expr.to_string_repr())
            }
            MapProjectionItem::Variable(v) => v.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Quantifier {
    All,
    Any,
    Single,
    None,
}

impl std::fmt::Display for Quantifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Quantifier::All => f.write_str("ALL"),
            Quantifier::Any => f.write_str("ANY"),
            Quantifier::Single => f.write_str("SINGLE"),
            Quantifier::None => f.write_str("NONE"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Xor,
    Regex,
    Contains,
    StartsWith,
    EndsWith,
    ApproxEq,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
            BinaryOp::Pow => "^",
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "<>",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::And => "AND",
            BinaryOp::Or => "OR",
            BinaryOp::Xor => "XOR",
            BinaryOp::Regex => "=~",
            BinaryOp::Contains => "CONTAINS",
            BinaryOp::StartsWith => "STARTS WITH",
            BinaryOp::EndsWith => "ENDS WITH",
            BinaryOp::ApproxEq => "~=",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Neg,
}

impl std::fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOp::Not => f.write_str("NOT "),
            UnaryOp::Neg => f.write_str("-"),
        }
    }
}

// ============================================================================
// Parser Helper Types (Internal - Used for resolving [Identifier ambiguity)
// ============================================================================

/// Intermediate type for resolving [Identifier ambiguity in list expressions.
/// After parsing "[" Identifier, we branch based on the next token to determine
/// whether this is a list comprehension or a list literal.
#[derive(Debug, Clone)]
pub enum ListAfterIdentifier {
    /// [x IN list WHERE pred | expr] - List comprehension
    Comprehension {
        list: Expr,
        filter: Option<Expr>,
        projection: Box<Expr>,
    },

    /// List literal: \[id, ...\], \[id.prop, ...\], or \[id + 1, ...\]
    /// Empty suffixes means the identifier stands alone as the first element.
    ExpressionTail {
        suffix: Vec<ExprSuffix>,
        more: Vec<Expr>,
    },
}

impl ListAfterIdentifier {
    /// Resolve this intermediate representation into a final Expr, given the identifier.
    pub fn resolve(self, id: String) -> Expr {
        match self {
            ListAfterIdentifier::Comprehension {
                list,
                filter,
                projection,
            } => Expr::ListComprehension {
                variable: id,
                list: Box::new(list),
                where_clause: filter.map(Box::new),
                map_expr: projection,
            },
            ListAfterIdentifier::ExpressionTail { suffix, more } => {
                let first = apply_suffixes(Expr::Variable(id), suffix);
                let items = std::iter::once(first).chain(more).collect();
                Expr::List(items)
            }
        }
    }
}

/// Expression suffix for building complex expressions after an identifier.
/// Used to parse things like: id.prop, id\[0\], id(), id+1, etc.
#[derive(Debug, Clone)]
pub enum ExprSuffix {
    Property(String),
    Index(Expr),
    Slice {
        start: Option<Expr>,
        end: Option<Expr>,
    },
    FunctionCall(Vec<Expr>),
    IsNull,
    IsNotNull,
    Binary(BinaryOp, Expr),
    In(Expr),
}

/// Postfix operations for building expressions from primary expressions.
///
/// Used by the parser's `PostfixExpression` rule to collect operations like
/// property access (`.prop`), function calls (`(args)`), and indexing (`[i]`).
/// This approach avoids LR(1) conflicts that would occur with left-recursive
/// grammar rules for dotted function names like `uni.vector.query()`.
///
/// Note: This is separate from `ExprSuffix` which serves the list comprehension
/// factoring logic (resolving `[Identifier ...` ambiguity).
#[derive(Debug, Clone, PartialEq)]
pub enum PostfixSuffix {
    Property(String),
    Call {
        args: Vec<Expr>,
        distinct: bool,
        window_spec: Option<WindowSpec>,
    },
    Index(Expr),
    Slice {
        start: Option<Expr>,
        end: Option<Expr>,
    },
    MapProjection(Vec<MapProjectionItem>),
}

/// Extracts a dotted name from a variable or property chain.
///
/// Used to convert property access chains into dotted function names.
///
/// # Examples
///
/// - `Variable("func")` => `Some("func")`
/// - `Property(Variable("uni"), "validAt")` => `Some("uni.validAt")`
/// - `Property(Property(Variable("db"), "idx"), "query")` => `Some("db.idx.query")`
///
/// Returns `None` for expressions that are not simple identifier chains.
pub fn extract_dotted_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Variable(name) => Some(name.clone()),
        Expr::Property(base, prop) => {
            let base_name = extract_dotted_name(base)?;
            Some(format!("{base_name}.{prop}"))
        }
        _ => None,
    }
}

/// Applies a postfix suffix to an expression, building a new expression.
///
/// This function handles the key transformation for dotted function names:
/// when a `Call` suffix follows a property chain like `db.idx.vector`,
/// it extracts the dotted name and creates a `FunctionCall` expression.
pub fn apply_suffix(expr: Expr, suffix: PostfixSuffix) -> Expr {
    match suffix {
        PostfixSuffix::Property(prop) => Expr::Property(Box::new(expr), prop),

        PostfixSuffix::Call {
            args,
            distinct,
            window_spec,
        } => {
            let name = extract_dotted_name(&expr).unwrap_or_else(|| {
                panic!(
                    "apply_suffix: function call requires variable or property chain, got: {expr:?}"
                )
            });
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            }
        }

        PostfixSuffix::Index(index) => Expr::ArrayIndex {
            array: Box::new(expr),
            index: Box::new(index),
        },

        PostfixSuffix::Slice { start, end } => Expr::ArraySlice {
            array: Box::new(expr),
            start: start.map(Box::new),
            end: end.map(Box::new),
        },

        PostfixSuffix::MapProjection(items) => Expr::MapProjection {
            base: Box::new(expr),
            items,
        },
    }
}

/// Apply a sequence of expression suffixes to build a complete expression.
/// Example: id.prop[0] + 1 => ((id.prop)[0]) + 1
fn apply_suffixes(mut expr: Expr, suffixes: Vec<ExprSuffix>) -> Expr {
    for suffix in suffixes {
        expr = match suffix {
            ExprSuffix::Property(name) => Expr::Property(Box::new(expr), name),

            ExprSuffix::Index(idx) => Expr::ArrayIndex {
                array: Box::new(expr),
                index: Box::new(idx),
            },

            ExprSuffix::Slice { start, end } => Expr::ArraySlice {
                array: Box::new(expr),
                start: start.map(Box::new),
                end: end.map(Box::new),
            },

            ExprSuffix::FunctionCall(args) => {
                let name = extract_dotted_name(&expr)
                    .unwrap_or_else(|| panic!("Function call suffix requires variable or property chain expression, got: {expr:?}"));
                Expr::FunctionCall {
                    name,
                    args,
                    distinct: false,
                    window_spec: None,
                }
            }

            ExprSuffix::IsNull => Expr::IsNull(Box::new(expr)),
            ExprSuffix::IsNotNull => Expr::IsNotNull(Box::new(expr)),

            ExprSuffix::Binary(op, rhs) => Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(rhs),
            },

            ExprSuffix::In(right) => Expr::In {
                expr: Box::new(expr),
                list: Box::new(right),
            },
        };
    }
    expr
}

/// Join a slice of expressions with a separator using `to_string_repr`.
fn join_exprs(exprs: &[Expr], sep: &str) -> String {
    exprs
        .iter()
        .map(|e| e.to_string_repr())
        .collect::<Vec<_>>()
        .join(sep)
}

impl Expr {
    /// Sentinel expression representing a literal `true`.
    ///
    /// Useful in the planner for predicate reduction: when all conjuncts have
    /// been pushed down, the remaining predicate is replaced with this constant.
    pub const TRUE: Expr = Expr::Literal(CypherLiteral::Bool(true));

    /// Returns `true` if this expression is the literal boolean `true`.
    pub fn is_true_literal(&self) -> bool {
        matches!(self, Expr::Literal(CypherLiteral::Bool(true)))
    }

    /// Extract a simple variable name if this expression is just a variable reference
    pub fn extract_variable(&self) -> Option<String> {
        match self {
            Expr::Variable(v) => Some(v.clone()),
            _ => None,
        }
    }

    /// Substitute all occurrences of a variable with a new variable name
    pub fn substitute_variable(&self, old_var: &str, new_var: &str) -> Expr {
        let sub = |e: &Expr| e.substitute_variable(old_var, new_var);
        let sub_box = |e: &Expr| Box::new(sub(e));
        let sub_opt = |o: &Option<Box<Expr>>| o.as_ref().map(|e| sub_box(e));
        let sub_vec = |v: &[Expr]| v.iter().map(sub).collect();

        match self {
            Expr::Variable(v) if v == old_var => Expr::Variable(new_var.to_string()),
            Expr::Variable(_) | Expr::Literal(_) | Expr::Parameter(_) | Expr::Wildcard => {
                self.clone()
            }

            Expr::Property(base, prop) => Expr::Property(sub_box(base), prop.clone()),

            Expr::List(exprs) => Expr::List(sub_vec(exprs)),

            Expr::Map(entries) => {
                Expr::Map(entries.iter().map(|(k, v)| (k.clone(), sub(v))).collect())
            }

            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => Expr::FunctionCall {
                name: name.clone(),
                args: sub_vec(args),
                distinct: *distinct,
                window_spec: window_spec.clone(),
            },

            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: sub_box(left),
                op: *op,
                right: sub_box(right),
            },

            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: sub_box(expr),
            },

            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => Expr::Case {
                expr: sub_opt(expr),
                when_then: when_then.iter().map(|(w, t)| (sub(w), sub(t))).collect(),
                else_expr: sub_opt(else_expr),
            },

            // Don't substitute inside subqueries
            Expr::Exists { .. } | Expr::CountSubquery(_) | Expr::CollectSubquery(_) => self.clone(),

            Expr::IsNull(e) => Expr::IsNull(sub_box(e)),
            Expr::IsNotNull(e) => Expr::IsNotNull(sub_box(e)),
            Expr::IsUnique(e) => Expr::IsUnique(sub_box(e)),

            Expr::In { expr, list } => Expr::In {
                expr: sub_box(expr),
                list: sub_box(list),
            },

            Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
                array: sub_box(array),
                index: sub_box(index),
            },

            Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
                array: sub_box(array),
                start: sub_opt(start),
                end: sub_opt(end),
            },

            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => {
                let shadowed = variable == old_var;
                Expr::Quantifier {
                    quantifier: *quantifier,
                    variable: variable.clone(),
                    list: sub_box(list),
                    predicate: if shadowed {
                        predicate.clone()
                    } else {
                        sub_box(predicate)
                    },
                }
            }

            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr,
            } => {
                let shadowed = variable == old_var || accumulator == old_var;
                Expr::Reduce {
                    accumulator: accumulator.clone(),
                    init: sub_box(init),
                    variable: variable.clone(),
                    list: sub_box(list),
                    expr: if shadowed {
                        expr.clone()
                    } else {
                        sub_box(expr)
                    },
                }
            }

            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                map_expr,
            } => {
                let shadowed = variable == old_var;
                Expr::ListComprehension {
                    variable: variable.clone(),
                    list: sub_box(list),
                    where_clause: if shadowed {
                        where_clause.clone()
                    } else {
                        sub_opt(where_clause)
                    },
                    map_expr: if shadowed {
                        map_expr.clone()
                    } else {
                        sub_box(map_expr)
                    },
                }
            }

            Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause,
                map_expr,
            } => {
                if path_variable.as_deref() == Some(old_var) {
                    self.clone()
                } else {
                    Expr::PatternComprehension {
                        path_variable: path_variable.clone(),
                        pattern: pattern.clone(),
                        where_clause: sub_opt(where_clause),
                        map_expr: sub_box(map_expr),
                    }
                }
            }

            Expr::ValidAt {
                entity,
                timestamp,
                start_prop,
                end_prop,
            } => Expr::ValidAt {
                entity: sub_box(entity),
                timestamp: sub_box(timestamp),
                start_prop: start_prop.clone(),
                end_prop: end_prop.clone(),
            },

            Expr::MapProjection { base, items } => Expr::MapProjection {
                base: sub_box(base),
                items: items
                    .iter()
                    .map(|item| match item {
                        MapProjectionItem::LiteralEntry(key, expr) => {
                            MapProjectionItem::LiteralEntry(key.clone(), sub_box(expr))
                        }
                        MapProjectionItem::Variable(v) if v == old_var => {
                            MapProjectionItem::Variable(new_var.to_string())
                        }
                        other => other.clone(),
                    })
                    .collect(),
            },

            Expr::LabelCheck { expr, labels } => Expr::LabelCheck {
                expr: sub_box(expr),
                labels: labels.clone(),
            },
        }
    }

    /// Check if this expression contains an aggregate function
    pub fn is_aggregate(&self) -> bool {
        match self {
            Expr::FunctionCall {
                name, window_spec, ..
            } => {
                window_spec.is_none()
                    && matches!(
                        name.to_lowercase().as_str(),
                        "count"
                            | "sum"
                            | "avg"
                            | "min"
                            | "max"
                            | "collect"
                            | "stdev"
                            | "stdevp"
                            | "percentiledisc"
                            | "percentilecont"
                    )
            }
            Expr::CountSubquery(_) | Expr::CollectSubquery(_) => true,
            Expr::Property(base, _) => base.is_aggregate(),
            Expr::List(exprs) => exprs.iter().any(|e| e.is_aggregate()),
            Expr::Map(entries) => entries.iter().any(|(_, v)| v.is_aggregate()),
            Expr::BinaryOp { left, right, .. } => left.is_aggregate() || right.is_aggregate(),
            Expr::UnaryOp { expr, .. } => expr.is_aggregate(),
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                expr.as_ref().is_some_and(|e| e.is_aggregate())
                    || when_then
                        .iter()
                        .any(|(w, t)| w.is_aggregate() || t.is_aggregate())
                    || else_expr.as_ref().is_some_and(|e| e.is_aggregate())
            }
            Expr::In { expr, list } => expr.is_aggregate() || list.is_aggregate(),
            Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::IsUnique(e) => e.is_aggregate(),
            Expr::ArrayIndex { array, index } => array.is_aggregate() || index.is_aggregate(),
            Expr::ArraySlice { array, start, end } => {
                array.is_aggregate()
                    || start.as_ref().is_some_and(|e| e.is_aggregate())
                    || end.as_ref().is_some_and(|e| e.is_aggregate())
            }
            Expr::Quantifier {
                list, predicate, ..
            } => list.is_aggregate() || predicate.is_aggregate(),
            Expr::Reduce {
                init, list, expr, ..
            } => init.is_aggregate() || list.is_aggregate() || expr.is_aggregate(),
            Expr::ListComprehension {
                list,
                where_clause,
                map_expr,
                ..
            } => {
                list.is_aggregate()
                    || where_clause.as_ref().is_some_and(|e| e.is_aggregate())
                    || map_expr.is_aggregate()
            }
            Expr::PatternComprehension {
                where_clause,
                map_expr,
                ..
            } => where_clause.as_ref().is_some_and(|e| e.is_aggregate()) || map_expr.is_aggregate(),
            _ => false,
        }
    }

    /// Generate a string representation of this expression for debugging/display
    pub fn to_string_repr(&self) -> String {
        match self {
            Expr::Literal(v) => v.to_string(),
            Expr::Parameter(p) => format!("${p}"),
            Expr::Variable(v) => v.clone(),
            Expr::Wildcard => "*".to_string(),
            Expr::Property(base, prop) => {
                format!("{}.{prop}", base.to_string_repr())
            }
            Expr::List(exprs) => format!("[{}]", join_exprs(exprs, ", ")),
            Expr::Map(entries) => {
                let items = entries
                    .iter()
                    .map(|(k, v)| format!("{k}: {}", v.to_string_repr()))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{items}}}")
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => {
                let args_str = join_exprs(args, ", ");
                let distinct_str = if *distinct { "DISTINCT " } else { "" };
                let base = format!("{name}({distinct_str}{args_str})");
                let Some(window) = window_spec else {
                    return base;
                };
                let mut parts = Vec::new();
                if !window.partition_by.is_empty() {
                    parts.push(format!(
                        "PARTITION BY {}",
                        join_exprs(&window.partition_by, ", ")
                    ));
                }
                if !window.order_by.is_empty() {
                    let items = window
                        .order_by
                        .iter()
                        .map(|s| {
                            let dir = if s.ascending { "ASC" } else { "DESC" };
                            format!("{} {dir}", s.expr.to_string_repr())
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    parts.push(format!("ORDER BY {items}"));
                }
                format!("{base} OVER ({})", parts.join(" "))
            }
            Expr::BinaryOp { left, op, right } => {
                format!(
                    "{} {} {}",
                    left.to_string_repr(),
                    op,
                    right.to_string_repr()
                )
            }
            Expr::UnaryOp { op, expr } => {
                format!("{op}{}", expr.to_string_repr())
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                let mut s = "CASE".to_string();
                if let Some(e) = expr {
                    s.push_str(&format!(" {}", e.to_string_repr()));
                }
                for (w, t) in when_then {
                    s.push_str(&format!(
                        " WHEN {} THEN {}",
                        w.to_string_repr(),
                        t.to_string_repr()
                    ));
                }
                if let Some(e) = else_expr {
                    s.push_str(&format!(" ELSE {}", e.to_string_repr()));
                }
                s.push_str(" END");
                s
            }
            Expr::Exists { .. } => "EXISTS {...}".to_string(),
            Expr::CountSubquery(_) => "COUNT {...}".to_string(),
            Expr::CollectSubquery(_) => "COLLECT {...}".to_string(),
            Expr::IsNull(e) => format!("{} IS NULL", e.to_string_repr()),
            Expr::IsNotNull(e) => format!("{} IS NOT NULL", e.to_string_repr()),
            Expr::IsUnique(e) => format!("{} IS UNIQUE", e.to_string_repr()),
            Expr::In { expr, list } => {
                format!("{} IN {}", expr.to_string_repr(), list.to_string_repr())
            }
            Expr::ArrayIndex { array, index } => {
                format!("{}[{}]", array.to_string_repr(), index.to_string_repr())
            }
            Expr::ArraySlice { array, start, end } => {
                let start_str = start
                    .as_ref()
                    .map(|e| e.to_string_repr())
                    .unwrap_or_default();
                let end_str = end.as_ref().map(|e| e.to_string_repr()).unwrap_or_default();
                format!("{}[{}..{}]", array.to_string_repr(), start_str, end_str)
            }
            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => {
                format!(
                    "{quantifier}({variable} IN {} WHERE {})",
                    list.to_string_repr(),
                    predicate.to_string_repr()
                )
            }
            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr,
            } => {
                format!(
                    "REDUCE({accumulator} = {}, {variable} IN {} | {})",
                    init.to_string_repr(),
                    list.to_string_repr(),
                    expr.to_string_repr()
                )
            }

            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                map_expr,
            } => {
                let where_str = where_clause
                    .as_ref()
                    .map_or(String::new(), |e| format!(" WHERE {}", e.to_string_repr()));
                format!(
                    "[{variable} IN {}{where_str}  | {}]",
                    list.to_string_repr(),
                    map_expr.to_string_repr()
                )
            }

            Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause,
                map_expr,
            } => {
                let var_part = path_variable
                    .as_ref()
                    .map(|v| format!("{v} = "))
                    .unwrap_or_default();
                let where_str = where_clause
                    .as_ref()
                    .map_or(String::new(), |e| format!(" WHERE {}", e.to_string_repr()));
                format!(
                    "[{var_part}{pattern:?}{where_str} | {}]",
                    map_expr.to_string_repr()
                )
            }

            Expr::ValidAt {
                entity,
                timestamp,
                start_prop,
                end_prop,
            } => match (start_prop, end_prop) {
                (Some(start), Some(end)) => format!(
                    "{} VALID_AT({}, '{start}', '{end}')",
                    entity.to_string_repr(),
                    timestamp.to_string_repr(),
                ),
                _ => format!(
                    "{} VALID_AT {}",
                    entity.to_string_repr(),
                    timestamp.to_string_repr(),
                ),
            },

            Expr::MapProjection { base, items } => {
                let items_str = items
                    .iter()
                    .map(MapProjectionItem::to_string_repr)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}{{{items_str}}}", base.to_string_repr())
            }

            Expr::LabelCheck { expr, labels } => {
                let labels_str: String = labels.iter().map(|l| format!(":{l}")).collect();
                format!("{}{labels_str}", expr.to_string_repr())
            }
        }
    }

    /// Call `f` on each direct child expression (non-recursive).
    ///
    /// Does NOT descend into subqueries (Exists, CountSubquery, CollectSubquery)
    /// since those have separate variable scope.
    pub fn for_each_child(&self, f: &mut dyn FnMut(&Expr)) {
        match self {
            Expr::Literal(_) | Expr::Parameter(_) | Expr::Variable(_) | Expr::Wildcard => {}
            Expr::Property(base, _) => f(base),
            Expr::List(items) => {
                for item in items {
                    f(item);
                }
            }
            Expr::Map(entries) => {
                for (_, expr) in entries {
                    f(expr);
                }
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    f(arg);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                f(left);
                f(right);
            }
            Expr::UnaryOp { expr, .. } => f(expr),
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                if let Some(e) = expr {
                    f(e);
                }
                for (w, t) in when_then {
                    f(w);
                    f(t);
                }
                if let Some(e) = else_expr {
                    f(e);
                }
            }
            Expr::Exists { .. } | Expr::CountSubquery(_) | Expr::CollectSubquery(_) => {}
            Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::IsUnique(e) => f(e),
            Expr::In { expr, list } => {
                f(expr);
                f(list);
            }
            Expr::ArrayIndex { array, index } => {
                f(array);
                f(index);
            }
            Expr::ArraySlice { array, start, end } => {
                f(array);
                if let Some(s) = start {
                    f(s);
                }
                if let Some(e) = end {
                    f(e);
                }
            }
            Expr::Quantifier {
                list, predicate, ..
            } => {
                f(list);
                f(predicate);
            }
            Expr::Reduce {
                init, list, expr, ..
            } => {
                f(init);
                f(list);
                f(expr);
            }
            Expr::ListComprehension {
                list,
                where_clause,
                map_expr,
                ..
            } => {
                f(list);
                if let Some(w) = where_clause {
                    f(w);
                }
                f(map_expr);
            }
            Expr::PatternComprehension {
                where_clause,
                map_expr,
                ..
            } => {
                if let Some(w) = where_clause {
                    f(w);
                }
                f(map_expr);
            }
            Expr::ValidAt {
                entity, timestamp, ..
            } => {
                f(entity);
                f(timestamp);
            }
            Expr::MapProjection { base, items } => {
                f(base);
                for item in items {
                    if let MapProjectionItem::LiteralEntry(_, expr) = item {
                        f(expr);
                    }
                }
            }
            Expr::LabelCheck { expr, .. } => f(expr),
        }
    }

    /// Map each direct child expression through `f`, producing a new Expr.
    ///
    /// Same scoping rules as `for_each_child`: does not descend into subqueries.
    pub fn map_children(self, f: &mut dyn FnMut(Expr) -> Expr) -> Expr {
        match self {
            Expr::Literal(_) | Expr::Parameter(_) | Expr::Variable(_) | Expr::Wildcard => self,
            Expr::Property(base, prop) => Expr::Property(Box::new(f(*base)), prop),
            Expr::List(items) => Expr::List(items.into_iter().map(&mut *f).collect()),
            Expr::Map(entries) => Expr::Map(entries.into_iter().map(|(k, v)| (k, f(v))).collect()),
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => Expr::FunctionCall {
                name,
                args: args.into_iter().map(&mut *f).collect(),
                distinct,
                window_spec,
            },
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(f(*left)),
                op,
                right: Box::new(f(*right)),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(f(*expr)),
            },
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => Expr::Case {
                expr: expr.map(|e| Box::new(f(*e))),
                when_then: when_then.into_iter().map(|(w, t)| (f(w), f(t))).collect(),
                else_expr: else_expr.map(|e| Box::new(f(*e))),
            },
            Expr::Exists { .. } | Expr::CountSubquery(_) | Expr::CollectSubquery(_) => self,
            Expr::IsNull(e) => Expr::IsNull(Box::new(f(*e))),
            Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(f(*e))),
            Expr::IsUnique(e) => Expr::IsUnique(Box::new(f(*e))),
            Expr::In { expr, list } => Expr::In {
                expr: Box::new(f(*expr)),
                list: Box::new(f(*list)),
            },
            Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
                array: Box::new(f(*array)),
                index: Box::new(f(*index)),
            },
            Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
                array: Box::new(f(*array)),
                start: start.map(|s| Box::new(f(*s))),
                end: end.map(|e| Box::new(f(*e))),
            },
            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => Expr::Quantifier {
                quantifier,
                variable,
                list: Box::new(f(*list)),
                predicate: Box::new(f(*predicate)),
            },
            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr,
            } => Expr::Reduce {
                accumulator,
                init: Box::new(f(*init)),
                variable,
                list: Box::new(f(*list)),
                expr: Box::new(f(*expr)),
            },
            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                map_expr,
            } => Expr::ListComprehension {
                variable,
                list: Box::new(f(*list)),
                where_clause: where_clause.map(|w| Box::new(f(*w))),
                map_expr: Box::new(f(*map_expr)),
            },
            Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause,
                map_expr,
            } => Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause: where_clause.map(|w| Box::new(f(*w))),
                map_expr: Box::new(f(*map_expr)),
            },
            Expr::ValidAt {
                entity,
                timestamp,
                start_prop,
                end_prop,
            } => Expr::ValidAt {
                entity: Box::new(f(*entity)),
                timestamp: Box::new(f(*timestamp)),
                start_prop,
                end_prop,
            },
            Expr::MapProjection { base, items } => Expr::MapProjection {
                base: Box::new(f(*base)),
                items: items
                    .into_iter()
                    .map(|item| match item {
                        MapProjectionItem::LiteralEntry(key, expr) => {
                            MapProjectionItem::LiteralEntry(key, Box::new(f(*expr)))
                        }
                        other => other,
                    })
                    .collect(),
            },
            Expr::LabelCheck { expr, labels } => Expr::LabelCheck {
                expr: Box::new(f(*expr)),
                labels,
            },
        }
    }
}
