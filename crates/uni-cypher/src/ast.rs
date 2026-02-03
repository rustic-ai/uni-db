use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub options: std::collections::HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateFullTextIndex {
    pub name: String,
    pub label: String,
    pub properties: Vec<String>,
    pub options: std::collections::HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateScalarIndex {
    pub name: String,
    pub label: String,
    pub expressions: Vec<Expr>,
    pub where_clause: Option<Expr>,
    pub options: std::collections::HashMap<String, Value>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateJsonFtsIndex {
    pub name: String,
    pub label: String,
    pub column: String,
    pub options: std::collections::HashMap<String, Value>,
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
    pub options: std::collections::HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CopyFromCommand {
    pub label: String,
    pub path: String,
    pub format: String,
    pub options: std::collections::HashMap<String, Value>,
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

// Helper enum for parser
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
    LoadCsv(LoadCsvClause),
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
    Expr { expr: Expr, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnwindClause {
    pub expr: Expr,
    pub variable: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadCsvClause {
    pub url: String,
    pub variable: String,
    pub with_headers: bool,
    pub field_terminator: Option<char>,
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
}

impl CypherLiteral {
    /// Convert to `serde_json::Value` at the executor boundary where runtime
    /// values are still JSON.
    pub fn to_json_value(&self) -> Value {
        match self {
            CypherLiteral::Null => Value::Null,
            CypherLiteral::Bool(b) => Value::Bool(*b),
            CypherLiteral::Integer(i) => Value::from(*i),
            CypherLiteral::Float(f) => {
                // serde_json::Number::from_f64 returns None for NaN/Infinity
                match serde_json::Number::from_f64(*f) {
                    Some(n) => Value::Number(n),
                    None => Value::Null, // NaN/Inf → null in JSON
                }
            }
            CypherLiteral::String(s) => Value::String(s.clone()),
        }
    }
}

impl std::fmt::Display for CypherLiteral {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CypherLiteral::Null => write!(f, "null"),
            CypherLiteral::Bool(b) => write!(f, "{}", b),
            CypherLiteral::Integer(i) => write!(f, "{}", i),
            CypherLiteral::Float(v) => write!(f, "{}", v),
            CypherLiteral::String(s) => write!(f, "\"{}\"", s),
        }
    }
}

impl From<i64> for CypherLiteral {
    fn from(v: i64) -> Self {
        CypherLiteral::Integer(v)
    }
}

impl From<f64> for CypherLiteral {
    fn from(v: f64) -> Self {
        CypherLiteral::Float(v)
    }
}

impl From<bool> for CypherLiteral {
    fn from(v: bool) -> Self {
        CypherLiteral::Bool(v)
    }
}

impl From<String> for CypherLiteral {
    fn from(v: String) -> Self {
        CypherLiteral::String(v)
    }
}

impl From<&str> for CypherLiteral {
    fn from(v: &str) -> Self {
        CypherLiteral::String(v.to_string())
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
    Exists(Box<Query>),
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MapProjectionItem {
    Property(String),                // .name
    AllProperties,                   // .*
    LiteralEntry(String, Box<Expr>), // key: expr
    Variable(String),                // variable
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Quantifier {
    All,
    Any,
    Single,
    None,
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Neg,
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

    /// [id.prop, ...] or [id + 1, ...] - List literal with complex first element
    ExpressionTail {
        suffix: Vec<ExprSuffix>,
        more: Vec<Expr>,
    },

    /// \[id, ...\] or \[id\] - List literal with simple identifier element
    SimpleTail { more: Vec<Expr> },
}

impl ListAfterIdentifier {
    /// Resolve this intermediate representation into a final Expr, given the identifier
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
                let mut items = vec![first];
                items.extend(more);
                Expr::List(items)
            }
            ListAfterIdentifier::SimpleTail { more } => {
                let mut items = vec![Expr::Variable(id)];
                items.extend(more);
                Expr::List(items)
            }
        }
    }
}

/// Expression suffix for building complex expressions after an identifier.
/// Used to parse things like: id.prop, id\[0\], id(), id+1, etc.
#[derive(Debug, Clone)]
pub enum ExprSuffix {
    // Postfix operators
    Property(String),
    Index(Expr),
    Slice {
        start: Option<Expr>,
        end: Option<Expr>,
    },
    FunctionCall(Vec<Expr>),
    IsNull,
    IsNotNull,

    // Binary operators - capture right-hand side
    Add(Expr),
    Sub(Expr),
    Mul(Expr),
    Div(Expr),
    Mod(Expr),
    Pow(Expr),

    // Comparison operators
    Eq(Expr),
    NotEq(Expr),
    Lt(Expr),
    LtEq(Expr),
    Gt(Expr),
    GtEq(Expr),

    // Logical operators
    And(Expr),
    Or(Expr),
    Xor(Expr),

    // String operators
    Contains(Expr),
    StartsWith(Expr),
    EndsWith(Expr),
    Regex(Expr),

    // List membership
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
            Some(format!("{}.{}", base_name, prop))
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
                    "apply_suffix: function call requires variable or property chain, got: {:?}",
                    expr
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
            // Postfix operators
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
                // Extract function name from variable or property chain
                // Supports: func(...), uni.func(...), db.idx.func(...)
                let name = extract_dotted_name(&expr)
                    .unwrap_or_else(|| panic!("Function call suffix requires variable or property chain expression, got: {:?}", expr));
                Expr::FunctionCall {
                    name,
                    args,
                    distinct: false,
                    window_spec: None,
                }
            }

            ExprSuffix::IsNull => Expr::IsNull(Box::new(expr)),

            ExprSuffix::IsNotNull => Expr::IsNotNull(Box::new(expr)),

            // Binary operators
            ExprSuffix::Add(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Add,
                right: Box::new(right),
            },

            ExprSuffix::Sub(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Sub,
                right: Box::new(right),
            },

            ExprSuffix::Mul(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Mul,
                right: Box::new(right),
            },

            ExprSuffix::Div(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Div,
                right: Box::new(right),
            },

            ExprSuffix::Mod(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Mod,
                right: Box::new(right),
            },

            ExprSuffix::Pow(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Pow,
                right: Box::new(right),
            },

            // Comparison operators
            ExprSuffix::Eq(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Eq,
                right: Box::new(right),
            },

            ExprSuffix::NotEq(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::NotEq,
                right: Box::new(right),
            },

            ExprSuffix::Lt(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Lt,
                right: Box::new(right),
            },

            ExprSuffix::LtEq(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::LtEq,
                right: Box::new(right),
            },

            ExprSuffix::Gt(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Gt,
                right: Box::new(right),
            },

            ExprSuffix::GtEq(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::GtEq,
                right: Box::new(right),
            },

            // Logical operators
            ExprSuffix::And(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::And,
                right: Box::new(right),
            },

            ExprSuffix::Or(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Or,
                right: Box::new(right),
            },

            ExprSuffix::Xor(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Xor,
                right: Box::new(right),
            },

            // String operators
            ExprSuffix::Contains(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Contains,
                right: Box::new(right),
            },

            ExprSuffix::StartsWith(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::StartsWith,
                right: Box::new(right),
            },

            ExprSuffix::EndsWith(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::EndsWith,
                right: Box::new(right),
            },

            ExprSuffix::Regex(right) => Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOp::Regex,
                right: Box::new(right),
            },

            // List membership
            ExprSuffix::In(right) => Expr::In {
                expr: Box::new(expr),
                list: Box::new(right),
            },
        };
    }
    expr
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
        match self {
            Expr::Variable(v) if v == old_var => Expr::Variable(new_var.to_string()),
            Expr::Variable(_) | Expr::Literal(_) | Expr::Parameter(_) | Expr::Wildcard => {
                self.clone()
            }

            Expr::Property(base, prop) => Expr::Property(
                Box::new(base.substitute_variable(old_var, new_var)),
                prop.clone(),
            ),

            Expr::List(exprs) => Expr::List(
                exprs
                    .iter()
                    .map(|e| e.substitute_variable(old_var, new_var))
                    .collect(),
            ),

            Expr::Map(entries) => Expr::Map(
                entries
                    .iter()
                    .map(|(k, v)| (k.clone(), v.substitute_variable(old_var, new_var)))
                    .collect(),
            ),

            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => Expr::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|e| e.substitute_variable(old_var, new_var))
                    .collect(),
                distinct: *distinct,
                window_spec: window_spec.clone(),
            },

            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(left.substitute_variable(old_var, new_var)),
                op: *op,
                right: Box::new(right.substitute_variable(old_var, new_var)),
            },

            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(expr.substitute_variable(old_var, new_var)),
            },

            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => Expr::Case {
                expr: expr
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| {
                        (
                            w.substitute_variable(old_var, new_var),
                            t.substitute_variable(old_var, new_var),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
            },

            Expr::Exists(query) => Expr::Exists(query.clone()), // Don't substitute inside subqueries
            Expr::CountSubquery(query) => Expr::CountSubquery(query.clone()),
            Expr::CollectSubquery(query) => Expr::CollectSubquery(query.clone()),

            Expr::IsNull(e) => Expr::IsNull(Box::new(e.substitute_variable(old_var, new_var))),
            Expr::IsNotNull(e) => {
                Expr::IsNotNull(Box::new(e.substitute_variable(old_var, new_var)))
            }
            Expr::IsUnique(e) => Expr::IsUnique(Box::new(e.substitute_variable(old_var, new_var))),

            Expr::In { expr, list } => Expr::In {
                expr: Box::new(expr.substitute_variable(old_var, new_var)),
                list: Box::new(list.substitute_variable(old_var, new_var)),
            },

            Expr::ArrayIndex { array, index } => Expr::ArrayIndex {
                array: Box::new(array.substitute_variable(old_var, new_var)),
                index: Box::new(index.substitute_variable(old_var, new_var)),
            },

            Expr::ArraySlice { array, start, end } => Expr::ArraySlice {
                array: Box::new(array.substitute_variable(old_var, new_var)),
                start: start
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
                end: end
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
            },

            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => {
                // Don't substitute inside the quantifier if it shadows the variable
                if variable == old_var {
                    Expr::Quantifier {
                        quantifier: *quantifier,
                        variable: variable.clone(),
                        list: Box::new(list.substitute_variable(old_var, new_var)),
                        predicate: predicate.clone(),
                    }
                } else {
                    Expr::Quantifier {
                        quantifier: *quantifier,
                        variable: variable.clone(),
                        list: Box::new(list.substitute_variable(old_var, new_var)),
                        predicate: Box::new(predicate.substitute_variable(old_var, new_var)),
                    }
                }
            }

            Expr::Reduce {
                accumulator,
                init,
                variable,
                list,
                expr,
            } => {
                // Don't substitute inside the reduce if it shadows the variable or accumulator
                let new_list = Box::new(list.substitute_variable(old_var, new_var));
                let new_init = Box::new(init.substitute_variable(old_var, new_var));

                if variable == old_var || accumulator == old_var {
                    Expr::Reduce {
                        accumulator: accumulator.clone(),
                        init: new_init,
                        variable: variable.clone(),
                        list: new_list,
                        expr: expr.clone(),
                    }
                } else {
                    Expr::Reduce {
                        accumulator: accumulator.clone(),
                        init: new_init,
                        variable: variable.clone(),
                        list: new_list,
                        expr: Box::new(expr.substitute_variable(old_var, new_var)),
                    }
                }
            }

            Expr::ListComprehension {
                variable,
                list,
                where_clause,
                map_expr,
            } => {
                // Don't substitute inside the comprehension if it shadows the variable
                let new_list = Box::new(list.substitute_variable(old_var, new_var));

                if variable == old_var {
                    Expr::ListComprehension {
                        variable: variable.clone(),
                        list: new_list,
                        where_clause: where_clause.clone(),
                        map_expr: map_expr.clone(),
                    }
                } else {
                    Expr::ListComprehension {
                        variable: variable.clone(),
                        list: new_list,
                        where_clause: where_clause
                            .as_ref()
                            .map(|e| Box::new(e.substitute_variable(old_var, new_var))),
                        map_expr: Box::new(map_expr.substitute_variable(old_var, new_var)),
                    }
                }
            }

            Expr::PatternComprehension {
                path_variable,
                pattern,
                where_clause,
                map_expr,
            } => {
                let new_where = where_clause
                    .as_ref()
                    .map(|e| Box::new(e.substitute_variable(old_var, new_var)));
                let new_map = Box::new(map_expr.substitute_variable(old_var, new_var));

                if path_variable.as_deref() == Some(old_var) {
                    Expr::PatternComprehension {
                        path_variable: path_variable.clone(),
                        pattern: pattern.clone(),
                        where_clause: where_clause.clone(),
                        map_expr: map_expr.clone(),
                    }
                } else {
                    Expr::PatternComprehension {
                        path_variable: path_variable.clone(),
                        pattern: pattern.clone(),
                        where_clause: new_where,
                        map_expr: new_map,
                    }
                }
            }

            Expr::ValidAt {
                entity,
                timestamp,
                start_prop,
                end_prop,
            } => Expr::ValidAt {
                entity: Box::new(entity.substitute_variable(old_var, new_var)),
                timestamp: Box::new(timestamp.substitute_variable(old_var, new_var)),
                start_prop: start_prop.clone(),
                end_prop: end_prop.clone(),
            },

            Expr::MapProjection { base, items } => Expr::MapProjection {
                base: Box::new(base.substitute_variable(old_var, new_var)),
                items: items
                    .iter()
                    .map(|item| match item {
                        MapProjectionItem::Property(prop) => {
                            MapProjectionItem::Property(prop.clone())
                        }
                        MapProjectionItem::AllProperties => MapProjectionItem::AllProperties,
                        MapProjectionItem::LiteralEntry(key, expr) => {
                            MapProjectionItem::LiteralEntry(
                                key.clone(),
                                Box::new(expr.substitute_variable(old_var, new_var)),
                            )
                        }
                        MapProjectionItem::Variable(v) if v == old_var => {
                            MapProjectionItem::Variable(new_var.to_string())
                        }
                        MapProjectionItem::Variable(v) => MapProjectionItem::Variable(v.clone()),
                    })
                    .collect(),
            },
        }
    }

    /// Check if this expression contains an aggregate function
    pub fn is_aggregate(&self) -> bool {
        match self {
            Expr::FunctionCall {
                name, window_spec, ..
            } => {
                // Window functions are not aggregates - they're window functions
                if window_spec.is_some() {
                    return false;
                }
                matches!(
                    name.to_lowercase().as_str(),
                    "count"
                        | "sum"
                        | "avg"
                        | "min"
                        | "max"
                        | "collect"
                        | "stdev"
                        | "stdevp"
                        | "percentileDisc"
                        | "percentileCont"
                )
            }
            Expr::CountSubquery(_) => true,
            Expr::CollectSubquery(_) => true,
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
            Expr::Literal(v) => format!("{}", v),
            Expr::Parameter(p) => format!("${}", p),
            Expr::Variable(v) => v.clone(),
            Expr::Wildcard => "*".to_string(),
            Expr::Property(base, prop) => format!("{}.{}", base.to_string_repr(), prop),
            Expr::List(exprs) => {
                let items: Vec<_> = exprs.iter().map(|e| e.to_string_repr()).collect();
                format!("[{}]", items.join(", "))
            }
            Expr::Map(entries) => {
                let items: Vec<_> = entries
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v.to_string_repr()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
            Expr::FunctionCall {
                name,
                args,
                distinct,
                window_spec,
            } => {
                let args_str: Vec<_> = args.iter().map(|e| e.to_string_repr()).collect();
                let distinct_str = if *distinct { "DISTINCT " } else { "" };
                let base = format!("{}({}{})", name, distinct_str, args_str.join(", "));
                if let Some(window) = window_spec {
                    let partition_str = if !window.partition_by.is_empty() {
                        format!(
                            "PARTITION BY {}",
                            window
                                .partition_by
                                .iter()
                                .map(|e| e.to_string_repr())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    } else {
                        String::new()
                    };
                    let order_str = if !window.order_by.is_empty() {
                        let items = window
                            .order_by
                            .iter()
                            .map(|s| {
                                format!(
                                    "{} {}",
                                    s.expr.to_string_repr(),
                                    if s.ascending { "ASC" } else { "DESC" }
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("ORDER BY {}", items)
                    } else {
                        String::new()
                    };
                    let over_contents = vec![partition_str, order_str]
                        .into_iter()
                        .filter(|s| !s.is_empty())
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!("{} OVER ({})", base, over_contents)
                } else {
                    base
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let op_str = match op {
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
                format!(
                    "({} {} {})",
                    left.to_string_repr(),
                    op_str,
                    right.to_string_repr()
                )
            }
            Expr::UnaryOp { op, expr } => {
                let op_str = match op {
                    UnaryOp::Not => "NOT ",
                    UnaryOp::Neg => "-",
                };
                format!("{}{}", op_str, expr.to_string_repr())
            }
            Expr::Case {
                expr,
                when_then,
                else_expr,
            } => {
                let mut result = "CASE".to_string();
                if let Some(e) = expr {
                    result.push_str(&format!(" {}", e.to_string_repr()));
                }
                for (w, t) in when_then {
                    result.push_str(&format!(
                        " WHEN {} THEN {}",
                        w.to_string_repr(),
                        t.to_string_repr()
                    ));
                }
                if let Some(e) = else_expr {
                    result.push_str(&format!(" ELSE {}", e.to_string_repr()));
                }
                result.push_str(" END");
                result
            }
            Expr::Exists(_) => "EXISTS {...}".to_string(),
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
                    .map_or("".to_string(), |e| e.to_string_repr());
                let end_str = end.as_ref().map_or("".to_string(), |e| e.to_string_repr());
                format!("{}[{}..{}]", array.to_string_repr(), start_str, end_str)
            }
            Expr::Quantifier {
                quantifier,
                variable,
                list,
                predicate,
            } => {
                let q_str = match quantifier {
                    Quantifier::All => "ALL",
                    Quantifier::Any => "ANY",
                    Quantifier::Single => "SINGLE",
                    Quantifier::None => "NONE",
                };
                format!(
                    "{}({} IN {} WHERE {})",
                    q_str,
                    variable,
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
                    "REDUCE({} = {}, {} IN {} | {})",
                    accumulator,
                    init.to_string_repr(),
                    variable,
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
                    "[{} IN {}{}  | {}]",
                    variable,
                    list.to_string_repr(),
                    where_str,
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
                    .map(|v| format!("{} = ", v))
                    .unwrap_or_default();
                let where_str = where_clause
                    .as_ref()
                    .map_or(String::new(), |e| format!(" WHERE {}", e.to_string_repr()));
                format!(
                    "[{}{:?}{} | {}]",
                    var_part,
                    pattern,
                    where_str,
                    map_expr.to_string_repr()
                )
            }

            Expr::ValidAt {
                entity,
                timestamp,
                start_prop,
                end_prop,
            } => {
                if let (Some(start), Some(end)) = (start_prop, end_prop) {
                    format!(
                        "{} VALID_AT({}, '{}', '{}')",
                        entity.to_string_repr(),
                        timestamp.to_string_repr(),
                        start,
                        end
                    )
                } else {
                    format!(
                        "{} VALID_AT {}",
                        entity.to_string_repr(),
                        timestamp.to_string_repr()
                    )
                }
            }

            Expr::MapProjection { base, items } => {
                let items_str: Vec<_> = items
                    .iter()
                    .map(|item| match item {
                        MapProjectionItem::Property(prop) => format!(".{}", prop),
                        MapProjectionItem::AllProperties => ".*".to_string(),
                        MapProjectionItem::LiteralEntry(key, expr) => {
                            format!("{}: {}", key, expr.to_string_repr())
                        }
                        MapProjectionItem::Variable(v) => v.clone(),
                    })
                    .collect();
                format!("{}{{{}}}", base.to_string_repr(), items_str.join(", "))
            }
        }
    }
}
