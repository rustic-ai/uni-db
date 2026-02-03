# Uni Cypher Parser: Design & Implementation

**Date**: 2026-01-24
**Status**: Implemented (Phase 1-3 Complete)
**Crate**: `crates/uni-cypher`

---

## 1. Overview

This document describes the design and implementation of the new OpenCypher parser for the Uni database. The goal of this component is to replace the legacy hand-written parser with a robust, specification-compliant parser generated from the official OpenCypher grammar.

**Key Technologies:**
*   **Parser Generator**: [LALRPOP](https://github.com/lalrpop/lalrpop) (LR(1) parser generator for Rust).
*   **Lexer**: [Logos](https://github.com/maciejhirsz/logos) (High-performance, macro-based lexer).
*   **Source of Truth**: `grammar/openCypher.bnf` (Official GQL/OpenCypher specification).

---

## 2. Architecture

The parsing pipeline consists of three distinct stages:

```mermaid
graph LR
    A[Input String] --> B[Lexer (Logos)]
    B --> C[Token Stream]
    C --> D[Parser (LALRPOP)]
    D --> E[AST (Abstract Syntax Tree)]
```

### 2.1 The Lexer (`src/lexer.rs`)
We utilize `logos` for tokenization due to its speed and ease of use.
*   **Case Insensitivity**: Cypher keywords (`MATCH`, `Create`, `mAtCh`) are case-insensitive. We handle this using `#[token("MATCH", ignore(ascii_case))]`.
*   **Context handling**: The lexer is stateless regarding grammar context but handles string escaping and numeric parsing immediately.
*   **New Tokens**: We extended the standard keyword set to include DDL commands (`INDEX`, `CONSTRAINT`, `SHOW`) and Bitwise operators (`&`, `|`, `~`, `<<`, `>>`) which were missing in older specs.

### 2.2 The Parser (`src/cypher.lalrpop`)
We use LALRPOP to define the grammar rules. The grammar file mimics the structure of `openCypher.bnf` but is adapted for LALRPOP's syntax and Rust's type system.

*   **Entry Point**: `Query` enum, which can be a `Single` statement, a `Union` of statements, or a `Schema` command.
*   **Error Recovery**: LALRPOP provides precise error locations (byte offsets) which we map back to the input string.

### 2.3 The AST (`src/ast.rs`)
The AST is designed to be serializable (`serde`) and strictly typed. Unlike the legacy parser which had a loose AST, this version enforces structure (e.g., a `SET` clause must contain `SetItem`s, not generic Expressions).

---

## 3. Implementation Details & Challenges

### 3.1 Operator Precedence
One of the most complex parts of parsing expressions is handling precedence correctly. We implemented a granular precedence hierarchy to ensure operations bind in the correct order without requiring complex post-processing.

**Hierarchy (Lowest to Highest Binding):**
1.  `OR`
2.  `XOR`
3.  `AND`
4.  `NOT`
5.  Comparisons (`=`, `<`, `IN`, `STARTS WITH`, etc.)
6.  Bitwise OR (`|`)
7.  Bitwise XOR (`^^`)
8.  Bitwise AND (`&`)
9.  Bitwise Shift (`<<`, `>>`)
10. Arithmetic Add/Sub (`+`, `-`)
11. Arithmetic Mul/Div/Mod (`*`, `/`, `%`)
12. Power (`^`)
13. Unary (`-`, `~`)
14. Property Access / Method Calls (`.`)
15. Atoms (Literals, Variables, Parenthesized groups)

### 3.2 Solving LR(1) Conflicts
We encountered several Shift/Reduce conflicts inherent to the grammar, which we resolved through specific strategies:

**Ambiguity: `SET` vs `REMOVE` vs `Property`**
*   *Conflict*: `SET n.prop` vs `SET n = map`.
*   *Resolution*: We defined specific rules for `SetItem` that look ahead. E.g., `Variable` `=` `Expression` is handled separately from `PropertyExpression` `=` `Expression`.

**Ambiguity: `Node Key` Constraints**
*   *Conflict*: `CONSTRAINT ON (n.p)` (Single property) vs `CONSTRAINT ON (n.p, n.q)` (Multiple).
*   *Resolution*: We flattened the rule to always expect a list of property variables, making the single-property case just a list of length 1.

### 3.3 Quantified Path Patterns (QPP)
We implemented support for modern GPM (Graph Pattern Matching) syntax, allowing paths to be grouped and repeated.

*   **Syntax**: `MATCH ((a)-[]->(b))+`
*   **Implementation**:
    *   We introduced a `PatternElement` enum variant `Parenthesized`.
    *   We modified the `PatternElements` rule to accept `NodeOrQPP` instead of just `NodePattern`.
    *   This allows recursive structures: A `PathPattern` contains `PatternElements`, which contains `PatternElement`, which can be `Parenthesized`, which contains a `PathPattern`.

### 3.4 Data Definition Language (DDL)
We extended the grammar to support schema management, treating them as a distinct top-level `SchemaCommand` variant of `Query`.

**Supported Commands:**
*   `CREATE INDEX [name] ON :Label(prop)`
*   `DROP INDEX name`
*   `CREATE CONSTRAINT [name] ON (n:Label) ASSERT ...` (Unique, Node Key, Exists)
*   `DROP CONSTRAINT name`

---

## 4. Key Improvements over Legacy Parser

| Feature | Legacy Parser | New LALRPOP Parser |
| :--- | :--- | :--- |
| **Maintainability** | Hand-written `match` blocks (2900+ lines) | Declarative Grammar (~500 lines) |
| **Correctness** | Ad-hoc, bug-prone | Formally verified against BNF |
| **QPP Support** | No | Yes (`((a)-[]->(b))+`) |
| **Bitwise Ops** | No | Yes (`&`, `|`, `~`, `<<`, `>>`) |
| **DDL** | Limited | Comprehensive (Index & Constraints) |
| **Error Messages** | Vague ("Parse error") | Precise ("Unexpected token at 10...") |

---

## 5. Next Steps: Integration

The parser is currently a standalone crate (`uni-cypher`). The next phase involves integrating it into the main query engine (`uni-query`):

1.  **AST Conversion**: Implement `TryFrom<uni_cypher::ast::Query>` for the logical planner's internal representation.
2.  **Planner Update**: Switch the entry point in `uni-query` to call `uni_cypher::parser::parse`.
3.  **Legacy Retirement**: Remove the old `parser.rs` and `tokenizer.rs`.
