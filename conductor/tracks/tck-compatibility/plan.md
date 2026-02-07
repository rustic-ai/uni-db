# Implementation Plan - TCK Compatibility

## Phase 1: Foundations (Type System & Literals)
- [ ] **Literals**
    - [ ] Fix nested List literal parsing and execution (`Literals7`).
    - [ ] Fix Map literal parsing and execution (`Literals8`).
- [ ] **Type Conversion**
    - [ ] Fix `toInteger` for mixed types and edge cases.
    - [ ] Fix `toFloat` behavior.
    - [ ] Fix `toBoolean` behavior.
- [ ] **Comparisons & Logic**
    - [ ] Fix Range comparisons (`Comparison2`, `Comparison3`).
    - [ ] Fix `NOT` operator logic (`Boolean4`).

## Phase 2: Read Operations
- [ ] **Variable Length Match**
    - [ ] Fix `MATCH (a)-[*]->(b)` semantics (`Match4`, `Match5`).
    - [ ] Fix path length bounds (e.g., `*1..3`).
- [ ] **Aggregations (High Priority)**
    - [ ] Implement `COUNT` (including `COUNT(DISTINCT ...)`).
    - [ ] Implement `MIN` / `MAX`.
    - [ ] Implement `SUM`.
    - [ ] Implement `COLLECT`.
    - [ ] Implement `DISTINCT` in projections.
- [ ] **Projections**
    - [ ] Fix `RETURN` expression evaluation (`Return2`).
    - [ ] Fix Column renaming/aliasing (`Return4`).
    - [ ] Fix Implicit Grouping (`Return6`).
- [ ] **Ordering**
    - [ ] Fix `ORDER BY` with complex expressions (`ReturnOrderBy2`).
    - [ ] Fix Sort stability and direction.

## Phase 3: Write Operations
- [ ] **Create**
    - [ ] Fix Node creation side effects (`Create1`).
    - [ ] Fix Relationship creation (`Create2`).
- [ ] **Delete**
    - [ ] Fix `DETACH DELETE` logic (`Delete1`).
    - [ ] Ensure connected relationships are deleted.
- [ ] **Set/Remove**
    - [ ] Fix `SET` property logic (`Set1`).
    - [ ] Fix `REMOVE` property logic (`Remove1`).
- [ ] **Merge**
    - [ ] Implement `MERGE` basic node matching (`Merge1`).
    - [ ] Implement `ON CREATE` actions (`Merge2`).
    - [ ] Implement `ON MATCH` actions (`Merge3`).

## Phase 4: Functions & Expressions
- [ ] **List Functions**
    - [ ] Fix `range()` function.
    - [ ] Fix `size()` for lists and strings.
    - [ ] Fix List Slicing (`List2`).
- [ ] **Map Functions**
    - [ ] Fix `keys()` function.
    - [ ] Fix dynamic map access (`map[key]`).
- [ ] **String Functions**
    - [ ] Fix `substring()`.
    - [ ] Fix `contains`, `starts with`, `ends with`.
- [ ] **Path Functions**
    - [ ] Fix `nodes(p)`, `relationships(p)`, `length(p)`.

## Phase 5: Temporal & Advanced
- [ ] **Temporal**
    - [ ] Fix creation from string (`Temporal2`).
    - [ ] Fix projection/extraction (`Temporal3`).
- [ ] **Advanced**
    - [ ] Fix Pattern Comprehensions.
    - [ ] Fix Quantifier invariants.
