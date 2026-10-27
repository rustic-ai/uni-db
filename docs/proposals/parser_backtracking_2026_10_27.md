# Cypher parser backtracking issue and repair status

Date: 2026-10-27. Status: parser repair validated; remaining CI run completed with a strict IAI comparison alert.

The Cypher parser could spend seconds parsing a query only a few hundred bytes
long. Several grammar alternatives could read the same nested expression, fail
late, and make another alternative read it again. Repeating that work at each
nesting level caused rapidly compounding CPU cost.

The first repair handled the original list-comprehension timeout and its small
valid reproduction. A subsequent five-minute fuzz run exposed a second timeout
through a malformed `CREATE` pattern. That second path has now been minimized,
isolated, and repaired alongside a broader grammar audit of shared-prefix rules.

## Observable behavior

The original nightly failure was a 1,633-byte malformed UTF-8 query that exceeded
libFuzzer's 10-second input budget. A smaller **valid** query exposes the same
original problem:

```cypher
RETURN [(a {k: [(a {k: 1})]})]
```

The small example illustrates the structure; nesting it further makes the cost
visible. These measurements used preserved Python bindings before the parser
repair and include query execution:

| Nesting depth | Query bytes | Lance 11 binding | Lance 13 binding |
| --- | ---: | ---: | ---: |
| 6 | 74 | 0.083 s | 0.086 s |
| 8 | 96 | 0.722 s | 0.720 s |
| 10 | 118 | 6.601 s | 6.368 s |

The bindings returned rows for these inputs. This is not restricted to invalid
syntax, and the original path demonstrably predates the Lance 13 upgrade.
These are diagnostic timings, not a general engine benchmark.

Generate the 118-byte reproduction with:

```python
query = 'RETURN ' + '[(a {k: ' * 10 + '1' + '})]' * 10
```

## Why the parser repeats work

Uni uses a parsing expression grammar implemented with `pest`. An ordered choice
tries its alternatives in order. When an alternative fails, the parser can return
to the beginning of that choice and try the next alternative.

The relevant rules live in
[`cypher.pest`](../../crates/uni-cypher/src/grammar/cypher.pest).
Before the first repair, two ambiguities compounded:

1. A list beginning with a parenthesized map projection could first be read as a
   **pattern comprehension**. The parser descended into the node's properties
   before discovering that the required comprehension separator, `|`, was absent.
   It then retried the same interior as a **list literal**.
2. A node predicate tried `properties WHERE expression` before trying plain
   `properties`. When no `WHERE` followed, it parsed the property expression a
   second time.

A single retry is cheap. Nested retries are not: each outer attempt can repeat
all the work below it. A nesting-depth limit does not solve this. The valid
reproduction is already slow at depth 10, well below the existing limit of 200.

The work happens during parsing, before the storage engine executes the query.
Changing Lance compaction, indexes, or I/O concurrency cannot repair this grammar
behavior.

## What the first repair changes

The current implementation checks for a pipe outside nested groups before
attempting a pattern comprehension. The lookahead skips strings, escaped
identifiers, and comments, so pipes inside those constructs do not falsely
identify a comprehension separator.

Node predicates now parse their properties once, followed by an optional
`WHERE` expression. The parse-tree structure consumed by the AST walker remains
compatible.

Regression coverage includes the exact original artifact, a valid query nested
to depth 40, and comprehension syntax involving nested expressions, quoted
pipes, and comments. Both Cypher and Locy entry points are checked because they
share the expression grammar.

All 15 backtracking regressions passed. The original artifact completed in
approximately 0.24 seconds in the combined regression check, and the full
144-test Cypher crate suite passed. These results establish that the first
repair handles the tested paths; they do not establish that every ambiguous
pattern path is safe.

## Second timeout root cause and complete repair

A subsequent run with the full five-minute fuzz budget and the curated seeds
found a second timeout (`timeout-22dd3a1ad7231ccf72c3123651672fc3ac023eb3`,
1,791 bytes; associated slow unit
`slow-unit-cb7a42f08f51df8b2dde6e1da01d27f8d4426276`, 1,254 bytes). Delta
debugging reduced the slow unit to a 107-byte core and exposed the exact
ambiguities in [`cypher.pest`](../../crates/uni-cypher/src/grammar/cypher.pest)
and [`locy.pest`](../../crates/uni-cypher/src/grammar/locy.pest):

1. **`pattern_ahead` false positives on `<` / `-` and unskipped comments/backticks.**
   In `primary_expression`, `pattern_expression` is guarded by `&pattern_ahead`
   before falling back to `"(" ~ expression ~ ")"`. Previously,
   `pattern_ahead = @{ balanced_parens ~ ws_or_comment* ~ ("<" | "-") }` checked
   only that the closing `)` was followed by `<` or `-`, and `balanced_parens`
   skipped only `string` literals (not `COMMENT` or `escaped_name`).
   Consequently:
   - Any parenthesized map projection or expression followed by subtraction or
     comparison — including valid queries such as
     `RETURN (a {k: (a {k: 1}) - 1}) - 1`, `RETURN (a {k: (a {k: 1}) < 1}) < 1`,
     or `RETURN (a {k: (a {k: 1})})--(1)` — satisfied `pattern_ahead`, parsed
     the entire nested interior via `node_pattern`, failed when `relationship_pattern`
     or the target `node_pattern` did not match, and re-parsed the interior via
     `"(" ~ expression ~ ")"`.
   - Comments (`/* )-< */`, `// )-`) or backtick identifiers (`` `)-` ``) inside
     a `node_pattern`'s properties prematurely closed `balanced_parens` on a
     fake `)` followed by `-` or `<`, triggering the same double parse per level.
2. **`pattern_comprehension_ahead` false positives on label-disjunction pipes.**
   A list literal whose element carries a `:Label1|Label2` predicate outside
   parens (e.g. `RETURN [(a {k: ...}):A|B]` or `RETURN [(a {k: ...})-->(b), c:A|B]`)
   had a top-level `|` inside `[...]`, passing `pattern_comprehension_ahead`,
   parsing the nested interior as `pattern_comprehension_body`, failing before
   `|`, and re-parsing as a list literal.
3. **Unfactored `rel_predicate`, `node_labels`, and `prob_projection`.**
   - `rel_predicate` still tried `properties ~ WHERE ~ expression` before bare
     `properties`, re-parsing relationship property maps whenever `WHERE` was
     omitted (`RETURN [(a)-[r {k: ...}]->(b) | b]`).
   - `node_labels` tried `node_label_disjunction` before `node_label_conjunction`,
     scanning the first `:Label` twice when no `|` followed.
   - `prob_projection` in `locy.pest` tried three separate `expression ~ ... ~ PROB`
     branches before `locy_yield_item` fell back to `expression ~ (AS ~ alias_identifier)?`.

### Repair summary

- **`pattern_ahead`** now requires `node_ahead ~ (ws_or_comment* ~ rel_ahead ~ ws_or_comment* ~ node_ahead)+`,
  where `node_ahead` and `rel_detail_ahead` lexically validate the node and
  relationship headers up to their balanced property/WHERE groups, and
  `balanced_group_body` skips `string`, `escaped_name`, and `COMMENT`.
- **`pattern_comprehension_ahead`** now anchors the comprehension prefix to
  `(identifier =)? (shortestPath(...) | pattern_element_ahead+)` followed by an
  optional `WHERE` clause and `|`, and skips `:Label|Label` disjunction pipes in
  `comprehension_scan_token`.
- **`rel_predicate`** is factored to `properties ~ (WHERE ~ expression)? | WHERE ~ expression`
  (matching `node_predicate`), **`node_labels`** factors the shared
  `":" ~ identifier_or_keyword` prefix, and **`prob_projection`** factors the
  shared `expression` prefix.
- Regression tests in
  [`parser_backtracking_regression.rs`](../../crates/uni-cypher/tests/parser_backtracking_regression.rs)
  and seed corpus entries under `fuzz/seeds/{cypher_parse,locy_parse}/` now
  retain both the exact 1,791-byte timeout and 1,254-byte slow artifacts and
  readable structural reproductions at depth 30–40.

## Validation results

The remaining CI run is complete. Correctness, conformance, fuzzing, and static
checks pass. The strict IAI comparison remains non-green; its alert is retained below.

- Parser and Locy compiler suites: **384 passed**.
- Cypher fuzzing: **661,531 executions in 301 seconds**, with the unchanged
  10-second per-input timeout.
- Final Locy fuzzing after the probability fix: **487,816 executions in 301
  seconds**, with the same timeout.
- Binding soak: **50,000 cases and 1,315,911 query executions** across all five
  distinct-seed shards; every shard passed within the 40-minute test limit.
- Cypher TCK: **3,925 passed and one skipped in each mode**.
- Locy TCK: **558 passed, zero failed in each mode**.
- Clippy with warnings denied, formatting, and all **nine static checks**
  including fuzz-lock consistency: passed.
- TCK wrapper failure-propagation tests and IAI collector/gate self-tests: passed.
- Current-source release IAI benchmark: all five measurement runs completed with
  the expected lookup-row and name assertions passing. Lookup median:
  **9,564,478 instructions**, CV **0.183%**, change against the committed CI
  baseline **-53.84%**. Baseline and thresholds remain unchanged.

The old Locy wrapper's exit code did not reflect its four scenario failures per
mode. The final results above use the corrected wrapper and verify the actual
scenario summaries. The final source and configuration snapshot was verified
unchanged at completion. See the
[completion manifest](parser_backtracking_2026_10_27/ci-completion-validation.json)
for commands, exit codes, saved logs, and hashes.

## Probability projection regression found by log audit

The shared-prefix refactor used `AS ~ alias_identifier? ~ PROB`. In `p AS PROB`,
the optional alias greedily consumed `PROB`; the remaining required `PROB`
then failed. The ordinary projection alternative accepted it as an alias instead,
losing the probability marker and the expected output name `p`.

The corrected choice is `AS ~ (alias_identifier ~ PROB | PROB)`. It keeps the
expression prefix factored while preserving shorthand and explicit aliases.
A regression test checks both forms, property-derived aliases, and ordinary
non-probability aliases. The parser and Locy compiler suites pass all 384 tests.

Both TCK report scripts previously used `cargo nextest ... || true` and
returned success after report generation, even when scenarios failed. They now
retain the nextest failure status, finish reporting both modes, and exit with
that status. Regression checks cover filtered failures, either failing mode,
successful runs, and aggregation failures. These checks are part of the static
CI guards. The corrected full Locy TCK run passes all 558 scenarios in each
mode, with zero failures.

## Relation to the other CI failures

The binding-soak timeout was a separate workload-budget problem. Splitting the
50,000 cases across five independent 10,000-case shards (`binding-shard-1`..`5`)
has now been validated end-to-end above: all five shards passed.

The strict instruction-count comparison against the CI baseline remains
unresolved. Five fresh benchmark runs passed assertions requiring the
correct lookup row and name, while local Rust 1.99.1 / Valgrind 3.27.1 measures
a 53.84% reduction against the CI baseline recorded on Rust 1.98.1 / Valgrind 3.22.
Its baseline and thresholds remain unchanged. A matching CI qualification run
is required before claiming this comparison is green; these local checks do not
establish the exact cause of the historical baseline difference.

## Evidence

- [Original upgrade and CI report](lance_13_upgrade_2026_10_26.md).
- [Parser regression tests](../../crates/uni-cypher/tests/parser_backtracking_regression.rs).
- [Evidence directory](parser_backtracking_2026_10_27/), including old/new
  diagnostic timings, parser test results, the failing inputs, and the
  compressed fuzz logs (`fuzz-cypher_parse-fixed.log.gz`, `locy-fuzz-final.log.gz`).
- [Latest CI completion results](parser_backtracking_2026_10_27/ci-completion-validation.json).
- [Upstream baseline qualification](https://github.com/rustic-ai/uni-db/actions/runs/35253386248).
