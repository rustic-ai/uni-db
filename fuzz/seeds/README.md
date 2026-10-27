# Fuzz seed corpus

Regression seeds for inputs that previously crashed. `cargo fuzz run`
merges these into the working corpus:

```bash
cargo +nightly fuzz run btic_decode corpus/btic_decode seeds/btic_decode
```

**Name `corpus/` first — the order matters.** libFuzzer writes every
newly-discovered input into the *first* corpus directory on the command line and
treats the remaining ones as read-only. The shorter form
`cargo fuzz run btic_decode seeds/btic_decode` therefore makes *this*
directory the output corpus, and one 30-second run buries its handful of
curated regression inputs under several hundred generated files. (Measured:
1 file became 384.) These are git-tracked, so that lands in `git status` as
hundreds of untracked files rather than anything louder.

Seeds also cover **timeouts**, not just crashes. Exponential backtracking in the
grammar is the failure this corpus has caught four times, and a 30-second blind run
rediscovers it only by luck — the nightly found the second instance three times
in eight nights.

- `btic_decode/utf8-boundary-bce-suffix` — multi-byte UTF-8 straddling the
  `len - 3` byte index panicked `strip_bce_suffix` (fixed 2026-06-10).
- `locy_parse/paren-brace-mapkey-timeout` — the 942-byte nightly artifact that
  exceeded the 10 s per-input budget: `pattern_expression` and
  `"(" ~ expression ~ ")"` both descended into the same `(`-led interior, so the
  work doubled per nesting level (fixed 2026-09-18).
- `locy_parse/paren-brace-mapkey-minimal`,
  `cypher_parse/paren-brace-mapkey-minimal` — the same shape reduced to its
  repeating core, `G=({G:`. Cheap to replay and it reproduces in both
  front-ends, which share `primary_expression`.
- `cypher_parse/nested-valid-map-pattern` — the same defect reached through a
  **valid** query, `RETURN (a {k: (a {k: ... }) })`. Worth keeping separate: a
  mitigation aimed at malformed or unclosed input would leave this one hanging.
- `locy_parse/list-comprehension-in-timeout` — the 338-byte PR-lane artifact
  (2026-10-11): unclosed `[x IN` runs. `list_expression` tried a comprehension,
  then a literal, and both parsed the source after `IN`, so the work doubled per
  level (36.7 s pre-fix). `locy_parse/list-comprehension-in-minimal` is its
  core, `[x IN ` 18 times (35.5 s pre-fix).
- `cypher_parse/nested-valid-in-led-literal` — the same defect through a
  **valid** query, `RETURN [x IN [x IN ... [1], 1], 1]` at depth 16 (10.3 s
  pre-fix).

- `{cypher_parse,locy_parse}/list-pattern-properties-timeout` — the 1,633-byte
  2026-10-27 Cypher artifact. Pattern-comprehension and list-literal alternatives
  reparsed nested node properties; `node_predicate` also parsed properties twice
  when no `WHERE` followed. Both front-ends share these rules.
- `{cypher_parse,locy_parse}/nested-list-map-projection` — a valid nested literal,
  `RETURN [(a {k: [(a {k: 1})]})]`, extended to depth 40. Depth 10 (118 bytes)
  already took over six seconds through both the Lance 11 and Lance 13 bindings.
- `{cypher_parse,locy_parse}/create-pattern-properties-timeout`,
  `{cypher_parse,locy_parse}/create-pattern-properties-slow` — the 1,791-byte
  timeout and 1,254-byte slow unit from the follow-up `cypher_parse` run
  (2026-10-27): `pattern_ahead` accepted any `-` or `<` after `)` and did not
  skip comments or backtick-quoted identifiers inside `balanced_parens`, while
  `rel_predicate` tried `properties ~ WHERE ~ expression` before `properties`.
- `{cypher_parse,locy_parse}/nested-paren-map-subtraction` — valid nested map
  projections followed by `- 1` (`RETURN (a {k: (a {k: ... 1}) - 1}) - 1`) at
  depth 40, exercising the `pattern_ahead` false-positive path on valid syntax.
