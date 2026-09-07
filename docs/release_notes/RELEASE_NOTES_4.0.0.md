# uni-db 4.0.0 (in progress)

**Release focus: APOC parity where a plausible answer was hiding a wrong one.** Five
`uni-plugin-apoc-core` procedures returned something that looked like a result and was not the
result Neo4j gives. Three of them turned a real value into NULL or a truncated one without
saying so; two disagreed with APOC's documented semantics.

---

## ⚠️ Breaking changes

All five land under one `feat!`. Two of them change the result of queries that look correct today
(`text.indexOf`, and the four-function string→number family), so re-check any code that pinned the
old values.

**`apoc.convert.toString` no longer returns NULL for lists, maps and other non-primitives.** The
argument is declared `ArgType::CypherValue`, so anything that is not a `Bool`/`Int`/`Float`/`String`
arrives as an opaque `LargeBinary` envelope; it used to fall through a catch-all and yield NULL.
It now decodes the envelope and renders the value the way Neo4j does — `apoc.convert.toString([1,2,3])`
is the string `"[1, 2, 3]"`, a map is `{k: v}`, and NULL stays NULL. Both live envelope encoders are
accepted (the procedure dispatcher's `serde_json` bytes and the scalar-function adapter's tagged
`cypher_value_codec`); they are unambiguous at the first byte.

**`apoc.create.uuids(n)` and `apoc.text.repeat(s, n)` error instead of silently truncating.** Both
bound their output at 1,000,000 (`MAX_SYNTHESIZED_LEN`) and both used to clamp with `min()`, so
`apoc.create.uuids(2_000_000)` quietly returned exactly half the rows asked for and an over-long
`text.repeat` returned a clipped string — neither distinguishable from a complete answer. The cap is
unchanged; exceeding it is now a `CODE_RESOURCE_LIMIT` error that names the cap. A single
`support::reject_over_cap` helper serves both, so the two siblings cannot drift apart again.

**`apoc.text.indexOf` returns a character index, not a UTF-8 byte offset.**
`apoc.text.indexOf('cafés','s')` was 5 and is now 4, matching Neo4j. It also matches this module's
own `text.length`, which always counted characters — the two units disagreed, and three doc sites
described the byte offset as intended.

**The string→number family parses like Java `DecimalFormat`.** `apoc.number.parseInt`,
`apoc.number.parseFloat`, `apoc.convert.toInteger` and `apoc.convert.toFloat` all used
`str::parse`, which demands the entire string be numeric. They now take the leading numeric prefix,
accept `,` grouping separators between digits, and truncate toward zero for the integer variants:
`"3.7"` → 3, `"-3.7"` → -3, `"1,234"` → 1234, `"1,234.5"` → 1234.5, `"12abc"` → 12. A string with no
digits at all is still NULL — NULL on genuine garbage is correct APOC behavior and is preserved.
One narrowing comes with this: `parseFloat("inf")`/`"NaN"` used to reach `f64::from_str` and now
yield NULL, matching `DecimalFormat`.

---

## Upgrading

`cargo update -p uni-db` / `pip install -U uni-db`. Nothing fails at compile time — these are
result changes. The two to audit are `text.indexOf` (any caller slicing with the returned index, or
comparing it against a byte length) and the number-parsing family (any caller relying on `"3.7"` or
`"1,234"` coming back NULL). Callers that were silently receiving truncated `create.uuids` /
`text.repeat` output now see an error naming the cap.
