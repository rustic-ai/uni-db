# LDBC SNB Interactive — complex reads

`ic1.cypher` … `ic14.cypher` are the Interactive **complex read** queries,
vendored **byte-for-byte** from the LDBC reference implementation:

- Source: <https://github.com/ldbc/ldbc_snb_interactive_v1_impls>, `cypher/queries/`
- Licence: Apache-2.0 — see that repository's `LICENSE.txt` and `NOTICE.txt`
- Copyright: the LDBC project and contributors

## Why they are not edited

uni-db targets openCypher and passes the openCypher TCK, and these queries are
plain openCypher. So they are run **exactly as published**, including their
`:param` header comments.

That is a deliberate testing property, not tidiness. If a query needed editing to
run here, the edit would be evidence of a semantic gap between uni-db and the
dialect LDBC wrote against — a finding worth chasing, not routine adaptation. An
unrecorded edit would hide exactly that signal, so any change must be listed
below with its reason.

**Edits applied: none.**

## Parameters

The `:param` blocks in each header name ids from LDBC's own micro dataset and
select nothing at SF1. Parameters are instead derived from the loaded graph by
`../params.rs`, which picks *busy* values — the most-connected person, the
most-used tag, the two most-populated countries — so the queries have something
to find. Being busy is only a heuristic; the runner separately asserts that every
query returned rows, and that assertion is what makes the comparison meaningful.
