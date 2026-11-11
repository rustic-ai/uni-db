# uni-db 5.1.0

Changes since [5.0.0](RELEASE_NOTES_5.0.0.md) (released 2026-10-28).

5.1.0 adds text-to-graph extraction with the kniv v7 models: `uni.nlp.*`
procedures and `db.xervo()` methods that extract typed entities and facts,
and analyse text word by word and sentence by sentence, in-process with no
LLM, plus a documented Locy program that resolves extracted mentions to
entities. It also upgrades uni-xervo to 0.19.1, which removes the
`local/mistralrs` provider (breaking for Rust builds that enable its
feature), and fixes a read-only query running write procedures registered
by the built-in plugins. Behaviour and API changes are listed first.

## ⚠️ Behaviour and API changes

**The `local/mistralrs` provider is removed (breaking for Rust feature
users).** uni-xervo 0.19 dropped it upstream, so the `provider-mistralrs`
feature is gone from `uni-db` and `uni-cli`, and a `Cargo.toml` that names it
no longer resolves. A catalog alias with `provider_id: "local/mistralrs"` now
fails to load. Replacements:

| Capability | Use instead |
|---|---|
| local text / vision generation | `remote/llamacpp` against a `llama-server` you run |
| local embeddings | `local/onnx` or `local/candle` |
| image generation, speech synthesis | none |

The Python wheels need no code change: `uni-db`, `uni-db-cuda` and
`uni-db-metal` simply stop bundling mistral.rs, which makes them smaller and
faster to build. The default wheels now carry 11 providers and the slim
`-onnx` wheels 10.

**ONNX models on GPU wheels fall back to CPU when the accelerator cannot run
them.** With uni-xervo 0.19.1, an ONNX model is probed with one small
inference at load. If the accelerator fails it and the alias's
`execution_providers` list includes `"cpu"` (the default lists do), the model
is reloaded on CPU with a warning; a list without `"cpu"`, such as
`["coreml"]`, makes it a load error rather than a failure on the first query.
CPU-only builds are not probed. This fixes Qwen3-Reranker and
Qwen3-Embedding on Apple Silicon, which failed on every query under
uni-xervo 0.19.0, on the CPU-only wheels as well as `uni-db-metal`.

**Python: a malformed auto-embedding config now raises instead of being
dropped.** `LabelBuilder.index(prop, {"type": "vector", "embedding": {...}})`
parsed the `embedding` dict with an `Option` chain, so any mistake removed it
without a word. The common case was `"source"`, the key Cypher DDL uses: the
index was created without auto-embedding, and the first insert failed with
`Property 'embedding' cannot be null`, which never mentions the config. The
binding now mirrors the Cypher parser:

- `"source"` is accepted alongside `"source_properties"`; giving both is an
  error.
- `alias` and a non-empty source list are required, unknown keys are rejected
  with the accepted list, and `batch_size` must be a positive integer.
- The default `batch_size` is 32, matching Cypher DDL, instead of 100.

Each of these raises `ValueError` from `apply()`. A config that was being
silently ignored before will now fail there instead; with a typo-free config,
nothing changes apart from the batch-size default.

## New features

- **Entity and fact extraction from text: `uni.nlp.extract` and
  `uni.nlp.entities`.** With a kniv v7 model (`dragonscale-ai/kniv-v7-xlarge-u-en`
  or `-d-en`) under an `nlp` catalog alias, the procedures extract typed
  entities and the facts between them, in-process with no LLM. Relations and
  entity types are given per call by name and description, or derived from
  the declared schema with `schema: 'graph'` (a forked session uses its own
  merged schema). Each fact carries polarity, modality, holder, time, a
  calibrated margin, character spans of its mentions (turn-relative for a
  conversation, with the turn index) and the model alias and id. Input can
  be one text, several documents, or a conversation of `{speaker, text}`
  turns, where each speaker becomes an entity. Unknown options are rejected,
  and a v1 kniv model, which has no fact extraction, is refused with an
  error naming the alias.
- **`db.xervo().extract_facts(...)` / `extract_entities(...)`** return the
  same rows from Rust (`ExtractedFact`, `ExtractedEntity`, with `NlpInput`
  and `ExtractOptions`) and from Python (`Xervo` and `AsyncXervo`, with the
  `ExtractedFact` / `ExtractedEntity` data classes). Rust also gains
  `db.xervo().nlp(alias, requests)`, the raw per-window uni-xervo analysis.
  See `website/docs/features/nlp-extraction.md`.
- **Word and sentence analysis: `uni.nlp.words` and `uni.nlp.sentences`.**
  The same kniv v7 model analyses text word by word (Universal Dependencies
  part of speech, morphology, lemma, dependency head and relation) and
  sentence by sentence (sentiment, dialogue acts and each act's
  probability). Word, sentence and head indices count across the whole
  document, so a word's `head` joins to another row's `word`. Input is the
  same as for extraction, including conversations, where each row carries
  its turn and speaker; `options` takes only `alias`. Rust and Python get
  `db.xervo().analyze_words(...)` / `analyze_sentences(...)`, returning
  `AnalyzedWord` / `AnalyzedSentence`.
- **Resolving extracted entities with Locy.** The NLP extraction page now
  documents a tested Locy program that links each extracted mention to an
  existing entity: weighted evidence (exact name, surname, embedding
  similarity), combined with `MNOR` under a merge threshold, the best match
  per mention, and new entities for mentions nothing matched, with case and
  surname variants of a new name in one batch sharing one. Claims are stored
  against mentions, so they reach resolved entities; conversation speakers
  resolve like any name; and running resolution in a fork, reviewing the
  `diff` and promoting it is tested end to end. It is
  idempotent, `EXPLAIN RULE` shows the evidence behind each merge, and
  `VALIDATE` scores the policy against labelled mentions. Earlier docs said
  extraction `confidence` could be calibrated with `CALIBRATE`; that command
  fits only `CREATE MODEL` predicates, and the docs now point to `VALIDATE`.

## Correctness fixes

- **A read-only query no longer runs write procedures registered by the
  built-in plugins.** The read-only gate, and the authorization verbs for
  Cypher and Locy, looked a procedure's mode up by splitting its name at
  the last dot (`uni.plugin` + `declareTrigger`), while the built-in plugins
  register first-dot names (`uni` + `nlp.extract`) or names reached through
  the `uni.` prefix (`custom` + `plugin.declareTrigger`). They found no
  mode, treated the procedure as a read, and `session.query` could declare
  a trigger or call any other such `WRITE` procedure. The lookup now tries
  every split, as dispatch does.
- **`DERIVE` over a rule that joins an `IS` reference no longer times out
  on a few hundred nodes.** Before writing, a DERIVE re-joins its rule's
  clause: it ran the clause's whole MATCH (for `MATCH (m), (e) WHERE (m, e)
  IS r`, the cartesian product), converted every row, and compared each
  row with each fact, cloning both nodes every time. 300 × 300 nodes timed
  out after 30 s. The MATCH is now narrowed to the nodes the reference's
  facts hold, and the in-memory joins look facts up by entity identity; the
  same DERIVE takes tens of milliseconds and derives the same facts.

- **Fork promote now recognises auto-embedded vertices.** Auto-embedding
  stored its vectors as lists of floats until flush, and the flush registers
  each vertex's content UID from those values. The row read back holds a
  `Vector`, which hashes differently, so promote never found such a vertex on
  primary: `promote_from_fork` inserted a second copy of every auto-embedded
  vertex the fork had inherited, and skipped edges to them as having no
  endpoint. Embeddings are now stored in the declared `Vector` form from the
  start. Vertices flushed before this fix still carry the old UID entry, so
  promote can still duplicate them until they are rewritten.
- **Forked sessions can write to auto-embedded labels.** A fork's writer was
  built without the database's Uni-Xervo runtime, so any fork write that
  needed an embedding failed with "Uni-Xervo runtime not configured for
  auto-embedding", though queries on the same fork had the runtime.

- **Model-backed procedures now work inside trigger actions and declared
  procedures.** The executor that runs an inner query (a
  `uni.plugin.declareTrigger` action, a declared procedure's body) was built
  without the database's Uni-Xervo runtime, so a `uni.nlp.*` call there
  failed with "Uni-Xervo runtime is not configured", and so did
  `uni.vector.query` with a text query, on a database that had one. The inner
  executor now inherits the caller's runtime.
- **`size()` and `length()` of a vector return its dimension count.** A
  stored `VECTOR(n)` value is read as an Arrow `FixedSizeList`, which the
  columnar `size()` did not handle, so `size(d.embedding)` failed, and its error
  message printed every element of the vector. The row-at-a-time evaluator had
  the same gap for both functions. Both paths now return `n` (null for a null
  vector), and an unsupported argument raises a `TypeError` naming the type
  rather than the value.
- **Type errors raised during a query no longer end in a dangling "got".**
  The query layer wraps an engine `TypeError:` message in `UniError::Type`
  with the whole message as `expected` and an empty `actual`, which displayed
  as `Type error: expected <message>, got ` (in Python, `UniTypeError` carries
  the same text). That shape now displays as `Type error: <message>`; type
  errors that name both types are unchanged. The TCK matcher reads detail codes
  from both shapes.

## Dependencies and build requirements

- uni-xervo floor raised to **0.19.1**. 0.19.0 must not be admitted: its ONNX
  Runtime let CoreML claim nodes on CPU-only macOS builds (see above).
- candle 0.10 → 0.11.1, hf-hub 0.5 → 1.0, ort 2.0.0-rc.12 → rc.15 (ONNX
  Runtime 1.24 → 1.31), all through uni-xervo.
- zip 7.2 → 9.4: RUSTSEC-2026-0367, -0368 and -0369 no longer apply, and
  their `deny.toml` ignores are removed.
- The unused `candle-core`, `candle-nn`, `candle-transformers`, `hf-hub` and
  `tokenizers` entries are removed from `[workspace.dependencies]`.
