# Proposal: text-to-graph extraction with kniv v7 (`uni.nlp.*`)

Status: **Phases 1–3 and 2b implemented** (rev. 7); phases 4–5 proposed.
Date: 2026-11-07 (rev. 2, 3 and 4: 2026-11-08; rev. 5: 2026-11-08; rev. 6, 7 and 8: 2026-11-09)

Revision 8: the declared-trigger path is confirmed not to expose
`TriggerDeferral` (§10, question 4). Resolution from a trigger action (§7.4,
#329) stays open: a `uni.locy.run` Cypher procedure was built and dropped as
too indirect (a trigger running Cypher running Locy running Cypher), and
resolution runs from the application.

Revision 7: phase 2b ships as two procedures, `uni.nlp.words` and
`uni.nlp.sentences`, not one `uni.nlp.analyze`: words and sentences are
different row grains, and one procedure would repeat every sentence column
on each of its words (§7.2).

Revision 6: claims are stored against `:Mention`s, so resolution reaches
them (§5.1, §6), and testing the full path (claims, speakers, a fork review,
300 × 300 scale) needed three engine fixes, listed in §6. Phase 3 is still a
Locy program; the fixes are general bugs it exposed.

Revision 5: phase 3 ships as a tested Locy program (§6), not engine code.
Running the rev. 2 sketch corrected it in four places: `MNOR` is a `FOLD`
over evidence clauses, not a function of two scores; `BEST BY` cannot share
a rule with `MNOR`, so the best match is its own rule; `DERIVE` creates an
edge on every run, so evidence skips resolved mentions; and `CALIBRATE` fits
only `CREATE MODEL` predicates, so a rule's threshold is measured with
`VALIDATE` instead. The last also corrects what §4.1 and §10 said about
calibrating `confidence`.

Revision 4: span offsets count **characters** (Unicode scalar values), not
UTF-8 bytes. Cypher's `substring()` and Python's `str` index by character,
so byte offsets sliced the wrong text for any non-ASCII input. Rust callers
map back with `char_indices`.
Scope: `uni-query` (new procedures), `uni` (Xervo facade), `bindings/uni-db`
(Python), docs. Later phases reach `uni-query`'s Locy operators and the
trigger surface.

Revision 3 records the implementation of phases 1–2 and four changes the
code forced, marked "rev. 3" where they apply:

- **`model_id`, not `model_revision`.** `ModelInfo` exposes only `model_id`,
  the runtime cannot return a spec by alias, and uni-db holds the catalog only
  when the builder was given one, so the resolved revision is not available.
- **Turn-relative spans.** uni-xervo analyses each conversation turn as its
  own text, so offsets index the turn; rows carry `head_turn` / `tail_turn`
  (and `turns` on entity rows). §10's open question 3 is answered.
- **Speakers have no spans.** uni-xervo leaves a speaker entity's `spans`
  and `mentions` empty, so its span columns are null.
- **v1 detected up front** from `NlpModel::supported_tasks()` rather than
  from empty `windows`.

Code: `crates/uni-query/src/nlp.rs` (shared core),
`crates/uni-query/src/procedures_plugin/nlp.rs` (procedures),
`crates/uni/src/api/xervo.rs` (facade), `bindings/uni-db/src/` (Python).
User docs: `website/docs/features/nlp-extraction.md`.

Revision 2 adds the north star (§3), replaces the entity-resolution recipe
with a Locy-based design (§6), specifies provenance (§5.2, §4.1), drops
`min_confidence`, settles the yield shape, corrects the phase-3 and phase-4
analysis against the code, and adds a benchmark gate (§9).

## 1. Summary

uni-xervo 0.19 added kniv v7 to its `nlp` task (uni-xervo commit `07c4c9a`).
It is a local, Apache-2.0, CPU-friendly model that turns text into **typed
entities and facts between them**. Each fact carries a polarity, a modality,
the entity that holds the belief, and an optional time. Entity types and
relations are supplied **per request by name and description**, so a schema
the model was never trained on needs no fine-tuning.

uni-db already links this code. The catalog accepts an alias with
`"task": "nlp"`, and `ModelRuntime::nlp_model(alias)` returns the model.
Nothing in uni-db calls it. This proposal adds it in phases:

1. **`CALL uni.nlp.extract(...)` and `CALL uni.nlp.entities(...)`.** Read-only
   procedures that yield one row per fact or per entity, with character spans
   and model provenance. Reachable from Python at once through Cypher.
2. **`db.xervo().nlp(...)`** on the Rust facade, with matching Python wrappers.
3. **Entity resolution as Locy rules** over the extracted rows, with
   `EXPLAIN RULE` showing why two mentions were merged.
4. **A Locy extraction operator**, following the pattern `LocyModelInvokeExec`
   already uses for async classifiers.
5. **Write-time extraction** through `uni.plugin.declareTrigger`, which already
   runs off the commit path.

Phase 1 is specified in enough detail to build. The rest record the design
constraints found so far, so that phase 1 does not paint them into a corner.

## 2. Why this belongs in uni-db

uni-db's positioning (`website/docs/why-uni.md`) is cognitive infrastructure
for agents, with **structured memory** as its first pillar. Today the engine
cannot *produce* structure from text: the RAG use-case page assumes `Entity`
nodes and `MENTIONS` edges already exist, and auto-embedding gets text into
vectors but not into nodes and edges. Every competitor that extracts
(Graphiti, Mem0, Neo4j's KG builder, Cognee, Microsoft GraphRAG) does it with
an LLM call outside the database.

So uni-db currently has reasoning without extraction, while `why-uni.md`
criticises Mem0, Zep and Graphiti for memory without reasoning. kniv v7
closes that gap in a way that matches the stated philosophy: it runs
in-process with no network, needs no token, is Apache-2.0, and produces
repeatable output under calibrated thresholds, which is what "formal
inference, not token generation" and "auditable by design" ask for.

The attributes it extracts are also exactly what Locy reasons over.
Polarity is evidence for or against a fact (`PROB`, probabilistic negation).
Modality separates claims about the world from plans and wishes, and the
latter are natural `ASSUME … THEN` inputs. A `time` entity plus
`VERSION AS OF` gives "what did we believe about X in March". No competitor
couples extraction attributes to a logic engine.

## 3. North star

The end state is an **agent-memory path**: a conversation turn or document
goes in, and resolved, provenance-carrying claims come out, ready for rules.

```
text / turns ──► uni.nlp.extract ──► candidate facts (spans, margin, attributes)
                                            │
                                            ▼
                        Locy resolution rules ──► same_as(mention, node) AS PROB
                                            │
                                            ▼
                      :Claim nodes (provenance, polarity, modality, holder, time)
                                            │
                              ┌─────────────┼───────────────┐
                              ▼             ▼               ▼
                    believed(h, r, t)   ASSUME planned   EXPLAIN RULE
                    AS PROB (Locy)      facts (Locy)     down to the sentence
```

Forks are the curation loop: extract into a fork, `diff`, review, `promote`.
The UID-keyed promote path already handles claims, since they are ordinary
vertices and edges.

Phase 1 is the first slice of this path, not a self-contained feature. Its
output shape (§4.1) is chosen so that every later phase consumes it unchanged.

## 4. Phase 1: `uni.nlp.extract` and `uni.nlp.entities`

### 4.1 Signatures

```
CALL uni.nlp.extract(input, options)
  YIELD doc, window, head, head_text, head_type, head_speaker,
        head_start, head_end, head_turn,
        relation, tail, tail_text, tail_type, tail_speaker,
        tail_start, tail_end, tail_turn,
        polarity, modality, holder, time, margin, confidence,
        model_alias, model_id

CALL uni.nlp.entities(input, options)
  YIELD doc, window, entity, text, entity_type, speaker, mentions,
        mention_starts, mention_ends, mention_turns,
        model_alias, model_id
```

`input` (`CypherValue`) takes one of three forms:

- a **string**, analysed as prose;
- a **list of strings**, analysed as several documents in one batched
  `analyze` call;
- a **list of `{speaker, text}` maps**, analysed as one conversation via
  `NlpRequest::from_turns`.

`options` (`CypherValue` map):

| Key | Type | Default | Meaning |
|---|---|---|---|
| `alias` | string | `"nlp/default"` | Catalog alias; must resolve to a v7 model. |
| `relations` | list | none | `[{name, description?}]` or bare strings. |
| `entity_types` | list | none | Same shape. Omitted means the model's coarse types. |
| `schema` | string | none | `"graph"` derives `relations` and `entity_types` from the declared schema (§4.3). Mutually exclusive with the two keys above. |
| `top_per_pair` | int | model's `rel_top` | Passed through. |
| `rel_offsets` | map | none | Relation name → offset; becomes `NlpCalibrationOverrides::rel_offsets`. This is the tuning knob. |

Unknown keys are rejected. `uni.nlp.extract` without `relations` (or
`schema: "graph"`) is an error, because the relation pass needs a relation
list. `uni.nlp.entities` accepts no relations and requests only
`NlpTasks::ENTITIES`, which skips the relation pass entirely.

There is deliberately **no `min_confidence`**. The model's documented tuning
path is `rel_offsets`, and Locy's `VALIDATE` already measures a score
against labelled data. A second threshold
knob would duplicate both and contradict "simplicity over generality".

Yield columns. Built-in procedures yield **flat scalar columns**; lists and
maps become opaque `LargeBinary` CypherValue columns (see
`procedures_plugin/algo.rs`). The two procedures follow that convention:

| Column | Type | Notes |
|---|---|---|
| `doc` | int | Index of the input document (0 for a single string or one conversation). |
| `window` | int | Window index within the document. |
| `entity`, `head`, `tail`, `holder`, `time` | int (`holder`, `time` nullable) | Window-local entity indexes, so `extract` and `entities` rows from one call join on `(doc, window, entity)`. |
| `head_text`, `tail_text`, `text` | string | `NlpEntity::text`, the first mention's surface form. |
| `head_type`, `tail_type`, `entity_type` | string | `NlpEntity::ty`. Rev. 3: `entity_type` on entity rows, matching the Rust and Python field. |
| `head_speaker`, `tail_speaker`, `speaker` | bool | `NlpEntity::speaker`. |
| `head_start`, `head_end`, `tail_start`, `tail_end` | int | Character offsets (rev. 4; UTF-8 bytes before) of the **first mention** in the input text, or in its turn for a conversation. Null for a speaker entity. See §4.2 on derivation. |
| `head_turn`, `tail_turn` | int | Index of the turn the first mention is in (rev. 3). Null for prose and for speakers. |
| `mentions` | list<string> | `NlpEntity::mentions`. |
| `mention_starts`, `mention_ends`, `mention_turns` | list<int> | Character offsets and turn of every mention, parallel to `mentions`. Rev. 3: not `starts` / `ends`, which Cypher reads as the `STARTS WITH` / `ENDS WITH` keywords in `RETURN`. |
| `relation` | string | The relation **as the caller named it**; under `schema: "graph"`, the edge type name (§4.3). |
| `polarity` | string | `FactPolarity::as_str()`: `positive` or `negative`. |
| `modality` | string | `FactModality::as_str()`. |
| `margin` | float | Raw calibrated margin, always > 0. |
| `confidence` | float | `1 / (1 + e^-margin)`. A monotone convenience for rules that want a [0, 1] input without a sigmoid in Cypher. **Not a calibrated probability**; the docs must say so and point at `CALIBRATE`. |
| `model_alias`, `model_id` | string | The catalog alias and the model's id, for provenance (§5.2). The resolved revision is not available (rev. 3). |

The span columns exist because agents need citations and `EXPLAIN RULE`
needs to reach the sentence. Without them a claim's provenance stops at the
document.

### 4.2 Implementation

- **Module.** Add `crates/uni-query/src/procedures_plugin/nlp.rs`, registered
  from `procedures_plugin/mod.rs`, following `vector.rs`.
- **Mode.** `ProcedureMode::Read` and `SideEffects::ReadOnly`. The model runs
  in-process with no network I/O once loaded. A first call may download the
  model from the Hugging Face Hub, but so may `uni.vector.query`'s query
  embedding, which is also `ReadOnly`. Whether both should be `ExternalIo`
  is a shared decision, not an NLP one (§10).
- **Model access.** `host.xervo_runtime()` with the same "Uni-Xervo runtime
  not configured" error as the vector procedures, then
  `runtime.nlp_model(alias)`.
- **Building requests.** One `NlpRequest` per input document, one `analyze`
  call. Requested tasks: `FACTS` for `extract` (it implies `ENTITIES`, per
  `NlpTasks::expanded`), `ENTITIES` for `entities`, never `ALL`, since a
  narrow set skips the graph calls behind unused heads.
- **Lifetimes.** `ExtractionSchema` and `NlpCalibrationOverrides` are borrowed
  by `NlpRequest<'a>`, so build them in the stream future and keep them alive
  across the `analyze` await.
- **Byte offsets.** `NlpEntity::spans` are window-local *word* spans.
  `NlpResult::tokens` (the flat projection) carries UTF-8 byte offsets and a
  dense, global `word_index`
  (`nlp_v7.rs::projection_indexes_tokens_globally_across_windows`). The
  global word index of a window-local word is the window's token base plus
  the local index; the span's byte range is the first token's `start` to the
  last token's `end` for that word range; rev. 4 converts that byte range to
  character offsets for the rows. For conversation input, verify
  during implementation which text the offsets are relative to. Rev. 3:
  uni-xervo analyses each turn as its own text with offset 0, so offsets are
  turn-relative; the turn is the containing `WindowSentence::unit`, mapped
  back past the blank turns uni-xervo drops.
- **v1 guard.** Rev. 3: a model whose `supported_tasks()` lacks the
  requested `FACTS` / `ENTITIES` flag is a v1 model (v1 reports `CASCADE`).
  Fail before `analyze` with a message naming the alias and the v7 repos,
  rather than silently yielding nothing.
- **Truncation.** `NlpWindow::analysed_words < tokens.len()` means a sentence
  overflowed the encoder. Report it as a `warn!` once per call; do not error.
- **Batch size.** Per-row `CALL` over a `MATCH` (the Cypher subquery form) is
  the common pattern. Follow `uni.vector.query` for whether `batch_input` is
  used; batching across rows is a later optimisation.

### 4.3 `schema: "graph"`

- **Relations.** Derive `relations` from declared edge types and
  `entity_types` from declared labels, both in the `Active` state.
- **Readable names.** The model scores a relation's *name* as text, so
  `LIVES_IN` is passed as `"lives in"` (lowercase, underscores to spaces) and
  mapped back to `LIVES_IN` in the `relation` column. Labels are handled the
  same way.
- **Descriptions.** Each `LabelSpec` carries the element's `description`
  when set (`LabelMeta` / `EdgeTypeMeta` already have the field, set via the
  schema builders or the `DESCRIPTION` DDL keyword).
- **Missing descriptions.** uni-xervo's guidance is to say in the description
  which side is the head (`"the place where the head resides"`). Without
  that, results degrade. Log one `warn!` per call listing the edge types that
  have no description, rather than failing.
- **Subsets.** Accept `schema: {labels: [...], edge_types: [...]}` as well as
  `"graph"`. The relation pass is quadratic in entities per window, so a
  non-trivial schema needs a way to extract a slice of it.
- **Fork sessions.** On a forked session, use the session's merged schema
  (primary plus overlay). Not yet verified: whether the procedure host's
  schema handle is already the fork's merged view. Confirm this first, and
  pin it with a test either way.
- **Schema cost.** Schema label embeddings are cached per exact label list,
  so a stable schema costs one extra graph pass for the life of the model.
  `schema: "graph"` is stable until DDL changes it, which is the cheapest
  case.

### 4.4 Example

```cypher
// Conversation memory: store what the user asserted about themselves.
CALL uni.nlp.extract(
  [{speaker: 'user', text: 'I moved to Lisbon last year and started at a bank.'},
   {speaker: 'assistant', text: 'Congratulations on the move!'}],
  {relations: [
     {name: 'lives in', description: 'the place where the head resides'},
     {name: 'employer', description: 'organization the head works for'}],
   entity_types: ['location', 'organization', 'person']})
YIELD head_text, head_speaker, relation, tail_text, tail_type,
      tail_start, tail_end, polarity, modality, margin
WHERE modality IN ['asserted', 'reported']
RETURN head_text, relation, tail_text, tail_type, polarity, margin
```

### 4.5 Tests

- **Unit tests** with a hand-built `NlpModel` returning fixed windows: column
  mapping, byte-offset derivation across windows, the v1 guard,
  `schema: "graph"` name round-trip, schema subsets, option validation,
  multi-document `doc` indexes and conversation input.
  `uni_xervo::mock::MockNlpModel` returns a single token and no windows, so
  the fake has to be local; `NlpFact` and `NlpEntity` are `#[non_exhaustive]`
  but derive `Default` with public fields, so
  `let mut f = NlpFact::default(); f.relation = ...` compiles.
- **Integration test** as a new `mod` in `crates/uni/tests/integration.rs`,
  not a new test binary (`docs/test_layout.md`). It registers the fake model
  in the runtime and runs the procedure through `Session::query`, including a
  forked session for the `schema: "graph"` overlay.
- **Real-model test**, `#[ignore]`d, against `kniv-v7-xlarge-u-en` INT8,
  asserting the conversation example's two facts and their byte spans. It
  runs in the nightly `ignored-sweep` and is not listed in `ON_DEMAND`; it is
  slow, not unattended-unsafe.

### 4.6 Documentation

- `website/docs/`: a feature page for `uni.nlp.*` with the catalog entry,
  option table, modality/polarity semantics, cost table, the §5 storage
  shape, the §6 resolution rules and the §8 LLM comparison.
- `skills/uni-db/references/xervo.md`: the `nlp` task and the procedures.
- `docs/UNI_BLACK_BOOK.md`: one paragraph under the Xervo integration
  section.
- `website/docs/use-cases/rag-knowledge-graph.md`: replace "entities already
  exist" with an extraction step.

## 5. Storing claims

### 5.1 Shape

Holder and time point at entities, and an edge property cannot point at a
node, so a faithful store reifies each fact as a claim node:

```
(:Claim {relation, polarity, modality, margin, confidence,
         model_alias, model_id, doc_id, window,
         head_start, head_end, tail_start, tail_end, extracted_at})
  -[:HEAD]->(:Mention) -[:TAIL]->(:Mention)
  -[:HELD_BY]->(holder)? -[:AT]->(time)? -[:FROM]->(:Document)
```

A plain `(head)-[:REL]->(tail)` edge with the attributes as properties is
fine when the holder and time can be dropped or kept as text, and when no
rule needs provenance below the document.

Rev. 6: `HEAD` and `TAIL` point at the `:Mention`s the fact was extracted
from, not at entities, so the §6 program resolves them; a claim reaches its
entities through `-[:HEAD]->()-[:REFERS_TO]->`.

### 5.2 Provenance

Locy already has a provenance convention: `NeuralProvenance` records
`model_name`, `raw_probability`, `calibrated_probability` and
`confidence_band` so an auditor can reproduce a score. Claims follow it.
`model_alias` and `model_id` identify the extractor, `margin` is the
raw score, any calibrated probability a rule derives is the calibrated one,
and the byte spans plus `:FROM` reach the sentence. `EXPLAIN RULE` on a
conclusion built from claims then bottoms out in text, which is what an
agent needs for a citation and a regulator needs for an audit.

### 5.3 Idempotency

`doc_id` identifies the originating document node. Re-extracting a document
means deleting its claims and inserting new ones in one transaction. That rule
is what phase 5 automates.

## 6. Phase 3: entity resolution in Locy

Implemented (rev. 5) as a Locy program. Rev. 6 adds claims, speakers,
forks and scale to it, and fixes three engine bugs that testing them found
(below). Entities and
coreference links hold **within one window**: "Ada Lovelace" in two
documents comes back as two unrelated entities. Graphiti and Neo4j's
pipeline resolve them with LLM-based deduplication; here the policy is rules
the user can read, version and explain, and `EXPLAIN RULE` shows why two
mentions were merged.

Extraction stores each mention as a `:Mention {text, type}` node (claims add
`doc`, `turn` and `start`, which key it by where it occurs) whose name is
auto-embedded, next to `:Entity {name, type}` nodes embedded the same
way. The program (`website/docs/features/nlp-extraction.md#resolving-entities`,
kept in step with `RESOLVE` in `crates/uni/tests/common/nlp_resolve.rs`):

- **`match_evidence`**, one clause per signal, each yielding a weight `p` for
  a `(mention, entity)` pair of the same type: exact name (0.9), surname
  (0.7) and name similarity (`similar_to × 0.8`, from 0.5 up).
- **`same_as`** folds a pair's evidence with `MNOR` and keeps pairs at or
  above 0.75, yielding the result `AS PROB`.
- **`resolved`** keeps each mention's best match with `BEST BY prob DESC`.
- **`refers_to`** `DERIVE`s `(m)-[:REFERS_TO]->(e)`, run by a `DERIVE`
  command.
- Mentions still unmatched then seed new entities (one per name ignoring
  case, none for a surname of a longer unmatched name), a second `DERIVE`
  links the rest to them, and a last command gives anything left its own
  entity. So variants of a new name in one batch resolve as they would
  across batches.

What running it settled, against the rev. 2 sketch:

- **`MNOR` is a fold, not a function.** `MNOR(a, b)` over two scores is not
  Locy; one clause per signal into a rule that folds them is, and it reads
  better: a new signal is a new clause.
- **`BEST BY` and `MNOR` cannot share a rule** (`BestByWithMonotonicFold`),
  hence `same_as` and `resolved` as two rules.
- **`DERIVE` is not idempotent.** It creates an edge each time it runs, as
  Cypher `CREATE` does, so every evidence clause starts with
  `NOT (m)-[:REFERS_TO]->()`: re-running resolves only new mentions.
- **No `CALIBRATE` for rules.** `CALIBRATE` fits `CREATE MODEL` predicates.
  `VALIDATE` does take a rule with a `PROB` column, joining its facts to
  labelled rows by `KEY`, so `VALIDATE same_as … TARGET e.name = m.gold`
  measures the policy. It sees only pairs above the threshold, so recall is
  measured by validating with a lower one.
- **Claims go through mentions.** §5.1's claim node points `HEAD` / `TAIL`
  at `:Mention`s keyed by where they occur (a speaker, which has no span,
  gets -1), so the program resolves claim endpoints too and a claim reaches
  entities through `REFERS_TO`. Rev. 2's store `MERGE`d entities by name and
  bypassed resolution.
- **Speakers** resolve the same way: kniv types them `speaker`, so one
  speaker across conversations is one entity and the type clause keeps it
  apart from a `person` of the same name.
- **Forks.** Running resolution in a fork, reviewing the `diff` and
  promoting `Entity` + `REFERS_TO` is the curation path for low-confidence
  merges. Testing it found two engine bugs, both fixed: a fork's writer had
  no Xervo runtime, so writes to auto-embedded labels failed in a fork; and
  auto-embed stored vectors as float lists, so the content UID registered at
  flush never matched the stored row and promote duplicated auto-embedded
  vertices and dropped edges to them.

Tests: `crates/uni/tests/common/nlp_resolve.rs` runs the program over
mentions extracted by the mock kniv v7 model and embedded by a trigram mock,
checks re-running is idempotent and that a later mention joins an entity a
previous run created, that the best of two candidates wins and types must
agree, that weak evidence makes a new entity, that case and surname
variants of a new name in one batch make one entity, that `EXPLAIN RULE` lists the
two clauses behind a merge, and that `VALIDATE` scores `same_as` and stored
`confidence` against labels. It also stores claims from prose and two
conversations and checks they meet at resolved entities (speaker included),
runs resolution in a fork and promotes it, and, `#[ignore]`d, runs the
claims path on the real `kniv-v7-xlarge-u-en` model. A Python test runs the
program read from the docs page. `autoembed_parity.rs` covers promote of
auto-embedded vertices (dense and multi-vector, eager and deferred).
A scale test resolves 300 mentions against 300 entities. Measuring it
found the third engine fix: the DERIVE re-joined its clause over the
cartesian product of its MATCH and timed out; it now narrows the MATCH to
the facts' nodes. The program's own cost is still mentions × entities: at
1,000 × 1,000 the similarity rule exceeds the default Locy timeout. #330
measured where that time goes and what to do about it:
`docs/proposals/locy_similarity_join_330.md`.

Not done: a built-in rule module with this program as a default. Locy has no
mechanism for shipping a module with the engine, and the program is short
enough to copy and adjust; revisit if users ask for one.

## 7. Later phases

### 7.1 Phase 2: facade and Python

Implemented (rev. 3). The shapes differ from rev. 2's sketch:

- **Rust.** `UniXervo::nlp(alias, requests) -> Result<Vec<NlpResult>>` is the
  raw passthrough, with uni-xervo's NLP types re-exported from
  `uni_db::api::xervo`. The conveniences are
  `extract_facts(&NlpInput, &ExtractOptions) -> Result<Vec<ExtractedFact>>`
  and `extract_entities(...) -> Result<Vec<ExtractedEntity>>`. The alias and
  the schema live in `ExtractOptions`, the same options the procedures
  parse, and the rows are the §4.1 columns. `schema: "graph"` uses primary's
  schema, since `db.xervo()` is not tied to a session; a fork's merged schema
  is reached through the procedures on a forked session.
- **Python.** `extract_facts` / `extract_entities` on `Xervo` and
  `AsyncXervo` (from `db.xervo()`), next to `embed` and `generate`, rather
  than on `Database` / `AsyncDatabase`: that is where the other Xervo calls
  already live, and the symmetry contract is met by the sync/async pair plus
  the `.pyi` stubs. Rows are `ExtractedFact` / `ExtractedEntity` classes
  mirroring §4.1. The raw `nlp()` passthrough is Rust-only, because its
  request types do not map usefully to Python.
- Phase 1 needs none of this: procedures reach Python through Cypher.

### 7.2 Phase 2b: linguistic procedures

Implemented (rev. 7) as two procedures, because words and sentences are
different row grains:

- `uni.nlp.words(input, options)`: one row per word, with UD part of speech,
  morphology, lemma, and dependency head and relation. Word, sentence and
  head indices count across the document (every window), so `head` joins to
  another row's `word`; a root has `head` null.
- `uni.nlp.sentences(input, options)`: one row per sentence, with sentiment,
  the dialogue acts above threshold (a list) and every act's probability
  (a map built from `label_maps().cls`, which `cls_scores` is parallel to).

`input` is as for extraction; `options` takes only `alias`. Each requests
only its own heads (`POS | MORPH | LEMMA | DEP`, or `CLS | SENTIMENT`), and
a model lacking them (a v1 cascade model has no lemmas or sentiment) is
refused. The facade is `analyze_words` / `analyze_sentences` in Rust and
Python. Code: `crates/uni-query/src/nlp/analyze.rs`. SRL frames, coreference
clusters and OntoNotes NER spans remain unexposed.

### 7.3 Phase 4: Locy extraction operator

An `extract(text) -> (head, relation, tail, polarity, modality, margin)`
condition would let rules derive facts from text properties directly.

Revision 1 called this blocked on `LocyGenerator::generate` being
synchronous while `NlpModel::analyze` is async. That overstated it.
`LocyModelInvokeExec` (`crates/uni-query/src/query/df_graph/locy_model_invoke.rs`)
already runs async Xervo classifiers inside a DataFusion plan node by
collecting the input stream, awaiting the model pass, and yielding a
`RecordBatchStreamAdapter`; its header notes it introduces "no novel async
machinery". A built-in `LocyNlpExtractExec` follows that pattern and never
touches the synchronous `LocyGenerator` trait, which stays the contract for
user plugins.

What remains is design, not plumbing: how the schema is named in a rule
(likely `USING xervo('nlp/default')` on a `CREATE MODEL` with a new
`OUTPUT FACTS` type, reusing the neural-predicate grammar), how one input
row fans out to many fact rows inside the fixpoint, and how
`NeuralProvenance` records an extraction. This is worth its own proposal once
phase 1 has users.

### 7.4 Phase 5: write-time extraction

Revision 1 proposed an auto-embedding-style index config and listed latency
on the commit path as a blocker. The engine already has the right mechanism.
`uni.plugin.declareTrigger` runs a Cypher action after a matching commit,
**always as `AfterCommit` + `Async`** on a blocking worker
(`website/docs/features/triggers-cdc.md`), so a trigger whose action is
`CALL uni.nlp.extract(...)` followed by `MERGE` is write-time extraction off
the commit path, with no engine change, from any binding:

```cypher
CALL uni.plugin.declareTrigger(
  'memory.extract',
  'CREATE ON :Document',
  'MATCH (d:Document) WHERE id(d) = $vid
   CALL uni.nlp.extract(d.text, {schema: "graph"}) YIELD ...
   MERGE ...')
```

Open items, none of which block shipping the pattern as documentation:

- **Idempotency.** `UPDATE ON :Document` must delete the document's old
  claims and insert the new ones in the trigger's transaction (§5.3).
- **Resolution.** A trigger action is Cypher and cannot run the Locy
  resolution program (#329), so resolution runs from the application after
  extraction commits; the program is idempotent. Rev. 8 built and dropped a
  `uni.locy.run` Cypher procedure for this as too indirect. A trigger whose
  action is a Locy program, or incrementally maintained rules, would be the
  direct forms.
- **Deferral.** Rev. 8: not exposed. A declared trigger's `fire` always
  returns `TriggerOutcome::Continue`, so an action that fails (model not
  loaded yet) is logged and the document is not retried. Only a Rust
  `TriggerPlugin` can return `TriggerDeferral`.
- **Forks.** Extracting into a fork and promoting after review falls out of
  the existing promote path; test it deliberately.

A dedicated index-style config remains an option for a later convenience
layer, but it is no longer the proposed mechanism.

### 7.5 Flagship example (parked, 2026-11-10)

A Locy-flagship notebook (overview, `prepare_*` / `generate_*` / `verify_*`
scripts, executed by `deploy-docs.yml`) for due diligence over news and
analyst notes was scoped and parked. Probing a ten-document synthetic corpus
found:

- **Model size.** The smaller v7 students load through `local/onnx`
  unchanged; `kniv-v7-small-en` is ~190 MB as INT8, small enough for the docs
  deploy to run live (pin `revision` in the catalog so a re-release cannot
  move results). It extracts the corpus in ~0.5 s after a ~20 s load;
  `xlarge-u` takes ~2.8 s after ~130 s.
- **What small does reliably:** entities, including short forms, and the
  calibrated built-in relations (`employer`, `headquarters location`).
  Custom org-to-org relations (`supplier of`, `acquired`) appear only with
  raised `rel_offsets`, mixed with type-violating facts; `sued` never
  appears. Medium behaves the same; xlarge finds more org-to-org facts.
- **Polarity and modality are not dependable on any size** for plain
  negation ("did not acquire", "no longer works for") or hedging ("might",
  "plans to", "denied"): facts come back `positive` / `asserted`. The
  reference fixtures in uni-xervo agree (one negative fact in 34, margin
  0.05), so this is the model, not the port.

The proposed design, if resumed: run small with lowered offsets, store every
candidate claim with its span and confidence, and let Locy vet them (type
signatures per relation, corroboration across sources via `MNOR`), resolve
mentions, derive a related-party red flag (an executive of the target holds
a stake in its supplier), and review a new batch in a fork before promote.
Contradictions and denials come from structured inputs, not polarity.

## 8. Why not an LLM?

uni-db already has `generate`, and an LLM with a JSON schema can extract
facts. The right answer is both, and the docs should say which to use when:

| | kniv v7 via `uni.nlp.*` | LLM via `generate` |
|---|---|---|
| Runs | in-process, CPU, no network | remote API or `llama-server` |
| Cost at volume | fixed, ~0.5 s per 200-word window | per token |
| Repeatability | deterministic for a given model, schema and calibration | sampling-dependent |
| Tuning | `rel_offsets`, `VALIDATE` against labels | prompt edits |
| Attributes | polarity, modality, holder, time extracted as signals | whatever the prompt asks, judged by the model |
| Languages | English only | many |
| Complex or implicit relations | limited to what the window states | stronger |
| Privacy | data never leaves the process | depends on the provider |

Use kniv for high-volume, private, calibratable English extraction with a
stable schema; use an LLM for complex, implicit or multilingual text, or when
the schema changes per request. Phase 2's facade should leave room for a
provider-agnostic `extract` entry point later, without designing it now.

## 9. Quality gate

kniv v7's quality against a GPT-class extractor is the first number users
will ask for, and nothing in this proposal establishes it. Before any
user-facing page makes a quality claim:

- Assemble a labelled sample of at least a few hundred facts across the
  conversation-memory and document cases, with the schema fixed.
- Report precision and recall **per relation**, plus modality and polarity
  accuracy, for both v7 encoders.
- Run the same sample through a `generate`-based baseline with the same
  schema as a JSON prompt, and report the same numbers.
- Publish the result under `compliance_reports/` and link it from the feature
  page, the way the recall benchmarks for fork indexes are handled.

## 10. Risks and open questions

Risks:

- **English only.** Both releases are `-en`.
- **Memory.** ~2–3 GB resident per loaded model. Use `"warmup": "lazy"` in
  catalogs that rarely extract.
- **Latency.** Quadratic in entities per window. Long entity-type lists are
  the usual cause of slow calls; schema subsets (§4.3) are the mitigation.
- **`confidence` is not calibrated.** It is a sigmoid of a margin. Rules
  that need a probability should measure it with `VALIDATE` rather than
  trust it. (Rev. 5: earlier revisions said `CALIBRATE`, which fits only
  `CREATE MODEL` predicates.)
- **Schema wording dominates results.** Near-synonym relations split the
  decision between them, and generic relations (`related to`) attract
  everything. The docs page must carry uni-xervo's naming guidance.
- **Concentration.** The integration depends on uni-xervo and the kniv
  releases, both from the same team. A strength for integration speed, a
  risk users will weigh.

Open questions:

1. Should `"nlp/default"` be the default alias, or should an alias be
   required, as `uni.vector.query` requires an index?
2. Should `uni.nlp.extract` and `uni.vector.query` both be classed
   `SideEffects::ExternalIo`, since a first call may download a model?
3. ~~For conversation input, are byte offsets relative to the turn or the
   joined text (§4.2)?~~ Rev. 3: the turn.
4. ~~Does the declared-trigger path expose `TriggerDeferral` (§7.4)?~~ Rev. 8: no.

## 11. References

- uni-xervo `07c4c9a` — "feat(onnx): run kniv v7 structured NLP"
- `uni-xervo/crates/uni-xervo/src/traits/nlp.rs` — request and result types
- `uni-xervo/crates/uni-xervo/src/provider/local_onnx/nlp_v7.rs` — flat projection with global `word_index` and byte offsets
- `uni-xervo/website/docs/guides/nlp.md` — semantics, calibration, cost, catalog entry
- `website/docs/why-uni.md` — positioning this proposal is measured against
- `crates/uni-query/src/procedures_plugin/vector.rs` — procedure template
- `crates/uni-query/src/query/executor/procedure_host.rs` — `xervo_runtime()`
- `crates/uni-query/src/query/df_graph/locy_model_invoke.rs` — async-in-stream model pass
- `crates/uni-plugin/src/traits/locy.rs` — `LocyGenerator`
- `crates/uni-common/src/core/schema.rs` — `LabelMeta` / `EdgeTypeMeta` descriptions
- `crates/uni/src/api/xervo.rs` — `UniXervo` facade
- `website/docs/features/triggers-cdc.md` — `declareTrigger` runs `AfterCommit` + `Async`
- `website/docs/locy/advanced/neural-predicates.md` — `CREATE MODEL`, `CALIBRATE`, `VALIDATE`, `NeuralProvenance`
