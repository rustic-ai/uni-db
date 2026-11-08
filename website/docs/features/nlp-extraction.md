# NLP Extraction

Uni can turn text into graph structure in-process: typed **entities** and the
**facts** between them, extracted by a local kniv v7 model through the
Uni-Xervo runtime. There's no LLM call and no network once the model is
loaded, and the relations and entity types are given **per call, by name and
description**, so a schema the model never saw needs no fine-tuning.

Every fact carries the signals reasoning needs: whether it is asserted or
denied (**polarity**), whether it is a claim about the world or a plan, wish,
question or hypothetical (**modality**), **who holds** the belief, an optional
**time**, and a calibrated **margin**, plus the byte offsets of each mention
in your text.

## What It Provides

- **`CALL uni.nlp.extract(input, options)`**: one row per fact,
  `head --relation--> tail`, with polarity, modality, holder, time, margin,
  byte spans and model provenance.
- **`CALL uni.nlp.entities(input, options)`**: one row per entity, grouped
  across its mentions, with each mention's span.
- **Prose or conversations.** `input` is a string, a list of strings (one
  document each, analysed in one batched call), or a list of
  `{speaker, text}` maps (one conversation). In a conversation each speaker
  becomes an entity, so "I live in Lisbon" is a fact *about that speaker*.
- **`schema: 'graph'`**: derive relations from your declared edge types and
  entity types from your labels, using their descriptions. Results come back
  under the schema names (`LIVES_IN`, `Person`), ready to `MERGE`. On a
  forked session this is the fork's merged schema.
- **The same rows from Rust and Python**: `db.xervo().extract_facts(...)` and
  `extract_entities(...)`.

## Setup

Add a kniv v7 model to the Xervo catalog under an `nlp` alias. The default
alias the procedures use is `nlp/default`.

```json
[{
  "alias": "nlp/default",
  "task": "nlp",
  "provider_id": "local/onnx",
  "model_id": "dragonscale-ai/kniv-v7-xlarge-u-en",
  "warmup": "lazy",
  "options": {"variant": "int8,fp16", "execution_providers": ["cpu"]}
}]
```

Two English releases exist, both Apache-2.0 and needing no token:

| | `kniv-v7-xlarge-u-en` (UltraBERT) | `kniv-v7-xlarge-d-en` (DeBERTa) |
|---|---|---|
| Better at | zero-shot types, agent memory, long inputs | parsing, fixed-schema relations, plans vs assertions |
| INT8, 200-word window | ~0.47 s | ~0.58 s |
| Resident memory, INT8 | ~2 GB | ~3 GB |

The first use downloads the model from the Hugging Face Hub and loads it,
which takes longer than the default query timeout. Load it before the first
query: `db.xervo().prefetch(["nlp/default"])`, or `"warmup": "eager"` /
`"background"` in the catalog entry. On a GPU use
`"variant": "fp16"`. A v1 model (`kniv-deberta-nlp-base-en-*`) serves the same
`nlp` task but has no entity or fact extraction; the procedures refuse it
with an error naming the alias.

## Minimal Example

=== "Cypher"

    ```cypher
    CALL uni.nlp.extract(
      [{speaker: 'user', text: 'I moved to Lisbon last year and started at a bank.'},
       {speaker: 'assistant', text: 'Congratulations on the move!'}],
      {relations: [
         {name: 'lives in', description: 'the place where the head resides'},
         {name: 'employer', description: 'organization the head works for'}],
       entity_types: ['location', 'organization', 'person']})
    YIELD head_text, relation, tail_text, polarity, modality, margin
    WHERE modality IN ['asserted', 'reported']
    RETURN head_text, relation, tail_text, polarity, margin
    ```

=== "Python"

    ```python
    import uni_db

    CATALOG = """[{"alias": "nlp/default", "task": "nlp", "provider_id": "local/onnx",
                   "model_id": "dragonscale-ai/kniv-v7-xlarge-u-en",
                   "options": {"variant": "int8,fp16"}}]"""
    db = uni_db.UniBuilder.temporary().xervo_catalog_from_str(CATALOG).build()

    facts = db.xervo().extract_facts(
        [{"speaker": "user", "text": "I moved to Lisbon last year and started at a bank."}],
        relations=[
            {"name": "lives in", "description": "the place where the head resides"},
            {"name": "employer", "description": "organization the head works for"},
        ],
        entity_types=["location", "organization", "person"],
    )
    for f in facts:
        print(f.head_text, f.relation, f.tail_text, f.polarity, f.modality)
    # user lives in Lisbon positive asserted
    # user employer a bank positive asserted
    ```

=== "Rust"

    ```rust
    use uni_db::xervo::{ExtractOptions, NlpInput};

    let facts = db
        .xervo()
        .extract_facts(
            &NlpInput::Turns(vec![(
                "user".into(),
                "I moved to Lisbon last year and started at a bank.".into(),
            )]),
            &ExtractOptions::new()
                .relation("lives in", Some("the place where the head resides"))
                .relation("employer", Some("organization the head works for"))
                .entity_type("location", None)
                .entity_type("organization", None),
        )
        .await?;
    ```

## Options

| Key | Type | Default | Meaning |
|---|---|---|---|
| `alias` | string | `"nlp/default"` | Catalog alias of a kniv v7 model. |
| `relations` | list | — | Names or `{name, description}` maps. Required by `uni.nlp.extract` unless `schema` is given. |
| `entity_types` | list | model's coarse types | Same shape. |
| `schema` | string or map | — | `'graph'` derives `relations` and `entity_types` from the declared schema; `{labels: [...], edge_types: [...]}` uses a slice of it. Can't be combined with the two keys above. |
| `top_per_pair` | int | model's calibrated value | Facts kept per entity pair. |
| `rel_offsets` | map | — | Relation → keep-threshold offset. A fact is kept when its raw margin exceeds `-offset`, so a **lower** offset is a **higher** bar. |

Unknown keys are rejected. `uni.nlp.entities` takes `alias`, `entity_types`
and `schema` only, and skips the relation pass entirely.

Write relation descriptions that say which side is the head (`"the place
where the head resides"`), avoid near-synonyms in one schema (they split the
decision), and prefer specific relations over a generic `related to`. With
`schema: 'graph'`, edge types without a description are logged once per call.

## Yields

`uni.nlp.extract`:

| Column | Meaning |
|---|---|
| `doc`, `window` | Input document index, and the window of whole sentences (≤ 512 words) within it. |
| `head`, `tail` | Window-local entity indexes; join with `uni.nlp.entities` on `(doc, window, entity)`. |
| `head_text`, `head_type`, `head_speaker` | First mention's text (or the speaker name), type, and whether it is a conversation participant. Same for `tail_*`. |
| `head_start`, `head_end`, `head_turn` | UTF-8 byte span of the first mention. For a conversation, offsets are within the turn named by `head_turn`. Null for speakers. Same for `tail_*`. |
| `relation` | As you named it, or the edge type name under `schema: 'graph'`. |
| `polarity` | `positive` or `negative`. |
| `modality` | `asserted`, `reported`, `planned`, `desired`, `questioned` or `hypothetical`. |
| `holder`, `time` | Entity index of whoever holds the belief, and of a temporal entity (only when the schema has temporal types). |
| `margin` | Calibrated margin over the keep threshold, always > 0. |
| `confidence` | `1 / (1 + e^-margin)`. A monotone convenience in (0, 1), **not a calibrated probability**. |
| `model_alias`, `model_id` | Which model produced the row, for provenance. |

`uni.nlp.entities` yields `doc, window, entity, text, entity_type, speaker`,
`mentions` (list), `mention_starts`, `mention_ends`, `mention_turns` (lists
parallel to `mentions`; not `starts` / `ends`, which Cypher reads as the
`STARTS WITH` / `ENDS WITH` keywords),
and `model_alias, model_id`.

## Reading the Attributes

- Only **`asserted`** and **`reported`** facts are claims about the world.
  Folding `planned`, `desired`, `questioned` or `hypothetical` facts into
  current state is a mistake: "next month I'm joining Northstar" is not an
  employer. They are natural inputs to Locy's `ASSUME … THEN`.
- **`negative`** polarity is evidence *against* the same
  `(head, relation, tail)`, not a separate fact.
- Tune a relation that fires too eagerly with `rel_offsets`, not by filtering
  on `margin`. To turn margins into probabilities, label a sample and use
  Locy's `CALIBRATE` / `VALIDATE` ([Neural Predicates](neural-predicates.md)).

## Storing Facts

Holder and time point at other entities, and an edge property can't point at
a node, so a faithful store makes each fact a claim node with its provenance:

```cypher
MATCH (d:Document {id: $doc})
CALL uni.nlp.extract(d.text, {schema: 'graph'})
YIELD head_text, head_type, relation, tail_text, tail_type, polarity, modality,
      margin, head_start, head_end, tail_start, tail_end, model_alias, model_id
WHERE modality IN ['asserted', 'reported']
MERGE (h:Entity {name: head_text, type: head_type})
MERGE (t:Entity {name: tail_text, type: tail_type})
CREATE (c:Claim {relation: relation, polarity: polarity, modality: modality,
                 margin: margin, model_alias: model_alias, model_id: model_id,
                 head_start: head_start, head_end: head_end,
                 tail_start: tail_start, tail_end: tail_end})
CREATE (c)-[:HEAD]->(h), (c)-[:TAIL]->(t), (c)-[:FROM]->(d)
```

A plain `(h)-[:LIVES_IN {margin: margin}]->(t)` edge is enough when you don't
need the holder, time or spans.

Entities are only grouped **within one window**: "Ada Lovelace" in two
documents comes back as two entities. Matching them to existing nodes (by
normalised name, then `uni.vector.query` over an embedded name property) is
up to you for now. A rule-based approach in Locy, with `same_as … AS PROB`
calibrated against known matches, is described in the
[design proposal](https://github.com/rustic-ai/uni-db/blob/main/docs/proposals/kniv_v7_nlp_extraction_2026_11_07.md).

## Extracting on Write

`uni.plugin.declareTrigger` runs a Cypher action after a matching commit, off
the commit path, so extraction can follow every new document:

```cypher
CALL uni.plugin.declareTrigger(
  'memory.extract',
  'CREATE ON :Document',
  'MATCH (d:Document) WHERE id(d) = $vid
   CALL uni.nlp.extract(d.text, {schema: "graph"})
   YIELD head_text, relation, tail_text, modality
   WHERE modality IN ["asserted", "reported"]
   MERGE (h:Entity {name: head_text})
   MERGE (t:Entity {name: tail_text})
   CREATE (h)-[:CLAIM {relation: relation}]->(t)'
)
```

Extracting into a [fork](forks.md) and reviewing the `diff` before `promote`
is the curation path for low-confidence results.

## Cost

- **Quadratic in entities.** The relation pass scores every ordered entity
  pair in a window, so long entity-type lists are the usual cause of slow
  calls. Narrow them, or use `schema: {labels: [...], edge_types: [...]}`.
- **Stable schemas are cheap.** Label embeddings are cached per exact label
  list, so a fixed schema costs one extra pass for the life of the model.
- **Memory.** 2–3 GB resident per loaded model; keep `"warmup": "lazy"` in
  catalogs that rarely extract.

## kniv v7 or an LLM?

| | `uni.nlp.*` (kniv v7) | LLM via `db.xervo().generate` |
|---|---|---|
| Runs | in-process, CPU, no network | remote API or `llama-server` |
| Cost at volume | fixed, ~0.5 s per 200-word window | per token |
| Repeatability | deterministic for a model, schema and calibration | sampling-dependent |
| Tuning | `rel_offsets`, `CALIBRATE` against labels | prompt edits |
| Languages | English only | many |
| Complex or implicit relations | what the window states | stronger |
| Privacy | text never leaves the process | depends on the provider |

Use kniv v7 for high-volume, private, calibratable English extraction with a
stable schema; use an LLM for complex, implicit or multilingual text.

## Use Cases

- **Agent memory**: extract what users assert about themselves from
  conversation turns, keep plans and questions apart, and reason over them.
- **Knowledge graphs from documents**: fill `Entity` / `MENTIONS` structure
  for GraphRAG instead of assuming it exists.
- **Auditable extraction**: every claim carries its spans and model, so a
  Locy conclusion can be traced to the sentence.

## When To Use

Use it when the text is English, the schema is known or stable, and you want
extraction that is local, repeatable and tunable. Prefer an LLM when the
relations are implicit, the language varies, or the schema changes per call.
