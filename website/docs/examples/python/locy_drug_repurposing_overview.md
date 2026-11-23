# Drug Repurposing: Finding New Uses for Approved Drugs

**Industry**: Pharmaceutical R&D | **Role**: VP Translational Science, Computational Biology Lead | **Time to value**: 2-3 hours

## The Problem

There are thousands of approved drugs with well-characterized safety profiles. There are thousands of diseases with unmet therapeutic need. Somewhere in the intersection are repurposing candidates — approved drugs that could treat diseases they were never designed for. The challenge is finding them systematically, ranking them by evidence strength, and explaining why each candidate was predicted.

## The Traditional Approach

Computational repurposing pipelines are typically assembled from separate components: literature mining to extract drug-target and target-disease associations, custom graph traversal code to walk protein-protein interaction networks, bespoke aggregation logic to combine evidence from multiple mechanistic pathways, and a structural similarity search for analogue discovery. A team of 3-4 computational biologists spends a sprint wiring these together — typically 500+ lines of Python — with no built-in explainability. When a reviewer asks "why was Drug X predicted for Disease Y?", someone traces the logic manually. Adding a new evidence source means modifying the traversal code.

## With Uni

The notebook encodes the entire pipeline in a handful of declarative rules. Multi-hop protein-protein interaction traversal (up to 3 hops) finds indirect mechanistic paths from drug targets to disease-associated proteins. Noisy-OR aggregation (`FOLD MNOR`) combines evidence from independent pathways — if a drug reaches a disease through several separate mechanisms, each adds independent support. Structural analogue discovery (`similar_to` on molecular fingerprint vectors) identifies approved drugs with similar structure that share target diseases. Novelty filtering (`IS NOT`) removes known indications. Built-in `EXPLAIN RULE` traces exactly why each drug-disease pair was predicted — the full mechanistic derivation tree. `ASSUME` simulates a hypothetical new binding target and re-evaluates, then rolls back. `ABDUCE` proposes single graph edits that would support a new indication, and validates each one.

## What You'll See

- 28 novel drug-disease candidates ranked by combined pathway evidence (noisy-OR scores in [0, 1])
- Structural analogue matches linking candidates to known approved drugs via molecular-fingerprint similarity
- Full mechanistic derivation trees (`EXPLAIN RULE`) for a prediction — reviewable by domain experts
- What-if simulation (`ASSUME`) of a new protein binding showing how hypothetical evidence changes rankings, then rolled back
- Validated single-edit candidates (`ABDUCE`) for what graph change would make a target indication appear

## Why It Matters

The same analysis that traditionally requires a team, a sprint, and 500+ lines of custom code is expressed in a few declarative rules that a domain expert can read, audit, and extend. That changes repurposing from a software engineering project to a scientific reasoning exercise.

---

[Run the notebook →](locy_drug_repurposing.md)
