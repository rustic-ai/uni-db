# Semiconductor Yield Excursion Root Cause Analysis

**Industry**: Semiconductor Manufacturing | **Role**: VP Yield Engineering, Fab Director | **Time to value**: 4 hours

## The Problem

A batch of wafers fails final test. Somewhere in a 400-step process, one or more tools introduced a defect. The fab has hundreds of tools, each processing thousands of lots per month. Identifying the responsible tool — and deciding what to quarantine without shutting down the line — is the most expensive diagnostic problem in semiconductor manufacturing.

## The Traditional Approach

Yield engineers manually correlate sensor readings from SECOM-style manufacturing data against fail/pass outcomes. They pull lot histories from MES, cross-reference tool assignments, and build pivot tables. A typical excursion investigation involves 50-100 process parameters across 1,500+ lots. Identifying the root cause takes 1-3 weeks. During that time, the suspect tool continues processing wafers, compounding the loss. Containment decisions — which tools to take offline — are made by experience and intuition, not quantified trade-offs.

## With Uni

The notebook loads real SECOM manufacturing data (1,567 lots, 590 sensor features) and focuses on a 96-lot cohort, defining a handful of declarative rules that cover the diagnostic workflow. Fail-lot to tool mapping identifies which tools processed failing wafers. Hotspot ranking orders tools by the number of failed lots whose excursions trace back to each tool. Containment simulation answers "what if we quarantine Tool X?" by counting how many failed lots a hold on the suspect tool would contain versus leave residual (`ASSUME`). `ABDUCE` proposes single graph edits, each validated, that would stop a given lot from triggering the quarantine rule. Every conclusion includes a derivation trace (`EXPLAIN RULE`) showing the lot-to-feature-to-tool evidence chain that supports it.

## What You'll See

- Ranked tool hotspots by failed-lot count, each backed by a traceable evidence chain
- Containment scenarios: how many failed lots a hold on the suspect tool would contain versus leave residual
- Validated single-edit recommendations that would clear a lot's quarantine trigger
- Derivation evidence for each conclusion, tracing the lot-to-feature-to-tool chain

## Why It Matters

A single yield excursion at a modern fab can cost $2-5M in scrapped wafers per week. Reducing time-to-root-cause from weeks to hours and replacing intuition-based containment with cost-optimized quarantine decisions directly protects margin.

---

[Run the notebook →](locy_semiconductor_yield_excursion.md)
