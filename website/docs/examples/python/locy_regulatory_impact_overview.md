# Regulatory Impact Analysis: Tracing Obligations to Systems

**Industry**: Financial Services / Compliance | **Role**: Chief Compliance Officer, Head of GRC | **Time to value**: 2-3 hours

## The Problem

DORA takes effect. Your compliance team needs to answer: which systems are exposed, through what chain of dependencies, and what is the cheapest way to close the gaps? The same question applies to GDPR, SOX, HIPAA, and PCI-DSS — and the dependencies overlap. A single system may be impacted by obligations from three different regulations through six different paths.

## The Traditional Approach

Compliance teams maintain the regulation-to-obligation-to-control-to-process-to-system-to-vendor chain in spreadsheets or GRC platforms that store mappings but do not reason over them. Tracing a 6-hop dependency path means clicking through multiple screens or joining multiple tabs. Impact assessment for a new regulation takes weeks of manual mapping. When an auditor asks "why is this system flagged?", the answer is reconstructed from email threads and meeting notes. Semantic gaps — obligations that lack explicit control mappings but are partially covered by related controls — are invisible. What-if analysis ("if this vendor improves their controls, how much does our risk drop?") requires rebuilding the spreadsheet.

## With Uni

The notebook evaluates the full 6-hop dependency chain declaratively: regulation creates obligation, obligation requires control, control protects process, process runs on system, system depends on vendor. Risk accumulates through noisy-OR aggregation — each regulation that creates risk for a system independently increases that system's exposure score. Semantic matching identifies controls that partially cover obligations even without explicit mappings, surfacing coverage gaps and near-misses. What-if scenarios remediate a control and recompute system exposure instantly. `ABDUCE` proposes single dependency-chain edits, each validated and ranked by cost, that would bring a flagged system below its risk threshold.

## What You'll See

- System exposure scores aggregated across 5 regulations (GDPR, SOX, DORA, HIPAA, PCI-DSS) via noisy-OR
- Vendor risk rollup incorporating each vendor's own risk rating and its propagation to dependent systems
- Semantic gap analysis flagging obligations that lack a direct control match
- What-if analysis of remediating a control (e.g., implementing "ICT resilience testing" drops ERP Core exposure from 0.995 to 0.909 and Trading from 0.988 to 0.947)
- Full 6-hop audit trail from regulation to impacted system, traceable by any auditor

## Why It Matters

The average financial institution spends 6-8 weeks on impact assessment for a major new regulation. Encoding the dependency chain as rules that can be queried, explained, and simulated reduces that to days — and the audit trail is built in, not reconstructed after the fact.

---

[Run the notebook →](locy_regulatory_impact.md)
