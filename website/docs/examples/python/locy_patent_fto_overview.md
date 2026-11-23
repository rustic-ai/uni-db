# Patent Freedom-to-Operate: Computational Claim Analysis

**Industry**: Legal / Intellectual Property | **Role**: VP IP Strategy, Patent Counsel | **Time to value**: 3-4 hours

## The Problem

You are about to launch a product. Your IP team needs to assess whether it infringes existing patents. For a typical analysis covering 8 patents with 18 claims and 48 claim elements, this means evaluating dozens of feature-to-element mappings — and a single missing mapping can mean the difference between infringement and freedom to operate.

## The Traditional Approach

Patent attorneys manually build claim charts, mapping each product feature to each claim element across every relevant patent. For 8 patents, this takes 2-4 weeks of attorney time. The logic is implicit: infringement requires that every element of a claim maps to a product feature (all-elements rule), but this is tracked in spreadsheets, not enforced structurally. Dependent claims inherit parent elements, which attorneys track manually and occasionally miss. Design-around analysis — identifying the smallest product change that eliminates infringement — is done by intuition, because systematically testing every possible change is impractical by hand. Prior art searches happen in a separate workflow with no connection to the claim analysis.

## With Uni

The notebook models claim infringement as a joint probability — the product of element-level mapping confidences. If any single element lacks a feature mapping, that claim's infringement probability drops to zero, correctly encoding the all-elements rule. Patent-level risk combines independent claims using noisy-OR: if any independent claim is infringed, the patent poses risk. Dependent claims automatically inherit parent elements through recursive rules, eliminating manual tracking errors. Prior art matching uses semantic similarity to find relevant prior art for each claim element. Design-around simulation removes one feature-element mapping at a time and recomputes patent risk, identifying single product changes that eliminate infringement.

## What You'll See

- Infringement probability per claim computed as the product of element-level mapping confidences
- Patent risk per product combining independent claims — any single infringed claim creates exposure
- Computational claim charts with full derivation of which features map to which elements and at what confidence
- Design-around simulations answering "what if we redesign this feature?" with quantified risk impact
- Single design-change candidates ranked by cost, each validated to eliminate infringement

## Why It Matters

Reducing a 3-week manual claim chart exercise to a structured, auditable computation means IP teams can assess freedom-to-operate earlier in the product cycle — when design changes are still cheap. The design-around analysis alone can save months of litigation-driven redesign.

---

[Run the notebook →](locy_patent_fto.md)
