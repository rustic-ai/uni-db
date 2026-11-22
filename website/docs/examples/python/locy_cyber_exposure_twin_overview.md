# Cyber Exposure Twin: Vulnerability Prioritization at Scale

**Industry**: Cybersecurity | **Role**: CISO, Security Engineering Lead | **Time to value**: 2-3 hours

## The Problem

Your scanner just reported 10,000 vulnerabilities. CVSS scores tell you 3,200 are "critical" or "high." Your team has capacity to remediate 400 this quarter. Picking the wrong 400 means the breach comes through a medium-severity finding on a business-critical asset that was two hops from the internet.

## The Traditional Approach

Most teams start with CVSS and layer on manual context: asset owners tag criticality in spreadsheets, network topology lives in a separate CMDB, and SLA deadlines are tracked in ticketing systems. A senior analyst spends days cross-referencing these sources to build a prioritized list. Blast radius analysis — understanding which assets an attacker can reach from a compromised host — requires custom graph traversal code or expensive specialized tools. When leadership asks "why did we patch server X before server Y?", the answer is tribal knowledge. Remediation planning is done by gut feel, not by modeling which fixes reduce the most risk per dollar spent.

## With Uni

The notebook computes an exposure score for each finding by combining CVSS base severity, EPSS exploit probability, CISA KEV status, and observed exploit evidence, then layers on a hybrid-retrieval (vector + full-text) evidence boost drawn from advisory and runbook documents. Findings above a critical threshold are flagged, and declarative Locy rules propagate blast risk along asset dependencies (recursive `ALONG` traversal) to find which assets are reachable downstream of each compromised host. For every urgent finding, a `BEST BY` rule selects a single best remediation action, ranked first by residual risk and then by cost and downtime. Every result comes with a full derivation: `EXPLAIN RULE` traces exactly why a blast path or score was produced.

## What You'll See

- Exposure-ranked vulnerability list that blends CVSS, EPSS, KEV status, exploit evidence, and hybrid-retrieval advisory evidence — not just CVSS
- Blast radius per source asset, traced by recursive `ALONG` propagation across dependency edges (paths up to several hops deep)
- Team-level exposure rollups grouping findings by remediation owner, with average, max, and urgent-finding counts
- A best-action remediation plan that picks one action per urgent finding by lowest residual risk, then cost and downtime (`BEST BY`)
- Counterfactual containment with `ASSUME` (applying virtual patches to high-criticality assets) and single-edit candidates from `ABDUCE`, each validated, for what would remove an urgent patch requirement

## Why It Matters

Prioritizing by exposure context — topology, exploit likelihood, and advisory evidence — instead of raw CVSS scores focuses scarce remediation capacity on the findings that actually reduce risk. The full derivation trail means every prioritization decision is defensible.

---

[Run the notebook →](locy_cyber_exposure_twin.md)
