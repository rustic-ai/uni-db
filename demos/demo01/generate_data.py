#!/usr/bin/env python3
"""Generate synthetic demo data matching Semantic Scholar schema."""

import json
import random
import numpy as np
import os
from faker import Faker

fake = Faker()

VENUES = ["NeurIPS", "ICML", "ICLR", "CVPR", "ACL", "EMNLP", "AAAI", "IJCAI"]

def generate_paper(paper_id: int) -> dict:
    """Generate a single paper with embedding."""
    return {
        "vid": paper_id,
        "title": fake.sentence(nb_words=8),
        "abstract": fake.paragraph(nb_sentences=5),
        "year": random.randint(2018, 2024),
        "citation_count": int(np.random.power(0.3) * 10000),
        "embedding": np.random.randn(768).astype(np.float32).tolist(),
        "_doc": {
            "venue": random.choice(VENUES),
            "doi": f"10.{random.randint(1000,9999)}/{fake.uuid4()[:8]}",
            "authors": [
                {
                    "name": fake.name(),
                    "affiliation": fake.company(),
                    "orcid": f"0000-000{random.randint(1,9)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"
                }
                for _ in range(random.randint(1, 6))
            ],
            "keywords": [fake.word() for _ in range(random.randint(3, 8))]
        }
    }

def generate_citations(num_papers: int, avg_citations: int = 12) -> list:
    """Generate citation edges with power-law distribution."""
    edges = []
    for src in range(num_papers):
        # Power-law: some papers cite many, most cite few
        num_cites = int(np.random.power(0.5) * avg_citations * 2)
        targets = random.sample(range(num_papers), min(num_cites, num_papers - 1))
        for dst in targets:
            if dst != src:
                edges.append({
                    "src_vid": src,
                    "dst_vid": dst,
                    "type": "CITES",
                    "context": fake.sentence()
                })
    return edges

if __name__ == "__main__":
    # Ensure output directory exists
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Generate 1000 papers for local demo (smaller than spec for speed, but enough to show features)
    # Spec said 10K, let's do 1K for quick iteration, user can increase.
    # Actually, 1K is very fast. Let's do 5K.
    NUM_PAPERS = 5000
    
    print(f"Generating {NUM_PAPERS} papers...")
    papers = []
    
    # Paper 0: The Seed (Transformer)
    seed_paper = generate_paper(0)
    seed_paper["title"] = "Attention Is All You Need"
    seed_paper["_doc"]["venue"] = "NeurIPS"
    seed_paper["year"] = 2017
    # Fixed embedding for reproducibility (just random fixed)
    rng = np.random.RandomState(42)
    seed_paper["embedding"] = rng.randn(768).astype(np.float32).tolist()
    papers.append(seed_paper)

    for i in range(1, NUM_PAPERS):
        papers.append(generate_paper(i))

    citations = generate_citations(NUM_PAPERS)
    
    with open(os.path.join(output_dir, "papers.jsonl"), "w") as f:
        for p in papers:
            f.write(json.dumps(p) + "\n")
    
    with open(os.path.join(output_dir, "citations.jsonl"), "w") as f:
        for c in citations:
            f.write(json.dumps(c) + "\n")
    
    print(f"Generated {len(papers)} papers and {len(citations)} citations in {output_dir}/")
