# Cloud-Ontology — Fetcher & Post-processing Toolkit (Thesis Project)

A Python-based toolkit developed for my MSc thesis to **systematically retrieve, filter, and export literature and resources** related to **cloud computing** and **semantic technologies** (ontologies, knowledge graphs, Semantic Web, Linked Data) from multiple sources.

The project supports a **multivocal literature review (MLR)** workflow by combining:
- **Academic databases** (Scopus)
- **Grey literature & research artifacts** (Zenodo, GitHub)
- **Open semantic datasets** (LOD Cloud)

It provides **reproducible extraction**, **methodologically-aligned keyword filtering**, **deduplication**, and **export** (CSV, BibTeX, Excel).

---

## ✨ Key Features

### ✅ Multi-source extraction
- **LOD Cloud**: fetch + filter datasets from the official catalog JSON
- **Scopus**: automated search via Elsevier API (rate-limit aware)
- **Zenodo**: robust querying with Python-side year filtering + deduplication
- **GitHub**: repository search with rate-limit handling + deduplication

### ✅ Methodology-oriented filtering (TITLE–ABS–KEY style)
- AND logic between:
  - **cloud_terms** (e.g., "cloud computing", "multi-cloud")
  - **semantic_terms** (e.g., ontolog*, "knowledge graph*")
- NOT logic for exclusion terms (e.g., IoT)
- **Wildcard support** (e.g., `ontolog*`, `knowledge graph*`)
- Optional **year filtering** (Python-side where needed)

### ✅ Export & reproducibility
- Output formats:
  - **CSV**
  - **BibTeX**
  - **Excel (formatted table)** for Zenodo post-processing
- **Deduplication** based on DOI / URL / title+authors (source-dependent)
- Robust cleaning of text (HTML stripping, Unicode normalization)

---

## 🧱 Project Structure

> Note: the structure below reflects the main components shown in the code snippets.
