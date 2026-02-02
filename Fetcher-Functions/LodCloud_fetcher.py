import requests
import csv
import time
import os
import re

BASE_CATALOG_URL = "https://lod-cloud.net/versions/2025-12-19/lod-data.json"
BASE_PAGE_URL = "https://lod-cloud.net/dataset/"


class LodCloudFetcher:
    """
    LOD Cloud fetcher metodologicamente allineato a query TITLE–ABS–KEY.
    - Filtro testuale su title / description / tags
    - AND cloud_terms + semantic_terms
    - NOT exclude_terms
    - Supporto wildcard (*)
    - Filtro per anno (issued)
    - Deduplicazione su URL
    - Export CSV + BibTeX
    """

    def __init__(self, max_retries=3, delay=2):
        self.catalog = {}
        self.max_retries = max_retries
        self.delay = delay

    # ================= CATALOG =================
    def fetch_catalog(self):
        if self.catalog:
            return
        print("[INFO] Download LOD Cloud catalog...")
        for attempt in range(self.max_retries):
            try:
                r = requests.get(BASE_CATALOG_URL, timeout=30)
                r.raise_for_status()
                self.catalog = r.json()
                print(f"[INFO] Catalog loaded: {len(self.catalog)} datasets")
                return
            except Exception as e:
                print(f"[WARN] Error: {e} – retry in {self.delay}s")
                time.sleep(self.delay)
        raise RuntimeError("LOD Cloud catalog download failed")

    # ================= UTILS =================
    def _normalize_text(self, value, lang="en"):
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            v = value.get(lang)
            return v if isinstance(v, str) else ""
        if isinstance(value, list):
            return " ".join(
                self._normalize_text(v, lang)
                for v in value
                if v is not None
            )
        return ""

    def _match_term(self, term, text):
        term = term.lower()
        if term.endswith("*"):
            prefix = term[:-1]
            return any(w.startswith(prefix) for w in re.findall(r"\w+", text))
        return term in text

    def _match_any(self, terms, text):
        return any(self._match_term(t, text) for t in terms)

    # ================= FILTER =================
    def filter_dataset(
        self,
        dataset,
        cloud_terms,
        semantic_terms,
        exclude_terms,
        year_min,
        year_max,
    ):
        # Normalizza tutto in stringhe
        title = self._normalize_text(dataset.get("title"))
        desc = self._normalize_text(dataset.get("description"))
        tags = self._normalize_text(dataset.get("tags"))

        text = " ".join([title, desc, tags]).lower()

        # AND group 1: cloud
        if not self._match_any(cloud_terms, text):
            return False

        # AND group 2: semantic
        if not self._match_any(semantic_terms, text):
            return False

        # NOT group
        if self._match_any(exclude_terms, text):
            return False

        # Year filter (issued)
        year = dataset.get("created")
        if year:
            try:
                year = int(year)
                if year < year_min or year > year_max:
                    return False
            except ValueError:
                pass

        return True


    # ================= FETCH =================
    def fetch(
        self,
        cloud_terms,
        semantic_terms,
        exclude_terms,
        year_min=2014,
        year_max=2025,
    ):
        self.fetch_catalog()
        results = []

        for dataset_id, entry in self.catalog.items():
            dataset = {
                "id": dataset_id,
                "title": entry.get("title", ""),
                "description": entry.get("description", ""),
                "tags": entry.get("keywords") or entry.get("tags", []),
                "created": entry.get("issued", "")[:4] if entry.get("issued") else None,
                "url": f"{BASE_PAGE_URL}{dataset_id}",
            }

            if self.filter_dataset(
                dataset,
                cloud_terms,
                semantic_terms,
                exclude_terms,
                year_min,
                year_max,
            ):
                results.append(dataset)

        # Deduplication by URL
        unique = {}
        for d in results:
            unique[d["url"]] = d

        print(f"[INFO] Filtered unique datasets: {len(unique)}")
        return list(unique.values())

    # ================= SAVE CSV =================
    def save_csv(self, datasets, path):
        if not datasets:
            print("[WARN] No datasets to save (CSV).")
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["title", "description", "tags", "created", "url"],
            )
            writer.writeheader()
            for d in datasets:
                writer.writerow({
                    "title": self._normalize_text(d["title"]),
                    "description": self._normalize_text(d["description"]),
                    "tags": self._normalize_text(d["tags"]),
                    "created": d["created"],
                    "url": d["url"],
                })
        print(f"[INFO] CSV saved: {path}")

    # ================= SAVE BIBTEX =================
    def save_bibtex(self, datasets, path):
        if not datasets:
            print("[WARN] No datasets to save (BibTeX).")
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)

        def esc(s):
            if not s:
                return ""
            return re.sub(r"([&_#%{}])", r"\\\1", s)

        with open(path, "w", encoding="utf-8") as f:
            for i, d in enumerate(datasets, 1):
                title = esc(self._normalize_text(d["title"]))
                year = d["created"] or ""
                url = d["url"]
                desc = esc(self._normalize_text(d["description"]))
                tags = esc(self._normalize_text(d["tags"]))

                note = " -- ".join(
                    part for part in [desc, f"Tags: {tags}" if tags else ""]
                    if part
                )

                f.write(
                    f"@dataset{{lodcloud{i},\n"
                    f"  title={{ {title} }},\n"
                    f"  year={{ {year} }},\n"
                    f"  publisher={{LOD Cloud}},\n"
                    f"  url={{ {url} }},\n"
                    f"  note={{ {note} }}\n"
                    f"}}\n\n"
                )

        print(f"[INFO] BibTeX saved: {path}")


# ================= MAIN =================
if __name__ == "__main__":
    OUTPUT_DIR = "output-lodcloud"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cloud_terms = [
        "cloud computing",
        "cloud-computing",
        "multi-cloud",
    ]

    semantic_terms = [
        "ontolog*",
        "semantic web",
        "knowledge graph*",
        "linked data",
        "linked open data",
    ]

    exclude_terms = [
        "internet of things",
        "iot",
    ]

    fetcher = LodCloudFetcher()

    datasets = fetcher.fetch(
        cloud_terms=cloud_terms,
        semantic_terms=semantic_terms,
        exclude_terms=exclude_terms,
        year_min=2014,
        year_max=2026,
    )

    fetcher.save_csv(datasets, f"{OUTPUT_DIR}/lodcloud_results.csv")
    fetcher.save_bibtex(datasets, f"{OUTPUT_DIR}/lodcloud_results.bib")

    print("[OK] LOD Cloud extraction completed")
