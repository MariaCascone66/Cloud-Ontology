import requests
import csv
import time
import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

class ZenodoFetcherResume:
    """
    Fetcher Zenodo parallelo, resume-friendly.
    - Query OR-based (simile a Scopus / ACM)
    - Filtro post-fetch TITLE / ABSTRACT / KEYWORDS
    - Fetch anno per anno
    - Deduplicazione DOI / URL/ID
    - Salvataggio CSV + BibTeX
    """

    def __init__(self, token_path=None, per_page=100, max_retries=3, sleep_time=1, max_workers=3):
        self.base_url = "https://zenodo.org/api/records"
        self.per_page = min(per_page, 100)
        self.max_retries = max_retries
        self.sleep_time = sleep_time
        self.max_workers = max_workers
        self.token = self.load_token(token_path)
        self.headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}
        if self.token:
            print("[INFO] Token Zenodo caricato correttamente.")
        else:
            print("[WARN] Nessun token Zenodo trovato, user non autenticato (limite 25 record/page).")

    # ==================== TOKEN =========================
    def load_token(self, path):
        if not path or not os.path.isfile(path):
            return None
        with open(path, "r") as f:
            for line in f:
                if line.startswith("ZENODO_TOKEN="):
                    return line.strip().split("=", 1)[1]
        return None

    # ==================== QUERY BUILDER =================
    def build_zenodo_query(self, cloud_terms, semantic_terms, exclude_terms):
        q_cloud = "(" + " OR ".join(f'"{t}"' for t in cloud_terms) + ")"
        q_semantic = "(" + " OR ".join(f'"{t}"' for t in semantic_terms) + ")"
        q_exclude = "(" + " OR ".join(f'"{t}"' for t in exclude_terms) + ")"
        return f"{q_cloud} AND {q_semantic} AND NOT {q_exclude}"

    # ==================== FIELD FILTER ==================
    def record_matches_fields(self, md, cloud_terms, semantic_terms):
        title = md.get("title", "") or ""
        abstract = md.get("description", "") or ""
        keywords = " ".join(md.get("keywords", [])) if md.get("keywords") else ""
        text = f"{title} {abstract} {keywords}".lower()

        cloud_ok = any(t.lower() in text for t in cloud_terms)
        semantic_ok = any(t.lower() in text for t in semantic_terms)
        return cloud_ok and semantic_ok

    # ==================== FETCH PAGE ====================
    def fetch_page(self, query, page):
        params = {"q": query, "page": page, "size": self.per_page}
        url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                if response.status_code >= 500:
                    wait = 2 ** attempt
                    print(f"[WARN] Errore {response.status_code} pagina {page}, retry in {wait}s")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json().get("hits", {}).get("hits", [])
            except requests.exceptions.RequestException as e:
                wait = 2 ** attempt
                print(f"[WARN] Eccezione pagina {page}: {e}, retry in {wait}s")
                time.sleep(wait)
        return []

    # ==================== FETCH YEAR ====================
    def fetch_year_parallel(self, query, year, cloud_terms, semantic_terms):
        print(f"\n[INFO] Fetch anno {year}")

        year_results = []
        seen_keys = set()
        page = 1
        more_pages = True

        while more_pages:
            pages_batch = list(range(page, page + self.max_workers))
            futures = {}
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for p in pages_batch:
                    futures[executor.submit(self.fetch_page, query, p)] = p
                for future in as_completed(futures):
                    hits = future.result()
                    if not hits:
                        more_pages = False
                        continue
                    for item in hits:
                        md = item.get("metadata", {})
                        if not self.record_matches_fields(md, cloud_terms, semantic_terms):
                            continue

                        doi = md.get("doi")
                        url = item.get("links", {}).get("html")
                        rec_id = item.get("id")
                        key = doi or url or str(rec_id)
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)

                        creators = md.get("creators", [])
                        year_results.append({
                            "title": md.get("title"),
                            "author": ", ".join(a.get("name", "") for a in creators),
                            "description": md.get("description"),
                            "created": item.get("created"),
                            "updated": item.get("updated"),
                            "keywords": ", ".join(md.get("keywords", [])) if md.get("keywords") else None,
                            "doi": doi,
                            "url": url,
                            "license": md.get("license", {}).get("id") if md.get("license") else None,
                            "type": md.get("resource_type", {}).get("type"),
                        })
            page += self.max_workers
            time.sleep(self.sleep_time)

        # Salvataggio CSV + BibTeX anno per anno
        self.save_csv(year_results, f"output/zenodo_{year}.csv")
        self.save_bibtex(year_results, f"output/zenodo_{year}.bib")
        print(f"[INFO] Totale record anno {year}: {len(year_results)}")
        return year_results

    # ==================== FETCH ALL =====================
    def fetch(self, query, cloud_terms, semantic_terms, from_year=2015, until_year=2026):
        all_results = []
        for year in range(from_year, until_year + 1):
            csv_path = f"output/zenodo_{year}.csv"
            if os.path.exists(csv_path):
                with open(csv_path, "r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    all_results.extend(list(reader))
                continue
            year_results = self.fetch_year_parallel(query, year, cloud_terms, semantic_terms)
            all_results.extend(year_results)
        print(f"\n[INFO] Totale record Zenodo: {len(all_results)}")
        return all_results

    # ==================== SAVE CSV ======================
    def save_csv(self, records, path):
        if not records:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

    # ==================== SAVE BIBTEX ===================
    def record_to_bibtex(self, record):
        def sanitize(s):
            return s.replace("{", "").replace("}", "").replace("\n", " ").strip()
        key = record.get("doi") or record.get("url") or sanitize(record.get("title", "zenodo"))[:40]
        authors = record.get("author", "")
        year = (record.get("created") or "")[:4]
        bib = f"""@misc{{{key},
  title        = {{{sanitize(record.get('title', ''))}}},
  author       = {{{sanitize(authors)}}},
  year         = {{{year}}},
  howpublished = {{Zenodo}},
  url          = {{{record.get('url', '')}}},
"""
        if record.get("doi"):
            bib += f"  doi          = {{{record.get('doi')}}},\n"
        if record.get("license"):
            bib += f"  note         = {{License: {record.get('license')}}},\n"
        bib += "}\n\n"
        return bib

    def save_bibtex(self, records, path):
        if not records:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(self.record_to_bibtex(r))


# ==================== MAIN =============================
if __name__ == "__main__":
    os.makedirs("output-zenodo", exist_ok=True)

    cloud_terms = [
        "cloud computing",
        "cloud-computing",
        "multi-cloud"
    ]

    semantic_terms = [
        "ontology",
        "ontologies",
        "semantic web",
        "knowledge graph",
        "linked data",
        "linked open data"
    ]

    exclude_terms = [
        "internet of things",
        "iot"
    ]

    token_path = r"C:\Users\maria\Desktop\Cloud-Ontology\token-zenodo.env"

    zf = ZenodoFetcherResume(token_path=token_path, per_page=100, max_workers=3)

    query = zf.build_zenodo_query(cloud_terms, semantic_terms, exclude_terms)

    records = zf.fetch(query, cloud_terms, semantic_terms, from_year=2015, until_year=2026)

    if records:
        zf.save_csv(records, "output-zenodo/zenodo_all_years.csv")
        zf.save_bibtex(records, "output-zenodo/zenodo_all_years.bib")

    print("[INFO] Esecuzione completata")
