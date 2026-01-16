import requests
import csv
import time
import os

class ZenodoFetcher:
    """
    Zenodo fetcher robusto e metodologicamente corretto.
    - Query OR-based (Scopus / ACM–like)
    - NO filtro temporale nella query (best practice Zenodo)
    - Filtro per anno lato Python
    - Deduplicazione DOI / URL / ID
    - CSV + BibTeX
    """

    def __init__(self, token_path=None, per_page=100, max_retries=3, sleep_time=1):
        self.base_url = "https://zenodo.org/api/records"
        self.per_page = min(per_page, 100)
        self.max_retries = max_retries
        self.sleep_time = sleep_time
        self.token = self.load_token(token_path)
        self.headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}

        if self.token:
            print("[INFO] Token Zenodo caricato.")
        else:
            print("[WARN] Nessun token Zenodo (rate limit più basso).")

    # ================= TOKEN =================
    def load_token(self, path):
        if not path or not os.path.isfile(path):
            return None
        with open(path, "r") as f:
            for line in f:
                if line.startswith("ZENODO_TOKEN="):
                    return line.strip().split("=", 1)[1]
        return None

    # ================= QUERY =================
    def build_query(self, cloud_terms, semantic_terms, exclude_terms):
        q_cloud = "(" + " OR ".join(f'"{t}"' for t in cloud_terms) + ")"
        q_sem = "(" + " OR ".join(t if "*" in t else f'"{t}"' for t in semantic_terms) + ")"
        q_exc = "(" + " OR ".join(f'"{t}"' for t in exclude_terms) + ")"
        return f"{q_cloud} AND {q_sem} AND NOT {q_exc}"

    # ================= FETCH PAGE =================
    def fetch_page(self, query, page):
        params = {
            "q": query,
            "page": page,
            "size": self.per_page
        }
        for attempt in range(self.max_retries):
            try:
                r = requests.get(self.base_url, params=params, headers=self.headers, timeout=30)
                r.raise_for_status()
                return r.json().get("hits", {}).get("hits", [])
            except requests.exceptions.RequestException as e:
                wait = 2 ** attempt
                print(f"[WARN] Pagina {page} errore: {e} – retry {wait}s")
                time.sleep(wait)
        return []

    # ================= FETCH YEAR =================
    def fetch_year(self, query, year):
        print(f"\n[INFO] Fetch anno {year}")
        results = []
        seen = set()
        page = 1

        while True:
            hits = self.fetch_page(query, page)
            if not hits:
                break

            for item in hits:
                md = item.get("metadata", {})
                created = item.get("created", "")
                rec_year = int(created[:4]) if created[:4].isdigit() else None
                if rec_year != year:
                    continue

                doi = md.get("doi")
                url = item.get("links", {}).get("html")
                key = doi or url or str(item.get("id"))
                if key in seen:
                    continue
                seen.add(key)

                creators = md.get("creators", [])
                results.append({
                    "title": md.get("title"),
                    "authors": ", ".join(a.get("name", "") for a in creators),
                    "abstract": md.get("description"),
                    "year": rec_year,
                    "keywords": ", ".join(md.get("keywords", [])) if md.get("keywords") else "",
                    "doi": doi,
                    "url": url,
                    "type": md.get("resource_type", {}).get("type"),
                })

            if len(hits) < self.per_page:
                break

            page += 1
            time.sleep(self.sleep_time)

        self.save_csv(results, f"output-zenodo/zenodo_{year}.csv")
        self.save_bibtex(results, f"output-zenodo/zenodo_{year}.bib")
        print(f"[INFO] Record anno {year}: {len(results)}")
        return results

    # ================= FETCH ALL =================
    def fetch_all(self, query, from_year, to_year):
        all_records = []
        for y in range(from_year, to_year + 1):
            all_records.extend(self.fetch_year(query, y))
        print(f"\n[INFO] Totale record {from_year}-{to_year}: {len(all_records)}")
        return all_records

    # ================= SAVE CSV =================
    def save_csv(self, records, path):
        if not records:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)

    # ================= SAVE BIBTEX =================
    def save_bibtex(self, records, path):
        if not records:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for i, r in enumerate(records, 1):
                f.write(
f"""@misc{{zenodo{i},
  title  = {{{r['title']}}},
  author = {{{r['authors']}}},
  year   = {{{r['year']}}},
  url    = {{{r['url']}}},
"""
                )
                if r["doi"]:
                    f.write(f"  doi    = {{{r['doi']}}},\n")
                f.write("}\n\n")


# ================= MAIN =================
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
        "knowledge graph*",
        "linked data",
        "linked open data"
    ]

    exclude_terms = [
        "internet of things",
        "iot"
    ]

    token_path = r"C:\Users\maria\Desktop\Cloud-Ontology\token-zenodo.env"

    zf = ZenodoFetcher(token_path=token_path)

    query = zf.build_query(cloud_terms, semantic_terms, exclude_terms)
    print("[INFO] Query Zenodo:", query)

    records = zf.fetch_all(query, from_year=2015, to_year=2025)

    zf.save_csv(records, "output-zenodo/zenodo_all_years.csv")
    zf.save_bibtex(records, "output-zenodo/zenodo_all_years.bib")

    print("[OK] Estrazione Zenodo completata")