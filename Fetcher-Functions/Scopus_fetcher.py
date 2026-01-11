import requests
import time
import csv
from datetime import datetime
import os
from dotenv import load_dotenv

# ================= LOAD ENV =================
dotenv_path = r"C:\Users\maria\Desktop\Cloud-Ontology\scopus_key.env"
load_dotenv(dotenv_path)
SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY")
if not SCOPUS_API_KEY:
    raise ValueError(f"SCOPUS_API_KEY not found in {dotenv_path}")

# ================= FETCHER CLASS =================
class ScopusFetcher:
    def __init__(self, api_key, per_page=25, max_retries=3):
        """
        api_key: Elsevier API Key
        per_page: results per page (max 25/200)
        max_retries: retries in case of network/rate limit errors
        """
        self.base_url = "https://api.elsevier.com/content/search/scopus"
        self.headers = {
            "Accept": "application/json",
            "X-ELS-APIKey": api_key
        }
        self.per_page = per_page
        self.max_retries = max_retries

    # ------------------ Build Query ------------------
    def build_query(self, base_query, start_year=None, end_year=None,
                    doc_types=None, language=None):
        query_parts = [base_query]

        if start_year:
            query_parts.append(f"AND PUBYEAR > {start_year}")
        if end_year:
            query_parts.append(f"AND PUBYEAR < {end_year + 1}")  # include end_year
        if doc_types:
            types_str = " OR ".join([f'DOCTYPE({t})' for t in doc_types])
            query_parts.append(f"AND ({types_str})")
        if language:
            query_parts.append(f"AND LANGUAGE({language})")

        return " ".join(query_parts)

    # ------------------ Fetch All ------------------
    def fetch_all(self, query):
        results = []
        start = 0
        total_retrieved = 0

        while True:
            params = {"query": query, "start": start, "count": self.per_page}

            for attempt in range(self.max_retries):
                r = requests.get(self.base_url, headers=self.headers, params=params)
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 30))
                    print(f"[WARN] Rate limit reached, waiting {retry_after}s...")
                    time.sleep(retry_after + 1)
                    continue
                elif r.status_code >= 500:
                    print(f"[WARN] Server error {r.status_code}, retrying...")
                    time.sleep(3)
                    continue
                else:
                    break
            else:
                print(f"[ERROR] Unable to complete request for start={start}")
                break

            data = r.json()
            entries = data.get("search-results", {}).get("entry", [])
            if not entries:
                break

            for e in entries:
                results.append({
                    "scopus_id": e.get("dc:identifier", "").replace("SCOPUS_ID:", ""),
                    "eid": e.get("eid", ""),
                    "title": e.get("dc:title") or "",
                    "abstract": e.get("dc:description") or "",
                    "authors": e.get("dc:creator") or "",
                    "doi": e.get("prism:doi") or "",
                    "year": e.get("prism:coverDate") or "",
                    "source": e.get("prism:publicationName") or "",
                    "volume": e.get("prism:volume") or "",
                    "issue": e.get("prism:issueIdentifier") or "",
                    "pages": e.get("prism:pageRange") or "",
                    "issn": e.get("prism:issn") or "",
                    "isbn": e.get("prism:isbn") or "",
                    "affiliations": e.get("affiliation") or "",
                    "subject_areas": e.get("subject-areas") or "",
                    "references": e.get("citedby-count", 0),
                    "citations": e.get("citedby-count", 0),
                    "url": e.get("link", [{}])[0].get("@href") or "",
                    "language": e.get("language") or "",
                    "publisher": e.get("prism:publisher") or "",
                })

            total_retrieved += len(entries)
            total_results = int(data.get("search-results", {}).get("opensearch:totalResults", 0))
            print(f"[INFO] Retrieved {total_retrieved}/{total_results} results...")

            start += len(entries)
            if start >= total_results:
                break

            time.sleep(1)  # polite pause

        print(f"[INFO] Total results retrieved: {len(results)}")
        return results

    # ------------------ Save CSV ------------------
    def save_csv(self, records, path):
        if not records:
            print("[WARN] No records to save.")
            return
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fieldnames = list(records[0].keys())
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        print(f"[INFO] CSV saved to: {path}")

    # ------------------ Save BibTeX ------------------
    def save_bib(self, records, path):
        if not records:
            print("[WARN] No records to save (BibTeX).")
            return
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for i, rec in enumerate(records):
                key = f"scopus{i+1}"
                title = (rec.get("title") or "").replace("{", "\\{").replace("}", "\\}")
                abstract = (rec.get("abstract") or "").replace("{", "\\{").replace("}", "\\}")
                authors = (rec.get("authors") or "").replace("{", "\\{").replace("}", "\\}")
                year = ''
                if rec.get("year"):
                    try:
                        year = datetime.strptime(rec["year"], "%Y-%m-%d").year
                    except Exception:
                        year = rec["year"][:4]
                url = rec.get("url") or ""
                f.write(f"""@article{{{key},
  title={{ {title} }},
  author={{ {authors} }},
  year={{ {year} }},
  abstract={{ {abstract} }},
  url={{ {url} }}
}}\n\n""")
        print(f"[INFO] BibTeX saved to: {path}")


# ================= MAIN =================
if __name__ == "__main__":
    OUTPUT_DIR = "output-scopus"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Query based on TITLE-ABS-KEY
    base_query = (
        'TITLE-ABS-KEY ( ( "cloud computing" OR "cloud-computing" OR "multi-cloud" ) '
        'AND ( "ontolog*" OR "semantic web" OR "knowledge graph*" OR "linked data" OR "linked open data" ) '
        'AND NOT ( "internet of things" OR "iot" ) ) '
        'AND PUBYEAR > 2014 AND PUBYEAR < 2027 '
        'AND ( DOCTYPE(ar) OR DOCTYPE(cp) ) '
        'AND LANGUAGE(English)'
    )
    doc_types = ["ar", "cp"]
    language = "English"
    start_year = 2014
    end_year = 2026

    fetcher = ScopusFetcher(SCOPUS_API_KEY)
    query = fetcher.build_query(base_query, start_year=start_year, end_year=end_year,
                                doc_types=doc_types, language=language)

    print(f"[INFO] Executing query: {query}")
    records = fetcher.fetch_all(query)

    csv_path = os.path.join(OUTPUT_DIR, "scopus_cloud.csv")
    bib_path = os.path.join(OUTPUT_DIR, "scopus_cloud.bib")

    fetcher.save_csv(records, csv_path)
    fetcher.save_bib(records, bib_path)

    print("[OK] Scopus extraction completed")
