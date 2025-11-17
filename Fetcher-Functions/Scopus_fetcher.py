import requests
import time
import csv
from datetime import datetime
import os
from dotenv import load_dotenv

class ScopusFetcher:
    def __init__(self, api_key, per_page=25):
        self.base_url = "https://api.elsevier.com/content/search/scopus"
        self.headers = {"Accept": "application/json", "X-ELS-APIKey": api_key}
        self.per_page = per_page

    def build_query(self, base_query, start_year=None, end_year=None,
                    doc_types=None, language=None):
        """
        Costruisce la query completa in modo dinamico (opzionale).
        """
        query_parts = [base_query]

        if start_year:
            query_parts.append(f"AND PUBYEAR > {start_year}")
        if end_year:
            query_parts.append(f"AND PUBYEAR < {end_year}")
        if doc_types:
            types_str = " OR ".join([f'DOCTYPE({t})' for t in doc_types])
            query_parts.append(f"AND ({types_str})")
        if language:
            query_parts.append(f"AND LANGUAGE({language})")

        return " ".join(query_parts)

    def fetch_all(self, query):
        results = []
        start = 0

        while True:
            params = {"query": query, "start": start, "count": self.per_page}
            r = requests.get(self.base_url, headers=self.headers, params=params)

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", 30))
                print(f"[WARN] Rate limit raggiunto, attendo {retry_after}s...")
                time.sleep(retry_after + 1)
                continue

            r.raise_for_status()
            data = r.json()
            entries = data.get("search-results", {}).get("entry", [])
            if not entries:
                break

            for e in entries:
                results.append({
                    "title": e.get("dc:title"),
                    "authors": e.get("dc:creator"),
                    "doi": e.get("prism:doi"),
                    "year": e.get("prism:coverDate"),
                    "source": e.get("prism:publicationName"),
                    "url": e.get("link", [{}])[0].get("@href")
                })

            start += len(entries)
            total = int(data.get("search-results", {}).get("opensearch:totalResults", 0))
            print(f"[INFO] Recuperati {start}/{total} risultati...")

            if start >= total:
                break
            time.sleep(1)

        print(f"[INFO] Totale risultati recuperati: {len(results)}")
        return results

    def save_csv(self, records, path="scopus_results.csv"):
        fieldnames = ["title", "authors", "doi", "year", "source", "url"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        print(f"[INFO] File CSV salvato come: {path}")

    def save_bib(self, records, path="scopus_results.bib"):
        with open(path, "w", encoding="utf-8") as f:
            for i, rec in enumerate(records):
                key = f"scopus{i+1}"
                title = rec['title'].replace('{', '\\{').replace('}', '\\}') if rec['title'] else ''
                authors = rec['authors'].replace('{', '\\{').replace('}', '\\}') if rec['authors'] else ''
                year = ''
                if rec['year']:
                    try:
                        year = datetime.strptime(rec['year'], '%Y-%m-%d').year
                    except Exception:
                        year = rec['year'][:4]
                doi = rec.get('doi', '')
                url = rec.get('url', '')
                source = rec.get('source', '')
                entry = f"""@article{{{key},
  title={{ {title} }},
  author={{ {authors} }},
  journal={{ {source} }},
  year={{ {year} }},
  doi={{ {doi} }},
  url={{ {url} }}
}}

"""
                f.write(entry)
        print(f"[INFO] File BibTeX salvato come: {path}")

