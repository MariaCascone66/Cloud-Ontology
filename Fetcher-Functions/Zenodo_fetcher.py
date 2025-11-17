import requests
import csv
from datetime import datetime
import time
import os
import urllib.parse


class ZenodoFetcher:
    """
    Fetcher generico per interrogare Zenodo API
    e recuperare metadati su record open data.
    """

    def __init__(self, per_page=100, max_retries=3):
        self.base_url = "https://zenodo.org/api/records"
        self.per_page = per_page
        self.max_retries = max_retries

    def fetch(self, query, max_results=200):
        print(f"[INFO] Query Zenodo: {query}")

        all_results = []
        page = 1

        while len(all_results) < max_results:
            params = {
                "q": query,
                "page": page,
                "size": self.per_page
            }
            url = f"{self.base_url}?{urllib.parse.urlencode(params)}"

            # retry loop
            for attempt in range(self.max_retries):
                response = requests.get(url)

                if response.status_code >= 500:
                    print(f"[WARN] Zenodo errore {response.status_code}, riprovo...")
                    time.sleep(3)
                    continue

                break
            else:
                print(f"[ERROR] Errore permanente Zenodo pagina {page}")
                break

            data = response.json()
            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            for item in hits:
                md = item.get("metadata", {})
                creators = md.get("creators", [])

                all_results.append({
                    "title": md.get("title"),
                    "author": ", ".join(a.get("name", "") for a in creators),
                    "description": md.get("description"),
                    "created": md.get("publication_date"),
                    "updated": item.get("updated"),
                    "keywords": ", ".join(md.get("keywords", [])) if md.get("keywords") else None,
                    "url": md.get("doi") or item.get("links", {}).get("html"),
                    "license": md.get("license", {}).get("id") if md.get("license") else None,
                })

            print(f"[INFO] Zenodo: pagina {page} ok ({len(hits)} risultati)")

            if len(hits) < self.per_page:
                break

            page += 1
            time.sleep(1)

        print(f"[INFO] Totale risultati Zenodo: {len(all_results)}")
        return all_results

    def save_csv(self, records, path):
        if not records:
            print("[WARN] Nessun dato da salvare.")
            return

        os.makedirs(os.path.dirname(path), exist_ok=True)
        fieldnames = list(records[0].keys())

        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"[INFO] CSV Zenodo salvato in: {path}")

    def save_bib(self, records, path):
        if not records:
            print("[WARN] Nessun record da salvare.")
            return

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            for i, rec in enumerate(records):
                key = f"zenodo{i+1}"

                year = ""
                if rec.get("created"):
                    try:
                        year = datetime.strptime(rec["created"], "%Y-%m-%d").year
                    except:
                        pass

                note = (
                    f"Keywords: {rec.get('keywords', '')}, "
                    f"License: {rec.get('license', '')}"
                )

                f.write(
                    f"@misc{{{key},\n"
                    f"  title={{{{{rec.get('title', '')}}}}},\n"
                    f"  author={{{{{rec.get('author', '')}}}}},\n"
                    f"  year={{{{{year}}}}},\n"
                    f"  howpublished={{\\url{{{rec.get('url', '')}}}}},\n"
                    f"  note={{{{{note}}}}}\n"
                    f"}}\n\n"
                )

        print(f"[INFO] BibTeX Zenodo salvato in: {path}")
