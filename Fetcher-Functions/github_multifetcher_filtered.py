import requests
import csv
import time
from datetime import datetime
import os


class GitHubFetcher:
    """
    Fetcher generico per interrogare GitHub Search API
    e recuperare metadati su repository.
    """

    def __init__(self, token=None, per_page=100, max_retries=3):
        self.base_url = "https://api.github.com/search/repositories"
        self.headers = {"Accept": "application/vnd.github+json"}
        if token:
            self.headers["Authorization"] = f"token {token}"

        self.per_page = per_page
        self.max_retries = max_retries

    def fetch(self, query, max_results=200):
        """
        Recupera tutti i risultati di una query GitHub.
        Restituisce una lista di dizionari uniformi.
        """
        print(f"[INFO] Query GitHub: {query}")

        all_results = []
        page = 1

        while len(all_results) < max_results:
            params = {
                "q": query,
                "per_page": self.per_page,
                "page": page
            }

            # retry loop
            for attempt in range(self.max_retries):
                response = requests.get(self.base_url, headers=self.headers, params=params)

                # rate limit
                if response.status_code == 403 and "rate limit" in response.text.lower():
                    reset_time = response.headers.get("X-RateLimit-Reset")
                    if reset_time:
                        wait = max(0, int(reset_time) - int(time.time())) + 5
                        print(f"[WARN] Rate limit GitHub. Attendo {wait}s...")
                        time.sleep(wait)
                    else:
                        print("[WARN] Rate limit. Attendo 60s...")
                        time.sleep(60)
                    continue

                # errori temporanei
                if response.status_code >= 500:
                    print(f"[WARN] Errore {response.status_code}. Riprovo...")
                    time.sleep(3)
                    continue

                break
            else:
                print(f"[ERROR] Impossibile completare richiesta GitHub pagina {page}")
                break

            data = response.json()
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                all_results.append({
                    "title": item.get("name"),
                    "author": item.get("owner", {}).get("login"),
                    "description": item.get("description"),
                    "created": item.get("created_at"),
                    "updated": item.get("updated_at"),
                    "language": item.get("language"),
                    "stars": item.get("stargazers_count"),
                    "url": item.get("html_url"),
                    "license": item.get("license", {}).get("name") if item.get("license") else None,
                })

            print(f"[INFO] GitHub: pagina {page} ok ({len(items)} risultati)")

            if len(items) < self.per_page:
                break

            page += 1
            time.sleep(2)

        print(f"[INFO] Totale risultati GitHub: {len(all_results)}")
        return all_results

    def save_csv(self, records, path):
        if not records:
            print("[WARN] Nessun record da salvare.")
            return

        os.makedirs(os.path.dirname(path), exist_ok=True)
        fieldnames = list(records[0].keys())

        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        print(f"[INFO] CSV GitHub salvato in: {path}")

    def save_bib(self, records, path):
        if not records:
            print("[WARN] Nessun record da salvare.")
            return

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            for i, rec in enumerate(records):
                key = f"github{i+1}"

                year = ""
                if rec.get("created"):
                    try:
                        year = datetime.strptime(rec["created"], "%Y-%m-%dT%H:%M:%SZ").year
                    except:
                        pass

                note = (
                    f"Language: {rec.get('language', '')}, "
                    f"Stars: {rec.get('stars', '')}, "
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

        print(f"[INFO] BibTeX GitHub salvato in: {path}")
