import requests
import time
import csv
from datetime import datetime
import os

class ScopusFetcher:
    def __init__(self, api_key, per_page=25, max_retries=3):
        """
        api_key: API Key Elsevier
        per_page: risultati per pagina (max 25/200)
        max_retries: tentativi in caso di errori di rete / rate limit
        """
        self.base_url = "https://api.elsevier.com/content/search/scopus"
        self.headers = {
            "Accept": "application/json",
            "X-ELS-APIKey": api_key
        }
        self.per_page = per_page
        self.max_retries = max_retries

    def fetch(self, query):
        """
        Recupera tutti i risultati di una query Scopus,
        cercando di estrarre il massimo dei campi possibili.
        """
        results = []
        start = 0
        total_retrieved = 0

        while True:
            params = {
                "query": query,
                "start": start,
                "count": self.per_page
            }

            for attempt in range(self.max_retries):
                r = requests.get(self.base_url, headers=self.headers, params=params)
                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", 30))
                    print(f"[WARN] Rate limit, attendo {retry_after}s...")
                    time.sleep(retry_after + 1)
                    continue
                elif r.status_code >= 500:
                    print(f"[WARN] Errore server {r.status_code}, riprovo...")
                    time.sleep(3)
                    continue
                else:
                    break
            else:
                print(f"[ERROR] Impossibile completare la richiesta per start={start}")
                break

            data = r.json()
            entries = data.get("search-results", {}).get("entry", [])
            if not entries:
                break

            for e in entries:
                # estrazione campi principali e "tutti quelli possibili"
                results.append({
                    "scopus_id": e.get("dc:identifier", "").replace("SCOPUS_ID:", ""),
                    "eid": e.get("eid", ""),
                    "title": e.get("dc:title") or "",
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
                    "references": e.get("citedby-count", 0),  # numero di citazioni
                    "citations": e.get("citedby-count", 0),   # numero di citazioni (puoi differenziare se usi altra API)
                    "url": e.get("link", [{}])[0].get("@href") or "",
                    "abstract": e.get("dc:description") or "",
                    "language": e.get("language") or "",
                    "publisher": e.get("prism:publisher") or "",
                })

            total_retrieved += len(entries)
            total_results = int(data.get("search-results", {}).get("opensearch:totalResults", 0))
            print(f"[INFO] Recuperati {total_retrieved}/{total_results} risultati...")

            start += len(entries)
            if start >= total_results:
                break

            time.sleep(1)  # pausa gentile tra le richieste

        print(f"[INFO] Totale risultati recuperati: {len(results)}")
        return results

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
        print(f"[INFO] CSV salvato in: {path}")

    def save_bib(self, records, path):
        """Salva i risultati Scopus in formato BibTeX"""
        if not records:
            print("[WARN] Nessun record da salvare in BibTeX.")
            return

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for i, r in enumerate(records, start=1):
                key = f"scopus{i}"
                title = r.get("title", "").replace("{", "\\{").replace("}", "\\}")
                year = r.get("year", "")
                abstract = r.get("abstract", "").replace("{", "\\{").replace("}", "\\}")
                url = r.get("url", "")
                f.write(
                    f"@article{{{key},\n"
                    f"  title={{{title}}},\n"
                    f"  year={{{year}}},\n"
                    f"  note={{{abstract}}},\n"
                    f"  howpublished={{\\url{{{url}}}}}\n"
                    f"}}\n\n"
                )
        print(f"[INFO] BibTeX salvato in: {path}")
