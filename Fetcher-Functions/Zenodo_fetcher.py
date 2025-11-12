import requests
import csv
from datetime import datetime
import time
import urllib.parse

class ZenodoFetcher:
    def __init__(self):
        self.base_url = "https://zenodo.org/api/records"

    def fetch_records(self, query, max_results=100):
        """
        Esegue la query su Zenodo API e restituisce una lista di dizionari con i metadati principali.
        """
        print(f"[INFO] Eseguo query: {query}")
        all_results = []
        page = 1
        per_page = 100  # massimo consentito da Zenodo

        while len(all_results) < max_results:
            params = {
                'q': query,
                'size': per_page,
                'page': page
            }
            url = f"{self.base_url}?{urllib.parse.urlencode(params)}"
            response = requests.get(url)

            if response.status_code != 200:
                raise Exception(f"Errore API Zenodo: {response.status_code} - {response.text}")

            data = response.json()
            items = data.get('hits', {}).get('hits', [])
            if not items:
                break

            for item in items:
                metadata = item.get('metadata', {})
                all_results.append({
                    'title': metadata.get('title'),
                    'author': ", ".join([a.get('name', '') for a in metadata.get('creators', [])]),
                    'description': metadata.get('description'),
                    'created': metadata.get('publication_date'),
                    'updated': item.get('updated'),
                    'keywords': ", ".join(metadata.get('keywords', [])) if metadata.get('keywords') else None,
                    'url': metadata.get('doi') or item.get('links', {}).get('html'),
                    'license': metadata.get('license', {}).get('id') if metadata.get('license') else None
                })

            print(f"[INFO] Pagina {page} completata ({len(items)} risultati)")
            time.sleep(1)  # pausa tra le richieste
            if len(items) < per_page:
                break
            page += 1

        print(f"[INFO] Totali risultati trovati per query: {len(all_results)}")
        return all_results

    def save_as_csv(self, data, filename):
        if not data:
            print("[WARN] Nessun dato da salvare.")
            return
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['title', 'author', 'description', 'created', 'updated', 'keywords', 'url', 'license']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"[INFO] File CSV salvato come '{filename}'")
        print(f"[COUNT] Articoli scritti nel CSV: {len(data)} ✅")

    def save_as_bib(self, data, filename):
        if not data:
            print("[WARN] Nessun dato da salvare.")
            return
        with open(filename, 'w', encoding='utf-8') as f:
            for i, rec in enumerate(data):
                key = f"zenodo{i+1}"
                title = rec.get('title', '')
                author = rec.get('author', '')
                year = ''
                if rec.get('created'):
                    try:
                        year = datetime.strptime(rec['created'], '%Y-%m-%d').year
                    except Exception:
                        pass
                url = rec.get('url', '')
                note = f"Keywords: {rec.get('keywords', '')}, License: {rec.get('license', '')}"
                f.write(f"@misc{{{key}, title={{{title}}}, author={{{author}}}, year={{{year}}}, howpublished={{\\url{{{url}}}}}, note={{{note}}}}}\n\n")
        print(f"[INFO] File BibTeX salvato come '{filename}'")
        print(f"[COUNT] Articoli scritti nel BibTeX: {len(data)} ✅")


if __name__ == "__main__":
    fetcher = ZenodoFetcher()

    # --- 🔹 Keyword derivate da query Scopus/ACM
    keywords_cloud = ['"cloud computing"', '"cloud-computing"', '"multi-cloud"']
    keywords_ontology = [
        '"ontology"', '"ontologies"', '"semantic web"', '"knowledge graph"',
        '"knowledge graphs"', '"linked data"', '"linked open data"'
    ]
    exclusions = ['"internet of things"', 'iot']

    queries = []
    for c in keywords_cloud:
        for o in keywords_ontology:
            q = (
                f'({c} OR {c} in:title OR {c} in:description) '
                f'AND ({o} OR {o} in:title OR {o} in:description) '
                f'NOT ({" OR ".join(exclusions)}) '
                f'AND publication_date:[2014-01-01 TO 2025-12-31]'
            )
            queries.append(q)

    all_results = []
    for q in queries:
        results = fetcher.fetch_records(query=q, max_results=200)
        all_results.extend(results)

    # --- 🔹 Rimuove duplicati
    seen = set()
    unique_results = []
    for r in all_results:
        if r['url'] not in seen:
            unique_results.append(r)
            seen.add(r['url'])

    print(f"[INFO] Totale risultati unici combinati: {len(unique_results)}")

    # --- 🔹 Salvataggio file
    csv_path = r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\zenodo_combined.csv"
    bib_path = r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results\zenodo_combined.bib"

    fetcher.save_as_csv(unique_results, csv_path)
    fetcher.save_as_bib(unique_results, bib_path)

    print("\n✅ Estrazione e salvataggio completati con successo!")
