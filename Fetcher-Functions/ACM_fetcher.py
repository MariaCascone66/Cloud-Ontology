import requests
import csv
from datetime import datetime
import os

class ACMFetcher:
    def __init__(self):
        self.api_url = "https://api.crossref.org/works"
        self.member_id = 320  # ACM Member ID in Crossref
        self.headers = {
            'User-Agent': 'ACMFetcherColab/1.0 (mailto:your-email@example.com)'
        }

    def fetch_records(self, query, max_results=30):
        """
        Cerca articoli ACM tramite Crossref.
        """
        print(f"[INFO] Ricerca ACM tramite Crossref: '{query}' con max {max_results} risultati")
        params = {
            'query': query,
            'filter': f'member:{self.member_id}',
            'rows': max_results,
            'sort': 'relevance'
        }

        try:
            response = requests.get(self.api_url, params=params, headers=self.headers)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"[ERRORE] Richiesta fallita: {e}")
            return []

        items = response.json().get("message", {}).get("items", [])
        results = []

        for item in items:
            record = {
                'title': item.get('title', [''])[0],
                'author': ", ".join([f"{a.get('family', '')} {a.get('given', '')}" for a in item.get('author', [])]) if item.get('author') else '',
                'published': item.get('issued', {}).get('date-parts', [[None]])[0][0],
                'DOI': item.get('DOI', ''),
                'URL': item.get('URL', ''),
                'publisher': item.get('publisher', ''),
                'journal': item.get('container-title', [''])[0] if item.get('container-title') else ''
            }
            results.append(record)

        print(f"[INFO] Trovati {len(results)} record ACM")
        return results

    def print_records(self, records, max_display=5):
        """
        Stampa i primi 'max_display' record.
        """
        print(f"\n[INFO] Visualizzazione dei primi {max_display} record:")
        for i, rec in enumerate(records[:max_display]):
            print(f"{i+1}. {rec['title']} ({rec['published']})")
            print(f"   Autore/i: {rec['author']}")
            print(f"   Rivista: {rec['journal']}")
            print(f"   DOI: {rec['DOI']}")
            print(f"   URL: {rec['URL']}\n")

    def save_as_csv(self, records, filename='acm_results.csv'):
        """
        Salva i record ACM in CSV.
        """
        if not records:
            print("[WARN] Nessun record da salvare in CSV.")
            return

        fieldnames = ['title', 'author', 'published', 'journal', 'publisher', 'DOI', 'URL']
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in records:
                writer.writerow(row)

        print(f"[INFO] File CSV salvato come: {filename}")

    def save_as_bib(self, records, filename='acm_results.bib'):
        """
        Salva i record ACM in formato BibTeX.
        """
        if not records:
            print("[WARN] Nessun record da salvare in BibTeX.")
            return

        with open(filename, 'w', encoding='utf-8') as f:
            for i, rec in enumerate(records):
                key = f"acm{i+1}"
                title = rec['title'].replace('{', '\\{').replace('}', '\\}')
                author = rec['author'].replace('{', '\\{').replace('}', '\\}')
                year = rec['published']
                doi = rec['DOI']
                url = rec['URL']
                journal = rec['journal']
                entry = f"""@article{{{key},
  title={{ {title} }},
  author={{ {author} }},
  journal={{ {journal} }},
  year={{ {year} }},
  doi={{ {doi} }},
  url={{ {url} }}
}}

"""
                f.write(entry)

        print(f"[INFO] File BibTeX salvato come: {filename}")



if __name__ == "__main__":
    acm = ACMFetcher()
    results = acm.fetch_records("machine learning", max_results=30)
    acm.print_records(results)

    # Salva i file
    acm.save_as_csv(results, "acm_ml.csv")
    acm.save_as_bib(results, "acm_ml.bib")

    def open_file_local(filename):
        """
        #Apre un file locale con l'app predefinita (solo su Windows).
        """
        try:
            os.startfile(filename)  # Solo su Windows
            print(f"[INFO] File '{filename}' aperto nel programma associato.")
        except Exception as e:
            print(f"[ERRORE] Impossibile aprire il file: {e}")

    def download_csv_file(filename='acm_ml.csv'):
        open_file_local(filename)

    def download_bib_file(filename='acm_ml.bib'):
        open_file_local(filename)

    # Scarica i file
    download_csv_file("acm_ml.csv")
    download_bib_file("acm_ml.bib")
