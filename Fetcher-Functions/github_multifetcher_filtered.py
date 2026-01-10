import os
import requests
import csv
from datetime import datetime
from dotenv import load_dotenv
import time

class GitHubFetcher:
    def __init__(self, token=None):
        self.base_url = "https://api.github.com/search/repositories"
        self.headers = {}
        if token:
            self.headers['Authorization'] = f'token {token}'

    def fetch_repositories(self, query, max_results=100):
        print(f"[INFO] Eseguo query: {query}")
        all_results = []
        page = 1

        while len(all_results) < max_results:
            params = {
                'q': query,
                'per_page': 100,
                'page': page
            }
            response = requests.get(self.base_url, headers=self.headers, params=params)

            # --- 🔹 Gestione automatica del rate limit
            if response.status_code == 403 and "rate limit" in response.text.lower():
                reset_time = response.headers.get('X-RateLimit-Reset')
                if reset_time:
                    reset_timestamp = datetime.fromtimestamp(int(reset_time))
                    wait_seconds = max(0, (reset_timestamp - datetime.utcnow()).seconds) + 5
                    print(f"[WARN] Limite API raggiunto — attendo {wait_seconds} secondi fino a {reset_timestamp}...")
                    time.sleep(wait_seconds)
                else:
                    print("[WARN] Limite API raggiunto — attendo 60 secondi...")
                    time.sleep(60)
                continue  # ripete la richiesta dopo l’attesa

            if response.status_code != 200:
                raise Exception(f"Errore API GitHub: {response.status_code} - {response.text}")

            data = response.json()
            items = data.get("items", [])
            if not items:
                break

            for item in items:
                all_results.append({
                    'title': item.get('name'),
                    'author': item.get('owner', {}).get('login'),
                    'description': item.get('description'),
                    'created': item.get('created_at'),
                    'updated': item.get('updated_at'),
                    'language': item.get('language'),
                    'stars': item.get('stargazers_count'),
                    'url': item.get('html_url'),
                    'license': item.get('license', {}).get('name') if item.get('license') else None
                })

            # --- 🔹 Informazioni sul rate limit
            remaining = response.headers.get('X-RateLimit-Remaining')
            print(f"[INFO] Pagina {page} completata ({len(items)} risultati). Limite residuo: {remaining}")

            # --- 🔹 Pausa di sicurezza tra le richieste
            time.sleep(3)

            if len(items) < 100:
                break
            page += 1

        print(f"[INFO] Trovati {len(all_results)} risultati per la query")
        return all_results


    def save_as_csv(self, data, filename):
        if not data:
            print("[WARN] Nessun dato da salvare.")
            return
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = ['title', 'author', 'description', 'created', 'updated',
                          'language', 'stars', 'url', 'license']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"[INFO] File CSV salvato come '{filename}'")

    def save_as_bib(self, data, filename):
        if not data:
            print("[WARN] Nessun dato da salvare.")
            return
        with open(filename, 'w', encoding='utf-8') as f:
            for i, rec in enumerate(data):
                key = f"github{i+1}"
                title = rec.get('title', '')
                author = rec.get('author', '')
                year = ''
                if rec.get('created'):
                    try:
                        year = datetime.strptime(rec['created'], '%Y-%m-%dT%H:%M:%SZ').year
                    except Exception:
                        pass
                url = rec.get('url', '')
                note = f"Language: {rec.get('language', '')}, Stars: {rec.get('stars', '')}, License: {rec.get('license', '')}"
                f.write(f"@misc{{{key}, title={{{title}}}, author={{{author}}}, year={{{year}}}, howpublished={{\\url{{{url}}}}}, note={{{note}}}}}\n\n")
        print(f"[INFO] File BibTeX salvato come '{filename}'")

# ==================== MAIN =============================
if __name__ == "__main__":
    # --- Cartella output dedicata
    output_dir = r"C:\Users\maria\Desktop\Cloud-Ontology\fetcher-Functions\output-github"
    os.makedirs(output_dir, exist_ok=True)

    # --- Caricamento token
    load_dotenv(r"C:\Users\maria\Desktop\Cloud-Ontology\token.env")
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("⚠️ Token GitHub non trovato. Verifica token.env")

    github_fetcher = GitHubFetcher(token=github_token)

    # --- Controllo rate limit ---
    check = requests.get(
        "https://api.github.com/rate_limit",
        headers={'Authorization': f'token {github_token}'}
    )
    if check.status_code == 200:
        limits = check.json().get('resources', {}).get('search', {})
        print(f"[INFO] Limite rimanente GitHub: {limits.get('remaining', '?')}/{limits.get('limit', '?')}")

    # --- Keywords principali ---
    keywords_cloud = ['"cloud computing"', '"cloud-computing"', '"multi-cloud"']
    keywords_ontology = [
        '"ontology"', '"ontologies"', '"semantic web"', '"knowledge graph"',
        '"knowledge graphs"', '"linked data"', '"linked open data"'
    ]
    exclusions = ['"internet of things"', 'iot']

    # --- Creazione query ---
    queries = []
    for c in keywords_cloud:
        for o in keywords_ontology:
            q = (f'{c} {o} NOT ({" OR ".join(exclusions)}) '
                 'in:name,description '
                 'created:>2014-01-01 created:<2027-01-01 '
                 'language:English')
            queries.append(q)

    # --- Recupero risultati ---
    all_results = []
    for q in queries:
        results = github_fetcher.fetch_repositories(query=q, max_results=200)
        all_results.extend(results)

    # --- Deduplicazione ---
    seen = set()
    unique_results = []
    for r in all_results:
        if r['url'] not in seen:
            unique_results.append(r)
            seen.add(r['url'])

    # --- Salvataggio ---
    github_fetcher.save_as_csv(unique_results, os.path.join(output_dir, "github_results.csv"))
    github_fetcher.save_as_bib(unique_results, os.path.join(output_dir, "github_results.bib"))

    # --- Info finale ---
    print(f"[INFO] Totale repository uniche salvate: {len(unique_results)}")
