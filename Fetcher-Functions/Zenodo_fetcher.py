import requests
import csv
from datetime import datetime
import time

class ZenodoFetcher:
    def __init__(self, max_retries=3, delay=5):
        self.base_url = "https://zenodo.org/api/records"
        self.max_retries = max_retries
        self.delay = delay

    def fetch_records(self, query, batch_size=200, max_pages=5):
        """Scarica tutti i record per la query, mostrando il progresso."""
        all_results = []
        page = 1

        while True:
            if max_pages and page > max_pages:
                print(f"[INFO] Raggiunto limite di {max_pages} pagine per questa query.")
                break

            print(f"[INFO] Query '{query}', pagina {page}...")
            retries = 0
            while retries < self.max_retries:
                try:
                    params = {
                        'q': query,
                        'size': batch_size,
                        'page': page,
                        'sort': 'mostdownloaded',
                        'all_versions': 'false'
                    }
                    response = requests.get(self.base_url, params=params, timeout=30)
                    if response.status_code == 200:
                        hits = response.json().get('hits', {}).get('hits', [])
                        print(f"[INFO] → {len(hits)} risultati trovati in pagina {page}")
                        if not hits:
                            print("[INFO] Nessun altro risultato. Fine query.")
                            return all_results

                        for item in hits:
                            metadata = item.get('metadata', {})
                            doc_type = metadata.get('resource_type', {}).get('type', '').lower()
                            if doc_type not in ['publication', 'conferencepaper']:
                                continue

                            created = item.get('created')
                            language = metadata.get('language')
                            if language and language.lower() != 'en':
                                continue

                            if created:
                                try:
                                    year = datetime.strptime(created, '%Y-%m-%dT%H:%M:%S.%fZ').year
                                    if year < 2014:
                                        continue
                                except:
                                    pass

                            record = {
                                'title': metadata.get('title'),
                                'author': ", ".join([c.get('name') for c in metadata.get('creators', [])]),
                                'description': metadata.get('description'),
                                'created': created,
                                'updated': item.get('updated'),
                                'language': language,
                                'downloads': item.get('stats', {}).get('downloads', 0),
                                'url': item.get('links', {}).get('html'),
                                'license': metadata.get('license', {}).get('id')
                                        if isinstance(metadata.get('license'), dict)
                                        else metadata.get('license')
                            }
                            all_results.append(record)

                        print(f"[INFO] Totale accumulato finora: {len(all_results)} record.")
                        break  # uscita dal loop dei retry

                    else:
                        print(f"[WARN] Status {response.status_code}, retry in {self.delay}s")
                        time.sleep(self.delay)
                        retries += 1
                except requests.exceptions.RequestException as e:
                    print(f"[ERRORE] {e}, retry in {self.delay}s")
                    time.sleep(self.delay)
                    retries += 1
            else:
                print(f"[ERRORE] Query '{query}' pagina {page} fallita dopo {self.max_retries} tentativi.")
                break

            page += 1
            time.sleep(0.5)  # piccola pausa per non saturare Zenodo

        return all_results

    def save_as_csv(self, data, filename='zenodo_results.csv'):
        if not data:
            print("[WARN] Nessun dato da salvare.")
            return

        data_sorted = sorted(data, key=lambda x: x.get('downloads', 0), reverse=True)
        for i, row in enumerate(data_sorted, start=1):
            row['n'] = i

        fieldnames = ['n'] + [k for k in data_sorted[0].keys() if k != 'n']
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in data_sorted:
                writer.writerow({k: " ".join(str(v).split()) if isinstance(v, str) else v for k, v in row.items()})

        print(f"[INFO] File CSV salvato come '{filename}' con {len(data_sorted)} record.")

# === MAIN ===
if __name__ == "__main__":
    zen = ZenodoFetcher()

    # Keywords principali / ontologiche / esclusioni
    keywords_cloud = ['cloud computing', 'cloud-computing', 'multi-cloud']
    keywords_ontology = ['ontology', 'ontologies', 'semantic web', 'knowledge graph', 
                         'knowledge graphs', 'linked data', 'linked open data']
    exclusions = ['internet of things', 'iot']

    all_records = []

    # Costruzione query semplice: OR per cloud + OR per ontologie, senza NOT
    for c in keywords_cloud:
        for o in keywords_ontology:
            q = f'(metadata.title:{c} OR metadata.description:{c} OR metadata.keywords:{c}) ' \
                f'AND (metadata.title:{o} OR metadata.description:{o} OR metadata.keywords:{o})'
            print(f"[INFO] Esecuzione query semplificata: {q}")
            recs = zen.fetch_records(q, batch_size=100, max_pages=None)
            all_records.extend(recs)

    print(f"[INFO] Totale record prima di applicare esclusioni: {len(all_records)}")

    # Filtro brutale delle esclusioni (a mano)
    filtered_records = []
    for r in all_records:
        text = " ".join([
            str(r.get('title','')),
            str(r.get('description','')),
            str(r.get('author',''))
        ]).lower()
        if not any(e.lower() in text for e in exclusions):
            filtered_records.append(r)

    print(f"[INFO] Totale record dopo esclusioni: {len(filtered_records)}")

    # Deduplicazione
    seen = set()
    unique_records = []
    for r in filtered_records:
        key = (r.get('title'), r.get('author'))
        if key not in seen:
            unique_records.append(r)
            seen.add(key)

    print(f"[INFO] Totale record unici: {len(unique_records)}")

    # Salvataggio
    zen.save_as_csv(unique_records, "zenodo_results.csv")
