import os
from dotenv import load_dotenv
from Zenodo_fetcher import ZenodoFetcher
from Scopus_fetcher import ScopusFetcher
from github_multifetcher_filtered import GitHubFetcher
from LodCloud_fetcher import LodCloudFetcher

# ----------------------
# --- Parametri comuni ---
# ----------------------
keywords_cloud = ['"cloud computing"', '"cloud-computing"', '"multi-cloud"']
keywords_ontology = [
    '"ontology"', '"ontologies"', '"semantic web"', '"knowledge graph"',
    '"knowledge graphs"', '"linked data"', '"linked open data"'
]
exclusions = ['"internet of things"', 'iot']

year_min = 2014
year_max = 2027

output_dir = r"C:\Users\maria\Desktop\Cloud-Ontology\Fetcher-Results"

# Funzione per rimuovere duplicati per URL
def remove_duplicates(records):
    seen = set()
    unique = []
    for r in records:
        if r['url'] not in seen:
            unique.append(r)
            seen.add(r['url'])
    return unique

# ----------------------
# --- Zenodo Fetcher ---
# ----------------------
print("[INFO] Avvio fetch Zenodo...")
fetcher_zenodo = ZenodoFetcher(per_page=100)

zenodo_queries = [
    f'({c} OR {c} in:title OR {c} in:description) '
    f'AND ({o} OR {o} in:title OR {o} in:description) '
    f'NOT ({" OR ".join(exclusions)}) '
    f'AND publication_date:[{year_min}-01-01 TO {year_max}-12-31]'
    for c in keywords_cloud for o in keywords_ontology
]

all_zenodo = []
for q in zenodo_queries:
    all_zenodo.extend(fetcher_zenodo.fetch(query=q, max_results=200))

unique_zenodo = remove_duplicates(all_zenodo)

print(f"[INFO] Zenodo: risultati unici = {len(unique_zenodo)}")
fetcher_zenodo.save_csv(unique_zenodo, os.path.join(output_dir, "zenodo_combined.csv"))
fetcher_zenodo.save_bib(unique_zenodo, os.path.join(output_dir, "zenodo_combined.bib"))

# ----------------------
# --- Scopus Fetcher ---
# ----------------------
print("[INFO] Avvio fetch Scopus...")
load_dotenv("scopus_key.env")
SCOPUS_API_KEY = os.getenv("SCOPUS_API_KEY")
if not SCOPUS_API_KEY:
    raise ValueError("⚠️ SCOPUS_API_KEY non trovata!")

fetcher_scopus = ScopusFetcher(api_key=SCOPUS_API_KEY, per_page=25)

all_scopus = []
for q in zenodo_queries:
    all_scopus.extend(fetcher_scopus.fetch(query=q))

unique_scopus = remove_duplicates(all_scopus)

print(f"[INFO] Scopus: risultati unici = {len(unique_scopus)}")
fetcher_scopus.save_csv(unique_scopus, os.path.join(output_dir, "scopus_combined.csv"))
fetcher_scopus.save_bib(unique_scopus, os.path.join(output_dir, "scopus_combined.bib"))

# ----------------------
# --- GitHub Fetcher ---
# ----------------------
print("[INFO] Avvio fetch GitHub...")
load_dotenv("token.env")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise ValueError("⚠️ GitHub token non trovato!")

fetcher_github = GitHubFetcher(token=GITHUB_TOKEN, per_page=100)

all_github = []
for c in keywords_cloud:
    for o in keywords_ontology:
        q = (
            f'{c} {o} NOT ({" OR ".join(exclusions)}) '
            'in:name,description '
            f'created:>{year_min}-01-01 created:<{year_max+1}-01-01 '
            'language:English'
        )
        all_github.extend(fetcher_github.fetch(query=q, max_results=200))

unique_github = remove_duplicates(all_github)

print(f"[INFO] GitHub: risultati unici = {len(unique_github)}")
fetcher_github.save_csv(unique_github, os.path.join(output_dir, "github_combined.csv"))
fetcher_github.save_bib(unique_github, os.path.join(output_dir, "github_combined.bib"))

# ----------------------
# --- LOD Cloud Fetcher ---
# ----------------------
print("[INFO] Avvio fetch LOD Cloud...")
lod = LodCloudFetcher()

include_terms = [
    ["cloud computing", "cloud-computing", "multi-cloud"],
    ["ontology", "ontologies", "semantic web", "knowledge graph",
     "knowledge graphs", "linked data", "linked open data"]
]

datasets = lod.fetch(include_terms=include_terms, exclude_terms=exclusions,
                     year_min=year_min, year_max=year_max)

lod.save_as_csv(datasets, os.path.join(output_dir, "lodcloud_results.csv"))
lod.save_as_bib(datasets, os.path.join(output_dir, "lodcloud_results.bib"))

print("\n✅ Tutti i fetch completati con successo!")
